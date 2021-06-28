// Package mongod contains a module that acts as a backend for Mongo proxy,
// which connects to a mongod instance and sends requests to (and receives responses from)
// the server.
package mongod

import (
	"context"

	"github.com/WyattNielsen/mongoproxy/bsonutil"
	"github.com/WyattNielsen/mongoproxy/convert"
	"github.com/WyattNielsen/mongoproxy/messages"
	"github.com/WyattNielsen/mongoproxy/server"
	log "github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// A MongodModule takes the request, sends it to a mongod instance, and then
// writes the response from mongod into the ResponseWriter before calling
// the next module. It passes on requests unchanged.
type MongodModule struct {
	ConnectionString string
	ReadOnly         bool
	Logger           *log.Logger
	Client           *mongo.Client
}

func init() {
}

func (m *MongodModule) New() server.Module {
	return &MongodModule{}
}

func (m *MongodModule) Name() string {
	return "mongod"
}

func (m *MongodModule) Configure(config server.Config) error {
	m.ConnectionString = config.AsConnectionString()

	m.ReadOnly = config.ReadOnly
	m.Logger = log.New()
	m.Logger.SetReportCaller(true)

	return nil
}

func (m *MongodModule) Process(req messages.Requester, res messages.Responder,
	next server.PipelineFunc) {

	var ctx = context.Background()

	// spin up the session if it doesn't exist
	if m.Client == nil {
		var err error
		m.Client, err = mongo.Connect(context.TODO(), options.Client().ApplyURI(m.ConnectionString))
		if err != nil {
			log.Errorf("Error connecting to MongoDB: %#v", err)
			next(req, res)
			return
		}
	}

	session, err := m.Client.StartSession()
	if err != nil {
		log.Errorf("Error starting session: %#v", err)
	}
	defer session.EndSession(ctx)

	switch req.Type() {
	case messages.CommandType:
		command, err := messages.ToCommandRequest(req)
		if err != nil {
			m.Logger.Warnf("Error converting to command: %#v", err)
			next(req, res)
			return
		}

		b := command.ToBSON()

		reply := bson.M{}
		switch command.CommandName {
		case "ismaster":
			b = bson.D{
				{"isMaster", 1},
			}

		case "createIndexes":
			m.Logger.Infof("Skipping command %v", command.CommandName)
			reply["ok"] = 1
			reply["code"] = 0
			response := messages.CommandResponse{
				Reply: reply,
			}
			res.Write(response)
			return
		case "ping":
		case "buildInfo":
		case "isMaster":
		default:
			m.Logger.Infof("processing %v", b)
		}
		err = session.Client().Database(command.Database).RunCommand(ctx, b).Decode(&reply)

		if err != nil {
			// log an error if we can
			qErr, ok := err.(*mongo.CommandError)
			m.Logger.Warnf("Error running command %v: %v", command.CommandName, err)
			if ok {
				res.Error(int32(qErr.Code), qErr.Message)
			} else {
				res.Error(-1, "Unknown error")
			}
			next(req, res)
			return
		}

		response := messages.CommandResponse{
			Reply: reply,
		}

		if convert.ToInt(reply["ok"]) == 0 {
			// we have a command error.
			res.Error(convert.ToInt32(reply["code"]), convert.ToString(reply["errmsg"]))
			next(req, res)
			return
		}

		res.Write(response)

	case messages.FindType:
		f, err := messages.ToFindRequest(req)
		if err != nil {
			m.Logger.Warnf("Error converting to a Find command: %#v", err)
			next(req, res)
			return
		}

		opts := options.Find()
		opts.SetBatchSize(int32(f.Limit))
		opts.SetLimit(int64(f.Limit))
		opts.SetSkip(int64(f.Skip))

		if f.Projection != nil {
			opts.SetProjection(f.Projection)
		}

		if f.Sort != nil {
			opts.SetSort(f.Sort)
		}

		c := session.Client().Database(f.Database).Collection(f.Collection)

		var cur *mongo.Cursor
		if cur, err = c.Find(ctx, f.Filter, opts); err != nil {
			m.Logger.Warnf("Error on Find Command: %#v", err)

			// log an error if we can
			qErr, ok := err.(*mongo.CommandError)
			if ok {
				res.Error(int32(qErr.Code), qErr.Message)
			}
		}

		var results []bson.D

		if f.Limit > 0 {
			// only store the amount specified by the limit
			for i := 0; i < int(f.Limit); i++ {
				var result bson.D
				ok := cur.Next(ctx)
				cur.Decode(&result)
				if !ok {
					err = cur.Err()
					if err != nil {
						m.Logger.Warnf("Error on Find Command: %#v", err)

						// log an error if we can
						qErr, ok := err.(*mongo.CommandError)
						if ok {
							res.Error(int32(qErr.Code), qErr.Message)
						}
						cur.Close(ctx)
						next(req, res)
						return
					}
					// we ran out of documents, but didn't have an error
					break
				}
				results = append(results, result)
			}
		} else {
			// dump all of them
			err = cur.All(ctx, &results)
			if err != nil {
				m.Logger.Warnf("Error on Find Command: %#v", err)

				// log an error if we can
				qErr, ok := err.(*mongo.CommandError)
				if ok {
					res.Error(int32(qErr.Code), qErr.Message)
				}
				next(req, res)
				return
			}
		}

		response := messages.FindResponse{
			Database:   f.Database,
			Collection: f.Collection,
			Documents:  results,
		}

		res.Write(response)

	case messages.InsertType:
		insert, err := messages.ToInsertRequest(req)
		if err != nil {
			m.Logger.Warnf("Error converting to Insert command: %#v", err)
			next(req, res)
			return
		}

		if m.ReadOnly {
			response := messages.InsertResponse{N: -1}
			res.Write(response)
			return
		}

		b := insert.ToBSON()

		reply := bson.M{}
		result := session.Client().Database(insert.Database).RunCommand(ctx, b)

		// collection = client.Database(dbName).Collection(collectionName)
		// if result, err = collection.InsertOne(ctx, doc); err != nil {
		// 	t.Fatal(err)
		// }

		if result.Err() != nil {
			// log an error if we can
			qErr, ok := err.(*mongo.WriteError)
			if ok {
				res.Error(int32(qErr.Code), qErr.Message)
			}
			next(req, res)
			return
		}

		result.Decode(&reply)

		response := messages.InsertResponse{
			// default to -1 if n doesn't exist to hide the field on export
			N: convert.ToInt32(reply["n"], -1),
		}
		writeErrors, err := convert.ConvertToBSONMapSlice(reply["writeErrors"])
		if err == nil {
			// we have write errors
			response.WriteErrors = writeErrors
		}

		if convert.ToInt(reply["ok"]) == 0 {
			// we have a command error.
			res.Error(convert.ToInt32(reply["code"]), convert.ToString(reply["errmsg"]))
			next(req, res)
			return
		}

		res.Write(response)

	case messages.UpdateType:
		u, err := messages.ToUpdateRequest(req)
		if err != nil {
			m.Logger.Warnf("Error converting to Update command: %v", err)
			next(req, res)
			return
		}

		if m.ReadOnly {
			response := messages.UpdateResponse{
				N:         -1,
				NModified: -1,
			}
			res.Write(response)
			return
		}

		b := u.ToBSON()

		reply := bson.D{}
		result := session.Client().Database(u.Database).RunCommand(ctx, b)

		// var update bson.M
		// json.Unmarshal([]byte(`{ "$set": {"year": 1998}}`), &update)
		// if result, err = collection.UpdateOne(ctx, bson.M{"_id": doc["_id"]}, update); err != nil {
		// 	t.Fatal(err)
		// }

		// if result, err = collection.UpdateMany(ctx, bson.M{"hometown": "Atlanta"}, update); err != nil {
		// 	t.Fatal(err)
		// }

		if result.Err() != nil {
			// log an error if we can
			qErr, ok := err.(*mongo.WriteError)
			if ok {
				res.Error(int32(qErr.Code), qErr.Message)
			}
			next(req, res)
			return
		}

		result.Decode(&reply)

		response := messages.UpdateResponse{
			N:         convert.ToInt32(bsonutil.FindValueByKey("n", reply), -1),
			NModified: convert.ToInt32(bsonutil.FindValueByKey("nModified", reply), -1),
		}

		writeErrors, err := convert.ConvertToBSONMapSlice(
			bsonutil.FindValueByKey("writeErrors", reply))
		if err == nil {
			// we have write errors
			response.WriteErrors = writeErrors
		}

		rawUpserted := bsonutil.FindValueByKey("upserted", reply)
		upserted, err := convert.ConvertToBSONDocSlice(rawUpserted)
		if err == nil {
			// we have upserts
			response.Upserted = upserted
		}

		if convert.ToInt(bsonutil.FindValueByKey("ok", reply)) == 0 {
			// we have a command error.
			res.Error(convert.ToInt32(bsonutil.FindValueByKey("code", reply)),
				convert.ToString(bsonutil.FindValueByKey("errmsg", reply)))
			next(req, res)
			return
		}

		res.Write(response)

	case messages.DeleteType:
		d, err := messages.ToDeleteRequest(req)
		if err != nil {
			m.Logger.Warnf("Error converting to Delete command: %v", err)
			next(req, res)
			return
		}

		if m.ReadOnly {
			response := messages.DeleteResponse{
				N: -1,
			}
			res.Write(response)
			return
		}

		b := d.ToBSON()

		reply := bson.M{}
		result := session.Client().Database(d.Database).RunCommand(ctx, b)

		// if result, err = collection.DeleteMany(ctx, bson.M{"hometown": "Atlanta"}); err != nil {
		// 	t.Fatal(err)
		// }

		if result.Err() != nil {
			// log an error if we can
			qErr, ok := err.(*mongo.WriteError)
			if ok {
				res.Error(int32(qErr.Code), qErr.Message)
			}
			next(req, res)
			return
		}

		result.Decode(&reply)

		response := messages.DeleteResponse{
			N: convert.ToInt32(reply["n"], -1),
		}
		writeErrors, err := convert.ConvertToBSONMapSlice(reply["writeErrors"])
		if err == nil {
			// we have write errors
			response.WriteErrors = writeErrors
		}

		if convert.ToInt(reply["ok"]) == 0 {
			// we have a command error.
			res.Error(convert.ToInt32(reply["code"]), convert.ToString(reply["errmsg"]))
			next(req, res)
			return
		}

		m.Logger.Infof("Reply: %#v", reply)

		res.Write(response)

	case messages.GetMoreType:
		g, err := messages.ToGetMoreRequest(req)
		if err != nil {
			m.Logger.Warnf("Error converting to GetMore command: %#v", err)
			next(req, res)
			return
		}
		m.Logger.Debugf("%#v", g)

		// make an iterable to get more
		// https://docs.mongodb.com/manual/reference/command/getMore/
		d := session.Client().Database(g.Database)
		cur, err := d.RunCommandCursor(ctx, g)
		defer cur.Close(ctx)

		if err != nil {
			// log an error if we can
			qErr, ok := err.(*mongo.CommandError)
			if ok {
				res.Error(int32(qErr.Code), qErr.Message)
			}
			next(req, res)
			return
		}

		var results []bson.D

		for i := 0; i < int(g.BatchSize); i++ {
			var result bson.D
			ok := cur.Next(ctx)
			if !ok {
				err = cur.Err()
				if err != nil {
					m.Logger.Warnf("Error on GetMore Command: %#v", err)

					if err == mongo.ErrNilCursor {
						// we return an empty getMore with an errored out
						// cursor
						response := messages.GetMoreResponse{
							CursorID:      g.CursorID,
							Database:      g.Database,
							Collection:    g.Collection,
							InvalidCursor: true,
						}
						res.Write(response)
						next(req, res)
						return
					}

					// log an error if we can
					qErr, ok := err.(*mongo.CommandError)
					if ok {
						res.Error(int32(qErr.Code), qErr.Message)
					}
					cur.Close(ctx)
					next(req, res)
					return
				}
				break
			}
			cur.Decode(&result)
			results = append(results, result)
		}

		response := messages.GetMoreResponse{
			CursorID:   g.CursorID,
			Database:   g.Database,
			Collection: g.Collection,
			Documents:  results,
		}

		res.Write(response)

	case messages.MsgType:

	case messages.KillCursorsType:

	default:
		m.Logger.Warnf("Unsupported operation: %v", req.Type())
	}

	next(req, res)

}
