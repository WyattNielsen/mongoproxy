// Package mongod contains a module that acts as a backend for Mongo proxy,
// which connects to a mongod instance and sends requests to (and receives responses from)
// the server.
package mongod

import (
	"crypto/tls"
	"net"

	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/tidepool-org/mongoproxy/bsonutil"
	"github.com/tidepool-org/mongoproxy/convert"
	"github.com/tidepool-org/mongoproxy/messages"
	"github.com/tidepool-org/mongoproxy/server"
)

// A MongodModule takes the request, sends it to a mongod instance, and then
// writes the response from mongod into the ResponseWriter before calling
// the next module. It passes on requests unchanged.
type MongodModule struct {
	Connection   mgo.DialInfo
	mongoSession *mgo.Session
	ReadOnly     bool
}

func init() {
	server.Publish(&MongodModule{})
}

func (m *MongodModule) New() server.Module {
	return &MongodModule{}
}

func (m *MongodModule) Name() string {
	return "mongod"
}

func (m *MongodModule) Configure(config server.Config) error {
	dialInfo, err := mgo.ParseURL(config.AsConnectionString())
	dialInfo.Timeout = config.Timeout

	if err != nil {
		return errors.Wrap(err, "URL is unparseable")
	}
	// override the DialServer is we are using TLS because we don't have the proper CA certs installed.
	if config.TLS {
		dialInfo.DialServer = func(serverAddr *mgo.ServerAddr) (net.Conn, error) {
			return tls.Dial("tcp", serverAddr.String(), &tls.Config{InsecureSkipVerify: true}) // TODO: Secure this connection
		}
	}
	m.ReadOnly = config.ReadOnly
	m.Connection = *dialInfo
	return nil
}

func (m *MongodModule) Process(req messages.Requester, res messages.Responder,
	next server.PipelineFunc) {

	// spin up the session if it doesn't exist
	if m.mongoSession == nil {
		var err error
		m.mongoSession, err = mgo.DialWithInfo(&m.Connection)
		if err != nil {
			log.Errorf("Error connecting to MongoDB: %#v", err)
			next(req, res)
			return
		}
		m.mongoSession.SetPrefetch(0)
	}

	session := m.mongoSession.Copy()
	defer session.Close()

	switch req.Type() {
	case messages.CommandType:
		command, err := messages.ToCommandRequest(req)
		if err != nil {
			log.Warnf("Error converting to command: %#v", err)
			next(req, res)
			return
		}

		b := command.ToBSON()

		reply := bson.M{}
		err = session.DB(command.Database).Run(b, reply)
		if err != nil {
			// log an error if we can
			qErr, ok := err.(*mgo.QueryError)
			log.Warnf("Error running command %v: %v", command.CommandName, err)
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
			log.Warnf("Error converting to a Find command: %#v", err)
			next(req, res)
			return
		}

		c := session.DB(f.Database).C(f.Collection)
		query := c.Find(f.Filter).Batch(int(f.Limit)).Skip(int(f.Skip)).Prefetch(0)

		if f.Projection != nil {
			query = query.Select(f.Projection)
		}

		var iter = query.Iter()
		var results []bson.D

		if f.Limit > 0 {
			// only store the amount specified by the limit
			for i := 0; i < int(f.Limit); i++ {
				var result bson.D
				ok := iter.Next(&result)
				if !ok {
					err = iter.Err()
					if err != nil {
						log.Warnf("Error on Find Command: %#v", err)

						// log an error if we can
						qErr, ok := err.(*mgo.QueryError)
						if ok {
							res.Error(int32(qErr.Code), qErr.Message)
						}
						iter.Close()
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
			err = iter.All(&results)
			if err != nil {
				log.Warnf("Error on Find Command: %#v", err)

				// log an error if we can
				qErr, ok := err.(*mgo.QueryError)
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
			log.Warnf("Error converting to Insert command: %#v", err)
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
		err = session.DB(insert.Database).Run(b, reply)
		if err != nil {
			// log an error if we can
			qErr, ok := err.(*mgo.QueryError)
			if ok {
				res.Error(int32(qErr.Code), qErr.Message)
			}
			next(req, res)
			return
		}

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
			log.Warnf("Error converting to Update command: %v", err)
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
		err = session.DB(u.Database).Run(b, &reply)
		if err != nil {
			// log an error if we can
			qErr, ok := err.(*mgo.QueryError)
			if ok {
				res.Error(int32(qErr.Code), qErr.Message)
			}
			next(req, res)
			return
		}

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
			log.Warnf("Error converting to Delete command: %v", err)
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
		err = session.DB(d.Database).Run(b, reply)
		if err != nil {
			// log an error if we can
			qErr, ok := err.(*mgo.QueryError)
			if ok {
				res.Error(int32(qErr.Code), qErr.Message)
			}
			next(req, res)
			return
		}

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

		log.Infof("Reply: %#v", reply)

		res.Write(response)

	case messages.GetMoreType:
		g, err := messages.ToGetMoreRequest(req)
		if err != nil {
			log.Warnf("Error converting to GetMore command: %#v", err)
			next(req, res)
			return
		}
		log.Debugf("%#v", g)

		// make an iterable to get more
		c := session.DB(g.Database).C(g.Collection)
		batch := make([]bson.Raw, 0)
		iter := c.NewIter(session, batch, g.CursorID, nil)
		//iter.SetBatch(int(g.BatchSize))

		var results []bson.D

		for i := 0; i < int(g.BatchSize); i++ {
			var result bson.D
			ok := iter.Next(&result)
			if !ok {
				err = iter.Err()
				if err != nil {
					log.Warnf("Error on GetMore Command: %#v", err)

					if err == mgo.ErrCursor {
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
					qErr, ok := err.(*mgo.QueryError)
					if ok {
						res.Error(int32(qErr.Code), qErr.Message)
					}
					iter.Close()
					next(req, res)
					return
				}
				break
			}
			results = append(results, result)
		}

		response := messages.GetMoreResponse{
			CursorID:   g.CursorID,
			Database:   g.Database,
			Collection: g.Collection,
			Documents:  results,
		}

		res.Write(response)
	default:
		log.Warnf("Unsupported operation: %v", req.Type())
	}

	next(req, res)

}
