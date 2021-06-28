// Package bi contains a real-time reporting and analytics module for the Mongo Proxy.
// It receives requests from a mongo client and creates time series data based on user-defined criteria.
package bi

import (
	"fmt"
	"time"

	"github.com/mongodb-labs/mongoproxy/bsonutil"
	"github.com/mongodb-labs/mongoproxy/convert"
	. "github.com/mongodb-labs/mongoproxy/log"
	"github.com/mongodb-labs/mongoproxy/messages"
	"github.com/mongodb-labs/mongoproxy/server"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

// BIModule calls the next module immediately, and then collects and aggregates
// data from inserts that successfully traveled the pipeline. The requests it analyzes
// and the metrics it aggregates is based upon its rules.
type BIModule struct {
	Rules        []Rule
	Connection   mgo.DialInfo
	mongoSession *mgo.Session
}

func init() {
	server.Publish(&BIModule{})
}

func (b *BIModule) New() server.Module {
	return &BIModule{}
}

func (b *BIModule) Name() string {
	return "bi"
}

/*
Configuration structure:
{
	connection: {
		addresses: []string,
		direct: boolean,
		timeout: integer,
		auth: {
			username: string,
			password: string,
			database: string
		}
	}
	rules: [
		{
			origin: string,
			prefix: string,
			timeGranularity: []string,
			valueField: string,
			timeField: string
		}
	]
}
*/
func (b *BIModule) Configure(conf bson.M) error {

	conn := convert.ToBSONMap(conf["connection"])
	if conn == nil {
		return fmt.Errorf("No connection data")
	}
	addrs, err := convert.ConvertToStringSlice(conn["addresses"])
	if err != nil {
		return fmt.Errorf("Invalid addresses: %v", err)
	}

	timeout := time.Duration(convert.ToInt64(conn["timeout"], -1))
	if timeout == -1 {
		timeout = time.Second * 10
	}

	dialInfo := mgo.DialInfo{
		Addrs:   addrs,
		Direct:  convert.ToBool(conn["direct"]),
		Timeout: timeout,
	}

	auth := convert.ToBSONMap(conn["auth"])
	if auth != nil {
		username, ok := auth["username"].(string)
		if ok {
			dialInfo.Username = username
		}
		password, ok := auth["password"].(string)
		if ok {
			dialInfo.Password = password
		}
		database, ok := auth["database"].(string)
		if ok {
			dialInfo.Database = database
		}
	}

	b.Connection = dialInfo

	// Rules
	b.Rules = make([]Rule, 0)
	rules, err := convert.ConvertToBSONMapSlice(conf["rules"])
	if err != nil {
		return fmt.Errorf("Error parsing rules: %v", err)
	}

	for i := 0; i < len(rules); i++ {
		r := rules[i]
		originD, originC, err := messages.ParseNamespace(convert.ToString(r["origin"]))
		if err != nil {
			return fmt.Errorf("Error parsing origin namespace: %v", err)
		}
		prefixD, prefixC, err := messages.ParseNamespace(convert.ToString(r["prefix"]))
		if err != nil {
			return fmt.Errorf("Error parsing prefix namespace: %v", err)
		}
		timeGranularities, err := convert.ConvertToStringSlice(r["timeGranularity"])
		if err != nil {
			return fmt.Errorf("Error parsing time granularities: %v", err)
		}
		valueField, ok := r["valueField"].(string)
		if !ok {
			return fmt.Errorf("Invalid valueField.")
		}
		rule := Rule{
			OriginDatabase:    originD,
			OriginCollection:  originC,
			PrefixDatabase:    prefixD,
			PrefixCollection:  prefixC,
			TimeGranularities: timeGranularities,
			ValueField:        valueField,
		}
		timeFieldRaw, ok := r["timeField"].(string)
		if ok {
			if len(timeFieldRaw) > 0 {
				rule.TimeField = &timeFieldRaw

			}

		}

		b.Rules = append(b.Rules, rule)
	}

	return nil
}

func (b *BIModule) Process(req messages.Requester, res messages.Responder,
	next server.PipelineFunc) {

	resNext := messages.ModuleResponse{}
	next(req, &resNext)

	res.Write(resNext.Writer)

	if resNext.CommandError != nil {
		res.Error(resNext.CommandError.ErrorCode, resNext.CommandError.Message)
		return // we're done. An error occured, so we shouldn't do any aggregating
	}

	// spin up the session if it doesn't exist
	if b.mongoSession == nil {
		var err error
		b.mongoSession, err = mgo.DialWithInfo(&b.Connection)
		if err != nil {
			Log(ERROR, "Error connecting to MongoDB: %v", err)
			return
		}
		b.mongoSession.SetPrefetch(0)
	}

	session := b.mongoSession.Copy()
	defer session.Close()

	updates := make([]messages.Update, 0)

	if req.Type() == messages.InsertType {
		// create metrics
		opi := req.(messages.Insert)

		for i := 0; i < len(b.Rules); i++ {
			rule := b.Rules[i]

			t := time.Now()

			// if the message matches the aggregation, create an upsert
			// and pass it on to mongod
			if opi.Collection != rule.OriginCollection ||
				opi.Database != rule.OriginDatabase {
				Log(DEBUG, "Didn't match database %v.%v. Was %v.%v", rule.OriginDatabase,
					rule.OriginCollection, opi.Database, opi.Collection)
				continue
			}

			// each time granularity needs a separate update
			for j := 0; j < len(rule.TimeGranularities); j++ {
				granularity := rule.TimeGranularities[j]
				suffix, err := GetSuffix(granularity)
				if err != nil {
					Log(INFO, "%v is not a time granularity", granularity)
					continue
				}

				for k := 0; k < len(opi.Documents); k++ {

					update := messages.Update{
						Database:   rule.PrefixDatabase,
						Collection: rule.PrefixCollection + suffix,
						Ordered:    false,
					}

					doc := opi.Documents[k]
					// use the time field instead if it exists
					if rule.TimeField != nil {
						docMap := doc.Map()
						tRaw := bsonutil.FindDeepValueInMap(*rule.TimeField, docMap)
						timeField, ok := tRaw.(time.Time)
						if ok {
							t = timeField
						} else {
							// time is a string in RFC 3339 format
							timeFieldRaw, ok := tRaw.(string)
							if ok {
								err := timeField.UnmarshalText([]byte(timeFieldRaw))
								if err == nil {
									t = timeField
								}

							} else {
								// time is in milliseconds from epoch
								timeFieldInt := convert.ToInt64(tRaw, -1)
								if timeFieldInt >= 0 {
									t = time.Unix(0, timeFieldInt*1000*1000)
								}
							}
						}

					}

					single, meta, err := createSingleUpdate(doc, t, granularity, rule)
					if err != nil {
						continue
					}

					update.Updates = append(update.Updates, *single)

					// upsert the metadata if needed
					if meta != nil {
						update.Updates = append(update.Updates, *meta)
					}

					updates = append(updates, update)

				}

			}
		}

		for i := 0; i < len(updates); i++ {
			u := updates[i]
			if len(updates[i].Updates) == 0 {
				continue
			}
			b := u.ToBSON()

			reply := bson.D{}
			err := session.DB(u.Database).Run(b, &reply)
			if err != nil {
				Log(ERROR, "Error updating database: %v", err)
			} else {
				Log(INFO, "Successfully updated database!")
			}
		}

	}

}
