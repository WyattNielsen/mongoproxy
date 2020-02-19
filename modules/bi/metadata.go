package bi

import (
	"github.com/tidepool-org/mongoproxy/messages"
	"github.com/globalsign/mgo/bson"
)

// helper function to upsert a metadata document into the metric collection. Metric
// documents have a special id, and contain the list of possible string values for a
// rule's valueField.
func saveMetadataForValue(rule Rule, granularity string,
	value string) messages.SingleUpdate {

	selector := bson.D{{"_id", "metadata"}}
	update := bson.D{{"$set", bson.D{{rule.ValueField + "." + value, true}}}}

	single := messages.SingleUpdate{
		Selector: selector,
		Update:   update,
		Upsert:   true,
	}
	return single
}
