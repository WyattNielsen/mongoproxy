package main

import (
	"flag"

	"github.com/globalsign/mgo/bson"
	"github.com/tidepool-org/mongoproxy"
	"github.com/tidepool-org/mongoproxy/log"
)

var (
	port            int
	logLevel        int
	mongoURI        string
	configNamespace string
	configFilename  string
)

func parseFlags() {
	flag.IntVar(&port, "port", 8124, "port to listen on")
	flag.IntVar(&logLevel, "logLevel", 3, "verbosity for logging")
	flag.StringVar(&mongoURI, "m", "mongodb://localhost:27017",
		"MongoDB instance to connect to for configuration.")
	flag.StringVar(&configNamespace, "c", "test.config",
		"Namespace to query for configuration.")
	flag.StringVar(&configFilename, "f", "",
		"JSON config filename. If set, will be used instead of mongoDB configuration.")
	flag.Parse()
}

func main() {

	parseFlags()
	log.SetLogLevel(logLevel)

	// grab config file
	var result bson.M
	var err error
	if len(configFilename) == 0 {
		result, err = mongoproxy.ParseConfigFromDB(mongoURI, configNamespace)
	} else {
		result, err = mongoproxy.ParseConfigFromFile(configFilename)
	}

	if err != nil {
		log.Log(log.WARNING, "%v", err)
	}

	mongoproxy.StartWithConfig(port, result)
}
