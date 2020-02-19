package main

import (
	"flag"
	"github.com/tidepool-org/mongoproxy"
	. "github.com/tidepool-org/mongoproxy/log"
	"github.com/tidepool-org/mongoproxy/server"
	_ "github.com/tidepool-org/mongoproxy/server/config"
	"github.com/globalsign/mgo/bson"
)

var (
	port     int
	logLevel int
)

func parseFlags() {
	flag.IntVar(&port, "port", 8124, "port to listen on")
	flag.IntVar(&logLevel, "logLevel", DEBUG, "verbosity for logging")

	flag.Parse()
}

func main() {

	parseFlags()
	SetLogLevel(logLevel)

	module := server.Registry["mongod"].New()

	connection := bson.M{}
	connection["addresses"] = []string{"localhost:27017"}

	// initialize the pipeline
	chain := server.CreateChain()
	module.Configure(connection)
	chain.AddModule(module)

	mongoproxy.Start(port, chain)
}
