package main

import (
	"flag"
	"fmt"

	"github.com/WyattNielsen/mongoproxy/modules/mongod"
	"github.com/WyattNielsen/mongoproxy/proxy"
	"github.com/WyattNielsen/mongoproxy/server"
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
	flag.StringVar(&configFilename, "f", "",
		"JSON config filename. If set, will be used instead of Environment configuration.")
	flag.Parse()
}

func main() {
	parseFlags()
	var c server.Config

	var err error
	// var result bson.M
	var chain *server.ModuleChain
	if len(configFilename) > 0 {
		err = c.ParseConfigFromFile(configFilename)
		if err != nil {
			fmt.Printf("config error: %v\n", err)
		}
	} else {
		c.FromEnv()
	}

	module := mongod.MongodModule{}
	module.Configure(c)
	chain = server.CreateChain()
	chain.AddModule(&module)

	proxy.Start(port, chain)
}
