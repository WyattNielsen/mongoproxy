package main

import (
	"github.com/tidepool-org/mongoproxy"
	"github.com/tidepool-org/mongoproxy/modules/mongod"
	"github.com/tidepool-org/mongoproxy/server"
)

func main() {
	var c server.Config
	c.Hosts = "localhost:27017"
	module := mongod.MongodModule{}
	module.Configure(c)
	chain := server.CreateChain()
	chain.AddModule(&module)
	mongoproxy.Start(8124, chain)
}
