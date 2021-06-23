package main

import (
	"github.com/tidepool-org/mongoproxy/modules/mongod"
	"github.com/tidepool-org/mongoproxy/proxy"
	"github.com/tidepool-org/mongoproxy/server"
)

func main() {
	var c server.Config
	c.FromEnv()
	module := mongod.MongodModule{}
	module.Configure(c)
	chain := server.CreateChain()
	chain.AddModule(&module)
	proxy.Start(c.Port, chain)
}
