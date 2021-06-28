package main

import (
	"github.com/WyattNielsen/mongoproxy/modules/mongod"
	"github.com/WyattNielsen/mongoproxy/proxy"
	"github.com/WyattNielsen/mongoproxy/server"
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
