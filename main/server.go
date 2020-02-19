package main

import (
	"os"
	"strconv"

	"github.com/tidepool-org/mongoproxy"
	"github.com/tidepool-org/mongoproxy/server"
)

func main() {
	var config server.Config
	config.FromEnv()
	portStr := os.Getenv("PORT")
	port, err := strconv.Atoi(portStr)
	if portStr == "" || err != nil {
		port = 27017
	}

	mongoproxy.StartWithConfig(port, config)
}
