package mongoproxy

import (
	"fmt"
	"io"
	"net"

	log "github.com/sirupsen/logrus"
	"github.com/tidepool-org/mongoproxy/messages"
	"github.com/tidepool-org/mongoproxy/modules/mongod"
	"github.com/tidepool-org/mongoproxy/server"
)

// Start starts the server at the provided port and with the given module chain.
func Start(port int, chain *server.ModuleChain) {

	ln, err := net.Listen("tcp", fmt.Sprintf(":%v", port))
	if err != nil {
		log.Errorf("Error listening on port %v: %v", port, err)
		return
	}

	pipeline := server.BuildPipeline(chain)
	log.Infof("Server running on port %v", port)
	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Errorf("error accepting connection: %v", err)
			continue
		}

		log.Infof("accepted connection from: %v", conn.RemoteAddr())
		go handleConnection(conn, pipeline)
	}

}

// StartWithConfig starts the server at the provided port, creating a module chain
// with the given configuration.
func StartWithConfig(port int, config server.Config) {
	chain := server.CreateChain()
	chain.AddModule(&mongod.MongodModule{})
	Start(port, chain)
}

func handleConnection(conn net.Conn, pipeline server.PipelineFunc) {
	for {

		message, msgHeader, err := messages.Decode(conn)

		if err != nil {
			if err != io.EOF {
				log.Errorf("Decoding error: %v", err)
			}
			conn.Close()
			return
		}

		log.Debugf("Request: %#v", message)

		res := &messages.ModuleResponse{}
		pipeline(message, res)

		bytes, err := messages.Encode(msgHeader, *res)

		// update, delete, and insert messages do not have a response, so we continue and write the
		// response on the getLastError that will be called immediately after. Kind of a hack.
		if msgHeader.OpCode == messages.OP_UPDATE || msgHeader.OpCode == messages.OP_INSERT ||
			msgHeader.OpCode == messages.OP_DELETE {
			log.Infof("Continuing on OpCode: %v", msgHeader.OpCode)
			continue
		}
		if err != nil {
			log.Errorf("Encoding error: %v", err)
			conn.Close()
			return
		}
		_, err = conn.Write(bytes)
		if err != nil {
			log.Errorf("Error writing to connection: %v", err)
			conn.Close()
			return
		}

	}
}
