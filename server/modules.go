// Package server contains interfaces and functions dealing with setting up proxy core,
// including code construct the module pipeline.
package server

import (
	"os"
	"strconv"
	"time"

	"github.com/tidepool-org/mongoproxy/messages"
)

//Config describe parameters need to make a connection to a Mongo database
type Config struct {
	Scheme    string        `json:"scheme"`
	Hosts     string        `json:"hosts"`
	TLS       bool          `json:"tls"`
	Database  string        `json:"database"`
	Username  string        `json:"-"`
	Password  string        `json:"-"`
	Timeout   time.Duration `json:"timeout"`
	OptParams string        `json:"optParams"`
	ReadOnly  bool          `json:"readOnly"`
}

// FromEnv populates Config from the environment
func (c *Config) FromEnv() {
	c.Scheme = os.Getenv("TIDEPOOL_STORE_SCHEME")
	c.Hosts = os.Getenv("TIDEPOOL_STORE_ADDRESSES")
	c.Username = os.Getenv("TIDEPOOL_STORE_USERNAME")
	c.Password = os.Getenv("TIDEPOOL_STORE_PASSWORD")
	c.Database = os.Getenv("TIDEPOOL_STORE_DATABASE")
	c.OptParams = os.Getenv("TIDEPOOL_STORE_OPT_PARAMS")
	c.TLS = os.Getenv("TIDEPOOL_STORE_TLS") == "true"
	timeoutStr := os.Getenv("TIDEPOOL_STORE_TIMEOUT")
	timeout, err := strconv.Atoi(timeoutStr)
	if (timeoutStr == "") || (err != nil) {
		c.Timeout = time.Duration(20 * time.Second)
	} else {
		c.Timeout = time.Duration(timeout) * time.Second
	}
	c.ReadOnly = os.Getenv("READONLY") == "true"
}

// AsConnectionString constructs a MongoDB connection string from a Config
func (c *Config) AsConnectionString() string {
	var url string
	if c.Scheme != "" {
		url += c.Scheme + "://"
	} else {
		url += "mongodb://"
	}

	if c.Username != "" {
		url += c.Username
		if c.Password != "" {
			url += ":"
			url += c.Password
		}
		url += "@"
	}
	url += c.Hosts
	url += "/"
	url += c.Database
	if c.TLS {
		url += "?ssl=true"
	} else {
		url += "?ssl=false"
	}
	if c.OptParams != "" {
		url += c.OptParams
	}

	return url
}

type Module interface {

	// Name returns the name to identify this module when registered.
	Name() string

	// Configure configures this module with the given configuration object. Returns
	// an error if the configuration is invalid for the module.
	Configure(config Config) error

	// Process is the function executed when a message is called in the pipeline.
	// It takes in a Requester from an upstream module (or proxy core), a
	// Responder that it writes a response to, and a PipelineFunc that should
	// be called to execute the next module in the pipeline.
	Process(messages.Requester, messages.Responder, PipelineFunc)

	// New creates a new instance of this module.
	New() Module
}
