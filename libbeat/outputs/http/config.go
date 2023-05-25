package http

import (
	"errors"
	"time"

	"github.com/elastic/beats/v7/libbeat/outputs/codec"
	"github.com/elastic/elastic-agent-libs/config"
)

type backoffConfig struct {
	Init time.Duration `config:"init"`
	Max  time.Duration `config:"max"`
}

type httpConfig struct {
	Scheme      string            `config:"scheme"`
	Host        string            `config:"host"                validate:"required"`
	HostType    string            `config:"host_type"`
	Method      string            `config:"method"`
	URLPath     string            `config:"url_path"            validate:"required"`
	Headers     map[string]string `config:"headers"`
	Timeout     time.Duration     `config:"timeout"             validate:"min=1"`
	MaxRetries  int               `config:"max_retries"         validate:"min=-1,nonzero"`
	Backoff     backoffConfig     `config:"backoff"`
	BulkMaxSize int               `config:"bulk_max_size"`
	Codec       codec.Config      `config:"codec"`
}

func defaultConfig() httpConfig {
	return httpConfig{
		Scheme:   "http",
		Host:     "srv.bench-system.tt",
		HostType: "tns",
		Method:   "POST",
		URLPath:  "/v1/taffic",
		Headers: map[string]string{
			"Content-Type": "application/json",
		},
		Timeout: 10 * time.Second,
		Backoff: backoffConfig{
			Init: 1 * time.Second,
			Max:  60 * time.Second,
		},
		BulkMaxSize: 10,
	}
}

func readConfig(cfg *config.C) (*httpConfig, error) {
	c := defaultConfig()
	if err := cfg.Unpack(&c); err != nil {
		return nil, err
	}
	return &c, nil
}

func (c *httpConfig) Validate() error {
	switch c.HostType {
	case "file", "static":
	case "tns":
	default:
		return errors.New("unknown host type, should be one of 'file' or 'tns'")
	}

	return nil
}
