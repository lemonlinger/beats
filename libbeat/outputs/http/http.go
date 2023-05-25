package http

import (
	"github.com/elastic/beats/v7/libbeat/beat"
	"github.com/elastic/beats/v7/libbeat/outputs"
	"github.com/elastic/beats/v7/libbeat/outputs/codec"
	"github.com/elastic/elastic-agent-libs/config"
	"github.com/elastic/elastic-agent-libs/logp"
)

const (
	logSelector = "http"
)

func init() {
	outputs.RegisterType("http", makeBenchSystem)
}

func makeBenchSystem(
	_ outputs.IndexManager,
	beat beat.Info,
	observer outputs.Observer,
	cfg *config.C,
) (outputs.Group, error) {
	log := logp.NewLogger(logSelector)
	log.Debug("initialize http output")

	config, err := readConfig(cfg)
	if err != nil {
		return outputs.Fail(err)
	}

	codec, err := codec.CreateEncoder(beat, config.Codec)
	if err != nil {
		return outputs.Fail(err)
	}

	client, err := newHTTPClient(observer, config.HostType, config.Method, config.Scheme, config.Host, config.URLPath, config.Headers, config.Timeout, codec)
	if err != nil {
		return outputs.Fail(err)
	}
	retry := config.MaxRetries
	if retry < 0 {
		retry = -1
	}
	return outputs.Success(config.BulkMaxSize, retry, client)
}
