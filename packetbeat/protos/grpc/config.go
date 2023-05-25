package grpc

import (
	"errors"

	"github.com/elastic/beats/v7/packetbeat/config"
	"github.com/elastic/beats/v7/packetbeat/protos"
	"github.com/elastic/beats/v7/packetbeat/protos/tcp"
)

type grpcConfig struct {
	config.ProtocolCommon    `config:",inline"`
	MaxMessageSize           int      `config:"max_message_size"`
	DecodeRequestBody        bool     `config:"decode_request_body"`
	DecodeResponseBody       bool     `config:"decode_request_body"`
	ProtoImportPaths         []string `config:"proto_import_paths"`
	ProtoFileNames           []string `config:"proto_file_names"`
	GRPCReflectionServerAddr string   `config:"grpc_reflection_server_addr"`
	GuessPaths               []string `config:"guess_paths"`
}

var (
	defaultConfig = grpcConfig{
		ProtocolCommon: config.ProtocolCommon{
			TransactionTimeout: protos.DefaultTransactionExpiration,
		},
		MaxMessageSize:     tcp.TCPMaxDataInStream,
		DecodeRequestBody:  true,
		DecodeResponseBody: false,
	}
)

func (c *grpcConfig) Validate() error {
	if c.DecodeRequestBody || c.DecodeResponseBody {
		if c.GRPCReflectionServerAddr == "" && (len(c.ProtoImportPaths) == 0 || len(c.ProtoFileNames) == 0) {
			return errors.New("when deocde_body = true, proto_import_paths or proto_file_names should be specified")
		}
		// TODO: parse proto files
	}
	return nil
}
