package grpc

import (
	"errors"

	"github.com/elastic/beats/v7/packetbeat/config"
	"github.com/elastic/beats/v7/packetbeat/protos"
	"github.com/elastic/beats/v7/packetbeat/protos/tcp"
)

type grpcConfig struct {
	config.ProtocolCommon    `config:",inline"`
	IncludeBodyFor           []string `config:"include_body_for"`
	IncludeRequestBodyFor    []string `config:"include_request_body_for"`
	IncludeResponseBodyFor   []string `config:"include_response_body_for"`
	MaxMessageSize           int      `config:"max_message_size"`
	DecodeBody               bool     `config:"decode_body"`
	ProtoImportPaths         []string `config:"proto_import_paths"`
	ProtoFileNames           []string `config:"proto_file_names"`
	GRPCReflectionServerAddr string   `config:"grpc_reflection_server_addr"`
	GuessPath                bool     `config:"guess_path"`
}

var (
	defaultConfig = grpcConfig{
		ProtocolCommon: config.ProtocolCommon{
			TransactionTimeout: protos.DefaultTransactionExpiration,
		},
		MaxMessageSize: tcp.TCPMaxDataInStream,
		DecodeBody:     true,
	}
)

func (c *grpcConfig) Validate() error {
	if c.DecodeBody {
		if c.GRPCReflectionServerAddr == "" && (len(c.ProtoImportPaths) == 0 || len(c.ProtoFileNames) == 0) {
			return errors.New("when deocde_body = true, proto_import_paths or proto_file_names should be specified")
		}
		// TODO: parse proto files
	}
	return nil
}
