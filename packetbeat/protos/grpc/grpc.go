package grpc

import (
	"time"

	"github.com/elastic/beats/v7/libbeat/common"
	"github.com/elastic/elastic-agent-libs/logp"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"

	"github.com/elastic/beats/v7/packetbeat/procs"
	"github.com/elastic/beats/v7/packetbeat/protos"
	"github.com/elastic/beats/v7/packetbeat/protos/tcp"
	conf "github.com/elastic/elastic-agent-libs/config"
)

// grpcPlugin application level protocol analyzer plugin
type grpcPlugin struct {
	ports        protos.PortsConfig
	parserConfig parserConfig
	transConfig  transactionConfig
	watcher      *procs.ProcessesWatcher
	pub          transPub

	protoParser  ProtoParser
	hpackDecoder *HPackDecoder
}

type HPackDecoder struct {
	decoder *hpack.Decoder
}

func newHPackDecoder() *HPackDecoder {
	decoder := hpack.NewDecoder(65536, nil)
	return &HPackDecoder{decoder: decoder}
}

func (h *HPackDecoder) DecodePartial(p []byte) (hfs []hpack.HeaderField) {
	emitFunc := func(hf hpack.HeaderField) { hfs = append(hfs, hf) }
	h.decoder.SetEmitFunc(emitFunc)
	h.decoder.SetEmitEnabled(true)
	h.decoder.Write(p)
	h.decoder.Close()
	h.decoder.SetEmitEnabled(false)
	h.decoder.SetEmitFunc(nil)
	return hfs
}

func (h *HPackDecoder) Decode(hf *http2.HeadersFrame) ([]hpack.HeaderField, error) {
	return h.decoder.DecodeFull(hf.HeaderBlockFragment())
}

// Application Layer tcp stream data to be stored on tcp connection context.
type connection struct {
	streams [2]*stream
	trans   transactions
}

// Uni-directional tcp stream state for parsing messages.
type stream struct {
	parser parser
}

var (
	debugf = logp.MakeDebug("grpc")

	// use isDebug/isDetailed to guard debugf/detailedf to minimize allocations
	// (garbage collection) when debug log is disabled.
	isDebug = false
)

func init() {
	protos.Register("grpc", New)
}

// New create and initializes a new grpc protocol analyzer instance.
func New(
	testMode bool,
	results protos.Reporter,
	watcher *procs.ProcessesWatcher,
	cfg *conf.C,
) (protos.Plugin, error) {
	p := &grpcPlugin{}
	config := defaultConfig
	if !testMode {
		if err := cfg.Unpack(&config); err != nil {
			return nil, err
		}
	}

	if err := p.init(results, watcher, &config); err != nil {
		return nil, err
	}
	return p, nil
}

func (gp *grpcPlugin) init(results protos.Reporter, watcher *procs.ProcessesWatcher, config *grpcConfig) error {
	if err := gp.setFromConfig(config); err != nil {
		return err
	}
	gp.pub.results = results
	gp.watcher = watcher
	gp.hpackDecoder = newHPackDecoder()

	if gp.parserConfig.decodeBody {
		// prior to use reflection
		if gp.parserConfig.grpcReflectionServerAddr != "" {
			debugf("new proto parser from reflection(%s)", gp.parserConfig.grpcReflectionServerAddr)
			protoParser, err := NewProtoParserFromReflection(gp.parserConfig.grpcReflectionServerAddr)
			if err != nil {
				debugf("new proto parser from reflection(%s): %v", gp.parserConfig.grpcReflectionServerAddr, err)
				return err
			}
			gp.protoParser = protoParser
		} else {
			protoParser, err := NewProtoParser(gp.parserConfig.protoImportPaths, gp.parserConfig.protoFileNames)
			if err != nil {
				return err
			}
			gp.protoParser = protoParser
		}
	}

	isDebug = logp.IsDebug("grpc")
	debugf("succeed to init grpc plugin")
	return nil
}

func (gp *grpcPlugin) setFromConfig(config *grpcConfig) error {

	// set module configuration
	if err := gp.ports.Set(config.Ports); err != nil {
		return err
	}

	// set parser configuration
	parser := &gp.parserConfig
	parser.maxBytes = tcp.TCPMaxDataInStream

	parser.decodeBody = config.DecodeBody
	parser.protoImportPaths = config.ProtoImportPaths
	parser.protoFileNames = config.ProtoFileNames
	parser.grpcReflectionServerAddr = config.GRPCReflectionServerAddr

	// set transaction correlator configuration
	trans := &gp.transConfig
	trans.transactionTimeout = config.TransactionTimeout

	// set transaction publisher configuration
	pub := &gp.pub
	pub.sendRequest = config.SendRequest
	pub.sendResponse = config.SendResponse

	return nil
}

// ConnectionTimeout returns the per stream connection timeout.
// Return <=0 to set default tcp module transaction timeout.
func (gp *grpcPlugin) ConnectionTimeout() time.Duration {
	return gp.transConfig.transactionTimeout
}

// GetPorts returns the ports numbers packets shall be processed for.
func (gp *grpcPlugin) GetPorts() []int {
	return gp.ports.Ports
}

// Parse processes a TCP packet. Return nil if connection
// state shall be dropped (e.g. parser not in sync with tcp stream)
func (gp *grpcPlugin) Parse(
	pkt *protos.Packet,
	tcptuple *common.TCPTuple, dir uint8,
	private protos.ProtocolData,
) protos.ProtocolData {
	defer logp.Recover("Parse grpcPlugin exception")

	conn := gp.ensureConnection(private)
	st := conn.streams[dir]
	if st == nil {
		st = &stream{}
		st.parser.init(&gp.parserConfig, gp.hpackDecoder, gp.protoParser, func(msg *message) error {
			return conn.trans.onMessage(tcptuple.IPPort(), dir, msg)
		})
		conn.streams[dir] = st
	}

	if err := st.parser.feed(pkt.Ts, pkt.Payload); err != nil {
		debugf("%v, dropping TCP stream for error in direction %v.", err, dir)
		gp.onDropConnection(conn)
		return nil
	}
	return conn
}

// ReceivedFin handles TCP-FIN packet.
func (gp *grpcPlugin) ReceivedFin(
	tcptuple *common.TCPTuple, dir uint8,
	private protos.ProtocolData,
) protos.ProtocolData {
	return private
}

// GapInStream handles lost packets in tcp-stream.
func (gp *grpcPlugin) GapInStream(tcptuple *common.TCPTuple, dir uint8,
	nbytes int,
	private protos.ProtocolData,
) (protos.ProtocolData, bool) {
	conn := getConnection(private)
	if conn != nil {
		gp.onDropConnection(conn)
	}

	return nil, true
}

// onDropConnection processes and optionally sends incomplete
// transaction in case of connection being dropped due to error
func (gp *grpcPlugin) onDropConnection(conn *connection) {
}

func (gp *grpcPlugin) ensureConnection(private protos.ProtocolData) *connection {
	conn := getConnection(private)
	if conn == nil {
		conn = &connection{}
		conn.trans.init(&gp.transConfig, gp.watcher, gp.pub.onTransaction)
	}
	return conn
}

func (conn *connection) dropStreams() {
	conn.streams[0] = nil
	conn.streams[1] = nil
}

func getConnection(private protos.ProtocolData) *connection {
	if private == nil {
		return nil
	}

	priv, ok := private.(*connection)
	if !ok {
		logp.Warn("grpc connection type error")
		return nil
	}
	if priv == nil {
		logp.Warn("Unexpected: grpc connection data not set")
		return nil
	}
	return priv
}
