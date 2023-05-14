package grpc

import (
	"bytes"
	"errors"
	"io"
	"io/ioutil"
	"strings"

	"github.com/jhump/protoreflect/dynamic"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"

	"github.com/elastic/beats/v7/libbeat/common/streambuf"
	"github.com/elastic/beats/v7/packetbeat/protos"
	"github.com/elastic/beats/v7/packetbeat/protos/applayer"
)

type parser struct {
	buf     streambuf.Buffer
	config  *parserConfig
	message *message

	onMessage    func(m *message) error
	hpackDecoer  *HPackDecoder
	protoPrarser ProtoParser

	// stream ID ->
	pathcache map[uint32]string
}

type parserConfig struct {
	maxBytes int

	servicePorts             map[int]struct{}
	decodeBody               bool
	protoImportPaths         []string
	protoFileNames           []string
	grpcReflectionServerAddr string
	guessPath                bool
}

type message struct {
	applayer.Message

	streamID uint32

	method      string
	path        string
	contentType string
	headers     map[string]string

	status string

	headerPartiallyParse bool
	pathGuessed          bool
	firstDataRecved      bool

	rawBody    []byte
	msgBody    *dynamic.Message
	bodyBuffer bytes.Buffer

	// indicator for parsed message being complete or requires more messages
	// (if false) to be merged to generate full message.
	isComplete bool

	// list element use by 'transactions' for correlation
	next *message
}

func (m *message) mergeHeaders(headers map[string]string) {
	if m.headers == nil {
		m.headers = make(map[string]string, len(headers))
	}
	for k, v := range headers {
		m.headers[k] = v
		switch k {
		case ":method":
			m.method = v
		case ":path":
			m.path = v
		case "content-type":
			m.contentType = v
		case ":status":
			m.status = v
		}
	}
}

func (m *message) isCompletedRequest() bool {
	return m.method != "" && m.path != ""
}

func (m *message) isCompletedResponse() bool {
	return m.path != "" && m.status != ""
}

// Error code if stream exceeds max allowed size on append.
var (
	ErrStreamTooLarge = errors.New("Stream data too large")
)

func (p *parser) init(
	cfg *parserConfig,
	decoder *HPackDecoder,
	protoParser ProtoParser,
	onMessage func(*message) error,
) {
	*p = parser{
		buf:          streambuf.Buffer{},
		config:       cfg,
		hpackDecoer:  decoder,
		protoPrarser: protoParser,
		onMessage:    onMessage,

		pathcache: make(map[uint32]string),
	}
}

func (p *parser) append(data []byte) error {
	_, err := p.buf.Write(data)
	if err != nil {
		return err
	}

	if p.config.maxBytes > 0 && p.buf.Total() > p.config.maxBytes {
		return ErrStreamTooLarge
	}
	return nil
}

func (p *parser) isServicePort(port int) bool {
	if p.config == nil {
		return false
	}
	_, ok := p.config.servicePorts[port]
	return ok
}

func (p *parser) feed(pkt *protos.Packet) error {
	data := pkt.Payload
	if err := p.append(data); err != nil {
		return err
	}

	for p.buf.Total() > 0 {
		if p.message == nil {
			// allocate new message object to be used by parser with current timestamp
			p.message = p.newMessage(pkt)
			p.message.IsRequest = p.isServicePort(int(pkt.Tuple.DstPort))
		}

		msg, err := p.parse()
		if err != nil {
			return err
		}
		if msg == nil {
			break // wait for more data
		}

		// reset buffer and message -> handle next message in buffer
		p.buf.Reset()
		p.message = nil

		// call message handler callback
		if err := p.onMessage(msg); err != nil {
			return err
		}
	}

	return nil
}

func (p *parser) newMessage(pkt *protos.Packet) *message {
	return &message{
		Message: applayer.Message{
			Ts:        pkt.Ts,
			Transport: applayer.TransportTCP,
			IsRequest: p.isServicePort(int(pkt.Tuple.DstPort)),
			Size:      0,
		},
	}
}

func (p *parser) parse() (*message, error) {
	framer := http2.NewFramer(ioutil.Discard, &p.buf)
	for {
		frame, err := framer.ReadFrame()
		if err != nil {
			if err == io.ErrUnexpectedEOF {
				// need more data
				return nil, nil
			}
			if err == io.EOF {
				return nil, nil
			}
			return nil, err
		}

		p.message.streamID = frame.Header().StreamID
		p.message.Size += uint64(frame.Header().Length)

		switch frame := frame.(type) {
		case *http2.HeadersFrame:
			headers := map[string]string{}
			hfs, err := p.hpackDecoer.Decode(frame)
			if err == nil {
				for _, field := range hfs {
					nf, ok := fixHeader(field)
					if !ok {
						continue
					}
					headers[nf.Name] = nf.Value
					if nf.Name == ":path" {
						p.pathcache[frame.StreamID] = nf.Value
					}
				}
			} else {
				// try to parse partially
				buf := frame.HeaderBlockFragment()
				for _, field := range p.hpackDecoer.DecodePartial(buf) {
					nf, ok := fixHeader(field)
					if !ok {
						continue
					}
					headers[field.Name] = field.Value
					if nf.Name == ":path" {
						p.pathcache[frame.StreamID] = nf.Value
					}
				}
				p.message.headerPartiallyParse = true
			}
			p.message.mergeHeaders(headers)

			if frame.StreamEnded() {
				if !p.message.IsRequest {
					delete(p.pathcache, frame.StreamID)
				}
				return p.message, nil
			}
		case *http2.DataFrame:
			data := frame.Data()
			if !p.message.firstDataRecved {
				p.message.firstDataRecved = true
				if len(data) > 5 {
					data = data[5:]
				}
			}
			_, err := p.message.bodyBuffer.Write(data)
			if err != nil {
				debugf("write data frame into buffer failed: %v", err)
				return nil, err
			}

			// needs more data
			if !frame.StreamEnded() {
				continue
			}

			var possiblePaths []string
			if path, ok := p.pathcache[frame.StreamID]; ok && path != "" {
				possiblePaths = append(possiblePaths, path)
			}
			if len(possiblePaths) == 0 && p.config.guessPath {
				possiblePaths = p.protoPrarser.GetAllPaths()
				p.message.pathGuessed = true
			}

			maxMsgSize := -1
			for _, path := range possiblePaths {
				var msgBody *dynamic.Message
				data := p.message.bodyBuffer.Bytes()
				if p.message.IsRequest {
					msgBody, err = p.protoPrarser.MarshalRequest(path, data)
				} else {
					msgBody, err = p.protoPrarser.MarshalResponse(path, data)
				}

				if err == nil {
					n := len(msgBody.String())
					if n > maxMsgSize {
						maxMsgSize = n
						p.message.msgBody = msgBody
						p.message.path = path
					}
				}
			}
			if p.message.msgBody != nil {
				bs, err := p.message.msgBody.MarshalJSON()
				if err == nil {
					p.message.rawBody = bs
				}
			}

			if !p.message.IsRequest {
				delete(p.pathcache, frame.StreamID)
			}
			return p.message, nil

		case *http2.RSTStreamFrame:
			return nil, errors.New("stream was reset")
		case *http2.GoAwayFrame:
			return nil, errors.New("server is going away")
		}
	}
}

func (p *parser) clear() {
	p.pathcache = map[uint32]string{}
}

func fixHeader(f hpack.HeaderField) (nf hpack.HeaderField, ok bool) {
	if f.Name == ":path" {
		if strings.HasPrefix(f.Value, "/") {
			nf = f
			ok = true
		}
		return
	}

	nf = f
	ok = true
	return
}
