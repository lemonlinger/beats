package grpc

import (
	"errors"
	"io"
	"io/ioutil"
	"time"

	"github.com/jhump/protoreflect/dynamic"
	"golang.org/x/net/http2"

	"github.com/elastic/beats/v7/libbeat/common/streambuf"
	"github.com/elastic/beats/v7/packetbeat/protos/applayer"
)

type parser struct {
	buf     streambuf.Buffer
	config  *parserConfig
	message *message

	onMessage    func(m *message) error
	hpackDecoer  *HPackDecoder
	protoPrarser ProtoParser
}

type parserConfig struct {
	maxBytes int

	decodeBody               bool
	protoImportPaths         []string
	protoFileNames           []string
	grpcReflectionServerAddr string
}

type message struct {
	applayer.Message

	method      string
	path        string
	contentType string
	headers     map[string]string

	status string

	headerPartiallyParse bool

	rawBody []byte
	msgBody *dynamic.Message

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
			m.IsRequest = false
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

func (p *parser) feed(ts time.Time, data []byte) error {
	if err := p.append(data); err != nil {
		return err
	}

	for p.buf.Total() > 0 {
		if p.message == nil {
			// allocate new message object to be used by parser with current timestamp
			p.message = p.newMessage(ts)
			p.message.IsRequest = true
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

func (p *parser) newMessage(ts time.Time) *message {
	return &message{
		Message: applayer.Message{
			Ts: ts,
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
				return p.message, nil
			}
		}

		switch frame := frame.(type) {
		case *http2.HeadersFrame:
			headers := map[string]string{}
			hfs, err := p.hpackDecoer.Decode(frame)
			if err == nil {
				for _, field := range hfs {
					headers[field.Name] = field.Value
				}
			} else {
				// try to parse partially
				buf := frame.HeaderBlockFragment()
				for _, field := range p.hpackDecoer.DecodePartial(buf) {
					headers[field.Name] = field.Value
				}
				p.message.headerPartiallyParse = true
			}
			p.message.mergeHeaders(headers)

			if frame.StreamEnded() {
				return p.message, nil
			}
		case *http2.DataFrame:
			var possiblePaths []string
			for key, value := range p.message.headers {
				if key == ":path" {
					possiblePaths = append(possiblePaths, value)
				}
			}
			if len(possiblePaths) == 0 {
				possiblePaths = p.protoPrarser.GetAllPaths()
			}

			maxMsgSize := -1
			for _, path := range possiblePaths {
				var msgBody *dynamic.Message
				data := frame.Data()
				if len(data) > 5 {
					data = data[5:]
				}
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

			if frame.StreamEnded() {
				return p.message, nil
			}
		case *http2.RSTStreamFrame:
			return nil, errors.New("stream was reset")
		default:
			continue
		}
	}
	return nil, nil
}
