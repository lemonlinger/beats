package grpc

import (
	"bytes"
	"errors"
	"io"
	"io/ioutil"
	"strings"
	"time"

	"github.com/dgraph-io/ristretto"
	"github.com/jhump/protoreflect/dynamic"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"

	"github.com/elastic/beats/v7/libbeat/common"
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

	// stream ID -> path
	pathcache *ristretto.Cache
}

type parserConfig struct {
	maxBytes int

	servicePorts             map[int]struct{}
	decodeRequestBody        bool
	decodeResponseBody       bool
	protoImportPaths         []string
	protoFileNames           []string
	grpcReflectionServerAddr string
	guessPaths               []string
	autoGuessPath            bool
}

func (c parserConfig) decodeBody() bool {
	return c.decodeRequestBody || c.decodeResponseBody
}

type message struct {
	applayer.Message

	tuple common.IPPortTuple

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

	createdAt time.Time

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
	cache *ristretto.Cache,
	onMessage func(*message) error,
) {
	*p = parser{
		buf:          streambuf.Buffer{},
		config:       cfg,
		hpackDecoer:  decoder,
		protoPrarser: protoParser,
		onMessage:    onMessage,
		pathcache:    cache,
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
		tuple: pkt.Tuple,
	}
}

func (p *parser) getPath(streamID uint32) string {
	s, ok := p.pathcache.Get(streamID)
	if ok {
		return s.(string)
	}
	return ""
}

func (p *parser) setPath(streamID uint32, path string) {
	p.pathcache.SetWithTTL(streamID, path, 1, 3*time.Second)
	p.pathcache.Wait()
}

func (p *parser) deletePath(streamID uint32) {
	p.pathcache.Del(streamID)
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
					nf, fixed, ok := fixHeaderByKey(field)
					if fixed && ok {
						headers[nf.Name] = nf.Value
						if nf.Name == ":path" {
							p.setPath(frame.StreamID, nf.Value)
						}
						continue
					}

					if !fixed {
						nf, ok = fixHeaderByValue(field)
						if ok {
							headers[nf.Name] = nf.Value
						}
					}
				}
			} else {
				// try to parse partially
				buf := frame.HeaderBlockFragment()
				for _, field := range p.hpackDecoer.DecodePartial(buf) {
					nf, fixed, ok := fixHeaderByKey(field)
					if fixed && ok {
						headers[nf.Name] = nf.Value
						if nf.Name == ":path" {
							p.setPath(frame.StreamID, nf.Value)
						}
						continue
					}

					if !fixed {
						nf, ok = fixHeaderByValue(field)
						if ok {
							headers[nf.Name] = nf.Value
						}
					}
				}
				p.message.headerPartiallyParse = true
			}
			p.message.mergeHeaders(headers)

			if frame.StreamEnded() {
				p.parseMessageBody(frame.StreamID)
				if !p.message.IsRequest {
					p.deletePath(frame.StreamID)
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

			p.parseMessageBody(frame.StreamID)

			if !p.message.IsRequest {
				p.deletePath(frame.StreamID)
			}
			return p.message, nil

		case *http2.RSTStreamFrame:
			p.pathcache.Clear()
			return nil, errors.New("stream was reset")
		case *http2.GoAwayFrame:
			p.pathcache.Clear()
			return nil, errors.New("server is going away")
		}
	}
}

func (p *parser) clear() {
	p.pathcache.Clear()
}

func (p *parser) parseMessageBody(streamID uint32) {
	if p.message.IsRequest && !p.config.decodeRequestBody {
		return
	}
	if !p.message.IsRequest && !p.config.decodeResponseBody {
		if path := p.getPath(streamID); path != "" {
			p.message.path = path
		}
		return
	}

	var (
		err           error
		possiblePaths []string
	)
	if path := p.getPath(streamID); path != "" {
		possiblePaths = append(possiblePaths, path)
	}
	if len(possiblePaths) == 0 {
		if p.config.autoGuessPath {
			possiblePaths = p.protoPrarser.GetAllPaths()
			p.message.pathGuessed = true
		} else if len(p.config.guessPaths) > 0 {
			possiblePaths = p.config.guessPaths
			p.message.pathGuessed = true
		}
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
		} else {
			if p.message.IsRequest {
				p.message.Notes = append(p.message.Notes, "parse req body failed: "+err.Error())
			} else {
				p.message.Notes = append(p.message.Notes, "parse resp body failed: "+err.Error())
			}
		}
	}
	if p.message.msgBody != nil {
		bs, err := p.message.msgBody.MarshalJSON()
		if err == nil {
			p.message.rawBody = bs
		}
	}
}

func fixHeaderByKey(f hpack.HeaderField) (nf hpack.HeaderField, fixed, ok bool) {
	if f.Value == "" {
		return f, false, false
	}

	switch f.Name {
	case ":path":
		fixed = true
		if strings.HasPrefix(f.Value, "/") {
			nf = f
			ok = true
		}
		return
	case "content-type":
		fixed = true
		if strings.HasPrefix(f.Value, "application/") {
			nf = f
			ok = true
		}
		return
	case "user-agent":
		fixed = true
		if strings.Contains(f.Value, "/") {
			nf = f
			ok = true
		}
		return
	case ":authority":
		fixed = true
		if strings.Count(f.Value, ".") >= 2 {
			nf = f
			ok = true
		}
		return
	case "x-tt-caller":
		fixed = true
		if !strings.ContainsAny(f.Value, "{}[]") && (strings.Contains(f.Value, "-") || strings.Contains(f.Value, ".")) {
			nf = f
			ok = true
		}
		return
	}

	return f, false, false
}

func fixHeaderByValue(f hpack.HeaderField) (nf hpack.HeaderField, ok bool) {
	if f.Value == "" {
		return f, false
	}

	if strings.Contains(f.Value, "REV_") && (strings.Contains(f.Value, ":exp") || strings.Contains(f.Value, ":control")) {
		nf.Name, nf.Value = "x-testing-group", f.Value
		ok = true
		return
	}

	if strings.Contains(f.Value, ";") && strings.Contains(f.Value, ".") {
		nf.Name, nf.Value = "x-tt-schain", f.Value
		ok = true
		return
	}

	if strings.HasPrefix(f.Value, "application/") {
		nf.Name, nf.Value = "content-type", f.Value
		ok = true
		return
	}

	if strings.Contains(f.Value, `"deviceToken"`) {
		nf.Name, nf.Value = "x-tt-clientinfo", f.Value
		ok = true
		return
	}

	return f, true
}
