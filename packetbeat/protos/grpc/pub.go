package grpc

import (
	"github.com/elastic/beats/v7/libbeat/beat"
	"github.com/elastic/beats/v7/libbeat/common"
	"github.com/elastic/beats/v7/packetbeat/protos"
	"github.com/elastic/elastic-agent-libs/mapstr"
)

// Transaction Publisher.
type transPub struct {
	sendRequest  bool
	sendResponse bool

	results protos.Reporter
}

func (pub *transPub) onTransaction(requ, resp *message) error {
	if pub.results == nil {
		return nil
	}

	pub.results(pub.createEvent(requ, resp))
	return nil
}

func (pub *transPub) createEvent(requ, resp *message) beat.Event {
	status := common.OK_STATUS

	// resp_time in milliseconds
	responseTime := int32(resp.Ts.Sub(requ.Ts).Nanoseconds() / 1e6)

	src := &common.Endpoint{
		IP:      requ.tuple.SrcIP.String(),
		Port:    requ.tuple.SrcPort,
		Process: requ.CmdlineTuple.Src,
	}
	dst := &common.Endpoint{
		IP:      requ.tuple.DstIP.String(),
		Port:    requ.tuple.DstPort,
		Process: requ.CmdlineTuple.Dst,
	}

	fields := mapstr.M{
		"type":         "grpc",
		"status":       status,
		"responsetime": responseTime,
		"bytes_in":     requ.Size,
		"bytes_out":    resp.Size,
		"source":       src,
		"destination":  dst,
	}

	// add processing notes/errors to event
	if len(requ.Notes)+len(resp.Notes) > 0 {
		fields["notes"] = append(requ.Notes, resp.Notes...)
	}

	grpcFields := mapstr.M{
		"stream_id": requ.streamID,
	}
	if pub.sendRequest {
		grpcFields["request"] = mapstr.M{
			"method":  requ.method,
			"path":    requ.path,
			"headers": requ.headers,
			"body": mapstr.M{
				"content": common.NetString(requ.rawBody),
				"bytes":   len(requ.rawBody),
			},
			"partial_header": requ.headerPartiallyParse,
			"guessed_path":   resp.pathGuessed,
		}
	}
	if pub.sendResponse {
		grpcFields["response"] = mapstr.M{
			"status":  resp.status,
			"path":    resp.path,
			"headers": resp.headers,
			"body": mapstr.M{
				"content": common.NetString(resp.rawBody),
				"bytes":   len(resp.rawBody),
			},
			"partial_header": resp.headerPartiallyParse,
			"guessed_path":   resp.pathGuessed,
		}
	}
	fields["grpc"] = grpcFields

	return beat.Event{
		Timestamp: requ.Ts,
		Fields:    fields,
	}
}
