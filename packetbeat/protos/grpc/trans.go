package grpc

import (
	"time"

	"github.com/elastic/beats/v7/libbeat/common"
	"github.com/elastic/beats/v7/packetbeat/procs"
	"github.com/elastic/beats/v7/packetbeat/protos/applayer"
	"github.com/elastic/elastic-agent-libs/logp"
)

type transactions struct {
	config *transactionConfig

	requests  map[uint32]*messageList
	responses map[uint32]*messageList

	onTransaction transactionHandler

	watcher *procs.ProcessesWatcher
}

type transactionConfig struct {
	transactionTimeout time.Duration
}

type transactionHandler func(requ, resp *message) error

// List of messages available for correlation
type messageList struct {
	head, tail *message
}

func (trans *transactions) init(c *transactionConfig, watcher *procs.ProcessesWatcher, cb transactionHandler) {
	trans.config = c
	trans.watcher = watcher
	trans.onTransaction = cb
	trans.requests = make(map[uint32]*messageList)
	trans.responses = make(map[uint32]*messageList)
}

func (trans *transactions) clear() {
	trans.requests = make(map[uint32]*messageList)
	trans.responses = make(map[uint32]*messageList)
}

func (trans *transactions) onMessage(
	tuple *common.IPPortTuple,
	dir uint8,
	msg *message,
) error {
	var err error

	msg.Tuple = *tuple
	msg.Transport = applayer.TransportTCP
	msg.CmdlineTuple = trans.watcher.FindProcessesTuple(&msg.Tuple, msg.Transport)

	if msg.IsRequest {
		if isDebug {
			debugf("Received request with tuple: %s", tuple)
		}
		err = trans.onRequest(tuple, dir, msg)
	} else {
		if isDebug {
			debugf("Received response with tuple: %s", tuple)
		}
		err = trans.onResponse(tuple, dir, msg)
	}

	return err
}

// onRequest handles request messages, merging with incomplete requests
// and adding non-merged requests into the correlation list.
func (trans *transactions) onRequest(
	tuple *common.IPPortTuple,
	dir uint8,
	msg *message,
) error {
	requests := trans.requests[msg.streamID]
	if requests == nil {
		requests = &messageList{}
		trans.requests[msg.streamID] = requests
	}
	prev := requests.last()
	merged, err := trans.tryMergeRequests(prev, msg)
	if err != nil {
		return err
	}

	if !msg.isComplete {
		return nil
	}

	if merged {
		if isDebug {
			debugf("request message got merged")
		}
		msg = prev
	} else {
		requests.append(msg)
	}

	if isDebug {
		debugf("request message complete: %+v", *msg)
	}

	return trans.correlate(msg.streamID)
}

// onRequest handles response messages, merging with incomplete requests
// and adding non-merged responses into the correlation list.
func (trans *transactions) onResponse(
	tuple *common.IPPortTuple,
	dir uint8,
	msg *message,
) error {
	responses := trans.responses[msg.streamID]
	if responses == nil {
		responses = &messageList{}
		trans.responses[msg.streamID] = responses
	}
	prev := responses.last()
	merged, err := trans.tryMergeResponses(prev, msg)
	if err != nil {
		return err
	}

	if !msg.isComplete {
		return nil
	}

	if merged {
		if isDebug {
			debugf("response message got merged")
		}
		msg = prev
	} else {
		responses.append(msg)
	}

	if isDebug {
		debugf("response message complete[]: %+v", *msg)
	}

	return trans.correlate(msg.streamID)
}

func (trans *transactions) tryMergeRequests(
	prev, msg *message,
) (merged bool, err error) {
	msg.isComplete = msg.isCompletedRequest()
	return false, nil
}

func (trans *transactions) tryMergeResponses(prev, msg *message) (merged bool, err error) {
	msg.isComplete = msg.isCompletedResponse()
	return false, nil
}

func (trans *transactions) correlate(streamID uint32) error {
	requests := trans.requests[streamID]
	responses := trans.responses[streamID]

	// drop responses with missing requests
	if requests == nil || requests.empty() {
		for responses != nil && !responses.empty() {
			logp.Warn("Response from unknown transaction. Ignoring.")
			responses.pop()
		}
		return nil
	}

	// merge requests with responses into transactions
	for (responses != nil && !responses.empty()) && (requests != nil && !requests.empty()) {
		resp := responses.first()
		if !resp.isComplete {
			break
		}

		requ := requests.pop()
		responses.pop()

		// Don't support grpc streaming traffic capturing, so we assume only a pair of request and response for each stream id.
		delete(trans.requests, streamID)
		delete(trans.responses, streamID)

		if err := trans.onTransaction(requ, resp); err != nil {
			return err
		}
	}

	return nil
}

func (ml *messageList) append(msg *message) {
	if ml.tail == nil {
		ml.head = msg
	} else {
		ml.tail.next = msg
	}
	msg.next = nil
	ml.tail = msg
}

func (ml *messageList) empty() bool {
	return ml.head == nil
}

func (ml *messageList) pop() *message {
	if ml.head == nil {
		return nil
	}

	msg := ml.head
	ml.head = ml.head.next
	if ml.head == nil {
		ml.tail = nil
	}
	return msg
}

func (ml *messageList) first() *message {
	return ml.head
}

func (ml *messageList) last() *message {
	return ml.tail
}
