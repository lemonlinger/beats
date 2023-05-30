package grpc

import (
	"time"

	"github.com/dgraph-io/ristretto"
	"github.com/elastic/beats/v7/libbeat/common"
	"github.com/elastic/beats/v7/packetbeat/procs"
	"github.com/elastic/beats/v7/packetbeat/protos/applayer"
	"github.com/elastic/elastic-agent-libs/logp"
)

type transactions struct {
	config *transactionConfig

	// responses map[uint32]*messageList
	requests *ristretto.Cache

	onTransaction transactionHandler

	watcher *procs.ProcessesWatcher
}

type transactionConfig struct {
	transactionTimeout time.Duration
}

type transactionHandler func(requ, resp *message) error

func (trans *transactions) init(c *transactionConfig, watcher *procs.ProcessesWatcher, cb transactionHandler) {
	trans.config = c
	trans.watcher = watcher
	trans.onTransaction = cb
	trans.requests, _ = ristretto.NewCache(&ristretto.Config{
		NumCounters: 5000,
		MaxCost:     512,
		BufferItems: 64,
	})
}

func (trans *transactions) clear() {
	trans.requests.Clear()
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

	logp.Info("cache.metrics: %s", trans.requests.Metrics.String())

	return err
}

// onRequest handles request messages, merging with incomplete requests
// and adding non-merged requests into the correlation list.
func (trans *transactions) onRequest(
	tuple *common.IPPortTuple,
	dir uint8,
	msg *message,
) error {
	msg.isComplete = msg.isCompletedRequest()
	if !msg.isComplete {
		return nil
	}
	trans.requests.SetWithTTL(msg.streamID, msg, 1, 3*time.Second)
	trans.requests.Wait()

	if isDebug {
		debugf("request message complete: %+v", *msg)
	}

	return nil
}

// onRequest handles response messages, merging with incomplete requests
// and adding non-merged responses into the correlation list.
func (trans *transactions) onResponse(
	tuple *common.IPPortTuple,
	dir uint8,
	msg *message,
) error {
	msg.isComplete = msg.isCompletedResponse()
	if !msg.isComplete {
		trans.requests.Del(msg.streamID)
		return nil
	}
	if isDebug {
		debugf("response message complete[]: %+v", *msg)
	}

	return trans.correlate(msg)
}

func (trans *transactions) correlate(resp *message) error {
	request, ok := trans.requests.Get(resp.streamID)
	if !ok {
		return nil
	}
	trans.requests.Del(resp.streamID)
	requ := request.(*message)
	if err := trans.onTransaction(requ, resp); err != nil {
		return err
	}

	return nil
}

type messageList struct {
	head, tail *message
	timeout    time.Duration
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

	ml.popExpired()

	msg := ml.head
	ml.head = ml.head.next
	if ml.head == nil {
		ml.tail = nil
	}
	return msg
}

func (ml *messageList) popExpired() {
	msg := ml.head
	for msg != nil && time.Since(msg.createdAt) >= ml.timeout {
		ml.head = msg.next
		msg = ml.head
	}
}
