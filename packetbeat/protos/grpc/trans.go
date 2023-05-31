package grpc

import (
	"time"

	"github.com/elastic/beats/v7/libbeat/common"
	"github.com/elastic/beats/v7/packetbeat/procs"
	"github.com/elastic/beats/v7/packetbeat/protos/applayer"
)

type transactions struct {
	config *transactionConfig

	// responses map[uint32]*messageList
	requests *common.Cache

	onTransaction transactionHandler

	watcher *procs.ProcessesWatcher
}

type transactionConfig struct {
	transactionTimeout time.Duration
}

type transactionHandler func(requ, resp *message) error

func (trans *transactions) init(c *transactionConfig, watcher *procs.ProcessesWatcher, cb transactionHandler, cache *common.Cache) {
	trans.config = c
	trans.watcher = watcher
	trans.onTransaction = cb
	trans.requests = cache
}

func (trans *transactions) clear() {
	trans.requests.CleanUp()
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
	msg.isComplete = msg.isCompletedRequest()
	if !msg.isComplete {
		return nil
	}
	trans.requests.Put(msg.key(), msg)

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
		trans.requests.Delete(msg.key())
		return nil
	}
	if isDebug {
		debugf("response message complete: %+v", *msg)
	}

	return trans.correlate(msg)
}

func (trans *transactions) correlate(resp *message) error {
	request := trans.requests.Get(resp.key())
	if request == nil {
		return nil
	}
	trans.requests.Delete(resp.key())
	requ := request.(*message)
	if err := trans.onTransaction(requ, resp); err != nil {
		return err
	}

	return nil
}
