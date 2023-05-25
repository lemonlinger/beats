package http

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"sync"
	"time"

	"github.com/elastic/beats/v7/libbeat/outputs"
	"github.com/elastic/beats/v7/libbeat/outputs/codec"
	"github.com/elastic/beats/v7/libbeat/publisher"
	"github.com/elastic/elastic-agent-libs/logp"
	commonhttp "gitlab.p1staff.com/backend/tantan-backend-common/http/client"
)

type client struct {
	log      *logp.Logger
	observer outputs.Observer
	scheme   string
	host     string
	hostType string
	urlPath  string
	method   string
	headers  map[string]string
	codec    codec.Codec

	mux    sync.Mutex
	done   chan struct{}
	client *http.Client

	wg sync.WaitGroup
}

func newHTTPClient(
	observer outputs.Observer,
	hostType string,
	method string,
	scheme string,
	host string,
	urlPath string,
	headers map[string]string,
	timeout time.Duration,
	codec codec.Codec,
) (*client, error) {
	c := &client{
		log:      logp.NewLogger(logSelector),
		observer: observer,
		scheme:   scheme,
		host:     host,
		urlPath:  urlPath,
		method:   method,
		headers:  headers,
		codec:    codec,
		done:     make(chan struct{}),
	}
	opts := []commonhttp.Option{commonhttp.WithTimeout(timeout)}
	if c.hostType == "tns" {
		opts = append(opts, commonhttp.WaitForResolved(true))
	} else {
		opts = append(opts, commonhttp.DisableServiceDiscovery(true))
	}
	cli, err := commonhttp.NewClient(c.host, opts...)
	if err != nil {
		return nil, err
	}
	c.client = cli
	return c, nil
}

func (c *client) url() string {
	if c.hostType == "tns" {
		return c.urlPath
	}
	return fmt.Sprintf("%s://%s%s", c.scheme, c.host, c.urlPath)
}

func (c *client) Connect() error {
	return nil
}

func (c *client) Close() error {
	c.client.CloseIdleConnections()
	return nil
}

func (c *client) Publish(ctx context.Context, batch publisher.Batch) error {
	events := batch.Events()
	c.observer.NewBatch(len(events))
	for i := range events {
		d := &events[i]
		data, err := c.codec.Encode("", &d.Content)
		if err != nil {
			c.log.Errorf("Dropping event: %+v", err)
			c.observer.Dropped(1)
			continue
		}

		req, err := c.newRequest(data)
		if err != nil {
			c.log.Errorf("Dropping event: %+v", err)
			c.observer.Dropped(1)
			continue
		}

		err = c.sendRequest(req)
		if err != nil {
			c.log.Errorf("Dropping event: %+v", err)
			c.observer.Failed(1)
			continue
		}
		c.observer.Acked(1)
	}
	return nil
}

func (c *client) newRequest(data []byte) (*http.Request, error) {
	buf := bytes.NewBuffer(data)
	req, err := http.NewRequest(c.method, c.url(), buf)
	if err != nil {
		return nil, fmt.Errorf("failed to new request: %w", err)
	}
	for k, v := range c.headers {
		req.Header.Set(k, v)
	}
	return req, nil
}

func (c *client) sendRequest(req *http.Request) error {
	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		return nil
	}

	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to send request[%d]: %w", resp.StatusCode, err)
	}
	return fmt.Errorf("failed to send request[%d]: %s", resp.StatusCode, string(data))
}

func (c *client) String() string {
	return fmt.Sprintf("http(%s://%s%s)", c.scheme, c.host, c.urlPath)
}
