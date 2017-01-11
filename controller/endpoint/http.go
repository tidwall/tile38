package endpoint

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"sync"
	"time"
)

const (
	httpExpiresAfter       = time.Second * 30
	httpRequestTimeout     = time.Second * 5
	httpMaxIdleConnections = 20
)

type HTTPEndpointConn struct {
	mu     sync.Mutex
	ep     Endpoint
	ex     bool
	t      time.Time
	client *http.Client
}

func newHTTPEndpointConn(ep Endpoint) *HTTPEndpointConn {
	return &HTTPEndpointConn{
		ep: ep,
		t:  time.Now(),
	}
}

func (conn *HTTPEndpointConn) Expired() bool {
	conn.mu.Lock()
	defer conn.mu.Unlock()
	if !conn.ex {
		if time.Now().Sub(conn.t) > httpExpiresAfter {
			conn.ex = true
			conn.client = nil
		}
	}
	return conn.ex
}

func (conn *HTTPEndpointConn) Send(msg string) error {
	conn.mu.Lock()
	defer conn.mu.Unlock()
	if conn.ex {
		return errors.New("expired")
	}
	conn.t = time.Now()
	if conn.client == nil {
		conn.client = &http.Client{
			Transport: &http.Transport{
				MaxIdleConnsPerHost: httpMaxIdleConnections,
			},
			Timeout: httpRequestTimeout,
		}
	}
	req, err := http.NewRequest("POST", conn.ep.Original, bytes.NewBufferString(msg))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")
	resp, err := conn.client.Do(req)
	if err != nil {
		return err
	}
	// close the connection to reuse it
	defer resp.Body.Close()
	// discard response
	if _, err := io.Copy(ioutil.Discard, resp.Body); err != nil {
		return err
	}
	// we only care about the 200 response
	if resp.StatusCode != 200 {
		return fmt.Errorf("invalid status: %s", resp.Status)
	}
	return nil
}
