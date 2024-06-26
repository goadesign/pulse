// Code generated by goa v3.16.1, DO NOT EDIT.
//
// Poller WebSocket server streaming
//
// Command:
// $ goa gen goa.design/pulse/examples/weather/services/poller/design -o
// services/poller

package server

import (
	"context"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	goahttp "goa.design/goa/v3/http"
	poller "goa.design/pulse/examples/weather/services/poller/gen/poller"
)

// ConnConfigurer holds the websocket connection configurer functions for the
// streaming endpoints in "Poller" service.
type ConnConfigurer struct {
	SubscribeFn goahttp.ConnConfigureFunc
}

// SubscribeServerStream implements the poller.SubscribeServerStream interface.
type SubscribeServerStream struct {
	once sync.Once
	// upgrader is the websocket connection upgrader.
	upgrader goahttp.Upgrader
	// configurer is the websocket connection configurer.
	configurer goahttp.ConnConfigureFunc
	// cancel is the context cancellation function which cancels the request
	// context when invoked.
	cancel context.CancelFunc
	// w is the HTTP response writer used in upgrading the connection.
	w http.ResponseWriter
	// r is the HTTP request.
	r *http.Request
	// conn is the underlying websocket connection.
	conn *websocket.Conn
}

// NewConnConfigurer initializes the websocket connection configurer function
// with fn for all the streaming endpoints in "Poller" service.
func NewConnConfigurer(fn goahttp.ConnConfigureFunc) *ConnConfigurer {
	return &ConnConfigurer{
		SubscribeFn: fn,
	}
}

// Send streams instances of "poller.Forecast" to the "subscribe" endpoint
// websocket connection.
func (s *SubscribeServerStream) Send(v *poller.Forecast) error {
	var err error
	// Upgrade the HTTP connection to a websocket connection only once. Connection
	// upgrade is done here so that authorization logic in the endpoint is executed
	// before calling the actual service method which may call Send().
	s.once.Do(func() {
		var conn *websocket.Conn
		conn, err = s.upgrader.Upgrade(s.w, s.r, nil)
		if err != nil {
			return
		}
		if s.configurer != nil {
			conn = s.configurer(conn, s.cancel)
		}
		s.conn = conn
	})
	if err != nil {
		return err
	}
	res := v
	body := NewSubscribeResponseBody(res)
	return s.conn.WriteJSON(body)
}

// Close closes the "subscribe" endpoint websocket connection.
func (s *SubscribeServerStream) Close() error {
	var err error
	if s.conn == nil {
		return nil
	}
	if err = s.conn.WriteControl(
		websocket.CloseMessage,
		websocket.FormatCloseMessage(websocket.CloseNormalClosure, "server closing connection"),
		time.Now().Add(time.Second),
	); err != nil {
		return err
	}
	return s.conn.Close()
}
