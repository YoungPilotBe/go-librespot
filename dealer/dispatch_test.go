//go:build test_unit

package dealer

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	librespot "github.com/devgianlu/go-librespot"
	"github.com/stretchr/testify/require"
)

// A player busy resolving a track must not prevent the websocket reader from
// receiving Spotify's keepalive. Exercise a real socket, not a queue mock.
func TestSlowPlayerDoesNotBlockDealerPong(t *testing.T) {
	for _, kind := range []string{"message", "request"} {
		t.Run(kind, func(t *testing.T) {
			peer := make(chan *websocket.Conn, 1)
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				conn, err := websocket.Accept(w, r, nil)
				if err != nil {
					return
				}
				peer <- conn
			}))
			defer ts.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			conn, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(ts.URL, "http"), nil)
			require.NoError(t, err)
			server := <-peer
			defer server.CloseNow()
			d := NewDealer(&librespot.NullLogger{}, http.DefaultClient, nil, nil)
			d.conn = conn
			recvDone := make(chan struct{})
			defer func() {
				closed := make(chan struct{})
				go func() { d.Close(); close(closed) }()
				// Mark the dealer as shutting down before closing its peer;
				// otherwise the reader legitimately attempts reconnection.
				<-d.done
				server.CloseNow()
				<-closed
				<-recvDone
			}()
			// Register an intentionally busy consumer before starting the reader.
			d.messageReceivers = []messageReceiver{{uriPrefixes: []string{"hm://test/"}, c: make(chan Message)}}
			before := time.Now().Add(-time.Second)
			d.lastPong = before
			d.requestReceivers["test"] = requestReceiver{c: make(chan Request, 1)}
			go func() { d.recvLoop(); close(recvDone) }()
			raw := RawMessage{Type: kind, Uri: "hm://test/busy", MessageIdent: "test", Key: "request-key"}
			raw.Payload.Compressed = []byte(`{"command":{"endpoint":"play"}}`)
			wire, err := json.Marshal(raw)
			require.NoError(t, err)
			require.NoError(t, server.Write(ctx, websocket.MessageText, wire))
			require.NoError(t, server.Write(ctx, websocket.MessageText, []byte(`{"type":"pong"}`)))
			require.Eventually(t, func() bool {
				d.lastPongLock.Lock()
				defer d.lastPongLock.Unlock()
				return d.lastPong.After(before)
			}, 500*time.Millisecond, 5*time.Millisecond, "busy playback starved the Spotify keepalive reader")

		})
	}
}
