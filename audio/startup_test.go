//go:build test_unit

package audio

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	librespot "github.com/devgianlu/go-librespot"
	"github.com/stretchr/testify/require"
)

func TestStartupCancellationInterruptsHeadersAndBody(t *testing.T) {
	for _, body := range []bool{false, true} {
		t.Run(map[bool]string{false: "headers", true: "body"}[body], func(t *testing.T) {
			entered := make(chan struct{})
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if body {
					w.Header().Set("Content-Range", "bytes 0-0/1")
					w.WriteHeader(206)
					w.(http.Flusher).Flush()
				}
				close(entered)
				<-r.Context().Done()
			}))
			defer ts.Close()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			done := make(chan error, 1)
			go func() {
				_, err := NewHttpChunkedReaderContext(ctx, &librespot.NullLogger{}, ts.Client(), ts.URL)
				done <- err
			}()
			<-entered
			cancel()
			select {
			case err := <-done:
				require.Error(t, err)
			case <-time.After(time.Second):
				t.Fatal("cancelled track load still owns the command loop")
			}
		})
	}
}

func TestCompletedStartupDoesNotTiePlaybackToCommandContext(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Range", "bytes 0-0/1")
		w.WriteHeader(206)
		_, _ = w.Write([]byte{42})
	}))
	defer ts.Close()
	ctx, cancel := context.WithCancel(t.Context())
	r, err := NewHttpChunkedReaderContext(ctx, &librespot.NullLogger{}, ts.Client(), ts.URL)
	require.NoError(t, err)
	defer r.Close()
	cancel()
	require.False(t, r.isClosed(), "returning from play cancelled a live audio stream")
	b := make([]byte, 1)
	n, err := r.ReadAt(b, 0)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	require.Equal(t, byte(42), b[0])
}
