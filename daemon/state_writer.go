package daemon

import (
	"context"
	"errors"
	"time"

	librespot "github.com/devgianlu/go-librespot"
	connectpb "github.com/devgianlu/go-librespot/proto/spotify/connectstate"
	"github.com/devgianlu/go-librespot/spclient"
)

type stateWrite struct {
	connectionID string
	request      *connectpb.PutStateRequest
	inactive     bool
	reply        chan stateWriteResult
}

type stateWriteResult struct {
	cluster *connectpb.Cluster
	err     error
}

// One network writer, one replaceable pending snapshot. All snapshots are owned
// by this worker; the player may continue handling commands while Spotify is
// slow. Registration still waits for its result. Inactive writes use the same
// lane, so an older active PUT can never land after a completed stop write.
type stateWriter struct {
	ctx     context.Context
	pending chan stateWrite
	done    chan struct{}
	send    func(context.Context, stateWrite) (*connectpb.Cluster, error)
	log     librespot.Logger
}

func newStateWriter(ctx context.Context, log librespot.Logger, send func(context.Context, stateWrite) (*connectpb.Cluster, error)) *stateWriter {
	w := &stateWriter{ctx: ctx, pending: make(chan stateWrite, 1), done: make(chan struct{}), send: send, log: log}
	go w.run()
	return w
}

// Called only by the player loop. A newer complete snapshot supersedes a queued
// one, never an in-flight write. The synchronous registration cannot be replaced
// because its caller is the same loop and waits for the answer.
func (w *stateWriter) submit(job stateWrite) {
	select {
	case <-w.pending:
	default:
	}
	select {
	case w.pending <- job:
	case <-w.ctx.Done():
	}
}

func (w *stateWriter) run() {
	defer close(w.done)
	for {
		var job stateWrite
		select {
		case <-w.ctx.Done():
			return
		case job = <-w.pending:
		}
		for attempt := 0; ; attempt++ {
			ctx, cancel := context.WithTimeout(w.ctx, 5*time.Second)
			cluster, err := w.send(ctx, job)
			cancel()
			if job.reply != nil {
				job.reply <- stateWriteResult{cluster, err}
				break
			}
			if err == nil {
				break
			}
			if w.ctx.Err() != nil {
				return
			}
			if attempt == 0 {
				w.log.WithError(err).Warn("connect-state update failed; retaining latest state for retry")
			}
			delay := min(time.Duration(attempt+1)*time.Second, 10*time.Second)
			var limited *spclient.RateLimitedError
			if errors.As(err, &limited) {
				delay = limited.RetryAfter
			}
			timer := time.NewTimer(delay)
		wait:
			for {
				select {
				case <-w.ctx.Done():
					timer.Stop()
					return
				case latest := <-w.pending:
					job = latest // retain cooldown, but never retry obsolete playback
				case <-timer.C:
					break wait
				}
			}
		}
	}
}
