//go:build test_unit

package daemon

import (
	"context"
	"errors"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	librespot "github.com/devgianlu/go-librespot"
	"github.com/devgianlu/go-librespot/player"
	connectpb "github.com/devgianlu/go-librespot/proto/spotify/connectstate"
	"github.com/devgianlu/go-librespot/spclient"
	"github.com/stretchr/testify/require"
)

func TestStateUpdatesDoNotBlockControlsAndStopSupersedesQueuedActiveState(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	entered := make(chan stateWrite, 4)
	release := make(chan struct{})
	w := newStateWriter(ctx, &librespot.NullLogger{}, func(ctx context.Context, job stateWrite) (*connectpb.Cluster, error) {
		entered <- job
		select {
		case <-release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		return &connectpb.Cluster{}, nil
	})
	defer func() { cancel(); <-w.done }()
	p := &AppPlayer{stateWriter: w, player: &player.Player{}, spotConnId: "connection-a",
		state: &State{active: true, device: &connectpb.DeviceInfo{Volume: 100},
			player: &connectpb.PlayerState{Track: &connectpb.ProvidedTrack{Uri: "first"}}}}
	require.NoError(t, p.putConnectState(ctx, connectpb.PutStateReason_PLAYER_STATE_CHANGED))
	first := <-entered
	// The player continues modifying its state while the network owns a snapshot.
	p.state.player.Track.Uri = "second"
	p.state.device.Volume = 20
	require.Equal(t, "first", first.request.Device.PlayerState.Track.Uri)
	require.Equal(t, uint32(100), first.request.Device.DeviceInfo.Volume)
	require.NoError(t, p.putConnectState(ctx, connectpb.PutStateReason_PLAYER_STATE_CHANGED))
	p.state.reset()
	require.NoError(t, p.putConnectState(ctx, connectpb.PutStateReason_BECAME_INACTIVE))
	close(release)
	select {
	case latest := <-entered:
		require.True(t, latest.inactive, "obsolete active state escaped after stop")
		require.False(t, latest.request.IsActive)
	case <-time.After(time.Second):
		t.Fatal("writer did not drain latest state")
	}
}

func TestStateRetryRetainsCooldownButUsesNewestSnapshot(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		var calls []string
		var callsMu sync.Mutex
		w := newStateWriter(ctx, &librespot.NullLogger{}, func(_ context.Context, job stateWrite) (*connectpb.Cluster, error) {
			callsMu.Lock()
			defer callsMu.Unlock()
			calls = append(calls, job.connectionID)
			if len(calls) == 1 {
				return nil, &spclient.RateLimitedError{RetryAfter: 10 * time.Second}
			}
			return &connectpb.Cluster{}, nil
		})
		w.submit(stateWrite{connectionID: "old"})
		synctest.Wait()
		w.submit(stateWrite{connectionID: "new"})
		time.Sleep(9 * time.Second)
		synctest.Wait()
		callsMu.Lock()
		require.Equal(t, []string{"old"}, calls)
		callsMu.Unlock()
		time.Sleep(time.Second)
		synctest.Wait()
		callsMu.Lock()
		require.Equal(t, []string{"old", "new"}, calls)
		callsMu.Unlock()
		cancel()
		<-w.done
	})
}

func TestRegistrationReportsFailureAndWriterCancelsBlockedNetwork(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	w := newStateWriter(ctx, &librespot.NullLogger{}, func(ctx context.Context, _ stateWrite) (*connectpb.Cluster, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	})
	result := make(chan stateWriteResult, 1)
	w.submit(stateWrite{reply: result})
	// Parent cancellation must release the network writer even with pending work.
	cancel()
	select {
	case <-w.done:
	case <-time.After(time.Second):
		t.Fatal("writer leaked on shutdown")
	}
	select {
	case r := <-result:
		require.True(t, errors.Is(r.err, context.Canceled))
	default: /* cancelled before dispatch */
	}
}
