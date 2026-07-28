package connect

import (
	"net/http"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
)

func TestPoolCloseClearsClientsClosesIdleOnceAndReopens(t *testing.T) {
	p := NewPool()
	firstTransport := &connectCloseTracker{}
	secondTransport := &connectCloseTracker{}
	p.clients[poolKey{cluster: "a", connect: "one"}] = cachedConnectClient{
		client: &connectClient{
			http:    &http.Client{Transport: firstTransport},
			baseURL: "http://first.invalid",
			authHdr: "Basic credential-one",
		},
	}
	p.clients[poolKey{cluster: "b", connect: "two"}] = cachedConnectClient{
		client: &connectClient{
			http:    &http.Client{Transport: secondTransport},
			baseURL: "http://second.invalid",
			authHdr: "Basic credential-two",
		},
	}

	p.Close()
	p.Close()

	require.Empty(t, p.clients)
	require.Equal(t, int32(1), firstTransport.closed.Load())
	require.Equal(t, int32(1), secondTransport.closed.Load())

	def := cluster.Definition{
		Name: "reopen",
		Connects: []cluster.ConnectSpec{{
			Name:    "main",
			Address: "http://localhost",
		}},
	}
	client, err := p.clientFor(def, "main")
	require.NoError(t, err)
	require.NotNil(t, client)
	require.Len(t, p.clients, 1)
	p.Close()
}

func TestPoolReplacementClosesOldClientAfterUnlock(t *testing.T) {
	p := NewPool()
	tracker := &connectCloseTracker{pool: p}
	key := poolKey{cluster: "local", connect: "main"}
	p.clients[key] = cachedConnectClient{
		client: &connectClient{http: &http.Client{Transport: tracker}},
	}
	def := cluster.Definition{
		Name: "local",
		Connects: []cluster.ConnectSpec{{
			Name:    "main",
			Address: "http://localhost",
		}},
	}

	_, err := p.clientFor(def, "main")

	require.NoError(t, err)
	require.Equal(t, int32(1), tracker.closed.Load())
	require.True(t, tracker.closedOutsideLock.Load())
	p.Close()
}

type connectCloseTracker struct {
	pool              *Pool
	closed            atomic.Int32
	closedOutsideLock atomic.Bool
}

func (*connectCloseTracker) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, http.ErrNotSupported
}

func (t *connectCloseTracker) CloseIdleConnections() {
	t.closed.Add(1)
	if t.pool == nil {
		return
	}
	if t.pool.mu.TryLock() {
		t.closedOutsideLock.Store(true)
		t.pool.mu.Unlock()
	}
}
