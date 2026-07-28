package ksql

import (
	"net/http"
	"sync/atomic"
	"testing"

	"github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
)

func TestPoolCloseClearsClientsAndTemplateClosesIdleOnceAndReopens(t *testing.T) {
	p := NewPool()
	childTransport := &ksqlCloseTracker{}
	templateTransport := &ksqlCloseTracker{}
	p.clients[clientKey{cluster: "local", url: "http://localhost"}] = &ksqlClient{
		http:    &http.Client{Transport: childTransport},
		authHdr: "Basic credential",
	}
	p.client = &http.Client{Transport: templateTransport}

	p.Close()
	p.Close()

	if len(p.clients) != 0 {
		t.Fatalf("cached clients = %d, want 0", len(p.clients))
	}
	if p.client != nil {
		t.Fatal("template client reference was not cleared")
	}
	if childTransport.closed.Load() != 1 || templateTransport.closed.Load() != 1 {
		t.Fatalf(
			"close counts child=%d template=%d, want 1 each",
			childTransport.closed.Load(),
			templateTransport.closed.Load(),
		)
	}

	client, err := p.clientFor(cluster.Definition{Name: "reopen", KsqlURL: "http://localhost"})
	if err != nil {
		t.Fatalf("clientFor after Close() error = %v", err)
	}
	if client == nil || len(p.clients) != 1 {
		t.Fatalf("clientFor after Close() = %p, cached=%d", client, len(p.clients))
	}
	p.Close()
}

func TestPoolReplacementClosesOldClientAfterUnlock(t *testing.T) {
	p := NewPool()
	tracker := &ksqlCloseTracker{pool: p}
	p.clients[clientKey{cluster: "local", url: "http://old.invalid"}] = &ksqlClient{
		http: &http.Client{Transport: tracker},
	}

	_, err := p.clientFor(cluster.Definition{Name: "local", KsqlURL: "http://localhost"})

	if err != nil {
		t.Fatalf("clientFor() error = %v", err)
	}
	if tracker.closed.Load() != 1 {
		t.Fatalf("old client close count = %d, want 1", tracker.closed.Load())
	}
	if !tracker.closedOutsideLock.Load() {
		t.Fatal("old client was closed while the pool mutex remained locked")
	}
	p.Close()
}

type ksqlCloseTracker struct {
	pool              *Pool
	closed            atomic.Int32
	closedOutsideLock atomic.Bool
}

func (*ksqlCloseTracker) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, http.ErrNotSupported
}

func (t *ksqlCloseTracker) CloseIdleConnections() {
	t.closed.Add(1)
	if t.pool == nil {
		return
	}
	if t.pool.mu.TryLock() {
		t.closedOutsideLock.Store(true)
		t.pool.mu.Unlock()
	}
}
