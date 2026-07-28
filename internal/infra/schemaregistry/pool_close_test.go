package schemaregistry

import (
	"crypto/tls"
	"net/http"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
)

func TestPoolUsesRetainedFiveSecondHTTPClientAndPreservesTLS(t *testing.T) {
	p := NewPool()
	def := cluster.Definition{
		Name: "tls",
		SchemaRegistry: cluster.SchemaRegistrySpec{
			URL: "https://localhost",
			SSL: &cluster.SRSSL{},
		},
	}

	_, err := p.clientFor(def)

	require.NoError(t, err)
	cached := p.clients["tls"]
	require.NotNil(t, cached.httpClient)
	require.Equal(t, 5*time.Second, cached.httpClient.Timeout)
	transport, ok := cached.httpClient.Transport.(*http.Transport)
	require.True(t, ok)
	require.NotNil(t, transport.TLSClientConfig)
	require.Equal(t, uint16(tls.VersionTLS12), transport.TLSClientConfig.MinVersion)
	p.Close()
}

func TestNewClientTruststoreFailureDoesNotCloseGlobalDefaultTransport(t *testing.T) {
	originalDefault := http.DefaultTransport
	globalTracker := &schemaCloseTracker{}
	http.DefaultTransport = globalTracker
	t.Cleanup(func() { http.DefaultTransport = originalDefault })

	client, httpClient, err := newClient(cluster.SchemaRegistrySpec{
		URL: "https://localhost",
		SSL: &cluster.SRSSL{
			TruststoreLocation: filepath.Join(t.TempDir(), "missing-truststore.pem"),
		},
	})

	require.Error(t, err)
	require.Nil(t, client)
	require.Nil(t, httpClient)
	require.Zero(t, globalTracker.closed.Load())
}

func TestSuccessfulClientPoolCloseClosesOwnedTransportNotGlobalDefault(t *testing.T) {
	originalDefault := http.DefaultTransport
	globalTracker := &schemaCloseTracker{}
	http.DefaultTransport = globalTracker
	t.Cleanup(func() { http.DefaultTransport = originalDefault })

	client, httpClient, err := newClient(cluster.SchemaRegistrySpec{URL: "http://localhost"})
	require.NoError(t, err)
	require.NotNil(t, client)
	require.NotNil(t, httpClient.Transport)
	require.NotSame(t, globalTracker, httpClient.Transport)
	ownedTracker := &schemaCloseTracker{delegate: httpClient.Transport}
	httpClient.Transport = ownedTracker
	p := NewPool()
	p.clients["successful"] = cachedClient{client: client, httpClient: httpClient}

	p.Close()

	require.Equal(t, int32(1), ownedTracker.closed.Load())
	require.Zero(t, globalTracker.closed.Load())
}

func TestPoolCloseClearsClientsSchemasAndCredentialsClosesIdleOnceAndReopens(t *testing.T) {
	p := NewPool()
	tracker := &schemaCloseTracker{}
	p.clients["local"] = cachedClient{
		httpClient: &http.Client{Transport: tracker},
		schemas: map[int]cluster.RawSchema{
			7: {Schema: "credential-marker", SchemaType: "AVRO"},
		},
		schemaOrder: []int{7},
	}

	p.Close()
	p.Close()

	require.Empty(t, p.clients)
	require.Equal(t, int32(1), tracker.closed.Load())

	client, err := p.clientFor(cluster.Definition{
		Name:           "reopen",
		SchemaRegistry: cluster.SchemaRegistrySpec{URL: "http://localhost"},
	})
	require.NoError(t, err)
	require.NotNil(t, client)
	require.Len(t, p.clients, 1)
	p.Close()
}

func TestPoolReplacementClosesOldClientAfterUnlock(t *testing.T) {
	p := NewPool()
	tracker := &schemaCloseTracker{pool: p}
	p.clients["local"] = cachedClient{
		httpClient: &http.Client{Transport: tracker},
		schemas:    map[int]cluster.RawSchema{},
	}
	def := cluster.Definition{
		Name:           "local",
		SchemaRegistry: cluster.SchemaRegistrySpec{URL: "http://localhost"},
	}

	_, err := p.clientFor(def)

	require.NoError(t, err)
	require.Equal(t, int32(1), tracker.closed.Load())
	require.True(t, tracker.closedOutsideLock.Load())
	p.Close()
}

type schemaCloseTracker struct {
	pool              *Pool
	delegate          http.RoundTripper
	closed            atomic.Int32
	closedOutsideLock atomic.Bool
}

func (t *schemaCloseTracker) RoundTrip(request *http.Request) (*http.Response, error) {
	if t.delegate != nil {
		return t.delegate.RoundTrip(request)
	}
	return nil, http.ErrNotSupported
}

func (t *schemaCloseTracker) CloseIdleConnections() {
	t.closed.Add(1)
	if closer, ok := t.delegate.(interface{ CloseIdleConnections() }); ok {
		closer.CloseIdleConnections()
	}
	if t.pool == nil {
		return
	}
	if t.pool.mu.TryLock() {
		t.closedOutsideLock.Store(true)
		t.pool.mu.Unlock()
	}
}
