// Package connect implements cluster.KafkaConnectPort over a hand-written
// net/http client against the Kafka Connect REST API (P2b Task 2). Unlike
// P2a's Schema Registry client (franz-go's pkg/sr), there is no maintained Go
// client for Kafka Connect's REST API worth taking a dependency on (P2b-D1)
// -- Connect's REST surface is plain JSON-over-HTTP, so a small hand-rolled
// client is the pragmatic choice.
package connect

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
	"github.com/cy-kaf/cy-kaf-client/internal/infra/kafka"
)

// ErrUnknownConnect is returned when a (Definition, connectName) pair names
// no cluster.ConnectSpec in Definition.Connects.
var ErrUnknownConnect = errors.New("unknown connect")

const maxConnectResponseBytes = 8 << 20

var ErrConnectResponseTooLarge = errors.New("connect response exceeds size limit")

// connectClient is one ConnectSpec's resolved *http.Client plus the bits of
// per-request state derived from that spec once (base URL, an already-
// b64-encoded Basic auth header) rather than recomputed on every call.
type connectClient struct {
	http    *http.Client
	baseURL string
	authHdr string // "" = no basic auth configured for this Connect
}

// poolKey is a Pool cache entry's key: a client is specific to one cluster's
// one named Connect worker, mirroring infra/schemaregistry's Pool (keyed by
// cluster name alone -- there is only ever one Schema Registry per cluster,
// whereas a cluster can configure several Connect workers, hence the extra
// key component here).
type poolKey struct {
	cluster string
	connect string
}

// Pool caches one connectClient per (cluster name, Connect name) pair.
// clientFor hashes the current Connect spec and replaces a stale entry after
// dynamic configuration changes; Close releases every retained HTTP client.
type Pool struct {
	mu      sync.Mutex
	clients map[poolKey]cachedConnectClient
}

type cachedConnectClient struct {
	identity [sha256.Size]byte
	client   *connectClient
}

// NewPool builds an empty Pool.
func NewPool() *Pool {
	return &Pool{clients: map[poolKey]cachedConnectClient{}}
}

var _ cluster.KafkaConnectPort = (*Pool)(nil)

// clientFor resolves (and caches) the connectClient for def's Connect named
// connectName. connectName not matching any def.Connects[].Name is
// ErrUnknownConnect.
func (p *Pool) clientFor(def cluster.Definition, connectName string) (*connectClient, error) {
	key := poolKey{cluster: def.Name, connect: connectName}
	spec, ok := specFor(def, connectName)
	if !ok {
		return nil, fmt.Errorf("connect client for cluster %q: %w: %q", def.Name, ErrUnknownConnect, connectName)
	}
	identity := connectIdentity(spec)
	p.mu.Lock()
	if cached, ok := p.clients[key]; ok && cached.identity == identity {
		p.mu.Unlock()
		return cached.client, nil
	}
	cl, err := newConnectClient(spec)
	if err != nil {
		p.mu.Unlock()
		return nil, fmt.Errorf("connect client for cluster %q connect %q: %w", def.Name, connectName, err)
	}
	var oldHTTP *http.Client
	if old, ok := p.clients[key]; ok && old.client != nil && old.client.http != nil {
		oldHTTP = old.client.http
	}
	p.clients[key] = cachedConnectClient{identity: identity, client: cl}
	p.mu.Unlock()
	if oldHTTP != nil {
		oldHTTP.CloseIdleConnections()
	}
	return cl, nil
}

// Close atomically detaches all cached clients, then closes their idle
// connections outside the pool mutex. A later call may lazily repopulate the
// pool, matching the Kafka pool's reopen behavior.
func (p *Pool) Close() {
	p.mu.Lock()
	closing := make([]*http.Client, 0, len(p.clients))
	for _, cached := range p.clients {
		if cached.client != nil && cached.client.http != nil {
			closing = append(closing, cached.client.http)
		}
	}
	p.clients = make(map[poolKey]cachedConnectClient)
	p.mu.Unlock()

	for _, client := range closing {
		client.CloseIdleConnections()
	}
}

// connectIdentity hashes every setting that influences requests. This rotates
// a cached client after dynamic configuration changes while keeping plaintext
// credentials out of the cache key.
func connectIdentity(spec cluster.ConnectSpec) [sha256.Size]byte {
	raw, _ := json.Marshal(spec)
	return sha256.Sum256(raw)
}

func specFor(def cluster.Definition, connectName string) (cluster.ConnectSpec, bool) {
	for _, s := range def.Connects {
		if s.Name == connectName {
			return s, true
		}
	}
	return cluster.ConnectSpec{}, false
}

func newConnectClient(spec cluster.ConnectSpec) (*connectClient, error) {
	transport := &http.Transport{}
	if spec.SSL != nil {
		tlsCfg, err := tlsConfigFor(*spec.SSL)
		if err != nil {
			return nil, err
		}
		transport.TLSClientConfig = tlsCfg
	}
	cl := &connectClient{
		http:    &http.Client{Transport: transport},
		baseURL: strings.TrimSuffix(spec.Address, "/"),
	}
	if spec.Auth != nil {
		token := base64.StdEncoding.EncodeToString([]byte(spec.Auth.Username + ":" + spec.Auth.Password))
		cl.authHdr = "Basic " + token
	}
	return cl, nil
}

// tlsConfigFor builds a *tls.Config from a Connect connection's custom TLS
// material. Only Truststore (verifying the Connect worker's own certificate)
// is implemented, reusing infra/kafka's exported LoadTruststore (PEM/JKS ->
// x509.CertPool) -- this mirrors infra/schemaregistry's own tlsConfigFor,
// which likewise never consumes its analogous SSL.Keystore* fields for
// mutual TLS client certs (P2b design spec's explicit out-of-scope call).
func tlsConfigFor(ssl cluster.ConnectSSL) (*tls.Config, error) {
	cfg := &tls.Config{MinVersion: tls.VersionTLS12}
	if ssl.TruststoreLocation == "" {
		return cfg, nil
	}
	pool, err := kafka.LoadTruststore(ssl.TruststoreLocation, ssl.TruststorePassword)
	if err != nil {
		return nil, err
	}
	cfg.RootCAs = pool
	return cfg, nil
}

// statusError is do's non-2xx wrapper. It carries the HTTP status code
// separately (not just interpolated into the error string) so callers can
// programmatically distinguish e.g. 404 from other failures via
// isStatusNotFound -- withStatus's create/update-triggered retry needs
// exactly that, without resorting to parsing the error text.
type statusError struct {
	method, path string
	code         int
	body         string
}

func (e *statusError) Error() string {
	return fmt.Sprintf("connect %s %s: status %d: %s", e.method, e.path, e.code, e.body)
}

func isStatusNotFound(err error) bool {
	var se *statusError
	return errors.As(err, &se) && se.code == http.StatusNotFound
}

// isRebalanceInProgress recognizes Kafka Connect's own "please retry, my
// worker group is mid-rebalance" signal. This isn't limited to a brief
// window right after startup -- a rebalance is triggered by any connector
// create/delete/reconfigure on the group, so any REST call can race one in
// ordinary use. Verified live against a single-worker
// confluentinc/cp-kafka-connect:7.8.0 container while writing
// client_integration_test.go: a GET .../config landed while the group was
// still settling after the immediately preceding CreateConnector and got
// `{"error_code":500,"message":"Request cannot be completed because a
// rebalance is expected"}`, self-resolving under a second later -- a real
// client must retry this specific condition rather than surface it as a
// hard failure, or callers would flake on ordinary sequences of calls.
func isRebalanceInProgress(err error) bool {
	var se *statusError
	return errors.As(err, &se) && strings.Contains(se.body, "rebalance")
}

// pollAttempts/pollInterval bound the short retries this client performs
// against the two distinct, well-known Kafka Connect REST transient
// conditions documented above do (isRebalanceInProgress) and on withStatus
// (its own 404-right-after-create race): both self-resolve within a couple
// of seconds on a healthy worker, so a fixed short bounded retry -- rather
// than either failing immediately or retrying forever -- is the right
// trade-off.
const (
	pollAttempts = 10
	pollInterval = 300 * time.Millisecond
)

// do issues one Connect REST call, transparently retrying doOnce a bounded
// number of times on isRebalanceInProgress before giving up -- every other
// error (including a non-retried non-2xx status) is returned immediately
// from the first attempt.
func (c *connectClient) do(ctx context.Context, method, path string, body, out any) error {
	var err error
	for attempt := 0; attempt < pollAttempts; attempt++ {
		err = c.doOnce(ctx, method, path, body, out)
		if err == nil || !isRebalanceInProgress(err) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(pollInterval):
		}
	}
	return err
}

// doOnce issues one Connect REST call attempt. body, if non-nil, is
// JSON-encoded as the request body; out, if non-nil, is JSON-decoded from a
// non-empty response body into it. A non-2xx status is wrapped into an
// error carrying the status code and a body excerpt -- callers never see a
// bare "request failed", always at least status + a diagnostic snippet of
// what Connect said.
func (c *connectClient) doOnce(ctx context.Context, method, path string, body, out any) error {
	var reqBody io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("connect %s %s: encode request body: %w", method, path, err)
		}
		reqBody = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, reqBody)
	if err != nil {
		return fmt.Errorf("connect %s %s: %w", method, path, err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("Accept", "application/json")
	if c.authHdr != "" {
		req.Header.Set("Authorization", c.authHdr)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("connect %s %s: %w", method, path, err)
	}
	defer func() { _ = resp.Body.Close() }()
	respBody, err := readConnectResponse(ctx, resp.Body)
	if err != nil {
		if errors.Is(err, ErrConnectResponseTooLarge) {
			return ErrConnectResponseTooLarge
		}
		return fmt.Errorf("connect %s %s: read response: %w", method, path, err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return &statusError{method: method, path: path, code: resp.StatusCode, body: excerpt(respBody)}
	}
	if out == nil || len(respBody) == 0 {
		return nil
	}
	if err := json.Unmarshal(respBody, out); err != nil {
		return fmt.Errorf("connect %s %s: decode response: %w", method, path, err)
	}
	return nil
}

func readConnectResponse(ctx context.Context, body io.Reader) ([]byte, error) {
	if ctx == nil {
		return nil, context.Canceled
	}
	response, err := io.ReadAll(io.LimitReader(body, maxConnectResponseBytes+1))
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(response) > maxConnectResponseBytes {
		return nil, ErrConnectResponseTooLarge
	}
	return response, nil
}

// excerpt caps a Connect error response body so wrapped errors stay a
// sensible size regardless of how verbose Connect's own error payload is.
func excerpt(b []byte) string {
	const max = 500
	s := string(b)
	if len(s) > max {
		return s[:max] + "..."
	}
	return s
}

// --- Connect REST API wire shapes (unexported; see this file's method
// bodies for which endpoint produces/consumes each one). Field names/casing
// match the Connect REST API's own JSON exactly (snake_case in several
// places, e.g. worker_id/error_count/default_value) -- verified against a
// live confluentinc/cp-kafka-connect:7.8.0 container's actual responses
// while implementing this file, not guessed from documentation alone. ---

type connectorInfoWire struct {
	Name   string         `json:"name"`
	Config map[string]any `json:"config"`
	Tasks  []taskIDWire   `json:"tasks"`
	Type   string         `json:"type"`
}

type taskIDWire struct {
	Connector string `json:"connector"`
	Task      int    `json:"task"`
}

type connectorStatusWire struct {
	Name      string             `json:"name"`
	Connector connectorStateWire `json:"connector"`
	Tasks     []taskStatusWire   `json:"tasks"`
	Type      string             `json:"type"`
}

type connectorStateWire struct {
	State    string `json:"state"`
	WorkerID string `json:"worker_id"`
	Trace    string `json:"trace,omitempty"`
}

type taskStatusWire struct {
	ID       int    `json:"id"`
	State    string `json:"state"`
	WorkerID string `json:"worker_id"`
	Trace    string `json:"trace,omitempty"`
}

type taskInfoWire struct {
	ID     taskIDWire     `json:"id"`
	Config map[string]any `json:"config"`
}

type pluginWire struct {
	Class   string `json:"class"`
	Type    string `json:"type"`
	Version string `json:"version"`
}

type createConnectorRequest struct {
	Name   string         `json:"name"`
	Config map[string]any `json:"config"`
}

type validateResponseWire struct {
	Name       string             `json:"name"`
	ErrorCount int                `json:"error_count"`
	Groups     []string           `json:"groups"`
	Configs    []pluginConfigWire `json:"configs"`
}

type pluginConfigWire struct {
	Definition pluginConfigDefWire   `json:"definition"`
	Value      pluginConfigValueWire `json:"value"`
}

type pluginConfigDefWire struct {
	Name          string   `json:"name"`
	Type          string   `json:"type"`
	Required      bool     `json:"required"`
	DefaultValue  *string  `json:"default_value"`
	Importance    string   `json:"importance"`
	Documentation string   `json:"documentation"`
	Group         string   `json:"group"`
	Width         string   `json:"width"`
	DisplayName   string   `json:"display_name"`
	Order         int      `json:"order"`
	Dependents    []string `json:"dependents"`
}

type pluginConfigValueWire struct {
	Name              string   `json:"name"`
	Value             *string  `json:"value"`
	RecommendedValues []string `json:"recommended_values"`
	Errors            []string `json:"errors"`
	Visible           bool     `json:"visible"`
}

// --- KafkaConnectPort ---

// Connects probes every def.Connects entry with a cheap GET /connectors call
// and reports the ones that answered -- P2b-D3: a Connect whose client
// resolution or REST probe fails is skipped (best-effort, logged), never
// failing the whole call or appearing as some "offline" placeholder entry.
func (p *Pool) Connects(ctx context.Context, def cluster.Definition) ([]cluster.ConnectCluster, error) {
	out := make([]cluster.ConnectCluster, 0, len(def.Connects))
	for _, spec := range def.Connects {
		cl, err := p.clientFor(def, spec.Name)
		if err != nil {
			slog.Debug("connect client resolution failed, skipping", "cluster", def.Name, "connect", spec.Name, "err", err)
			continue
		}
		var names []string
		if err := cl.do(ctx, http.MethodGet, "/connectors", nil, &names); err != nil {
			slog.Debug("connect unreachable, skipping", "cluster", def.Name, "connect", spec.Name, "err", err)
			continue
		}
		out = append(out, cluster.ConnectCluster{Name: spec.Name, Address: spec.Address})
	}
	return out, nil
}

// Plugins lists connectName's available connector plugin classes.
func (p *Pool) Plugins(ctx context.Context, def cluster.Definition, connectName string) ([]cluster.ConnectorPlugin, error) {
	cl, err := p.clientFor(def, connectName)
	if err != nil {
		return nil, err
	}
	var wire []pluginWire
	if err := cl.do(ctx, http.MethodGet, "/connector-plugins", nil, &wire); err != nil {
		return nil, err
	}
	out := make([]cluster.ConnectorPlugin, len(wire))
	for i, w := range wire {
		out[i] = cluster.ConnectorPlugin{Class: w.Class}
	}
	return out, nil
}

// ValidatePlugin dry-runs cfg against pluginName's config definitions on
// connectName (PUT /connector-plugins/{plugin}/config/validate -- the
// request body is cfg's raw key/value map, not wrapped, matching Connect's
// own PUT-config convention).
func (p *Pool) ValidatePlugin(ctx context.Context, def cluster.Definition, connectName, pluginName string, cfg map[string]any) (cluster.PluginValidation, error) {
	cl, err := p.clientFor(def, connectName)
	if err != nil {
		return cluster.PluginValidation{}, err
	}
	var wire validateResponseWire
	path := "/connector-plugins/" + url.PathEscape(pluginName) + "/config/validate"
	if err := cl.do(ctx, http.MethodPut, path, cfg, &wire); err != nil {
		return cluster.PluginValidation{}, err
	}
	return toPluginValidation(wire), nil
}

// connectorExpandWire is one entry of GET /connectors?expand=status&expand=info's
// response map (keyed by connector name; see AllConnectors) -- KIP-465, since
// Kafka 2.3.0. Its two fields reuse connectorStatusWire/connectorInfoWire
// verbatim: the "status" sub-object is byte-for-byte the same shape GET
// /connectors/{name}/status returns, and "info" is the same shape GET
// /connectors/{name} returns -- Connect's bulk endpoint is just those two
// per-connector responses nested under one map entry per connector, not a
// distinct wire shape of its own.
type connectorExpandWire struct {
	Status connectorStatusWire `json:"status"`
	Info   connectorInfoWire   `json:"info"`
}

// AllConnectors lists every connector across every def.Connects entry, tagged
// with which Connect it came from, its real status (state/workerId), type,
// and task counts. Like Connects, an unreachable Connect is skipped rather
// than failing the whole call (P2b-D3). This calls Kafka Connect's bulk
// GET /connectors?expand=status&expand=info (KIP-465) exactly once per
// configured Connect worker -- the same call count as a plain GET
// /connectors -- so getting real per-connector status here is NOT an N+1
// fan-out over every connector found; it's cheaper than fetching each
// connector's full Connector detail individually (which is what Connector
// still does, via three separate calls, for the fields this bulk endpoint
// doesn't carry: full Config, per-task IDs, Topics).
func (p *Pool) AllConnectors(ctx context.Context, def cluster.Definition) ([]cluster.ConnectorRef, error) {
	var out []cluster.ConnectorRef
	for _, spec := range def.Connects {
		cl, err := p.clientFor(def, spec.Name)
		if err != nil {
			slog.Debug("connect client resolution failed, skipping", "cluster", def.Name, "connect", spec.Name, "err", err)
			continue
		}
		var wire map[string]connectorExpandWire
		if err := cl.do(ctx, http.MethodGet, "/connectors?expand=status&expand=info", nil, &wire); err != nil {
			slog.Debug("connect unreachable, skipping", "cluster", def.Name, "connect", spec.Name, "err", err)
			continue
		}
		names := make([]string, 0, len(wire))
		for n := range wire {
			names = append(names, n)
		}
		sort.Strings(names) // map iteration order is random; sort for a deterministic, stable result
		for _, n := range names {
			out = append(out, toConnectorRef(spec.Name, n, wire[n]))
		}
	}
	return out, nil
}

// toConnectorRef assembles one AllConnectors entry from its bulk-expand wire
// shape: FailedTasksCount is derived by counting status.tasks entries whose
// state is FAILED (Connect's bulk response has no ready-made counter of its
// own).
func toConnectorRef(connectName, name string, w connectorExpandWire) cluster.ConnectorRef {
	failed := 0
	for _, ts := range w.Status.Tasks {
		if ts.State == "FAILED" {
			failed++
		}
	}
	return cluster.ConnectorRef{
		ConnectName:      connectName,
		Name:             name,
		Type:             w.Info.Type,
		State:            w.Status.Connector.State,
		WorkerID:         w.Status.Connector.WorkerID,
		TasksCount:       len(w.Status.Tasks),
		FailedTasksCount: failed,
	}
}

// Connectors lists connectName's connector names.
func (p *Pool) Connectors(ctx context.Context, def cluster.Definition, connectName string) ([]string, error) {
	cl, err := p.clientFor(def, connectName)
	if err != nil {
		return nil, err
	}
	var names []string
	if err := cl.do(ctx, http.MethodGet, "/connectors", nil, &names); err != nil {
		return nil, err
	}
	return names, nil
}

// Connector fetches name's full assembled detail on connectName: GET
// /connectors/{name} for name/type/config/task-ids, then withStatus below
// for state/trace/workerId (the Connect REST API has no single response
// carrying both).
func (p *Pool) Connector(ctx context.Context, def cluster.Definition, connectName, name string) (cluster.Connector, error) {
	cl, err := p.clientFor(def, connectName)
	if err != nil {
		return cluster.Connector{}, err
	}
	var info connectorInfoWire
	if err := cl.do(ctx, http.MethodGet, "/connectors/"+url.PathEscape(name), nil, &info); err != nil {
		return cluster.Connector{}, err
	}
	return p.withStatus(ctx, cl, connectName, info)
}

// withStatus completes an already-fetched connectorInfoWire (from GET
// /connectors/{name}, or from a create/update call whose response is the
// same shape) with a GET /connectors/{name}/status call, assembling the
// domain Connector.
//
// Immediately after CreateConnector/SetConnectorConfig, Connect's own
// status endpoint can 404 for a short window: it's backed by an
// internally-consumed Kafka status topic, which lags the REST call that
// triggered it by up to a second or two on a freshly (re)started worker --
// observed live while writing client_integration_test.go's soul test
// (CreateConnector immediately followed by GET status flaked with "No
// status found for connector" against a real container, not a guess). A
// 404 here is therefore retried (reusing do's own pollAttempts/pollInterval)
// before giving up; any other status/error is not retried here (do's own
// wrapping already retries the separate isRebalanceInProgress condition on
// every call, including this one).
func (p *Pool) withStatus(ctx context.Context, cl *connectClient, connectName string, info connectorInfoWire) (cluster.Connector, error) {
	statusPath := "/connectors/" + url.PathEscape(info.Name) + "/status"
	var status connectorStatusWire
	var err error
	for attempt := 0; attempt < pollAttempts; attempt++ {
		err = cl.do(ctx, http.MethodGet, statusPath, nil, &status)
		if err == nil || !isStatusNotFound(err) {
			break
		}
		select {
		case <-ctx.Done():
			return cluster.Connector{}, ctx.Err()
		case <-time.After(pollInterval):
		}
	}
	if err != nil {
		return cluster.Connector{}, err
	}
	return toConnector(connectName, info, status), nil
}

func toConnector(connectName string, info connectorInfoWire, status connectorStatusWire) cluster.Connector {
	ids := make([]int, len(info.Tasks))
	for i, t := range info.Tasks {
		ids[i] = t.Task
	}
	return cluster.Connector{
		Name:        info.Name,
		ConnectName: connectName,
		Type:        info.Type,
		State:       status.Connector.State,
		Trace:       status.Connector.Trace,
		WorkerID:    status.Connector.WorkerID,
		Config:      info.Config,
		TaskIDs:     ids,
	}
}

// ConnectorConfig fetches name's current config on connectName.
func (p *Pool) ConnectorConfig(ctx context.Context, def cluster.Definition, connectName, name string) (map[string]any, error) {
	cl, err := p.clientFor(def, connectName)
	if err != nil {
		return nil, err
	}
	var cfg map[string]any
	if err := cl.do(ctx, http.MethodGet, "/connectors/"+url.PathEscape(name)+"/config", nil, &cfg); err != nil {
		return nil, err
	}
	return cfg, nil
}

// ConnectorTasks fetches name's tasks on connectName, assembled from GET
// /connectors/{name}/tasks (id + config) and GET /connectors/{name}/status
// (state/trace/workerId per task id) -- again no single Connect REST
// response carries both.
func (p *Pool) ConnectorTasks(ctx context.Context, def cluster.Definition, connectName, name string) ([]cluster.ConnectorTask, error) {
	cl, err := p.clientFor(def, connectName)
	if err != nil {
		return nil, err
	}
	var tasks []taskInfoWire
	if err := cl.do(ctx, http.MethodGet, "/connectors/"+url.PathEscape(name)+"/tasks", nil, &tasks); err != nil {
		return nil, err
	}
	var status connectorStatusWire
	if err := cl.do(ctx, http.MethodGet, "/connectors/"+url.PathEscape(name)+"/status", nil, &status); err != nil {
		return nil, err
	}
	statusByID := make(map[int]taskStatusWire, len(status.Tasks))
	for _, s := range status.Tasks {
		statusByID[s.ID] = s
	}
	out := make([]cluster.ConnectorTask, len(tasks))
	for i, t := range tasks {
		s := statusByID[t.ID.Task]
		out[i] = cluster.ConnectorTask{
			ID:       t.ID.Task,
			State:    s.State,
			Trace:    s.Trace,
			WorkerID: s.WorkerID,
			Config:   t.Config,
		}
	}
	return out, nil
}

// CreateConnector creates a new connector named name on connectName with
// cfg (POST /connectors, request body {name, config} -- unlike the PUT
// config endpoints, creation IS wrapped), returning its assembled detail.
func (p *Pool) CreateConnector(ctx context.Context, def cluster.Definition, connectName string, name string, cfg map[string]any) (cluster.Connector, error) {
	cl, err := p.clientFor(def, connectName)
	if err != nil {
		return cluster.Connector{}, err
	}
	var info connectorInfoWire
	if err := cl.do(ctx, http.MethodPost, "/connectors", createConnectorRequest{Name: name, Config: cfg}, &info); err != nil {
		return cluster.Connector{}, err
	}
	return p.withStatus(ctx, cl, connectName, info)
}

// DeleteConnector deletes name from connectName.
func (p *Pool) DeleteConnector(ctx context.Context, def cluster.Definition, connectName, name string) error {
	cl, err := p.clientFor(def, connectName)
	if err != nil {
		return err
	}
	return cl.do(ctx, http.MethodDelete, "/connectors/"+url.PathEscape(name), nil, nil)
}

// SetConnectorConfig replaces name's whole config on connectName (PUT
// /connectors/{name}/config, request body is cfg's raw map, not wrapped),
// returning its assembled detail.
func (p *Pool) SetConnectorConfig(ctx context.Context, def cluster.Definition, connectName, name string, cfg map[string]any) (cluster.Connector, error) {
	cl, err := p.clientFor(def, connectName)
	if err != nil {
		return cluster.Connector{}, err
	}
	var info connectorInfoWire
	if err := cl.do(ctx, http.MethodPut, "/connectors/"+url.PathEscape(name)+"/config", cfg, &info); err != nil {
		return cluster.Connector{}, err
	}
	return p.withStatus(ctx, cl, connectName, info)
}

// UpdateConnectorState applies action to name on connectName, per P2b-D4's
// ConnectorAction -> REST mapping (connectorActionRequest below).
func (p *Pool) UpdateConnectorState(ctx context.Context, def cluster.Definition, connectName, name, action string) error {
	cl, err := p.clientFor(def, connectName)
	if err != nil {
		return err
	}
	method, path, err := connectorActionRequest(name, action)
	if err != nil {
		return err
	}
	return cl.do(ctx, method, path, nil, nil)
}

// connectorActionRequest maps the contract's ConnectorAction enum (P2b-D4)
// onto its Connect REST API call: PAUSE/RESUME/STOP are PUT lifecycle
// verbs; RESTART and its two task-scoped variants are POST /restart with a
// query flag.
func connectorActionRequest(name, action string) (method, path string, err error) {
	base := "/connectors/" + url.PathEscape(name)
	switch action {
	case "PAUSE":
		return http.MethodPut, base + "/pause", nil
	case "RESUME":
		return http.MethodPut, base + "/resume", nil
	case "STOP":
		return http.MethodPut, base + "/stop", nil
	case "RESTART":
		return http.MethodPost, base + "/restart", nil
	case "RESTART_ALL_TASKS":
		return http.MethodPost, base + "/restart?includeTasks=true", nil
	case "RESTART_FAILED_TASKS":
		return http.MethodPost, base + "/restart?includeTasks=true&onlyFailed=true", nil
	default:
		return "", "", fmt.Errorf("connect: unknown ConnectorAction %q", action)
	}
}

// ResetConnectorOffsets resets name's committed offsets on connectName
// (DELETE /connectors/{name}/offsets -- Connect 3.6+, and only while name is
// STOPPED; this method does not itself gate on either precondition, an
// unmet one surfaces as whatever error Connect itself returns, per P2b-D7).
func (p *Pool) ResetConnectorOffsets(ctx context.Context, def cluster.Definition, connectName, name string) error {
	cl, err := p.clientFor(def, connectName)
	if err != nil {
		return err
	}
	return cl.do(ctx, http.MethodDelete, "/connectors/"+url.PathEscape(name)+"/offsets", nil, nil)
}

// RestartConnectorTask restarts task taskID of name on connectName.
func (p *Pool) RestartConnectorTask(ctx context.Context, def cluster.Definition, connectName, name string, taskID int) error {
	cl, err := p.clientFor(def, connectName)
	if err != nil {
		return err
	}
	path := "/connectors/" + url.PathEscape(name) + "/tasks/" + strconv.Itoa(taskID) + "/restart"
	return cl.do(ctx, http.MethodPost, path, nil, nil)
}

func toPluginValidation(w validateResponseWire) cluster.PluginValidation {
	configs := make([]cluster.PluginConfigEntry, len(w.Configs))
	for i, c := range w.Configs {
		configs[i] = cluster.PluginConfigEntry{
			Definition: cluster.PluginConfigDef{
				Name:          c.Definition.Name,
				Type:          c.Definition.Type,
				Required:      c.Definition.Required,
				DefaultValue:  strDeref(c.Definition.DefaultValue),
				Importance:    c.Definition.Importance,
				Documentation: c.Definition.Documentation,
				Group:         c.Definition.Group,
				Width:         c.Definition.Width,
				DisplayName:   c.Definition.DisplayName,
				Order:         c.Definition.Order,
				Dependents:    c.Definition.Dependents,
			},
			Value: cluster.PluginConfigValue{
				Name:              c.Value.Name,
				Value:             strDeref(c.Value.Value),
				RecommendedValues: c.Value.RecommendedValues,
				Errors:            c.Value.Errors,
				Visible:           c.Value.Visible,
			},
		}
	}
	return cluster.PluginValidation{
		Name:       w.Name,
		ErrorCount: w.ErrorCount,
		Groups:     w.Groups,
		Configs:    configs,
	}
}

func strDeref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
