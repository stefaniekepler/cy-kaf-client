package ksql

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"

	"github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
	"github.com/cy-kaf/cy-kaf-client/internal/infra/kafka"
)

const (
	ksqlMediaType    = "application/vnd.ksql.v1+json"
	maxHTTPErrorBody = maxRemoteErrorBytes
	// Finite /ksql and LIST responses are buffered once, so keep a hard
	// aggregate ceiling separate from the per-frame streaming ceiling below.
	maxKsqlResponseBytes    = 16 << 20
	errKsqlResponseTooLarge = "KSQL response exceeds size limit"
	errKsqlFrameTooLarge    = "KSQL response frame exceeds size limit"
)

var (
	errKsqlResponseLimit = errors.New(errKsqlResponseTooLarge)
	errKsqlFrameLimit    = errors.New(errKsqlFrameTooLarge)
)

type commandWire struct {
	KSQL              string            `json:"ksql"`
	StreamsProperties map[string]string `json:"streamsProperties"`
}

type clientKey struct {
	cluster    string
	url        string
	authDigest string
	tlsDigest  string
}

type ksqlClient struct {
	http    *http.Client
	baseURL *url.URL
	safeURL string
	authHdr string
}

// Pool caches a client by all connection material that affects an HTTP
// request, while retaining only the current client for each cluster.  A
// config reload therefore cannot accidentally reuse an old endpoint or
// Authorization header, and repeated endpoint/credential rotations do not
// retain an unbounded set of clients in memory.
type Pool struct {
	mu      sync.Mutex
	clients map[clientKey]*ksqlClient
	client  *http.Client
}

func NewPool() *Pool {
	return &Pool{clients: make(map[clientKey]*ksqlClient)}
}

var _ cluster.KsqlPort = (*Pool)(nil)

func (p *Pool) ListStreams(ctx context.Context, def cluster.Definition) ([]cluster.KsqlStreamDescription, error) {
	body, err := p.postBytes(ctx, def, cluster.KsqlCommand{SQL: "LIST STREAMS;", Kind: cluster.KsqlStatement})
	if err != nil {
		return nil, err
	}
	streams, _, err := decodeListResponse(body, true)
	if err != nil {
		return nil, fmt.Errorf("decode KSQL streams response: %w", err)
	}
	return streams, nil
}

func (p *Pool) ListTables(ctx context.Context, def cluster.Definition) ([]cluster.KsqlTableDescription, error) {
	body, err := p.postBytes(ctx, def, cluster.KsqlCommand{SQL: "LIST TABLES;", Kind: cluster.KsqlStatement})
	if err != nil {
		return nil, err
	}
	_, tables, err := decodeListResponse(body, false)
	if err != nil {
		return nil, fmt.Errorf("decode KSQL tables response: %w", err)
	}
	return tables, nil
}

func (p *Pool) Execute(ctx context.Context, def cluster.Definition, command cluster.KsqlCommand, emit func(cluster.KsqlTable) error) error {
	cl, err := p.clientFor(def)
	if err != nil {
		return err
	}
	path := "/ksql"
	contentType := "application/json"
	if command.Kind == cluster.KsqlQuery {
		path = "/query"
		contentType = ksqlMediaType
	}
	body, err := json.Marshal(commandWire{
		KSQL:              command.SQL,
		StreamsProperties: cloneProperties(command.StreamsProperties),
	})
	if err != nil {
		return errors.New("encode KSQL request")
	}
	resp, err := cl.do(ctx, http.MethodPost, path, contentType, bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return emitTable(emit, responseErrorFrame(resp))
	}
	if command.Kind == cluster.KsqlQuery {
		return decodeQueryResponse(resp.Body, emit)
	}
	responseBody, err := readBoundedKSQLBody(resp.Body)
	if err != nil {
		return fmt.Errorf("read KSQL response from %s: %w", cl.safeURL, err)
	}
	return decodeKsqlResponse(responseBody, emit)
}

func (p *Pool) postBytes(ctx context.Context, def cluster.Definition, command cluster.KsqlCommand) ([]byte, error) {
	cl, err := p.clientFor(def)
	if err != nil {
		return nil, err
	}
	body, err := json.Marshal(commandWire{KSQL: command.SQL, StreamsProperties: map[string]string{}})
	if err != nil {
		return nil, errors.New("encode KSQL request")
	}
	resp, err := cl.do(ctx, http.MethodPost, "/ksql", "application/json", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		// Lists expose execution errors as an infrastructure failure to the
		// API layer. Keep the remote body out of the returned error because it
		// may contain credentials or local implementation details.
		_, _ = io.CopyN(io.Discard, resp.Body, maxHTTPErrorBody+1)
		return nil, errors.New("KSQL list request failed")
	}
	responseBody, err := readBoundedKSQLBody(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read KSQL response from %s: %w", cl.safeURL, err)
	}
	return responseBody, nil
}

func readBoundedKSQLBody(r io.Reader) ([]byte, error) {
	return readBoundedKSQLBodyWithLimit(r, maxKsqlResponseBytes)
}

func readBoundedKSQLBodyWithLimit(r io.Reader, limit int) ([]byte, error) {
	if r == nil {
		return nil, errors.New("KSQL response body is empty")
	}
	if limit < 0 {
		limit = 0
	}
	body, err := io.ReadAll(io.LimitReader(r, int64(limit)+1))
	if err != nil {
		return nil, err
	}
	if len(body) > limit {
		return nil, errKsqlResponseLimit
	}
	return body, nil
}

func (p *Pool) clientFor(def cluster.Definition) (*ksqlClient, error) {
	baseURL, safeURL, err := parseKSQLURL(def.KsqlURL)
	if err != nil {
		return nil, err
	}
	key := clientKey{
		cluster:    def.Name,
		url:        safeURL,
		authDigest: digestAuth(def.KsqlAuth),
		tlsDigest:  digestTLS(def.KsqlSSL),
	}
	p.mu.Lock()
	if p.clients == nil {
		p.clients = make(map[clientKey]*ksqlClient)
	}
	if cl, ok := p.clients[key]; ok {
		p.mu.Unlock()
		return cl, nil
	}
	cl, err := newKSQLClient(baseURL, safeURL, def, p.client)
	if err != nil {
		p.mu.Unlock()
		return nil, err
	}
	var closing []*http.Client
	for oldKey, oldClient := range p.clients {
		if oldKey.cluster != key.cluster {
			continue
		}
		delete(p.clients, oldKey)
		if oldClient != nil && oldClient.http != nil {
			closing = append(closing, oldClient.http)
		}
	}
	p.clients[key] = cl
	p.mu.Unlock()
	for _, client := range closing {
		client.CloseIdleConnections()
	}
	return cl, nil
}

// Close detaches every current cluster client and the optional injected
// template client before closing idle connections outside the pool mutex.
// clientFor lazily rebuilds the map after Close.
func (p *Pool) Close() {
	p.mu.Lock()
	closing := make([]*http.Client, 0, len(p.clients)+1)
	for _, client := range p.clients {
		if client != nil && client.http != nil {
			closing = append(closing, client.http)
		}
	}
	if p.client != nil {
		closing = append(closing, p.client)
	}
	p.clients = nil
	p.client = nil
	p.mu.Unlock()

	for _, client := range closing {
		client.CloseIdleConnections()
	}
}

func parseKSQLURL(raw string) (*url.URL, string, error) {
	normalized := strings.TrimSuffix(strings.TrimSpace(raw), "/")
	if normalized == "" {
		return nil, "", errors.New("KSQL server URL is empty")
	}
	u, err := url.Parse(normalized)
	if err != nil || u.Scheme == "" || u.Host == "" || u.User != nil || u.Hostname() == "" {
		return nil, "", errors.New("invalid KSQL server URL")
	}
	if u.Scheme != "http" && u.Scheme != "https" || u.RawQuery != "" || u.Fragment != "" {
		return nil, "", errors.New("invalid KSQL server URL")
	}
	u.Path = strings.TrimSuffix(u.Path, "/")
	u.RawPath = ""
	safe := (&url.URL{Scheme: u.Scheme, Host: u.Host, Path: u.Path}).String()
	return u, safe, nil
}

func newKSQLClient(baseURL *url.URL, safeURL string, def cluster.Definition, parent *http.Client) (*ksqlClient, error) {
	transport := defaultTransport()
	var injected http.RoundTripper
	if parent != nil && parent.Transport != nil {
		if configured, ok := parent.Transport.(*http.Transport); ok {
			transport = configured.Clone()
		} else {
			// A non-standard RoundTripper is useful to package tests and keeps
			// the Pool's small HTTP dependency injectable. Production pools use
			// the standard transport created above.
			injected = parent.Transport
		}
	}
	if def.KsqlSSL != nil {
		cfg := &tls.Config{MinVersion: tls.VersionTLS12}
		if def.KsqlSSL.TruststoreLocation != "" {
			pool, err := kafka.LoadTruststore(def.KsqlSSL.TruststoreLocation, def.KsqlSSL.TruststorePassword)
			if err != nil {
				return nil, fmt.Errorf("load KSQL truststore: %w", err)
			}
			cfg.RootCAs = pool
		}
		transport.TLSClientConfig = cfg
	} else {
		transport.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	}
	httpClient := &http.Client{Transport: transport}
	if injected != nil {
		httpClient.Transport = injected
	}
	if parent != nil {
		httpClient.CheckRedirect = parent.CheckRedirect
	}
	cl := &ksqlClient{http: httpClient, baseURL: baseURL, safeURL: safeURL}
	if def.KsqlAuth != nil {
		encoded := base64.StdEncoding.EncodeToString([]byte(def.KsqlAuth.Username + ":" + def.KsqlAuth.Password))
		cl.authHdr = "Basic " + encoded
	}
	return cl, nil
}

func defaultTransport() *http.Transport {
	if base, ok := http.DefaultTransport.(*http.Transport); ok {
		return base.Clone()
	}
	return &http.Transport{}
}

func (c *ksqlClient) do(ctx context.Context, method, path, contentType string, body io.Reader) (*http.Response, error) {
	u := *c.baseURL
	u.Path = strings.TrimSuffix(u.Path, "/") + path
	u.RawPath = ""
	u.RawQuery = ""
	u.Fragment = ""
	req, err := http.NewRequestWithContext(ctx, method, u.String(), body)
	if err != nil {
		return nil, fmt.Errorf("create KSQL request for %s: %w", c.safeURL, err)
	}
	req.Header.Set("Accept", ksqlMediaType)
	req.Header.Set("Content-Type", contentType)
	if c.authHdr != "" {
		req.Header.Set("Authorization", c.authHdr)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("KSQL request to %s: %w", c.safeURL, err)
	}
	return resp, nil
}

func responseErrorFrame(resp *http.Response) cluster.KsqlTable {
	if resp == nil {
		return errorTable("KSQL server request failed")
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxHTTPErrorBody+1))
	if err != nil {
		return errorTable(fmt.Sprintf("KSQL server returned HTTP %d", resp.StatusCode))
	}
	raw = raw[:minInt(len(raw), maxHTTPErrorBody)]
	if len(bytes.TrimSpace(raw)) != 0 {
		if node, parseErr := parseSingleNode(raw); parseErr == nil {
			return safeRemoteErrorTable(node, fmt.Sprintf("KSQL server returned HTTP %d", resp.StatusCode))
		}
		// The bounded excerpt may end inside a long message.  Recover only
		// complete object fields already decoded by the JSON parser; never
		// search arbitrary bytes or echo the incomplete suffix.
		if node, ok := parseSafeRemoteErrorPrefix(raw); ok {
			frame := safeRemoteErrorTable(node, fmt.Sprintf("KSQL server returned HTTP %d", resp.StatusCode))
			// When the 500-byte excerpt ends immediately after a numeric
			// error_code, retain the established single-cell diagnostic shape.
			// The code is still preserved, but no incomplete remote message is
			// reconstructed from arbitrary bytes.
			if len(frame.ColumnNames) == 1 && len(frame.Values) == 1 && len(frame.Values[0]) == 1 &&
				(frame.ColumnNames[0] == "error_code" || frame.ColumnNames[0] == "errorCode") {
				return errorTable(fmt.Sprintf("KSQL error code %v", frame.Values[0][0]))
			}
			return frame
		}
	}
	return errorTable(fmt.Sprintf("KSQL server returned HTTP %d", resp.StatusCode))
}

func parseSafeRemoteErrorPrefix(raw []byte) (*jsonNode, bool) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	tok, err := dec.Token()
	if err != nil || tok != json.Delim('{') {
		return nil, false
	}
	node := &jsonNode{kind: 'o'}
	for dec.More() {
		key, keyErr := dec.Token()
		if keyErr != nil {
			break
		}
		name, ok := key.(string)
		if !ok {
			break
		}
		value, valueErr := decodeNode(dec)
		if valueErr != nil {
			break
		}
		node.object = append(node.object, jsonField{name: name, value: value})
	}
	return node, len(node.object) > 0
}

func parseSingleNode(body []byte) (*jsonNode, error) {
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber()
	node, err := decodeNode(dec)
	if err != nil {
		return nil, err
	}
	if err := ensureDecoderEOF(dec); err != nil {
		return nil, err
	}
	return node, nil
}

func cloneProperties(in map[string]string) map[string]string {
	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func digestAuth(auth *cluster.KsqlAuth) string {
	if auth == nil {
		return ""
	}
	h := sha256.Sum256([]byte(auth.Username + "\x00" + auth.Password))
	return hex.EncodeToString(h[:])
}

func digestTLS(ssl *cluster.KsqlSSL) string {
	if ssl == nil {
		return ""
	}
	h := sha256.Sum256([]byte(strings.Join([]string{
		ssl.TruststoreLocation, ssl.TruststorePassword,
		ssl.KeystoreLocation, ssl.KeystorePassword,
	}, "\x00")))
	return hex.EncodeToString(h[:])
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
