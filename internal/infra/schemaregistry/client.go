// Package schemaregistry implements cluster.SchemaRegistryPort over
// franz-go's pkg/sr Confluent-compatible Schema Registry HTTP client
// (P2a Task 2).
package schemaregistry

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/sr"

	"github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
	"github.com/cy-kaf/cy-kaf-client/internal/infra/kafka"
)

// Pool caches one *sr.Client and a bounded set of immutable ID-addressed
// schemas per cluster name. clientFor hashes the current Schema Registry spec
// and replaces the entire stale entry after dynamic configuration changes.
type Pool struct {
	mu      sync.Mutex
	clients map[string]cachedClient
}

// schemaCacheCapacity bounds successful SchemaByID results per client
// identity. FIFO eviction keeps both memory use and bookkeeping predictable.
const schemaCacheCapacity = 1000

type cachedClient struct {
	identity    [sha256.Size]byte
	client      *sr.Client
	httpClient  *http.Client
	schemas     map[int]cluster.RawSchema
	schemaOrder []int
}

// NewPool builds an empty Pool.
func NewPool() *Pool {
	return &Pool{clients: map[string]cachedClient{}}
}

var _ cluster.SchemaRegistryPort = (*Pool)(nil)

func (p *Pool) clientFor(def cluster.Definition) (*sr.Client, error) {
	identity := schemaRegistryIdentity(def.SchemaRegistry)
	p.mu.Lock()
	if p.clients == nil {
		p.clients = make(map[string]cachedClient)
	}
	if cached, ok := p.clients[def.Name]; ok && cached.identity == identity {
		p.mu.Unlock()
		return cached.client, nil
	}
	cl, httpClient, err := newClient(def.SchemaRegistry)
	if err != nil {
		p.mu.Unlock()
		return nil, fmt.Errorf("schema registry client for cluster %q: %w", def.Name, err)
	}
	var oldHTTP *http.Client
	if old, ok := p.clients[def.Name]; ok {
		oldHTTP = old.httpClient
	}
	p.clients[def.Name] = cachedClient{
		identity:   identity,
		client:     cl,
		httpClient: httpClient,
		schemas:    make(map[int]cluster.RawSchema, schemaCacheCapacity),
	}
	p.mu.Unlock()
	if oldHTTP != nil {
		oldHTTP.CloseIdleConnections()
	}
	return cl, nil
}

// Close atomically drops Schema Registry clients, their credential-bearing
// SDK objects, and their schema caches, then closes idle HTTP connections
// outside the mutex. The pool remains reusable.
func (p *Pool) Close() {
	p.mu.Lock()
	closing := make([]*http.Client, 0, len(p.clients))
	for _, cached := range p.clients {
		if cached.httpClient != nil {
			closing = append(closing, cached.httpClient)
		}
	}
	p.clients = make(map[string]cachedClient)
	p.mu.Unlock()

	for _, client := range closing {
		client.CloseIdleConnections()
	}
}

// cachedSchema returns a schema only from the entry matching def's current
// connection identity.
func (p *Pool) cachedSchema(def cluster.Definition, id int) (cluster.RawSchema, bool) {
	identity := schemaRegistryIdentity(def.SchemaRegistry)
	p.mu.Lock()
	defer p.mu.Unlock()

	cached, ok := p.clients[def.Name]
	if !ok || cached.identity != identity {
		return cluster.RawSchema{}, false
	}
	schema, ok := cached.schemas[id]
	return schema, ok
}

// storeSchema adds one successful result if the client identity still
// matches, evicting the oldest ID once the fixed capacity is reached.
func (p *Pool) storeSchema(def cluster.Definition, id int, schema cluster.RawSchema) {
	identity := schemaRegistryIdentity(def.SchemaRegistry)
	p.mu.Lock()
	defer p.mu.Unlock()

	cached, ok := p.clients[def.Name]
	if !ok || cached.identity != identity {
		return
	}
	if _, exists := cached.schemas[id]; exists {
		return
	}
	if len(cached.schemaOrder) >= schemaCacheCapacity {
		oldest := cached.schemaOrder[0]
		delete(cached.schemas, oldest)
		copy(cached.schemaOrder, cached.schemaOrder[1:])
		cached.schemaOrder = cached.schemaOrder[:len(cached.schemaOrder)-1]
	}
	cached.schemas[id] = schema
	cached.schemaOrder = append(cached.schemaOrder, id)
	p.clients[def.Name] = cached
}

// schemaRegistryIdentity hashes every setting that influences outgoing
// requests. The cache therefore rotates on dynamic-config edits without
// retaining credentials in a map key.
func schemaRegistryIdentity(spec cluster.SchemaRegistrySpec) [sha256.Size]byte {
	raw, _ := json.Marshal(spec)
	return sha256.Sum256(raw)
}

func newClient(spec cluster.SchemaRegistrySpec) (*sr.Client, *http.Client, error) {
	httpClient := &http.Client{
		Timeout: 5 * time.Second,
	}
	opts := []sr.ClientOpt{sr.URLs(spec.URL), sr.HTTPClient(httpClient)}
	if spec.Auth != nil {
		opts = append(opts, sr.BasicAuth(spec.Auth.Username, spec.Auth.Password))
	}
	if spec.SSL != nil {
		tlsCfg, err := tlsConfigFor(*spec.SSL)
		if err != nil {
			closeOwnedHTTPClient(httpClient)
			return nil, nil, err
		}
		opts = append(opts, sr.DialTLSConfig(tlsCfg))
	}
	client, err := sr.NewClient(opts...)
	if err != nil {
		closeOwnedHTTPClient(httpClient)
		return nil, nil, err
	}
	if httpClient.Transport == nil {
		if transport, ok := http.DefaultTransport.(*http.Transport); ok {
			httpClient.Transport = transport.Clone()
		} else {
			httpClient.Transport = &http.Transport{}
		}
	}
	return client, httpClient, nil
}

func closeOwnedHTTPClient(client *http.Client) {
	if client == nil || client.Transport == nil {
		return
	}
	client.CloseIdleConnections()
}

// tlsConfigFor builds a *tls.Config from a SR connection's custom TLS
// material. Only Truststore (verifying the SR server's own certificate) is
// implemented, reusing infra/kafka's exported LoadTruststore (PEM/JKS ->
// x509.CertPool) -- this mirrors that package's own tlsConfigFor, which
// likewise never consumes its analogous ssl.keystore.* settings for mutual
// TLS client certs. SRSSL.KeystoreLocation/KeystorePassword are therefore
// currently unused; wiring client-cert mutual TLS is left as future work,
// not a P2a Task 2 requirement.
func tlsConfigFor(ssl cluster.SRSSL) (*tls.Config, error) {
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

// Subjects lists every registered subject name.
func (p *Pool) Subjects(ctx context.Context, def cluster.Definition) ([]string, error) {
	cl, err := p.clientFor(def)
	if err != nil {
		return nil, err
	}
	return cl.Subjects(ctx)
}

// SchemaByVersion fetches one subject's schema at version ("latest" or a
// numeric string), plus its effective compatibility level (see
// effectiveCompat below).
func (p *Pool) SchemaByVersion(ctx context.Context, def cluster.Definition, subject, version string) (cluster.SchemaVersion, error) {
	cl, err := p.clientFor(def)
	if err != nil {
		return cluster.SchemaVersion{}, err
	}
	verInt, err := parseVersion(version)
	if err != nil {
		return cluster.SchemaVersion{}, err
	}
	ss, err := cl.SchemaByVersion(ctx, subject, verInt)
	if err != nil {
		return cluster.SchemaVersion{}, err
	}
	compat, err := effectiveCompat(ctx, cl, subject)
	if err != nil {
		return cluster.SchemaVersion{}, err
	}
	return cluster.SchemaVersion{
		ID:          ss.ID,
		Subject:     ss.Subject,
		Version:     ss.Version,
		Schema:      ss.Schema.Schema,
		SchemaType:  ss.Type.String(),
		CompatLevel: compat,
		References:  toDomainReferences(ss.References),
	}, nil
}

// SchemaByID fetches a schema by its registry-global ID, caching successful
// immutable results within the current client identity.
func (p *Pool) SchemaByID(ctx context.Context, def cluster.Definition, id int) (cluster.RawSchema, error) {
	cl, err := p.clientFor(def)
	if err != nil {
		return cluster.RawSchema{}, err
	}
	if schema, ok := p.cachedSchema(def, id); ok {
		return schema, nil
	}
	s, err := cl.SchemaByID(ctx, id)
	if err != nil {
		return cluster.RawSchema{}, err
	}
	schema := cluster.RawSchema{Schema: s.Schema, SchemaType: s.Type.String()}
	p.storeSchema(def, id, schema)
	return schema, nil
}

// Versions lists a subject's registered version numbers.
func (p *Pool) Versions(ctx context.Context, def cluster.Definition, subject string) ([]int, error) {
	cl, err := p.clientFor(def)
	if err != nil {
		return nil, err
	}
	return cl.SubjectVersions(ctx, subject)
}

// Register submits a new schema under subject, returning its registry-global
// ID.
func (p *Pool) Register(ctx context.Context, def cluster.Definition, subject string, s cluster.NewSchema) (int, error) {
	cl, err := p.clientFor(def)
	if err != nil {
		return 0, err
	}
	schema, err := toSRSchema(s)
	if err != nil {
		return 0, err
	}
	ss, err := cl.CreateSchema(ctx, subject, schema)
	if err != nil {
		return 0, err
	}
	return ss.ID, nil
}

// DeleteSubject removes every version of subject.
func (p *Pool) DeleteSubject(ctx context.Context, def cluster.Definition, subject string, permanent bool) ([]int, error) {
	cl, err := p.clientFor(def)
	if err != nil {
		return nil, err
	}
	return cl.DeleteSubject(ctx, subject, deleteHow(permanent))
}

// DeleteVersion removes one version of subject, returning the concrete
// version number that was resolved and deleted (see the capability-audit
// comment on (*sr.Client).DeleteSchema for why this package resolves
// "latest" itself rather than relying on DeleteSchema's return value, which
// is error-only).
func (p *Pool) DeleteVersion(ctx context.Context, def cluster.Definition, subject, version string, permanent bool) (int, error) {
	cl, err := p.clientFor(def)
	if err != nil {
		return 0, err
	}
	verInt, err := resolveDeleteVersion(ctx, cl, subject, version)
	if err != nil {
		return 0, err
	}
	if err := cl.DeleteSchema(ctx, subject, verInt, deleteHow(permanent)); err != nil {
		return 0, err
	}
	return verInt, nil
}

// GlobalCompat reads the registry-wide default compatibility level.
func (p *Pool) GlobalCompat(ctx context.Context, def cluster.Definition) (string, error) {
	cl, err := p.clientFor(def)
	if err != nil {
		return "", err
	}
	return compatResult(cl.Compatibility(ctx))
}

// SetGlobalCompat writes the registry-wide default compatibility level.
func (p *Pool) SetGlobalCompat(ctx context.Context, def cluster.Definition, level string) error {
	cl, err := p.clientFor(def)
	if err != nil {
		return err
	}
	l, err := parseCompatLevel(level)
	if err != nil {
		return err
	}
	_, err = compatResult(cl.SetCompatibility(ctx, sr.SetCompatibility{Level: l}))
	return err
}

// SubjectCompat reads subject's effective compatibility level.
func (p *Pool) SubjectCompat(ctx context.Context, def cluster.Definition, subject string) (string, error) {
	cl, err := p.clientFor(def)
	if err != nil {
		return "", err
	}
	return effectiveCompat(ctx, cl, subject)
}

// SetSubjectCompat writes subject's own compatibility-level override.
func (p *Pool) SetSubjectCompat(ctx context.Context, def cluster.Definition, subject, level string) error {
	cl, err := p.clientFor(def)
	if err != nil {
		return err
	}
	l, err := parseCompatLevel(level)
	if err != nil {
		return err
	}
	_, err = compatResult(cl.SetCompatibility(ctx, sr.SetCompatibility{Level: l}, subject))
	return err
}

// CheckCompat reports whether s would be compatible with subject's latest
// registered version, under subject's effective compatibility rule.
func (p *Pool) CheckCompat(ctx context.Context, def cluster.Definition, subject string, s cluster.NewSchema) (bool, error) {
	cl, err := p.clientFor(def)
	if err != nil {
		return false, err
	}
	schema, err := toSRSchema(s)
	if err != nil {
		return false, err
	}
	res, err := cl.CheckCompatibility(ctx, subject, -1, schema) // -1 == latest version
	if err != nil {
		return false, err
	}
	return res.Is, nil
}

// --- helpers ---

// parseVersion converts the port's version string convention ("latest" or a
// numeric string) into sr's own -1-means-latest int convention.
func parseVersion(version string) (int, error) {
	if version == "latest" {
		return -1, nil
	}
	n, err := strconv.Atoi(version)
	if err != nil {
		return 0, fmt.Errorf("invalid version %q: %w", version, err)
	}
	return n, nil
}

// resolveDeleteVersion is like parseVersion, but additionally resolves
// "latest" to a concrete version number via SubjectVersions -- needed
// because (*sr.Client).DeleteSchema returns only an error, so the caller
// cannot otherwise learn which version "latest" actually deleted.
func resolveDeleteVersion(ctx context.Context, cl *sr.Client, subject, version string) (int, error) {
	if version != "latest" {
		return parseVersion(version)
	}
	versions, err := cl.SubjectVersions(ctx, subject)
	if err != nil {
		return 0, err
	}
	if len(versions) == 0 {
		return 0, fmt.Errorf("subject %q: no versions to delete", subject)
	}
	sort.Ints(versions)
	return versions[len(versions)-1], nil
}

// effectiveCompat reads subject's effective compatibility level: its own
// override if set, else the registry's global default. See the capability-
// audit comment on sr.DefaultToGlobal for why this Param is required here.
func effectiveCompat(ctx context.Context, cl *sr.Client, subject string) (string, error) {
	return compatResult(cl.Compatibility(sr.WithParams(ctx, sr.DefaultToGlobal), subject))
}

func compatResult(res []sr.CompatibilityResult) (string, error) {
	if len(res) == 0 {
		return "", fmt.Errorf("schema registry: empty compatibility response")
	}
	if res[0].Err != nil {
		return "", res[0].Err
	}
	return res[0].Level.String(), nil
}

func deleteHow(permanent bool) sr.DeleteHow {
	if permanent {
		return sr.HardDelete
	}
	return sr.SoftDelete
}

func parseSchemaType(s string) (sr.SchemaType, error) {
	var t sr.SchemaType
	if err := t.UnmarshalText([]byte(s)); err != nil {
		return 0, fmt.Errorf("schemaType: %w", err)
	}
	return t, nil
}

func parseCompatLevel(s string) (sr.CompatibilityLevel, error) {
	var l sr.CompatibilityLevel
	if err := l.UnmarshalText([]byte(s)); err != nil {
		return 0, fmt.Errorf("compatibility level: %w", err)
	}
	return l, nil
}

func toSRSchema(s cluster.NewSchema) (sr.Schema, error) {
	t, err := parseSchemaType(s.SchemaType)
	if err != nil {
		return sr.Schema{}, err
	}
	return sr.Schema{
		Schema:     s.Schema,
		Type:       t,
		References: toSRReferences(s.References),
	}, nil
}

func toDomainReferences(refs []sr.SchemaReference) []cluster.SchemaReference {
	if len(refs) == 0 {
		return nil
	}
	out := make([]cluster.SchemaReference, len(refs))
	for i, r := range refs {
		out[i] = cluster.SchemaReference{Name: r.Name, Subject: r.Subject, Version: r.Version}
	}
	return out
}

func toSRReferences(refs []cluster.SchemaReference) []sr.SchemaReference {
	if len(refs) == 0 {
		return nil
	}
	out := make([]sr.SchemaReference, len(refs))
	for i, r := range refs {
		out[i] = sr.SchemaReference{Name: r.Name, Subject: r.Subject, Version: r.Version}
	}
	return out
}
