package cluster

import (
	"context"
	cryptorand "crypto/rand"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	domain "github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
)

const (
	defaultKsqlPipeTTL = time.Minute
	defaultKsqlPipeMax = 1024
)

// KsqlClassifier is the application-facing part of the KSQL statement
// classifier.  Keeping this interface here prevents the application layer
// from depending on the ANTLR-backed infrastructure implementation.
type KsqlClassifier interface {
	Classify(string) (domain.KsqlStatementKind, error)
}

// KsqlAuthorize is called after a pipe has been atomically claimed and before
// its command reaches the KSQL port.
type KsqlAuthorize func(domain.KsqlStatementKind) error

var (
	// ErrKsqlPipeNotFound intentionally covers an absent, expired, or
	// cluster-mismatched pipe.  The distinction must not be observable to the
	// caller.
	ErrKsqlPipeNotFound    = errors.New("ksql pipe not found")
	ErrKsqlClusterNotFound = errors.New("ksql cluster not found")
	ErrKsqlNotConfigured   = errors.New("ksql not configured")
	ErrKsqlInvalidCommand  = errors.New("invalid ksql command")
	ErrKsqlPipeLimit       = errors.New("ksql pipe capacity reached")
)

// pipeKey scopes an ID to its registering cluster.  This makes a lookup from
// another cluster indistinguishable from a missing ID while retaining the
// cluster name required for the re-resolution boundary in KsqlService.Open.
type pipeKey struct {
	clusterName string
	id          string
}

type pipeEntry struct {
	command   domain.KsqlCommand
	createdAt time.Time
}

// PipeRegistry is a bounded, concurrency-safe, one-shot command registry.
// It deliberately stores only the cluster name, validated command and
// creation time; HTTP requests and response data never cross this boundary.
type PipeRegistry struct {
	mu      sync.Mutex
	now     func() time.Time
	ttl     time.Duration
	max     int
	entries map[pipeKey]pipeEntry
}

func newPipeRegistry(now func() time.Time, ttl time.Duration, max int) *PipeRegistry {
	if now == nil {
		now = time.Now
	}
	if max < 0 {
		max = 0
	}
	return &PipeRegistry{
		now:     now,
		ttl:     ttl,
		max:     max,
		entries: make(map[pipeKey]pipeEntry),
	}
}

// register stores command under a fresh UUID and returns the opaque ID.  It
// performs expiry cleanup before enforcing the capacity bound, so expired
// entries never consume the configured limit.
func (r *PipeRegistry) register(clusterName string, command domain.KsqlCommand) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.ensureState()
	now := r.now()
	r.cleanupExpired(now)
	if len(r.entries) >= r.max {
		return "", ErrKsqlPipeLimit
	}

	// A UUID collision is extraordinarily unlikely, but checking under the
	// same lock keeps the one-shot key invariant explicit.  Retry rather than
	// replacing an existing command if a faulty/random test source collides.
	for {
		id, err := newKsqlPipeID()
		if err != nil {
			return "", fmt.Errorf("generate ksql pipe id: %w", err)
		}
		key := pipeKey{clusterName: clusterName, id: id}
		if _, exists := r.entries[key]; exists {
			continue
		}
		r.entries[key] = pipeEntry{
			command:   cloneKsqlCommand(command),
			createdAt: now,
		}
		return id, nil
	}
}

// claim atomically checks expiry, removes and returns a command.  Deleting
// before the caller invokes the port guarantees at-most-once downstream
// execution even when Execute fails or the caller concurrently retries GET.
func (r *PipeRegistry) claim(clusterName, id string) (domain.KsqlCommand, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.ensureState()
	r.cleanupExpired(r.now())
	key := pipeKey{clusterName: clusterName, id: id}
	entry, ok := r.entries[key]
	if !ok {
		return domain.KsqlCommand{}, false
	}
	delete(r.entries, key)
	return cloneKsqlCommand(entry.command), true
}

func (r *PipeRegistry) ensureState() {
	if r.now == nil {
		r.now = time.Now
	}
	if r.entries == nil {
		r.entries = make(map[pipeKey]pipeEntry)
	}
}

func (r *PipeRegistry) cleanupExpired(now time.Time) {
	for key, entry := range r.entries {
		// Expire at the TTL boundary.  A backwards-moving test clock does not
		// make a newly registered entry disappear because Add(ttl) remains
		// after the earlier timestamp.
		if !entry.createdAt.Add(r.ttl).After(now) {
			delete(r.entries, key)
		}
	}
}

func newKsqlPipeID() (string, error) {
	var bytes [16]byte
	if _, err := cryptorand.Read(bytes[:]); err != nil {
		return "", err
	}
	// RFC 4122 version 4, variant 1.  Formatting is local so the service has
	// no dependency on an ID package and the source of entropy is explicit.
	bytes[6] = (bytes[6] & 0x0f) | 0x40
	bytes[8] = (bytes[8] & 0x3f) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		bytes[0:4], bytes[4:6], bytes[6:8], bytes[8:10], bytes[10:16]), nil
}

// KsqlService resolves configured clusters, validates/classifies commands,
// and manages the bounded one-shot pipe lifecycle.  The downstream port is
// called only from Open, after an atomic claim and a fresh Definition lookup.
type KsqlService struct {
	resolver   *Resolver
	port       domain.KsqlPort
	classifier KsqlClassifier
	pipes      *PipeRegistry
}

func NewKsqlService(resolver *Resolver, port domain.KsqlPort, classifier KsqlClassifier) *KsqlService {
	return &KsqlService{
		resolver:   resolver,
		port:       port,
		classifier: classifier,
		pipes:      newPipeRegistry(time.Now, defaultKsqlPipeTTL, defaultKsqlPipeMax),
	}
}

func (s *KsqlService) Register(_ context.Context, clusterName, sql string, streamsProperties map[string]string) (string, error) {
	if _, err := s.lookup(clusterName); err != nil {
		return "", err
	}
	if s.classifier == nil {
		return "", ErrKsqlInvalidCommand
	}
	kind, err := s.classifier.Classify(sql)
	if err != nil || (kind != domain.KsqlQuery && kind != domain.KsqlStatement) {
		// Never return the classifier's diagnostic: ANTLR messages can contain
		// caller SQL, and the HTTP layer only needs this stable safe sentinel.
		return "", ErrKsqlInvalidCommand
	}
	if s.pipes == nil {
		return "", ErrKsqlPipeLimit
	}
	return s.pipes.register(clusterName, domain.KsqlCommand{
		SQL:               sql,
		StreamsProperties: cloneStringMap(streamsProperties),
		Kind:              kind,
	})
}

func (s *KsqlService) Open(ctx context.Context, clusterName, pipeID string, emit func(domain.KsqlTable) error) error {
	return s.OpenAuthorized(ctx, clusterName, pipeID, nil, emit)
}

func (s *KsqlService) OpenAuthorized(
	ctx context.Context,
	clusterName,
	pipeID string,
	authorize KsqlAuthorize,
	emit func(domain.KsqlTable) error,
) error {
	definition, err := s.lookup(clusterName)
	if err != nil {
		return err
	}
	if s.pipes == nil {
		return ErrKsqlPipeNotFound
	}
	command, ok := s.pipes.claim(clusterName, pipeID)
	if !ok {
		return ErrKsqlPipeNotFound
	}
	if authorize != nil {
		if err := authorize(command.Kind); err != nil {
			return err
		}
	}
	if s.port == nil {
		return errors.New("ksql port is unavailable")
	}
	return s.port.Execute(ctx, definition, command, emit)
}

func (s *KsqlService) ListStreams(ctx context.Context, clusterName string) ([]domain.KsqlStreamDescription, error) {
	definition, err := s.lookup(clusterName)
	if err != nil {
		return nil, err
	}
	if s.port == nil {
		return nil, errors.New("ksql port is unavailable")
	}
	return s.port.ListStreams(ctx, definition)
}

func (s *KsqlService) ListTables(ctx context.Context, clusterName string) ([]domain.KsqlTableDescription, error) {
	definition, err := s.lookup(clusterName)
	if err != nil {
		return nil, err
	}
	if s.port == nil {
		return nil, errors.New("ksql port is unavailable")
	}
	return s.port.ListTables(ctx, definition)
}

func (s *KsqlService) lookup(clusterName string) (domain.Definition, error) {
	if s == nil || s.resolver == nil {
		return domain.Definition{}, ErrKsqlClusterNotFound
	}
	definition, err := s.resolver.Lookup(clusterName)
	if err != nil {
		return domain.Definition{}, ErrKsqlClusterNotFound
	}
	if strings.TrimSpace(definition.KsqlURL) == "" {
		return domain.Definition{}, ErrKsqlNotConfigured
	}
	return definition, nil
}

func cloneStringMap(in map[string]string) map[string]string {
	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func cloneKsqlCommand(in domain.KsqlCommand) domain.KsqlCommand {
	in.StreamsProperties = cloneStringMap(in.StreamsProperties)
	return in
}
