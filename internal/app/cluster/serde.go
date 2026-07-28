package cluster

import (
	"context"

	"github.com/cy-kaf/cy-kaf-client/internal/domain/serde"
)

// SerdeService resolves a cluster name then delegates straight to a
// serde.Provider for getSerdes' key/value suggestion list. Same "no
// StateCache" shape as GroupService (see its doc comment): Suggest is a
// pure, in-memory computation over the resolved cluster.Definition (no
// Kafka I/O at all today), so there's nothing here that would benefit from a
// periodically-refreshed cache.
type SerdeService struct {
	res      *Resolver
	provider serde.Provider
}

// NewSerdeService builds a SerdeService resolving cluster names through res
// and computing suggestions/lookups through provider (production: infra/
// serde.NewProvider's built-in-registry-backed implementation).
func NewSerdeService(res *Resolver, provider serde.Provider) *SerdeService {
	return &SerdeService{res: res, provider: provider}
}

// Suggest resolves name then reports topic's key/value serde suggestions for
// use (SERIALIZE/DESERIALIZE) via the configured serde.Provider.
func (s *SerdeService) Suggest(ctx context.Context, name, topic string, use serde.Usage) (serde.Suggestion, error) {
	def, err := s.res.Lookup(name)
	if err != nil {
		return serde.Suggestion{}, err
	}
	return s.provider.Suggest(def, topic, use), nil
}
