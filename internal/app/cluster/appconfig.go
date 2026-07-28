// appconfig.go is the app-layer wrapper the config-wizard api handlers
// (getCurrentConfig / validateConfig, P1c Task 13) consume: a thin passthrough
// over the domain cluster.ConfigStorePort, holding no state of its own. The
// messy generated.ApplicationConfig <-> ConfigSnapshot mapping lives in the api
// layer; the connectivity-probing lives in the infra store; this layer just
// wires a name-resolving-free read/validate path in the app's own package.
package cluster

import (
	"context"

	"github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
)

// ConfigService exposes the read + validate half of the config wizard over a
// ConfigStorePort (production: *infra/config.Store).
type ConfigService struct {
	store cluster.ConfigStorePort
}

// NewConfigService wraps store.
func NewConfigService(store cluster.ConfigStorePort) *ConfigService {
	return &ConfigService{store: store}
}

// Current reads the running configuration snapshot.
func (s *ConfigService) Current() (cluster.ConfigSnapshot, error) {
	return s.store.Current()
}

// Validate probes snap's clusters and returns the per-cluster verdict.
func (s *ConfigService) Validate(ctx context.Context, snap cluster.ConfigSnapshot) (cluster.ConfigValidation, error) {
	return s.store.Validate(ctx, snap)
}

// SaveRelatedFile stores an uploaded truststore/keystore/etc. and returns its
// on-disk location (config wizard file upload, P1c Task 14). A straight
// passthrough to the store, which owns the uploads dir + path-traversal guard.
func (s *ConfigService) SaveRelatedFile(ctx context.Context, name string, content []byte) (string, error) {
	return s.store.SaveRelatedFile(ctx, name, content)
}
