// store.go implements the domain ConfigStorePort over the on-disk config.yaml
// (P1c Task 13: read + probe). The write side (Save/SaveRelatedFile) lands in
// Task 14; until then those two report a not-yet-implemented error.
package config

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
)

// probeTimeout bounds a single cluster's connectivity probe so an unreachable
// broker fails the validation fast rather than hanging on the kgo client's own
// dial retries. Applied per cluster on top of whatever deadline the caller's
// ctx already carries (whichever fires first wins).
const probeTimeout = 10 * time.Second

// ProbeFunc tests one cluster's Kafka reachability, returning nil when
// reachable. Injected (rather than importing infra/kafka directly) so the store
// stays decoupled from the client layer and unit-testable with a fake probe;
// production wires infra/kafka.ProbeConnectivity.
type ProbeFunc func(context.Context, cluster.Definition) error

// Store reads/validates the config at path, probing connectivity through probe.
type Store struct {
	path  string
	probe ProbeFunc
}

// NewStore builds a Store over the config file at path, probing reachability
// through probe (production: infra/kafka.ProbeConnectivity).
func NewStore(path string, probe ProbeFunc) *Store {
	return &Store{path: path, probe: probe}
}

// Current reads config.yaml into a ConfigSnapshot: Raw is the whole file
// decoded into a generic tree (kept verbatim for a future Save to merge edits
// back without dropping unmodeled fields), Clusters is kafka.clusters[] parsed
// to domain Definitions through the same ToDomain the loader uses.
func (s *Store) Current() (cluster.ConfigSnapshot, error) {
	raw, err := os.ReadFile(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return cluster.ConfigSnapshot{
			Raw: map[string]any{
				"kafka": map[string]any{
					"clusters": []any{},
				},
			},
			Clusters: []cluster.Definition{},
		}, nil
	}
	if err != nil {
		return cluster.ConfigSnapshot{}, fmt.Errorf("read config: %w", err)
	}
	return parseConfig(raw)
}

// Parse decodes + schema-validates data (no file I/O). See parseConfig.
func (s *Store) Parse(data []byte) (cluster.ConfigSnapshot, error) {
	return parseConfig(data)
}

// Backup copies the current config file to a timestamped sibling .bak and
// returns that path; when no config file exists it returns "" (no error).
func (s *Store) Backup() (string, error) {
	if _, err := os.Stat(s.path); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return "", nil
		}
		return "", fmt.Errorf("stat config: %w", err)
	}
	data, err := os.ReadFile(s.path)
	if err != nil {
		return "", fmt.Errorf("read config for backup: %w", err)
	}
	backup := s.path + "." + time.Now().Format("2006-01-02T15-04-05") + ".bak"
	if err := os.WriteFile(backup, data, 0o600); err != nil {
		return "", fmt.Errorf("write config backup: %w", err)
	}
	return backup, nil
}

// Validate probes each cluster's Kafka reachability and reports a per-cluster
// verdict keyed by name. P1c scope: Kafka connectivity only -- the P2
// subsystems (schema registry, connect, ksqldb, prometheus) are left nil
// (unprobed), matching upstream's "don't validate a component that isn't
// there". A probe failure becomes Kafka.Error=true with the failure text;
// success leaves the zero (Error=false) verdict.
func (s *Store) Validate(ctx context.Context, snap cluster.ConfigSnapshot) (cluster.ConfigValidation, error) {
	out := cluster.ConfigValidation{Clusters: make(map[string]cluster.ClusterValidation, len(snap.Clusters))}
	for _, def := range snap.Clusters {
		cv := cluster.ClusterValidation{}
		pctx, cancel := context.WithTimeout(ctx, probeTimeout)
		err := s.probe(pctx, def)
		cancel()
		if err != nil {
			cv.Kafka = cluster.PropertyValidation{Error: true, ErrorMessage: err.Error()}
		}
		out.Clusters[def.Name] = cv
	}
	return out, nil
}

// Save persists snap.Raw back to config.yaml atomically (temp file + rename),
// writing the whole tree verbatim so fields this codebase doesn't model
// (auth/rbac/webclient/...) survive the round trip -- the reload path uses
// snap.Clusters for the runtime defs, but the on-disk truth is snap.Raw. A nil
// Raw is refused rather than truncating the file to an empty document.
func (s *Store) Save(ctx context.Context, snap cluster.ConfigSnapshot) error {
	if snap.Raw == nil {
		return errors.New("save config: empty snapshot")
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("save config: %w", err)
	}
	data, err := yaml.Marshal(snap.Raw)
	if err != nil {
		return fmt.Errorf("marshal config: %w", err)
	}
	dir := filepath.Dir(s.path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("create config dir: %w", err)
	}
	tmpFile, err := os.CreateTemp(dir, "."+filepath.Base(s.path)+".*.tmp")
	if err != nil {
		return fmt.Errorf("create config temp file: %w", err)
	}
	tmp := tmpFile.Name()
	cleanup := func() {
		_ = tmpFile.Close()
		_ = os.Remove(tmp)
	}
	if err := tmpFile.Chmod(0o600); err != nil {
		cleanup()
		return fmt.Errorf("secure config temp file: %w", err)
	}
	if _, err := tmpFile.Write(data); err != nil {
		cleanup()
		return fmt.Errorf("write config: %w", err)
	}
	if err := tmpFile.Sync(); err != nil {
		cleanup()
		return fmt.Errorf("sync config: %w", err)
	}
	if err := tmpFile.Close(); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("close config: %w", err)
	}
	if err := os.Rename(tmp, s.path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("replace config: %w", err)
	}
	return nil
}

// SaveRelatedFile stores an uploaded truststore/keystore/etc. under an uploads/
// directory beside config.yaml and returns its on-disk location. name is a bare
// filename: any directory component, parent reference, absolute path, "." or
// ".." is rejected before anything is written (path-traversal guard) so an
// upload can never escape the uploads dir.
func (s *Store) SaveRelatedFile(ctx context.Context, name string, content []byte) (string, error) {
	if name == "" || filepath.Base(name) != name || name == "." || name == ".." {
		return "", fmt.Errorf("invalid related file name %q", name)
	}
	dir := filepath.Join(filepath.Dir(s.path), "uploads")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", fmt.Errorf("create uploads dir: %w", err)
	}
	dest := filepath.Join(dir, name)
	if err := os.WriteFile(dest, content, 0o600); err != nil {
		return "", fmt.Errorf("write related file: %w", err)
	}
	return dest, nil
}

var _ cluster.ConfigStorePort = (*Store)(nil)
