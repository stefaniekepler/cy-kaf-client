// Package mcppolicy persists the local authorization boundary for the MCP server.
package mcppolicy

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

const CurrentVersion = 1

type Policy struct {
	Version     int  `json:"version"`
	Enabled     bool `json:"enabled"`
	AllowWrites bool `json:"allowWrites"`
}

type Store struct {
	path string
	mu   sync.RWMutex
}

func Default() Policy {
	return Policy{Version: CurrentVersion, Enabled: false, AllowWrites: false}
}

func PathForConfig(configPath string) string {
	return filepath.Join(filepath.Dir(configPath), "mcp-policy.json")
}

func NewStore(path string) *Store {
	return &Store{path: path}
}

func (s *Store) Path() string {
	return s.path
}

func normalize(p Policy) (Policy, error) {
	if p.Version != CurrentVersion {
		return Policy{}, fmt.Errorf("unsupported MCP policy version")
	}
	if !p.Enabled {
		p.AllowWrites = false
	}
	return p, nil
}

func (s *Store) Load() (Policy, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	raw, err := os.ReadFile(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return Default(), nil
	}
	if err != nil {
		return Policy{}, fmt.Errorf("read MCP policy: %w", err)
	}

	var p Policy
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&p); err != nil {
		return Policy{}, errors.New("invalid MCP policy")
	}
	if err := ensureJSONEOF(dec); err != nil {
		return Policy{}, errors.New("invalid MCP policy")
	}
	return normalize(p)
}

func ensureJSONEOF(dec *json.Decoder) error {
	var extra any
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("unexpected trailing JSON")
		}
		return err
	}
	return nil
}

func (s *Store) Save(ctx context.Context, p Policy) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("save MCP policy: %w", err)
	}
	p, err := normalize(p)
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	dir := filepath.Dir(s.path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("create MCP policy dir: %w", err)
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		return fmt.Errorf("secure MCP policy dir: %w", err)
	}
	tmpFile, err := os.CreateTemp(dir, "."+filepath.Base(s.path)+".*.tmp")
	if err != nil {
		return fmt.Errorf("create MCP policy temp file: %w", err)
	}
	tmpPath := tmpFile.Name()
	committed := false
	defer func() {
		if !committed {
			_ = tmpFile.Close()
			_ = os.Remove(tmpPath)
		}
	}()

	if err := tmpFile.Chmod(0o600); err != nil {
		return fmt.Errorf("secure MCP policy temp file: %w", err)
	}
	if err := json.NewEncoder(tmpFile).Encode(p); err != nil {
		return fmt.Errorf("write MCP policy: %w", err)
	}
	if err := tmpFile.Sync(); err != nil {
		return fmt.Errorf("sync MCP policy: %w", err)
	}
	if err := tmpFile.Close(); err != nil {
		return fmt.Errorf("close MCP policy: %w", err)
	}
	if err := replaceFile(tmpPath, s.path); err != nil {
		return fmt.Errorf("replace MCP policy: %w", err)
	}
	committed = true
	return nil
}
