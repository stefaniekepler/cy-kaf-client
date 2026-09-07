package cluster

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
)

type ConfigTransferService struct {
	store    cluster.ConfigStorePort
	codec    cluster.ConfigTransferCodec
	reloader *Reloader
}

func NewConfigTransferService(store cluster.ConfigStorePort, codec cluster.ConfigTransferCodec, reloader *Reloader) *ConfigTransferService {
	return &ConfigTransferService{store: store, codec: codec, reloader: reloader}
}
func (s *ConfigTransferService) Export() ([]byte, error) {
	current, err := s.store.Current()
	if err != nil {
		return nil, err
	}
	return s.codec.Export(current)
}
func (s *ConfigTransferService) Preview(content []byte) (cluster.ConfigImportPreview, error) {
	incoming, err := s.codec.Parse(content)
	if err != nil {
		return cluster.ConfigImportPreview{}, err
	}
	current, err := s.store.Current()
	if err != nil {
		return cluster.ConfigImportPreview{}, err
	}
	return importPreview(current, incoming, content)
}
func revision(snap cluster.ConfigSnapshot, content []byte) (string, error) {
	data, err := json.Marshal([]any{snap.Raw, string(content)})
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", sha256.Sum256(data)), nil
}
func conflictReason(a, b cluster.Definition) string {
	name := a.Name == b.Name
	address := strings.Join(a.Conn.BootstrapServers, ",") == strings.Join(b.Conn.BootstrapServers, ",")
	if name && address {
		return "name_and_address"
	}
	if name {
		return "name"
	}
	if address {
		return "address"
	}
	return ""
}
func importPreview(current, incoming cluster.ConfigSnapshot, content []byte) (cluster.ConfigImportPreview, error) {
	rev, err := revision(current, content)
	if err != nil {
		return cluster.ConfigImportPreview{}, err
	}
	p := cluster.ConfigImportPreview{Revision: rev, Entries: make([]cluster.ConfigImportEntry, 0, len(incoming.Clusters))}
	for i, c := range incoming.Clusters {
		entry := cluster.ConfigImportEntry{Index: i, Name: c.Name, BootstrapServers: strings.Join(c.Conn.BootstrapServers, ","), Conflicts: []cluster.ConfigImportConflict{}, Overlaps: []int{}}
		for _, local := range current.Clusters {
			if reason := conflictReason(c, local); reason != "" {
				entry.Conflicts = append(entry.Conflicts, cluster.ConfigImportConflict{Name: local.Name, BootstrapServers: strings.Join(local.Conn.BootstrapServers, ","), Reason: reason})
			}
		}
		p.Entries = append(p.Entries, entry)
	}
	for i := range p.Entries {
		for j := i + 1; j < len(p.Entries); j++ {
			overlap := conflictReason(incoming.Clusters[i], incoming.Clusters[j]) != ""
			for _, a := range p.Entries[i].Conflicts {
				for _, b := range p.Entries[j].Conflicts {
					if a.Name == b.Name {
						overlap = true
					}
				}
			}
			if overlap {
				p.Entries[i].Overlaps = append(p.Entries[i].Overlaps, j)
				p.Entries[j].Overlaps = append(p.Entries[j].Overlaps, i)
			}
		}
	}
	return p, nil
}

// Import rechecks the reviewed snapshot under the same lock as regular saves.
// Selected incoming entries replace every matching local entry as one write.
func (s *ConfigTransferService) Import(ctx context.Context, content []byte, selected []int, expectedRevision string) (cluster.ConfigImportResult, error) {
	result := cluster.ConfigImportResult{}
	incoming, err := s.codec.Parse(content)
	if err != nil {
		return result, err
	}
	s.reloader.applyMu.Lock()
	defer s.reloader.applyMu.Unlock()
	current, err := s.store.Current()
	if err != nil {
		return result, err
	}
	preview, err := importPreview(current, incoming, content)
	if err != nil {
		return result, err
	}
	if preview.Revision != expectedRevision {
		return result, cluster.ErrConfigChanged
	}
	chosen := map[int]bool{}
	for _, index := range selected {
		if index < 0 || index >= len(incoming.Clusters) || chosen[index] {
			return result, &cluster.ConfigImportError{Message: "导入选择无效"}
		}
		chosen[index] = true
	}
	replaced := map[string]bool{}
	for index := range chosen {
		entry := preview.Entries[index]
		for _, overlap := range entry.Overlaps {
			if chosen[overlap] {
				return result, &cluster.ConfigImportError{Message: "选中的导入环境互相冲突，请只选择其中一个"}
			}
		}
		if len(entry.Conflicts) == 0 {
			result.Added++
		}
		for _, local := range entry.Conflicts {
			replaced[local.Name] = true
		}
	}
	result.Replaced = len(replaced)
	result.Skipped = len(incoming.Clusters) - len(chosen)
	if len(chosen) == 0 {
		return result, nil
	}
	merged := mergeImported(current, incoming, chosen, replaced)
	if err := s.reloader.applyLocked(ctx, merged); err != nil {
		return cluster.ConfigImportResult{}, err
	}
	return result, nil
}
func mergeImported(current, incoming cluster.ConfigSnapshot, chosen map[int]bool, replaced map[string]bool) cluster.ConfigSnapshot {
	raw := make(map[string]any, len(current.Raw))
	for k, v := range current.Raw {
		raw[k] = v
	}
	kafka := map[string]any{}
	if old, ok := current.Raw["kafka"].(map[string]any); ok {
		for k, v := range old {
			kafka[k] = v
		}
	}
	oldEntries, _ := kafka["clusters"].([]any)
	newEntries := incoming.Raw["kafka"].(map[string]any)["clusters"].([]any)
	entries := []any{}
	defs := []cluster.Definition{}
	for i, c := range current.Clusters {
		if !replaced[c.Name] {
			entries = append(entries, oldEntries[i])
			defs = append(defs, c)
		}
	}
	for i, c := range incoming.Clusters {
		if chosen[i] {
			entries = append(entries, newEntries[i])
			defs = append(defs, c)
		}
	}
	kafka["clusters"] = entries
	raw["kafka"] = kafka
	return cluster.ConfigSnapshot{Raw: raw, Clusters: defs}
}
