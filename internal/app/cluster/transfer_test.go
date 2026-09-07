package cluster_test

import (
	"context"
	"errors"
	appcluster "github.com/cy-kaf/cy-kaf-client/internal/app/cluster"
	"github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

type transferStore struct {
	stubConfigStore
	current    cluster.ConfigSnapshot
	currentErr error
}

func (s *transferStore) Current() (cluster.ConfigSnapshot, error) { return s.current, s.currentErr }
func (s *transferStore) Save(ctx context.Context, snap cluster.ConfigSnapshot) error {
	if err := s.stubConfigStore.Save(ctx, snap); err != nil {
		return err
	}
	s.current = snap
	return nil
}

type transferCodec struct {
	incoming cluster.ConfigSnapshot
	err      error
}

func (c transferCodec) Parse([]byte) (cluster.ConfigSnapshot, error)  { return c.incoming, c.err }
func (c transferCodec) Export(cluster.ConfigSnapshot) ([]byte, error) { return []byte("yaml"), c.err }
func transferSnapshot(pairs ...string) cluster.ConfigSnapshot {
	defs := []cluster.Definition{}
	entries := []any{}
	for i := 0; i < len(pairs); i += 2 {
		defs = append(defs, cluster.Definition{Name: pairs[i], Conn: conn(pairs[i+1])})
		entries = append(entries, map[string]any{"name": pairs[i], "bootstrapServers": pairs[i+1], "unknown": "preserved"})
	}
	return cluster.ConfigSnapshot{Raw: map[string]any{"server": map[string]any{"port": 8080}, "kafka": map[string]any{"clusters": entries, "custom": "local"}}, Clusters: defs}
}
func transferFixture(t *testing.T, local, incoming cluster.ConfigSnapshot) (*appcluster.ConfigTransferService, *transferStore, *appcluster.Resolver, context.Context) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	store := &transferStore{current: local}
	res := appcluster.NewResolver(local.Clusters)
	life := &recordingLifecycle{}
	states := appcluster.NewStateCache(res, &fakeState{}, life, time.Hour)
	rl := appcluster.NewReloader(res, life, states, store)
	return appcluster.NewConfigTransferService(store, transferCodec{incoming: incoming}, rl), store, res, ctx
}
func TestTransferPreviewMatchesNameOrEntireAddressAndKeepsAllTargets(t *testing.T) {
	svc, store, _, _ := transferFixture(t, transferSnapshot("same", "a:9092", "alias", "b:9092", "port", "b:9093"), transferSnapshot("same", "b:9092", "new", "c:9092"))
	p, err := svc.Preview(nil)
	require.NoError(t, err)
	require.Len(t, p.Entries, 2)
	require.Len(t, p.Entries[0].Conflicts, 2)
	require.Equal(t, "name", p.Entries[0].Conflicts[0].Reason)
	require.Equal(t, "address", p.Entries[0].Conflicts[1].Reason)
	require.Empty(t, p.Entries[1].Conflicts)
	require.False(t, store.sawSave())
}
func TestTransferApplySelectedReplacesAllMatchedLocalsAndPreservesOtherSettings(t *testing.T) {
	svc, store, res, ctx := transferFixture(t, transferSnapshot("same", "a:9092", "alias", "b:9092", "keep", "c:9092"), transferSnapshot("same", "b:9092", "added", "d:9092", "skip", "e:9092"))
	p, err := svc.Preview(nil)
	require.NoError(t, err)
	result, err := svc.Import(ctx, nil, []int{0, 1}, p.Revision)
	require.NoError(t, err)
	require.Equal(t, cluster.ConfigImportResult{Added: 1, Replaced: 2, Skipped: 1}, result)
	require.Equal(t, []cluster.Definition{transferSnapshot("keep", "c:9092").Clusters[0], transferSnapshot("same", "b:9092").Clusters[0], transferSnapshot("added", "d:9092").Clusters[0]}, res.Definitions())
	require.Equal(t, map[string]any{"port": 8080}, store.current.Raw["server"])
	require.Equal(t, "local", store.current.Raw["kafka"].(map[string]any)["custom"])
	require.Equal(t, "preserved", store.current.Raw["kafka"].(map[string]any)["clusters"].([]any)[1].(map[string]any)["unknown"])
}
func TestTransferSkippedConflictKeepsLocalAndNoSelectionDoesNotSave(t *testing.T) {
	local := transferSnapshot("same", "a:9092")
	svc, store, _, ctx := transferFixture(t, local, transferSnapshot("same", "b:9092"))
	p, err := svc.Preview(nil)
	require.NoError(t, err)
	result, err := svc.Import(ctx, nil, []int{}, p.Revision)
	require.NoError(t, err)
	require.Equal(t, 1, result.Skipped)
	require.False(t, store.sawSave())
	require.Equal(t, local, store.current)
}
func TestTransferRejectsStalePreviewAndInvalidOrOverlappingSelections(t *testing.T) {
	for _, selected := range [][]int{{-1}, {2}, {0, 0}, {0, 1}} {
		svc, store, _, ctx := transferFixture(t, transferSnapshot("local", "a:9092"), transferSnapshot("first", "a:9092", "second", "a:9092"))
		p, err := svc.Preview(nil)
		require.NoError(t, err)
		require.Equal(t, []int{1}, p.Entries[0].Overlaps)
		_, err = svc.Import(ctx, nil, selected, p.Revision)
		require.Error(t, err)
		require.False(t, store.sawSave())
	}
	svc, store, _, ctx := transferFixture(t, transferSnapshot("old", "a:9092"), transferSnapshot("new", "b:9092"))
	p, err := svc.Preview(nil)
	require.NoError(t, err)
	store.current = transferSnapshot("edited", "a:9092")
	_, err = svc.Import(ctx, nil, []int{0}, p.Revision)
	require.ErrorIs(t, err, cluster.ErrConfigChanged)
	require.False(t, store.sawSave())
}
func TestTransferPersistenceFailureLeavesRuntimeUnchanged(t *testing.T) {
	local := transferSnapshot("local", "a:9092")
	svc, store, res, ctx := transferFixture(t, local, transferSnapshot("new", "b:9092"))
	p, err := svc.Preview(nil)
	require.NoError(t, err)
	store.saveErr = errors.New("disk full")
	_, err = svc.Import(ctx, nil, []int{0}, p.Revision)
	require.Error(t, err)
	require.Equal(t, local, store.current)
	require.Equal(t, local.Clusters, res.Definitions())
}
