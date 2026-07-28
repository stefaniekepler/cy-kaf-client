package cluster

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
)

// fakeConfigStore is a deterministic cluster.ConfigStorePort: Current/Validate
// return caller-preset values, and Validate records the snapshot it received so
// passthrough can be asserted. Save/SaveRelatedFile are unused by Task 13's
// ConfigService and stubbed to satisfy the interface.
type fakeConfigStore struct {
	current     cluster.ConfigSnapshot
	currentErr  error
	validation  cluster.ConfigValidation
	validateErr error
	lastSnap    cluster.ConfigSnapshot

	relatedLoc     string
	relatedErr     error
	lastRelName    string
	lastRelContent []byte
}

func (f *fakeConfigStore) Current() (cluster.ConfigSnapshot, error) { return f.current, f.currentErr }

func (f *fakeConfigStore) Validate(_ context.Context, snap cluster.ConfigSnapshot) (cluster.ConfigValidation, error) {
	f.lastSnap = snap
	return f.validation, f.validateErr
}

func (f *fakeConfigStore) Save(context.Context, cluster.ConfigSnapshot) error { return nil }
func (f *fakeConfigStore) SaveRelatedFile(_ context.Context, name string, content []byte) (string, error) {
	f.lastRelName, f.lastRelContent = name, content
	return f.relatedLoc, f.relatedErr
}

func TestConfigServiceCurrentPassesThroughSnapshot(t *testing.T) {
	store := &fakeConfigStore{current: cluster.ConfigSnapshot{
		Raw:      map[string]any{"kafka": map[string]any{}},
		Clusters: []cluster.Definition{{Name: "prod"}},
	}}
	svc := NewConfigService(store)

	snap, err := svc.Current()
	require.NoError(t, err)
	require.Len(t, snap.Clusters, 1)
	require.Equal(t, "prod", snap.Clusters[0].Name)
	require.Contains(t, snap.Raw, "kafka")
}

func TestConfigServiceCurrentPropagatesError(t *testing.T) {
	store := &fakeConfigStore{currentErr: errors.New("read config: boom")}
	svc := NewConfigService(store)

	_, err := svc.Current()
	require.Error(t, err)
	require.Contains(t, err.Error(), "boom")
}

func TestConfigServiceValidatePassesSnapshotAndReturnsVerdict(t *testing.T) {
	store := &fakeConfigStore{validation: cluster.ConfigValidation{
		Clusters: map[string]cluster.ClusterValidation{
			"prod": {Kafka: cluster.PropertyValidation{Error: true, ErrorMessage: "unreachable"}},
		},
	}}
	svc := NewConfigService(store)

	in := cluster.ConfigSnapshot{Clusters: []cluster.Definition{{Name: "prod"}}}
	val, err := svc.Validate(context.Background(), in)
	require.NoError(t, err)
	require.Equal(t, in.Clusters, store.lastSnap.Clusters, "the snapshot must reach the store unchanged")
	require.True(t, val.Clusters["prod"].Kafka.Error)
	require.Equal(t, "unreachable", val.Clusters["prod"].Kafka.ErrorMessage)
}

func TestConfigServiceValidatePropagatesError(t *testing.T) {
	store := &fakeConfigStore{validateErr: errors.New("probe wiring boom")}
	svc := NewConfigService(store)

	_, err := svc.Validate(context.Background(), cluster.ConfigSnapshot{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "boom")
}

func TestConfigServiceSaveRelatedFilePassesThroughToStore(t *testing.T) {
	store := &fakeConfigStore{relatedLoc: "/cfg/uploads/trust.pem"}
	svc := NewConfigService(store)

	loc, err := svc.SaveRelatedFile(context.Background(), "trust.pem", []byte("PEM"))
	require.NoError(t, err)
	require.Equal(t, "/cfg/uploads/trust.pem", loc)
	require.Equal(t, "trust.pem", store.lastRelName)
	require.Equal(t, []byte("PEM"), store.lastRelContent)
}

func TestConfigServiceSaveRelatedFilePropagatesError(t *testing.T) {
	store := &fakeConfigStore{relatedErr: errors.New("invalid related file name \"../evil\"")}
	svc := NewConfigService(store)

	_, err := svc.SaveRelatedFile(context.Background(), "../evil", []byte("x"))
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid related file name")
}
