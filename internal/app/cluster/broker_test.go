package cluster_test

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	appcluster "github.com/cy-kaf/cy-kaf-client/internal/app/cluster"
	"github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
)

// fakeBrokerAdmin implements cluster.BrokerAdminPort for BrokerService tests.
// Its fields/methods are a mechanical split of the pre-refactor fakeState
// (see state_test.go): everything StateScraper/ClientLifecycle-only (calls,
// fail, FetchState/Invalidate/Close) stayed there; everything
// BrokerAdminPort-only moved here unchanged.
type fakeBrokerAdmin struct {
	// LogDirs call recording: lastLogDirsDef/lastLogDirsBrokers capture the
	// arguments BrokerService.LogDirs forwarded to the port, so tests can
	// assert the name->Definition lookup happened correctly.
	logDirsCalls   atomic.Int32
	lastLogDirsDef cluster.Definition
	logDirsResult  []cluster.BrokerLogDirs
	logDirsErr     error

	// BrokerConfigs call recording (mirrors LogDirs fields above).
	brokerConfigsCalls   atomic.Int32
	lastBrokerConfigsDef cluster.Definition
	brokerConfigsResult  []cluster.ConfigEntry
	brokerConfigsErr     error

	// AlterBrokerConfig call recording: captures every forwarded argument so
	// tests can assert BrokerService passed the name->Definition lookup
	// result and the broker/name/value triple through unchanged.
	alterConfigCalls atomic.Int32
	lastAlterDef     cluster.Definition
	lastAlterBroker  int32
	lastAlterName    string
	lastAlterValue   string
	alterConfigErr   error

	// MoveReplicaLogDir call recording, same shape as AlterBrokerConfig above.
	moveLogDirCalls   atomic.Int32
	lastMoveDef       cluster.Definition
	lastMoveBroker    int32
	lastMoveTopic     string
	lastMovePartition int32
	lastMoveDir       string
	moveLogDirErr     error
}

func (f *fakeBrokerAdmin) LogDirs(_ context.Context, def cluster.Definition, _ []int32) ([]cluster.BrokerLogDirs, error) {
	f.logDirsCalls.Add(1)
	f.lastLogDirsDef = def
	return f.logDirsResult, f.logDirsErr
}

func (f *fakeBrokerAdmin) BrokerConfigs(_ context.Context, def cluster.Definition, _ int32) ([]cluster.ConfigEntry, error) {
	f.brokerConfigsCalls.Add(1)
	f.lastBrokerConfigsDef = def
	return f.brokerConfigsResult, f.brokerConfigsErr
}

func (f *fakeBrokerAdmin) AlterBrokerConfig(_ context.Context, def cluster.Definition, broker int32, name, value string) error {
	f.alterConfigCalls.Add(1)
	f.lastAlterDef = def
	f.lastAlterBroker = broker
	f.lastAlterName = name
	f.lastAlterValue = value
	return f.alterConfigErr
}

func (f *fakeBrokerAdmin) MoveReplicaLogDir(_ context.Context, def cluster.Definition, broker int32, topic string, partition int32, dir string) error {
	f.moveLogDirCalls.Add(1)
	f.lastMoveDef = def
	f.lastMoveBroker = broker
	f.lastMoveTopic = topic
	f.lastMovePartition = partition
	f.lastMoveDir = dir
	return f.moveLogDirErr
}

// TestBrokerServiceLogDirsDelegatesToPortForKnownCluster covers LogDirs'
// happy path: name resolves to the configured Definition, which
// BrokerService must forward to the port unchanged (same "find by name"
// lookup StateCache.Refresh uses, via Resolver), returning whatever the port
// returns.
func TestBrokerServiceLogDirsDelegatesToPortForKnownCluster(t *testing.T) {
	want := []cluster.BrokerLogDirs{{Broker: 1, Dir: "/kafka/data", Topics: []cluster.TopicLogDirs{
		{Topic: "t1", Partitions: []cluster.PartitionLogDir{{Partition: 0, Size: 100}}},
	}}}
	fb := &fakeBrokerAdmin{logDirsResult: want}
	def := cluster.Definition{Name: "a", Conn: cluster.ConnectionSpec{BootstrapServers: []string{"a:1"}}}
	res := appcluster.NewResolver([]cluster.Definition{def})
	bs := appcluster.NewBrokerService(res, fb)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	got, err := bs.LogDirs(ctx, "a", []int32{1})
	require.NoError(t, err)
	require.Equal(t, want, got)
	require.Equal(t, int32(1), fb.logDirsCalls.Load())
	require.Equal(t, def, fb.lastLogDirsDef) // name->Definition lookup forwarded the right Definition
}

// TestBrokerServiceLogDirsUnknownClusterErrors mirrors Refresh's
// unknown-cluster behaviour: LogDirs must not call the port at all for a
// name outside the configured definitions.
func TestBrokerServiceLogDirsUnknownClusterErrors(t *testing.T) {
	fb := &fakeBrokerAdmin{}
	res := appcluster.NewResolver([]cluster.Definition{{Name: "a", Conn: cluster.ConnectionSpec{BootstrapServers: []string{"a:1"}}}})
	bs := appcluster.NewBrokerService(res, fb)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	got, err := bs.LogDirs(ctx, "nope", nil)
	require.Error(t, err)
	require.Nil(t, got)
	require.Equal(t, int32(0), fb.logDirsCalls.Load())
}

// TestBrokerServiceBrokerConfigsDelegatesToPortForKnownCluster mirrors
// TestBrokerServiceLogDirsDelegatesToPortForKnownCluster for BrokerConfigs.
func TestBrokerServiceBrokerConfigsDelegatesToPortForKnownCluster(t *testing.T) {
	want := []cluster.ConfigEntry{{Name: "log.retention.ms", Value: "604800000", Source: "DYNAMIC_BROKER_CONFIG"}}
	fb := &fakeBrokerAdmin{brokerConfigsResult: want}
	def := cluster.Definition{Name: "a", Conn: cluster.ConnectionSpec{BootstrapServers: []string{"a:1"}}}
	res := appcluster.NewResolver([]cluster.Definition{def})
	bs := appcluster.NewBrokerService(res, fb)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	got, err := bs.BrokerConfigs(ctx, "a", 1)
	require.NoError(t, err)
	require.Equal(t, want, got)
	require.Equal(t, int32(1), fb.brokerConfigsCalls.Load())
	require.Equal(t, def, fb.lastBrokerConfigsDef)
}

// TestBrokerServiceBrokerConfigsUnknownClusterErrors mirrors
// TestBrokerServiceLogDirsUnknownClusterErrors for BrokerConfigs.
func TestBrokerServiceBrokerConfigsUnknownClusterErrors(t *testing.T) {
	fb := &fakeBrokerAdmin{}
	res := appcluster.NewResolver([]cluster.Definition{{Name: "a", Conn: cluster.ConnectionSpec{BootstrapServers: []string{"a:1"}}}})
	bs := appcluster.NewBrokerService(res, fb)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	got, err := bs.BrokerConfigs(ctx, "nope", 1)
	require.Error(t, err)
	require.Nil(t, got)
	require.Equal(t, int32(0), fb.brokerConfigsCalls.Load())
}

// TestBrokerServiceAlterBrokerConfigDelegatesToPortForKnownCluster asserts
// BrokerService.AlterBrokerConfig forwards the resolved Definition plus the
// broker/name/value triple unchanged.
func TestBrokerServiceAlterBrokerConfigDelegatesToPortForKnownCluster(t *testing.T) {
	fb := &fakeBrokerAdmin{}
	def := cluster.Definition{Name: "a", Conn: cluster.ConnectionSpec{BootstrapServers: []string{"a:1"}}}
	res := appcluster.NewResolver([]cluster.Definition{def})
	bs := appcluster.NewBrokerService(res, fb)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := bs.AlterBrokerConfig(ctx, "a", 1, "log.retention.ms", "1000")
	require.NoError(t, err)
	require.Equal(t, int32(1), fb.alterConfigCalls.Load())
	require.Equal(t, def, fb.lastAlterDef)
	require.Equal(t, int32(1), fb.lastAlterBroker)
	require.Equal(t, "log.retention.ms", fb.lastAlterName)
	require.Equal(t, "1000", fb.lastAlterValue)
}

// TestBrokerServiceAlterBrokerConfigUnknownClusterErrors mirrors
// TestBrokerServiceLogDirsUnknownClusterErrors for AlterBrokerConfig.
func TestBrokerServiceAlterBrokerConfigUnknownClusterErrors(t *testing.T) {
	fb := &fakeBrokerAdmin{}
	res := appcluster.NewResolver([]cluster.Definition{{Name: "a", Conn: cluster.ConnectionSpec{BootstrapServers: []string{"a:1"}}}})
	bs := appcluster.NewBrokerService(res, fb)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := bs.AlterBrokerConfig(ctx, "nope", 1, "x", "y")
	require.Error(t, err)
	require.Equal(t, int32(0), fb.alterConfigCalls.Load())
}

// TestBrokerServiceMoveReplicaLogDirDelegatesToPortForKnownCluster asserts
// BrokerService.MoveReplicaLogDir forwards the resolved Definition plus the
// broker/topic/partition/dir quadruple unchanged.
func TestBrokerServiceMoveReplicaLogDirDelegatesToPortForKnownCluster(t *testing.T) {
	fb := &fakeBrokerAdmin{}
	def := cluster.Definition{Name: "a", Conn: cluster.ConnectionSpec{BootstrapServers: []string{"a:1"}}}
	res := appcluster.NewResolver([]cluster.Definition{def})
	bs := appcluster.NewBrokerService(res, fb)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := bs.MoveReplicaLogDir(ctx, "a", 1, "t1", 0, "/kafka/data-1")
	require.NoError(t, err)
	require.Equal(t, int32(1), fb.moveLogDirCalls.Load())
	require.Equal(t, def, fb.lastMoveDef)
	require.Equal(t, int32(1), fb.lastMoveBroker)
	require.Equal(t, "t1", fb.lastMoveTopic)
	require.Equal(t, int32(0), fb.lastMovePartition)
	require.Equal(t, "/kafka/data-1", fb.lastMoveDir)
}

// TestBrokerServiceMoveReplicaLogDirUnknownClusterErrors mirrors
// TestBrokerServiceLogDirsUnknownClusterErrors for MoveReplicaLogDir.
func TestBrokerServiceMoveReplicaLogDirUnknownClusterErrors(t *testing.T) {
	fb := &fakeBrokerAdmin{}
	res := appcluster.NewResolver([]cluster.Definition{{Name: "a", Conn: cluster.ConnectionSpec{BootstrapServers: []string{"a:1"}}}})
	bs := appcluster.NewBrokerService(res, fb)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := bs.MoveReplicaLogDir(ctx, "nope", 1, "t1", 0, "/kafka/data-1")
	require.Error(t, err)
	require.Equal(t, int32(0), fb.moveLogDirCalls.Load())
}
