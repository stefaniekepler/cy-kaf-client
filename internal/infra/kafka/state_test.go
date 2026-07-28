package kafka

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"

	"github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
)

// TestDetectKafkaVersion catches a missing config-priority branch, version
// normalization regression, or fallback error being silently treated as a
// version. Its callback isolates the external API request while exercising the
// detector's observable result.
func TestDetectKafkaVersion(t *testing.T) {
	t.Run("broker config wins without probing", func(t *testing.T) {
		calls := 0
		got, err := detectKafkaVersion(context.Background(),
			[]cluster.ConfigEntry{{Name: "inter.broker.protocol.version", Value: "v3.9-IV0"}},
			func(context.Context) (string, error) {
				calls++
				return "v3.8", nil
			})
		require.NoError(t, err)
		require.Equal(t, "3.9", got)
		require.Zero(t, calls)
	})

	t.Run("API versions is the fallback", func(t *testing.T) {
		got, err := detectKafkaVersion(context.Background(), nil,
			func(context.Context) (string, error) { return "v3.8", nil })
		require.NoError(t, err)
		require.Equal(t, "3.8", got)
	})

	t.Run("probe failure is returned for the caller to degrade", func(t *testing.T) {
		got, err := detectKafkaVersion(context.Background(), nil,
			func(context.Context) (string, error) { return "", errors.New("unavailable") })
		require.Error(t, err)
		require.Empty(t, got)
	})
}

// TestTallyPartitionsCountsAcrossTopics 用手工构造的 kadm.Metadata 字面量覆盖
// online/offline/under-replicated 三态，纯函数、无网络依赖。同时覆盖 per-broker 归属：
// perBroker 故意不含 broker 3（t1p0/t1p1 的 replica、t1p0 的 ISR 都引用了它），断言未命中
// 分支静默跳过（不 panic、不影响其它 broker 的计数）。
func TestTallyPartitionsCountsAcrossTopics(t *testing.T) {
	meta := kadm.Metadata{
		Topics: kadm.TopicDetails{
			"t1": {
				Topic: "t1",
				Partitions: kadm.PartitionDetails{
					// online, fully in sync: 3 replicas, 3 ISR
					0: {Partition: 0, Leader: 1, Replicas: []int32{1, 2, 3}, ISR: []int32{1, 2, 3}},
					// offline (no leader) + under-replicated: 3 replicas, 1 ISR
					1: {Partition: 1, Leader: -1, Replicas: []int32{1, 2, 3}, ISR: []int32{1}},
				},
			},
			"t2": {
				Topic: "t2",
				Partitions: kadm.PartitionDetails{
					// online, fully in sync: 2 replicas, 2 ISR
					0: {Partition: 0, Leader: 2, Replicas: []int32{1, 2}, ISR: []int32{1, 2}},
				},
			},
		},
	}
	b1 := &cluster.BrokerInfo{ID: 1}
	b2 := &cluster.BrokerInfo{ID: 2}
	perBroker := map[int32]*cluster.BrokerInfo{1: b1, 2: b2} // broker 3 deliberately absent

	topics, pc, topicStates := tallyPartitions(meta, perBroker)

	require.Equal(t, 2, topics)
	require.Equal(t, 2, pc.Online)          // t1p0 + t2p0
	require.Equal(t, 1, pc.Offline)         // t1p1 (Leader<0)
	require.Equal(t, 1, pc.UnderReplicated) // t1p1 only (len(ISR)<len(Replicas))
	require.Equal(t, 6, pc.InSync)          // 3+1+2
	require.Equal(t, 2, pc.OutOfSync)       // 0+2+0

	// broker 1: leader of t1p0 only; replica of all 3 partitions; ISR of all 3.
	require.Equal(t, 1, b1.PartitionsLeader)
	require.Equal(t, 3, b1.Partitions)
	require.Equal(t, 3, b1.InSyncPartitions)
	// broker 2: leader of t2p0 only; replica of all 3; ISR of t1p0+t2p0 (not t1p1).
	require.Equal(t, 1, b2.PartitionsLeader)
	require.Equal(t, 3, b2.Partitions)
	require.Equal(t, 2, b2.InSyncPartitions)

	// topicStates: sorted by name (meta.Topics.Sorted()), partitions sorted by
	// number, offsets not yet filled in by tallyPartitions itself (that's
	// FetchState's job after the offsets fetch) so both default to -1.
	require.Len(t, topicStates, 2)
	t1 := topicStates[0]
	require.Equal(t, "t1", t1.Name)
	require.Equal(t, 3, t1.ReplicationFactor) // NumReplicas() of first partition
	require.Len(t, t1.Partitions, 2)
	require.Equal(t, cluster.PartitionState{ID: 0, Leader: 1, Replicas: []int32{1, 2, 3}, ISR: []int32{1, 2, 3}, StartOffset: -1, EndOffset: -1}, t1.Partitions[0])
	require.Equal(t, cluster.PartitionState{ID: 1, Leader: -1, Replicas: []int32{1, 2, 3}, ISR: []int32{1}, StartOffset: -1, EndOffset: -1}, t1.Partitions[1])
	t2 := topicStates[1]
	require.Equal(t, "t2", t2.Name)
	require.Equal(t, 2, t2.ReplicationFactor)
	require.Len(t, t2.Partitions, 1)
}

func TestTallyPartitionsMarksInternalFromMetadataFlag(t *testing.T) {
	// IsInternal comes straight from kadm's TopicDetail (source-confirmed
	// field, see metadata.go:85) rather than a "_"-prefix name heuristic:
	// covers both a non-underscore internal topic and an underscore-prefixed
	// one, so a future switch to the name heuristic would visibly break this.
	meta := kadm.Metadata{
		Topics: kadm.TopicDetails{
			"__consumer_offsets": {Topic: "__consumer_offsets", IsInternal: true,
				Partitions: kadm.PartitionDetails{0: {Partition: 0, Leader: 1, Replicas: []int32{1}, ISR: []int32{1}}}},
			"public": {Topic: "public", IsInternal: false,
				Partitions: kadm.PartitionDetails{0: {Partition: 0, Leader: 1, Replicas: []int32{1}, ISR: []int32{1}}}},
		},
	}
	_, _, topicStates := tallyPartitions(meta, map[int32]*cluster.BrokerInfo{1: {ID: 1}})
	require.Len(t, topicStates, 2)
	byName := map[string]cluster.TopicState{}
	for _, ts := range topicStates {
		byName[ts.Name] = ts
	}
	require.True(t, byName["__consumer_offsets"].Internal)
	require.False(t, byName["public"].Internal)
}

func TestTallyPartitionsEmptyMetadataIsZeroValue(t *testing.T) {
	topics, pc, topicStates := tallyPartitions(kadm.Metadata{}, nil)
	require.Equal(t, 0, topics)
	require.Equal(t, cluster.PartitionCounts{}, pc)
	require.Empty(t, topicStates)
}

// TestFetchStateRejectsUnsupportedSecurityEagerly exercises the clientFor
// error path inside FetchState (no dialing, no network): an unsupported
// security.protocol must surface as an OFFLINE state carrying the error,
// mirroring Probe's existing eager-rejection behaviour in admin_test.go.
func TestFetchStateRejectsUnsupportedSecurityEagerly(t *testing.T) {
	p := NewPool()
	defer p.Close()
	def := cluster.Definition{Name: "bad",
		Conn: cluster.ConnectionSpec{BootstrapServers: []string{"k:9092"},
			Security: map[string]string{"security.protocol": "SASL_SSL"}}}

	st, err := p.FetchState(context.Background(), def)
	require.ErrorIs(t, err, ErrUnsupportedSecurity)
	require.Equal(t, cluster.StatusOffline, st.Status)
	require.NotEmpty(t, st.Err)
	require.Equal(t, def, st.Definition)
}

// TestFetchStateReturnsOfflineWhenBrokerUnreachable exercises the rest of
// FetchState (client construction succeeds lazily, kadm.Metadata fails) against
// a loopback port nothing is listening on — deterministic and network-free,
// same trick as admin_test.go's TestProbeReturnsErrorWhenBrokerUnreachable.
func TestFetchStateReturnsOfflineWhenBrokerUnreachable(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := l.Addr().String()
	require.NoError(t, l.Close()) // free the port; nothing will accept on it now

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	p := NewPool()
	defer p.Close()
	def := cluster.Definition{Name: "unreachable", Conn: cluster.ConnectionSpec{BootstrapServers: []string{addr}}}

	st, err := p.FetchState(ctx, def)
	require.ErrorContains(t, err, "fetch metadata")
	require.Equal(t, cluster.StatusOffline, st.Status)
	require.NotEmpty(t, st.Err)
}

// TestLogDirsRejectsUnsupportedSecurityEagerly mirrors
// TestFetchStateRejectsUnsupportedSecurityEagerly for the LogDirs method: the
// clientFor error path must surface directly (no dialing, no network).
func TestLogDirsRejectsUnsupportedSecurityEagerly(t *testing.T) {
	p := NewPool()
	defer p.Close()
	def := cluster.Definition{Name: "bad",
		Conn: cluster.ConnectionSpec{BootstrapServers: []string{"k:9092"},
			Security: map[string]string{"security.protocol": "SASL_SSL"}}}

	dirs, err := p.LogDirs(context.Background(), def, nil)
	require.ErrorIs(t, err, ErrUnsupportedSecurity)
	require.Nil(t, dirs)
}

// TestLogDirsReturnsErrorWhenBrokerUnreachable mirrors
// TestFetchStateReturnsOfflineWhenBrokerUnreachable for the LogDirs method:
// client construction succeeds lazily, but the DescribeAllLogDirs call itself
// fails against a loopback port nothing is listening on.
func TestLogDirsReturnsErrorWhenBrokerUnreachable(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := l.Addr().String()
	require.NoError(t, l.Close())

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	p := NewPool()
	defer p.Close()
	def := cluster.Definition{Name: "unreachable", Conn: cluster.ConnectionSpec{BootstrapServers: []string{addr}}}

	dirs, err := p.LogDirs(ctx, def, nil)
	require.ErrorContains(t, err, "describe log dirs")
	require.Nil(t, dirs)
}

// TestBrokerConfigsRejectsUnsupportedSecurityEagerly mirrors
// TestLogDirsRejectsUnsupportedSecurityEagerly for the BrokerConfigs method.
func TestBrokerConfigsRejectsUnsupportedSecurityEagerly(t *testing.T) {
	p := NewPool()
	defer p.Close()
	def := cluster.Definition{Name: "bad",
		Conn: cluster.ConnectionSpec{BootstrapServers: []string{"k:9092"},
			Security: map[string]string{"security.protocol": "SASL_SSL"}}}

	cfgs, err := p.BrokerConfigs(context.Background(), def, 1)
	require.ErrorIs(t, err, ErrUnsupportedSecurity)
	require.Nil(t, cfgs)
}

// TestBrokerConfigsReturnsErrorWhenBrokerUnreachable mirrors
// TestLogDirsReturnsErrorWhenBrokerUnreachable for the BrokerConfigs method.
func TestBrokerConfigsReturnsErrorWhenBrokerUnreachable(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := l.Addr().String()
	require.NoError(t, l.Close())

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	p := NewPool()
	defer p.Close()
	def := cluster.Definition{Name: "unreachable", Conn: cluster.ConnectionSpec{BootstrapServers: []string{addr}}}

	cfgs, err := p.BrokerConfigs(ctx, def, 1)
	require.ErrorContains(t, err, "describe broker configs")
	require.Nil(t, cfgs)
}

// TestAlterBrokerConfigRejectsUnsupportedSecurityEagerly mirrors
// TestLogDirsRejectsUnsupportedSecurityEagerly for the AlterBrokerConfig method.
func TestAlterBrokerConfigRejectsUnsupportedSecurityEagerly(t *testing.T) {
	p := NewPool()
	defer p.Close()
	def := cluster.Definition{Name: "bad",
		Conn: cluster.ConnectionSpec{BootstrapServers: []string{"k:9092"},
			Security: map[string]string{"security.protocol": "SASL_SSL"}}}

	err := p.AlterBrokerConfig(context.Background(), def, 1, "log.cleaner.threads", "2")
	require.ErrorIs(t, err, ErrUnsupportedSecurity)
}

// TestAlterBrokerConfigReturnsErrorWhenBrokerUnreachable mirrors
// TestLogDirsReturnsErrorWhenBrokerUnreachable for the AlterBrokerConfig method.
func TestAlterBrokerConfigReturnsErrorWhenBrokerUnreachable(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := l.Addr().String()
	require.NoError(t, l.Close())

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	p := NewPool()
	defer p.Close()
	def := cluster.Definition{Name: "unreachable", Conn: cluster.ConnectionSpec{BootstrapServers: []string{addr}}}

	err = p.AlterBrokerConfig(ctx, def, 1, "log.cleaner.threads", "2")
	require.ErrorContains(t, err, "alter broker config")
}

// TestMoveReplicaLogDirRejectsUnsupportedSecurityEagerly mirrors
// TestLogDirsRejectsUnsupportedSecurityEagerly for the MoveReplicaLogDir method.
func TestMoveReplicaLogDirRejectsUnsupportedSecurityEagerly(t *testing.T) {
	p := NewPool()
	defer p.Close()
	def := cluster.Definition{Name: "bad",
		Conn: cluster.ConnectionSpec{BootstrapServers: []string{"k:9092"},
			Security: map[string]string{"security.protocol": "SASL_SSL"}}}

	err := p.MoveReplicaLogDir(context.Background(), def, 1, "t1", 0, "/kafka/data-1")
	require.ErrorIs(t, err, ErrUnsupportedSecurity)
}

// TestMoveReplicaLogDirReturnsErrorWhenBrokerUnreachable mirrors
// TestLogDirsReturnsErrorWhenBrokerUnreachable for the MoveReplicaLogDir method.
// --- topicDeletionEnabledFrom (pure function, P1b Task 5) ---

func TestTopicDeletionEnabledFrom(t *testing.T) {
	cases := []struct {
		name string
		cfgs []cluster.ConfigEntry
		want bool
	}{
		{"explicit true", []cluster.ConfigEntry{{Name: "delete.topic.enable", Value: "true"}}, true},
		{"explicit false", []cluster.ConfigEntry{{Name: "delete.topic.enable", Value: "false"}}, false},
		{"key missing defaults true", []cluster.ConfigEntry{{Name: "log.retention.ms", Value: "60000"}}, true},
		{"empty configs defaults true", nil, true},
		{"unparseable value defaults true", []cluster.ConfigEntry{{Name: "delete.topic.enable", Value: "not-a-bool"}}, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			require.Equal(t, c.want, topicDeletionEnabledFrom(c.cfgs))
		})
	}
}

func TestMoveReplicaLogDirReturnsErrorWhenBrokerUnreachable(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := l.Addr().String()
	require.NoError(t, l.Close())

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	p := NewPool()
	defer p.Close()
	def := cluster.Definition{Name: "unreachable", Conn: cluster.ConnectionSpec{BootstrapServers: []string{addr}}}

	err = p.MoveReplicaLogDir(ctx, def, 1, "t1", 0, "/kafka/data-1")
	require.ErrorContains(t, err, "move replica log dir")
}

func TestBuildBrokerLogDirsEnforcesAggregateNestedBudgets(t *testing.T) {
	described := kadm.DescribedAllLogDirs{
		1: {
			"/data-1": describedLogDir(1, "/data-1", "topic-a", 0, 1),
			"/data-2": describedLogDir(1, "/data-2", "topic-b", 2, 3),
		},
	}
	tests := []struct {
		name   string
		budget logDirBudget
	}{
		{
			name: "directories",
			budget: logDirBudget{
				brokers: 1, directories: 1, topicGroups: 4, partitions: 4, bytes: 1 << 20,
			},
		},
		{
			name: "topic groups aggregate across directories",
			budget: logDirBudget{
				brokers: 1, directories: 2, topicGroups: 1, partitions: 4, bytes: 1 << 20,
			},
		},
		{
			name: "partitions aggregate across topic groups",
			budget: logDirBudget{
				brokers: 1, directories: 2, topicGroups: 2, partitions: 3, bytes: 1 << 20,
			},
		},
		{
			name: "estimated bytes",
			budget: logDirBudget{
				brokers: 1, directories: 2, topicGroups: 2, partitions: 4, bytes: 1,
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := buildBrokerLogDirs(context.Background(), described, nil, test.budget)
			require.ErrorIs(t, err, cluster.ErrResultTooLarge)
		})
	}
}

func TestBuildBrokerLogDirsEnforcesAggregateBrokerBudget(t *testing.T) {
	described := kadm.DescribedAllLogDirs{
		1: {"/data-1": describedLogDir(1, "/data-1", "topic", 0)},
		2: {"/data-2": describedLogDir(2, "/data-2", "topic", 0)},
	}
	_, err := buildBrokerLogDirs(
		context.Background(),
		described,
		nil,
		logDirBudget{
			brokers: 1, directories: 2, topicGroups: 2, partitions: 2, bytes: 1 << 20,
		},
	)
	require.ErrorIs(t, err, cluster.ErrResultTooLarge)
}

func TestBuildBrokerLogDirsRejectsRawMapBeforeNestedKeyAllocation(t *testing.T) {
	directories := make(kadm.DescribedLogDirs, 100)
	for index := range 100 {
		name := fmt.Sprintf("/data-%03d", index)
		directories[name] = describedLogDir(1, name, "topic", 0)
	}
	output, err := buildBrokerLogDirs(
		context.Background(),
		kadm.DescribedAllLogDirs{1: directories},
		nil,
		logDirBudget{
			brokers: 1, directories: 1, topicGroups: 1, partitions: 1, bytes: 1 << 20,
		},
	)
	require.ErrorIs(t, err, cluster.ErrResultTooLarge)
	require.Nil(t, output)
}

func TestBuildBrokerLogDirsHonorsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := buildBrokerLogDirs(
		ctx,
		kadm.DescribedAllLogDirs{
			1: {"/data": describedLogDir(1, "/data", "topic", 0)},
		},
		nil,
		defaultLogDirBudget,
	)
	require.ErrorIs(t, err, context.Canceled)
}

func TestBuildBrokerLogDirsBuildsBoundedNestedResult(t *testing.T) {
	described := kadm.DescribedAllLogDirs{
		2: {"/data-b": describedLogDir(2, "/data-b", "topic-b", 3, 1)},
		1: {"/data-a": describedLogDir(1, "/data-a", "topic-a", 2, 0)},
	}
	output, err := buildBrokerLogDirs(
		context.Background(),
		described,
		nil,
		defaultLogDirBudget,
	)
	require.NoError(t, err)
	require.Len(t, output, 2)
	require.Equal(t, int32(1), output[0].Broker)
	require.Equal(t, "/data-a", output[0].Dir)
	require.Equal(t, "topic-a", output[0].Topics[0].Topic)
	require.Equal(
		t,
		[]int32{0, 2},
		[]int32{
			output[0].Topics[0].Partitions[0].Partition,
			output[0].Topics[0].Partitions[1].Partition,
		},
	)
}

func TestDescribeLogDirsUsesBoundedFilteredBrokerRequests(t *testing.T) {
	fake := &recordingLogDirDescriber{}
	got, err := describeLogDirs(context.Background(), fake, []int32{3, 1, 3})
	require.NoError(t, err)
	require.Equal(t, []int32{3, 1}, fake.brokers)
	require.Zero(t, fake.allCalls)
	require.Len(t, got, 2)
}

func describedLogDir(
	broker int32,
	directory string,
	topic string,
	partitions ...int32,
) kadm.DescribedLogDir {
	described := kadm.DescribedLogDir{
		Broker: broker,
		Dir:    directory,
		Topics: kadm.DescribedLogDirTopics{topic: {}},
	}
	for _, partition := range partitions {
		described.Topics[topic][partition] = kadm.DescribedLogDirPartition{
			Broker: broker, Dir: directory, Topic: topic, Partition: partition,
			Size: 1, OffsetLag: 0,
		}
	}
	return described
}

type recordingLogDirDescriber struct {
	allCalls int
	brokers  []int32
}

func (fake *recordingLogDirDescriber) DescribeAllLogDirs(
	context.Context,
	kadm.TopicsSet,
) (kadm.DescribedAllLogDirs, error) {
	fake.allCalls++
	return nil, nil
}

func (fake *recordingLogDirDescriber) DescribeBrokerLogDirs(
	_ context.Context,
	broker int32,
	_ kadm.TopicsSet,
) (kadm.DescribedLogDirs, error) {
	fake.brokers = append(fake.brokers, broker)
	directory := "/data-" + fmt.Sprint(broker)
	return kadm.DescribedLogDirs{
		directory: describedLogDir(broker, directory, "topic", 0),
	}, nil
}
