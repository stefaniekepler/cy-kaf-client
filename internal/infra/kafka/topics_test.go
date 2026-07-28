package kafka

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
)

// --- error paths (TDD core for this step; real success-path behaviour is
// locked down by the integration test, per the task brief) ---

// TestTopicConfigsRejectsUnsupportedSecurityEagerly mirrors
// TestBrokerConfigsRejectsUnsupportedSecurityEagerly (state_test.go) for the
// new TopicConfigs method.
func TestTopicConfigsRejectsUnsupportedSecurityEagerly(t *testing.T) {
	p := NewPool()
	defer p.Close()
	def := cluster.Definition{Name: "bad",
		Conn: cluster.ConnectionSpec{BootstrapServers: []string{"k:9092"},
			Security: map[string]string{"security.protocol": "SASL_SSL"}}}

	cfgs, err := p.TopicConfigs(context.Background(), def, "t1")
	require.ErrorIs(t, err, ErrUnsupportedSecurity)
	require.Nil(t, cfgs)
}

// TestTopicConfigsReturnsErrorWhenBrokerUnreachable mirrors
// TestBrokerConfigsReturnsErrorWhenBrokerUnreachable for TopicConfigs.
func TestTopicConfigsReturnsErrorWhenBrokerUnreachable(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := l.Addr().String()
	require.NoError(t, l.Close())

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	p := NewPool()
	defer p.Close()
	def := cluster.Definition{Name: "unreachable", Conn: cluster.ConnectionSpec{BootstrapServers: []string{addr}}}

	cfgs, err := p.TopicConfigs(ctx, def, "t1")
	require.ErrorContains(t, err, "describe topic configs")
	require.Nil(t, cfgs)
}

// TestTopicAclsRejectsUnsupportedSecurityEagerly mirrors the same pattern for
// TopicAcls.
func TestTopicAclsRejectsUnsupportedSecurityEagerly(t *testing.T) {
	p := NewPool()
	defer p.Close()
	def := cluster.Definition{Name: "bad",
		Conn: cluster.ConnectionSpec{BootstrapServers: []string{"k:9092"},
			Security: map[string]string{"security.protocol": "SASL_SSL"}}}

	acls, err := p.TopicAcls(context.Background(), def, "t1")
	require.ErrorIs(t, err, ErrUnsupportedSecurity)
	require.Nil(t, acls)
}

// TestTopicAclsReturnsErrorWhenBrokerUnreachable mirrors the same pattern for
// TopicAcls.
func TestTopicAclsReturnsErrorWhenBrokerUnreachable(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := l.Addr().String()
	require.NoError(t, l.Close())

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	p := NewPool()
	defer p.Close()
	def := cluster.Definition{Name: "unreachable", Conn: cluster.ConnectionSpec{BootstrapServers: []string{addr}}}

	acls, err := p.TopicAcls(ctx, def, "t1")
	require.ErrorContains(t, err, "describe topic acls")
	require.Nil(t, acls)
}

// TestActiveProducersRejectsUnsupportedSecurityEagerly mirrors the same
// pattern for ActiveProducers.
func TestActiveProducersRejectsUnsupportedSecurityEagerly(t *testing.T) {
	p := NewPool()
	defer p.Close()
	def := cluster.Definition{Name: "bad",
		Conn: cluster.ConnectionSpec{BootstrapServers: []string{"k:9092"},
			Security: map[string]string{"security.protocol": "SASL_SSL"}}}

	states, err := p.ActiveProducers(context.Background(), def, "t1")
	require.ErrorIs(t, err, ErrUnsupportedSecurity)
	require.Nil(t, states)
}

// TestActiveProducersReturnsErrorWhenBrokerUnreachable mirrors the same
// pattern for ActiveProducers.
func TestActiveProducersReturnsErrorWhenBrokerUnreachable(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := l.Addr().String()
	require.NoError(t, l.Close())

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	p := NewPool()
	defer p.Close()
	def := cluster.Definition{Name: "unreachable", Conn: cluster.ConnectionSpec{BootstrapServers: []string{addr}}}

	states, err := p.ActiveProducers(ctx, def, "t1")
	require.ErrorContains(t, err, "describe active producers")
	require.Nil(t, states)
}

// --- write-method error paths (P1b Task 5), same eager-security-rejection +
// unreachable-broker shape as the read methods above ---

func TestCreateTopicRejectsUnsupportedSecurityEagerly(t *testing.T) {
	p := NewPool()
	defer p.Close()
	def := cluster.Definition{Name: "bad",
		Conn: cluster.ConnectionSpec{BootstrapServers: []string{"k:9092"},
			Security: map[string]string{"security.protocol": "SASL_SSL"}}}

	err := p.CreateTopic(context.Background(), def, cluster.TopicSpec{Name: "t1", Partitions: 1, ReplicationFactor: 1})
	require.ErrorIs(t, err, ErrUnsupportedSecurity)
}

func TestCreateTopicReturnsErrorWhenBrokerUnreachable(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := l.Addr().String()
	require.NoError(t, l.Close())

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	p := NewPool()
	defer p.Close()
	def := cluster.Definition{Name: "unreachable", Conn: cluster.ConnectionSpec{BootstrapServers: []string{addr}}}

	err = p.CreateTopic(ctx, def, cluster.TopicSpec{Name: "t1", Partitions: 1, ReplicationFactor: 1})
	require.ErrorContains(t, err, "create topic")
}

func TestDeleteTopicRejectsUnsupportedSecurityEagerly(t *testing.T) {
	p := NewPool()
	defer p.Close()
	def := cluster.Definition{Name: "bad",
		Conn: cluster.ConnectionSpec{BootstrapServers: []string{"k:9092"},
			Security: map[string]string{"security.protocol": "SASL_SSL"}}}

	err := p.DeleteTopic(context.Background(), def, "t1")
	require.ErrorIs(t, err, ErrUnsupportedSecurity)
}

func TestDeleteTopicReturnsErrorWhenBrokerUnreachable(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := l.Addr().String()
	require.NoError(t, l.Close())

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	p := NewPool()
	defer p.Close()
	def := cluster.Definition{Name: "unreachable", Conn: cluster.ConnectionSpec{BootstrapServers: []string{addr}}}

	err = p.DeleteTopic(ctx, def, "t1")
	require.ErrorContains(t, err, "delete topic")
}

func TestAlterTopicConfigRejectsUnsupportedSecurityEagerly(t *testing.T) {
	p := NewPool()
	defer p.Close()
	def := cluster.Definition{Name: "bad",
		Conn: cluster.ConnectionSpec{BootstrapServers: []string{"k:9092"},
			Security: map[string]string{"security.protocol": "SASL_SSL"}}}

	err := p.AlterTopicConfig(context.Background(), def, "t1", "cleanup.policy", "compact")
	require.ErrorIs(t, err, ErrUnsupportedSecurity)
}

func TestAlterTopicConfigReturnsErrorWhenBrokerUnreachable(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := l.Addr().String()
	require.NoError(t, l.Close())

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	p := NewPool()
	defer p.Close()
	def := cluster.Definition{Name: "unreachable", Conn: cluster.ConnectionSpec{BootstrapServers: []string{addr}}}

	err = p.AlterTopicConfig(ctx, def, "t1", "cleanup.policy", "compact")
	require.ErrorContains(t, err, "alter topic config")
}

func TestCreatePartitionsRejectsUnsupportedSecurityEagerly(t *testing.T) {
	p := NewPool()
	defer p.Close()
	def := cluster.Definition{Name: "bad",
		Conn: cluster.ConnectionSpec{BootstrapServers: []string{"k:9092"},
			Security: map[string]string{"security.protocol": "SASL_SSL"}}}

	err := p.CreatePartitions(context.Background(), def, "t1", 6)
	require.ErrorIs(t, err, ErrUnsupportedSecurity)
}

func TestCreatePartitionsReturnsErrorWhenBrokerUnreachable(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := l.Addr().String()
	require.NoError(t, l.Close())

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	p := NewPool()
	defer p.Close()
	def := cluster.Definition{Name: "unreachable", Conn: cluster.ConnectionSpec{BootstrapServers: []string{addr}}}

	err = p.CreatePartitions(ctx, def, "t1", 6)
	require.ErrorContains(t, err, "create partitions")
}

func TestAlterPartitionAssignmentsRejectsUnsupportedSecurityEagerly(t *testing.T) {
	p := NewPool()
	defer p.Close()
	def := cluster.Definition{Name: "bad",
		Conn: cluster.ConnectionSpec{BootstrapServers: []string{"k:9092"},
			Security: map[string]string{"security.protocol": "SASL_SSL"}}}

	err := p.AlterPartitionAssignments(context.Background(), def, "t1", map[int32][]int32{0: {1, 2}})
	require.ErrorIs(t, err, ErrUnsupportedSecurity)
}

func TestAlterPartitionAssignmentsReturnsErrorWhenBrokerUnreachable(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := l.Addr().String()
	require.NoError(t, l.Close())

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	p := NewPool()
	defer p.Close()
	def := cluster.Definition{Name: "unreachable", Conn: cluster.ConnectionSpec{BootstrapServers: []string{addr}}}

	err = p.AlterPartitionAssignments(ctx, def, "t1", map[int32][]int32{0: {1, 2}})
	require.ErrorContains(t, err, "alter partition assignments")
}

// --- enum mapping table tests (sourceToGenerated-style contract lock-down) ---

func TestAclResourceTypeToContract(t *testing.T) {
	cases := []struct {
		in   kmsg.ACLResourceType
		want string
	}{
		{kmsg.ACLResourceTypeTopic, "TOPIC"},
		{kmsg.ACLResourceTypeGroup, "GROUP"},
		{kmsg.ACLResourceTypeCluster, "CLUSTER"},
		{kmsg.ACLResourceTypeTransactionalId, "TRANSACTIONAL_ID"},
		{kmsg.ACLResourceTypeDelegationToken, "DELEGATION_TOKEN"},
		{kmsg.ACLResourceTypeUser, "USER"},
		{kmsg.ACLResourceTypeAny, "UNKNOWN"},  // filter-only value, never a real ACL's own type
		{kmsg.ACLResourceType(99), "UNKNOWN"}, // unrecognized: must not leak a raw string
	}
	for _, c := range cases {
		require.Equal(t, c.want, aclResourceTypeToContract(c.in), "in=%v", c.in)
	}
}

func TestAclPatternTypeToContract(t *testing.T) {
	cases := []struct {
		in   kmsg.ACLResourcePatternType
		want string
	}{
		{kmsg.ACLResourcePatternTypeLiteral, "LITERAL"},
		{kmsg.ACLResourcePatternTypePrefixed, "PREFIXED"},
		{kmsg.ACLResourcePatternTypeMatch, "MATCH"},
		{kmsg.ACLResourcePatternTypeAny, "UNKNOWN"},
		{kmsg.ACLResourcePatternTypeUnknown, "UNKNOWN"},
	}
	for _, c := range cases {
		require.Equal(t, c.want, aclPatternTypeToContract(c.in), "in=%v", c.in)
	}
}

func TestAclOperationToContract(t *testing.T) {
	cases := []struct {
		in   kmsg.ACLOperation
		want string
	}{
		{kmsg.ACLOperationAll, "ALL"},
		{kmsg.ACLOperationRead, "READ"},
		{kmsg.ACLOperationWrite, "WRITE"},
		{kmsg.ACLOperationCreate, "CREATE"},
		{kmsg.ACLOperationDelete, "DELETE"},
		{kmsg.ACLOperationAlter, "ALTER"},
		{kmsg.ACLOperationDescribe, "DESCRIBE"},
		{kmsg.ACLOperationClusterAction, "CLUSTER_ACTION"},
		{kmsg.ACLOperationDescribeConfigs, "DESCRIBE_CONFIGS"},
		{kmsg.ACLOperationAlterConfigs, "ALTER_CONFIGS"},
		{kmsg.ACLOperationIdempotentWrite, "IDEMPOTENT_WRITE"},
		{kmsg.ACLOperationCreateTokens, "CREATE_TOKENS"},
		{kmsg.ACLOperationDescribeTokens, "DESCRIBE_TOKENS"},
		{kmsg.ACLOperationAny, "UNKNOWN"}, // filter-only value
	}
	for _, c := range cases {
		require.Equal(t, c.want, aclOperationToContract(c.in), "in=%v", c.in)
	}
}

func TestAclPermissionToContract(t *testing.T) {
	cases := []struct {
		in   kmsg.ACLPermissionType
		want string
	}{
		{kmsg.ACLPermissionTypeAllow, "ALLOW"},
		{kmsg.ACLPermissionTypeDeny, "DENY"},
		{kmsg.ACLPermissionTypeAny, "UNKNOWN"}, // filter-only value
		{kmsg.ACLPermissionTypeUnknown, "UNKNOWN"},
	}
	for _, c := range cases {
		require.Equal(t, c.want, aclPermissionToContract(c.in), "in=%v", c.in)
	}
}
