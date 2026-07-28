package kafka

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
)

func TestAclPatternFromContractAllValues(t *testing.T) {
	for text, want := range map[string]kadm.ACLPattern{
		"LITERAL": kadm.ACLPatternLiteral, "PREFIXED": kadm.ACLPatternPrefixed,
		"MATCH": kadm.ACLPatternMatch, "": kadm.ACLPatternAny,
	} {
		got, err := aclPatternFromContract(text)
		require.NoError(t, err)
		require.Equal(t, want, got)
	}
	for _, invalid := range []string{"UNKNOWN", "NOT_A_PATTERN"} {
		_, err := aclPatternFromContract(invalid)
		require.Error(t, err)
	}
}

func TestAclOperationFromContractAllValues(t *testing.T) {
	// 契约 KafkaAcl.operation 全枚举（UNKNOWN 除外，UNKNOWN 不是可创建的真实操作）
	cases := map[string]kmsg.ACLOperation{
		"ALL":              kmsg.ACLOperationAll,
		"READ":             kmsg.ACLOperationRead,
		"WRITE":            kmsg.ACLOperationWrite,
		"CREATE":           kmsg.ACLOperationCreate,
		"DELETE":           kmsg.ACLOperationDelete,
		"ALTER":            kmsg.ACLOperationAlter,
		"DESCRIBE":         kmsg.ACLOperationDescribe,
		"CLUSTER_ACTION":   kmsg.ACLOperationClusterAction,
		"DESCRIBE_CONFIGS": kmsg.ACLOperationDescribeConfigs,
		"ALTER_CONFIGS":    kmsg.ACLOperationAlterConfigs,
		"IDEMPOTENT_WRITE": kmsg.ACLOperationIdempotentWrite,
		"CREATE_TOKENS":    kmsg.ACLOperationCreateTokens,
		"DESCRIBE_TOKENS":  kmsg.ACLOperationDescribeTokens,
	}
	for text, want := range cases {
		got, err := aclOperationFromContract(text)
		require.NoError(t, err, text)
		require.Equal(t, want, got, text)
	}
	_, err := aclOperationFromContract("NOT_AN_OP")
	require.Error(t, err)
}

func TestApplyAclResourceSelectsBuilderByType(t *testing.T) {
	// 只断言不 panic、返回非 nil builder 且无 error（kadm builder 无导出 getter，
	// 交叉行为的正确性由集成灵魂用例把关）。
	for _, rt := range []string{"TOPIC", "GROUP", "CLUSTER", "TRANSACTIONAL_ID", "DELEGATION_TOKEN", ""} {
		b := kadm.NewACLs()
		err := applyAclResource(b, rt, "name")
		require.NoError(t, err, rt)
		require.NotNil(t, b)
	}
}

// TestApplyAclResourceRejectsUnsupportedTypes covers the USER/UNKNOWN
// capability gap (kadm's ACLBuilder has no Users() setter -- see
// applyAclResource's doc comment / ADR-0009): these must return an explicit
// error instead of silently widening to AnyResource().
func TestApplyAclResourceRejectsUnsupportedTypes(t *testing.T) {
	for _, rt := range []string{"USER", "UNKNOWN", "NOT_A_REAL_TYPE"} {
		b := kadm.NewACLs()
		err := applyAclResource(b, rt, "name")
		require.Error(t, err, rt)
		require.ErrorContains(t, err, "not supported")
		require.ErrorContains(t, err, rt)
	}
}

func TestAclResourceTypeRecognized(t *testing.T) {
	for _, rt := range []string{"", "TOPIC", "GROUP", "CLUSTER", "TRANSACTIONAL_ID", "DELEGATION_TOKEN"} {
		require.True(t, aclResourceTypeRecognized(rt), rt)
	}
	for _, rt := range []string{"USER", "UNKNOWN", "NOT_A_REAL_TYPE"} {
		require.False(t, aclResourceTypeRecognized(rt), rt)
	}
}

// TestAclBuilderForBindingCoversAllowDenyAndHostDefault exercises
// aclBuilderForBinding's branches (Allow vs Deny, host default when empty,
// and the operation-parse error passthrough) without touching a live
// cluster — same no-getter constraint as applyAclResource above, so this
// only asserts non-nil/no-panic/no-error on the happy paths and a non-nil
// error on the operation-parse failure path.
func TestAclBuilderForBindingCoversAllowDenyAndHostDefault(t *testing.T) {
	allow := cluster.AclBinding{
		Principal: "User:alice", Host: "*", ResourceName: "orders",
		ResourceType: "TOPIC", PatternType: "LITERAL", Operation: "READ", Permission: "ALLOW",
	}
	b, err := aclBuilderForBinding(allow, false)
	require.NoError(t, err)
	require.NotNil(t, b)

	deny := cluster.AclBinding{
		Principal: "User:bob", Host: "10.0.0.1", ResourceName: "g1",
		ResourceType: "GROUP", PatternType: "PREFIXED", Operation: "ALL", Permission: "DENY",
	}
	b, err = aclBuilderForBinding(deny, false)
	require.NoError(t, err)
	require.NotNil(t, b)

	bad := cluster.AclBinding{Principal: "User:x", ResourceType: "TOPIC", Operation: "NOT_AN_OP"}
	b, err = aclBuilderForBinding(bad, false)
	require.Error(t, err)
	require.Nil(t, b)

	// USER resource type: kadm has no Users() setter (ADR-0009 capability
	// gap) -- must error rather than silently widen to AnyResource().
	userBinding := cluster.AclBinding{
		Principal: "User:carol", Host: "*", ResourceName: "carol",
		ResourceType: "USER", PatternType: "LITERAL", Operation: "ALL", Permission: "ALLOW",
	}
	b, err = aclBuilderForBinding(userBinding, false)
	require.Error(t, err)
	require.ErrorContains(t, err, "USER")
	require.Nil(t, b)
}

func TestAclBuilderRejectsUnsafeFields(t *testing.T) {
	valid := cluster.AclBinding{Principal: "User:a", Host: "*", ResourceName: "orders", ResourceType: "TOPIC", PatternType: "LITERAL", Operation: "READ", Permission: "ALLOW"}
	for _, binding := range []cluster.AclBinding{
		withInfraAclField(valid, "principal", ""),
		withInfraAclField(valid, "principal", "alice"),
		withInfraAclField(valid, "host", ""),
		withInfraAclField(valid, "resourceName", ""),
		withInfraAclField(valid, "patternType", "UNKNOWN"),
		withInfraAclField(valid, "permission", "UNKNOWN"),
		withInfraAclField(valid, "operation", "UNKNOWN"),
		withInfraAclField(valid, "operation", "r-e_a.d"),
		withInfraAclField(valid, "resourceType", "USER"),
		{Principal: "User:a", Host: "*", ResourceName: "other", ResourceType: "CLUSTER", PatternType: "LITERAL", Operation: "ALTER", Permission: "ALLOW"},
	} {
		builder, err := aclBuilderForBinding(binding, false)
		require.Error(t, err, "%+v", binding)
		require.Nil(t, builder)
	}

	match := withInfraAclField(valid, "patternType", "MATCH")
	builder, err := aclBuilderForBinding(match, true)
	require.NoError(t, err)
	require.NotNil(t, builder)
	builder, err = aclBuilderForBinding(match, false)
	require.Error(t, err)
	require.Nil(t, builder)
}

func TestAclBuildersForCreatePrevalidatesWholeBatch(t *testing.T) {
	valid := cluster.AclBinding{Principal: "User:a", Host: "*", ResourceName: "orders", ResourceType: "TOPIC", PatternType: "LITERAL", Operation: "READ", Permission: "ALLOW"}
	invalidSecond := withInfraAclField(valid, "permission", "UNKNOWN")
	builders, err := aclBuildersForCreate([]cluster.AclBinding{valid, invalidSecond})
	require.Error(t, err)
	require.Nil(t, builders, "no partial builder batch may survive prevalidation")
}

func withInfraAclField(binding cluster.AclBinding, field, value string) cluster.AclBinding {
	switch field {
	case "principal":
		binding.Principal = value
	case "host":
		binding.Host = value
	case "resourceName":
		binding.ResourceName = value
	case "resourceType":
		binding.ResourceType = value
	case "patternType":
		binding.PatternType = value
	case "operation":
		binding.Operation = value
	case "permission":
		binding.Permission = value
	}
	return binding
}

// TestAclFilterToBuilderCoversResourcePresentAndAbsent exercises
// aclFilterToBuilder's branch between a fully-specified resource
// (type+name both present, pushed down via applyAclResource) and a broad
// filter (either missing, falls back to AnyResource).
func TestAclFilterToBuilderCoversResourcePresentAndAbsent(t *testing.T) {
	b, err := aclFilterToBuilder(cluster.AclFilter{ResourceType: "TOPIC", ResourceName: "orders", PatternType: "LITERAL"})
	require.NoError(t, err)
	require.NotNil(t, b)

	b, err = aclFilterToBuilder(cluster.AclFilter{})
	require.NoError(t, err)
	require.NotNil(t, b)

	b, err = aclFilterToBuilder(cluster.AclFilter{ResourceType: "TOPIC"})
	require.NoError(t, err)
	require.NotNil(t, b)
}

// TestAclFilterToBuilderRejectsUnsupportedResourceType covers the USER
// capability gap on the read/filter side too: both the fully-specified
// (type+name) and type-only (name absent) shapes must error, not degrade
// into an ANY-resource describe/list.
func TestAclFilterToBuilderRejectsUnsupportedResourceType(t *testing.T) {
	b, err := aclFilterToBuilder(cluster.AclFilter{ResourceType: "USER"})
	require.Error(t, err)
	require.ErrorContains(t, err, "USER")
	require.Nil(t, b)

	b, err = aclFilterToBuilder(cluster.AclFilter{ResourceType: "USER", ResourceName: "carol"})
	require.Error(t, err)
	require.ErrorContains(t, err, "USER")
	require.Nil(t, b)
}

func TestAclFilterToBuilderRejectsInvalidPattern(t *testing.T) {
	builder, err := aclFilterToBuilder(cluster.AclFilter{PatternType: "UNKNOWN"})
	require.Error(t, err)
	require.Nil(t, builder)
}

// --- error paths (mirrors topics_test.go's Rejects.../Returns...Unreachable
// pattern for the Pool write/read methods that need a live kadm client to
// exercise their happy path — the real success path is locked down by the
// integration soul test, per the task brief) ---

func unsupportedSecurityDef() cluster.Definition {
	return cluster.Definition{Name: "bad",
		Conn: cluster.ConnectionSpec{BootstrapServers: []string{"k:9092"},
			Security: map[string]string{"security.protocol": "SASL_SSL"}}}
}

func unreachableDef(t *testing.T) (context.Context, context.CancelFunc, cluster.Definition) {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := l.Addr().String()
	require.NoError(t, l.Close())

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	return ctx, cancel, cluster.Definition{Name: "unreachable", Conn: cluster.ConnectionSpec{BootstrapServers: []string{addr}}}
}

func TestListAclsRejectsUnsupportedSecurityEagerly(t *testing.T) {
	p := NewPool()
	defer p.Close()
	acls, err := p.ListAcls(context.Background(), unsupportedSecurityDef(), cluster.AclFilter{})
	require.ErrorIs(t, err, ErrUnsupportedSecurity)
	require.Nil(t, acls)
}

func TestListAclsReturnsErrorWhenBrokerUnreachable(t *testing.T) {
	ctx, cancel, def := unreachableDef(t)
	defer cancel()
	p := NewPool()
	defer p.Close()
	acls, err := p.ListAcls(ctx, def, cluster.AclFilter{})
	require.ErrorContains(t, err, "describe acls")
	require.Nil(t, acls)
}

// TestListAclsRejectsUnsupportedResourceTypeBeforeContactingBroker proves the
// USER rejection (ADR-0009 capability gap) fires from aclFilterToBuilder
// before DescribeACLs is ever called: the context is cancelled up front, so
// if the code path incorrectly reached the kadm call first, the error would
// come back wrapped as "describe acls: ..." (context cancelled) instead of
// this package's own "not supported" validation error.
func TestListAclsRejectsUnsupportedResourceTypeBeforeContactingBroker(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	p := NewPool()
	defer p.Close()
	def := cluster.Definition{Name: "x", Conn: cluster.ConnectionSpec{BootstrapServers: []string{"k:9092"}}}
	acls, err := p.ListAcls(ctx, def, cluster.AclFilter{ResourceType: "USER"})
	require.Error(t, err)
	require.ErrorContains(t, err, "USER")
	require.ErrorContains(t, err, "not supported")
	require.NotContains(t, err.Error(), "describe acls")
	require.Nil(t, acls)
}

func TestCreateAclsRejectsUnsupportedSecurityEagerly(t *testing.T) {
	p := NewPool()
	defer p.Close()
	err := p.CreateAcls(context.Background(), unsupportedSecurityDef(), []cluster.AclBinding{{
		Principal: "User:a", Host: "*", ResourceType: "TOPIC", ResourceName: "t",
		PatternType: "LITERAL", Operation: "READ", Permission: "ALLOW",
	}})
	require.ErrorIs(t, err, ErrUnsupportedSecurity)
}

func TestCreateAclsReturnsErrorWhenBrokerUnreachable(t *testing.T) {
	ctx, cancel, def := unreachableDef(t)
	defer cancel()
	p := NewPool()
	defer p.Close()
	err := p.CreateAcls(ctx, def, []cluster.AclBinding{{Principal: "User:a", Host: "*", ResourceType: "TOPIC", ResourceName: "t", PatternType: "LITERAL", Operation: "READ", Permission: "ALLOW"}})
	require.ErrorContains(t, err, "create acls")
}

func TestCreateAclsPropagatesOperationParseError(t *testing.T) {
	p := NewPool()
	defer p.Close()
	err := p.CreateAcls(context.Background(), cluster.Definition{Name: "x", Conn: cluster.ConnectionSpec{BootstrapServers: []string{"k:9092"}}},
		[]cluster.AclBinding{{Operation: "NOT_AN_OP"}})
	require.Error(t, err)
}

// TestCreateAclsRejectsUnsupportedResourceTypeBeforeContactingBroker mirrors
// TestListAclsRejectsUnsupportedResourceTypeBeforeContactingBroker for the
// write side: USER must error out of aclBuilderForBinding before CreateACLs
// is ever called (cancelled context makes any reach-the-network attempt
// distinguishable by its error text).
func TestCreateAclsRejectsUnsupportedResourceTypeBeforeContactingBroker(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	p := NewPool()
	defer p.Close()
	def := cluster.Definition{Name: "x", Conn: cluster.ConnectionSpec{BootstrapServers: []string{"k:9092"}}}
	err := p.CreateAcls(ctx, def, []cluster.AclBinding{{
		Principal: "User:carol", Host: "*", ResourceName: "carol",
		ResourceType: "USER", PatternType: "LITERAL", Operation: "ALL", Permission: "ALLOW",
	}})
	require.Error(t, err)
	require.ErrorContains(t, err, "USER")
	require.ErrorContains(t, err, "not supported")
	require.NotContains(t, err.Error(), "create acls")
}

func TestDeleteAclsRejectsUnsupportedSecurityEagerly(t *testing.T) {
	p := NewPool()
	defer p.Close()
	deleted, err := p.DeleteAcls(context.Background(), unsupportedSecurityDef(), cluster.AclBinding{Operation: "READ"})
	require.ErrorIs(t, err, ErrUnsupportedSecurity)
	require.Zero(t, deleted)
}

func TestDeleteAclsReturnsErrorWhenBrokerUnreachable(t *testing.T) {
	ctx, cancel, def := unreachableDef(t)
	defer cancel()
	p := NewPool()
	defer p.Close()
	deleted, err := p.DeleteAcls(ctx, def, cluster.AclBinding{Principal: "User:a", Host: "*", ResourceType: "TOPIC", ResourceName: "t", PatternType: "LITERAL", Operation: "READ", Permission: "ALLOW"})
	require.ErrorContains(t, err, "delete acls")
	require.Zero(t, deleted)
}

// TestDeleteAclsRejectsUnsupportedResourceTypeBeforeContactingBroker mirrors
// the Create/List variants above: USER must error out of
// aclBuilderForBinding before DeleteACLs is ever called.
func TestDeleteAclsRejectsUnsupportedResourceTypeBeforeContactingBroker(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	p := NewPool()
	defer p.Close()
	def := cluster.Definition{Name: "x", Conn: cluster.ConnectionSpec{BootstrapServers: []string{"k:9092"}}}
	deleted, err := p.DeleteAcls(ctx, def, cluster.AclBinding{
		Principal: "User:carol", Host: "*", ResourceName: "carol",
		ResourceType: "USER", PatternType: "LITERAL", Operation: "ALL", Permission: "ALLOW",
	})
	require.Error(t, err)
	require.ErrorContains(t, err, "USER")
	require.ErrorContains(t, err, "not supported")
	require.NotContains(t, err.Error(), "delete acls")
	require.Zero(t, deleted)
}
