package cluster_test

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	appcluster "github.com/cy-kaf/cy-kaf-client/internal/app/cluster"
	"github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
)

// fakeAclPort records CreateAcls batches and serves a fixed list.
type fakeAclPort struct {
	created  [][]cluster.AclBinding
	deleted  []cluster.AclBinding
	list     []cluster.AclBinding
	delCount int
}

func (f *fakeAclPort) ListAcls(_ context.Context, _ cluster.Definition, _ cluster.AclFilter) ([]cluster.AclBinding, error) {
	return f.list, nil
}
func (f *fakeAclPort) CreateAcls(_ context.Context, _ cluster.Definition, b []cluster.AclBinding) error {
	f.created = append(f.created, b)
	return nil
}
func (f *fakeAclPort) DeleteAcls(_ context.Context, _ cluster.Definition, b cluster.AclBinding) (int, error) {
	f.deleted = append(f.deleted, b)
	return f.delCount, nil
}

func newAclService(t *testing.T, port cluster.AclAdminPort) *appcluster.AclService {
	t.Helper()
	res := appcluster.NewResolver([]cluster.Definition{{Name: "c1"}})
	return appcluster.NewAclService(res, port)
}

func TestConsumerAclExpandsToTopicAndGroupReadDescribe(t *testing.T) {
	f := &fakeAclPort{}
	svc := newAclService(t, f)
	require.NoError(t, svc.CreateConsumerAcl(context.Background(), "c1", appcluster.ConsumerAclSpec{
		Principal: "User:alice", Host: "*", Topics: []string{"orders"}, ConsumerGroups: []string{"g1"},
	}))
	require.Len(t, f.created, 1)
	got := f.created[0]
	// 期望：TOPIC orders {READ,DESCRIBE} + GROUP g1 {READ,DESCRIBE}，全 LITERAL/ALLOW
	require.ElementsMatch(t, []cluster.AclBinding{
		{Principal: "User:alice", Host: "*", ResourceType: "TOPIC", ResourceName: "orders", PatternType: "LITERAL", Operation: "READ", Permission: "ALLOW"},
		{Principal: "User:alice", Host: "*", ResourceType: "TOPIC", ResourceName: "orders", PatternType: "LITERAL", Operation: "DESCRIBE", Permission: "ALLOW"},
		{Principal: "User:alice", Host: "*", ResourceType: "GROUP", ResourceName: "g1", PatternType: "LITERAL", Operation: "READ", Permission: "ALLOW"},
		{Principal: "User:alice", Host: "*", ResourceType: "GROUP", ResourceName: "g1", PatternType: "LITERAL", Operation: "DESCRIBE", Permission: "ALLOW"},
	}, got)
}

func TestConsumerAclPrefixUsesPrefixedPattern(t *testing.T) {
	f := &fakeAclPort{}
	svc := newAclService(t, f)
	require.NoError(t, svc.CreateConsumerAcl(context.Background(), "c1", appcluster.ConsumerAclSpec{
		Principal: "User:a", Host: "*", TopicsPrefix: "ord-", ConsumerGroupsPrefix: "grp-",
	}))
	got := f.created[0]
	for _, b := range got {
		require.Equal(t, "PREFIXED", b.PatternType, "%+v", b)
	}
	require.Len(t, got, 4) // TOPIC ord- {READ,DESCRIBE} + GROUP grp- {READ,DESCRIBE}
}

func TestProducerAclIdempotentAddsClusterIdempotentWrite(t *testing.T) {
	f := &fakeAclPort{}
	svc := newAclService(t, f)
	require.NoError(t, svc.CreateProducerAcl(context.Background(), "c1", appcluster.ProducerAclSpec{
		Principal: "User:p", Host: "*", Topics: []string{"t"}, TransactionalID: "txn", Idempotent: true,
	}))
	got := f.created[0]
	require.Contains(t, got, cluster.AclBinding{Principal: "User:p", Host: "*", ResourceType: "TOPIC", ResourceName: "t", PatternType: "LITERAL", Operation: "WRITE", Permission: "ALLOW"})
	require.Contains(t, got, cluster.AclBinding{Principal: "User:p", Host: "*", ResourceType: "TOPIC", ResourceName: "t", PatternType: "LITERAL", Operation: "DESCRIBE", Permission: "ALLOW"})
	require.Contains(t, got, cluster.AclBinding{Principal: "User:p", Host: "*", ResourceType: "TOPIC", ResourceName: "t", PatternType: "LITERAL", Operation: "CREATE", Permission: "ALLOW"})
	require.Contains(t, got, cluster.AclBinding{Principal: "User:p", Host: "*", ResourceType: "TRANSACTIONAL_ID", ResourceName: "txn", PatternType: "LITERAL", Operation: "WRITE", Permission: "ALLOW"})
	require.Contains(t, got, cluster.AclBinding{Principal: "User:p", Host: "*", ResourceType: "TRANSACTIONAL_ID", ResourceName: "txn", PatternType: "LITERAL", Operation: "DESCRIBE", Permission: "ALLOW"})
	require.Contains(t, got, cluster.AclBinding{Principal: "User:p", Host: "*", ResourceType: "CLUSTER", ResourceName: "kafka-cluster", PatternType: "LITERAL", Operation: "IDEMPOTENT_WRITE", Permission: "ALLOW"})
}

func TestProducerAclNonIdempotentOmitsCluster(t *testing.T) {
	f := &fakeAclPort{}
	svc := newAclService(t, f)
	require.NoError(t, svc.CreateProducerAcl(context.Background(), "c1", appcluster.ProducerAclSpec{
		Principal: "User:p", Host: "*", Topics: []string{"t"}, Idempotent: false,
	}))
	for _, b := range f.created[0] {
		require.NotEqual(t, "CLUSTER", b.ResourceType)
	}
}

func TestStreamAppAclExpansion(t *testing.T) {
	f := &fakeAclPort{}
	svc := newAclService(t, f)
	require.NoError(t, svc.CreateStreamAppAcl(context.Background(), "c1", appcluster.StreamAppAclSpec{
		Principal: "User:s", Host: "*", InputTopics: []string{"in"}, OutputTopics: []string{"out"}, ApplicationID: "app1",
	}))
	got := f.created[0]
	require.Contains(t, got, cluster.AclBinding{Principal: "User:s", Host: "*", ResourceType: "TOPIC", ResourceName: "in", PatternType: "LITERAL", Operation: "READ", Permission: "ALLOW"})
	require.Contains(t, got, cluster.AclBinding{Principal: "User:s", Host: "*", ResourceType: "TOPIC", ResourceName: "out", PatternType: "LITERAL", Operation: "WRITE", Permission: "ALLOW"})
	// applicationId → GROUP{ALL} + TOPIC{ALL}，PREFIXED
	require.Contains(t, got, cluster.AclBinding{Principal: "User:s", Host: "*", ResourceType: "GROUP", ResourceName: "app1", PatternType: "PREFIXED", Operation: "ALL", Permission: "ALLOW"})
	require.Contains(t, got, cluster.AclBinding{Principal: "User:s", Host: "*", ResourceType: "TOPIC", ResourceName: "app1", PatternType: "PREFIXED", Operation: "ALL", Permission: "ALLOW"})
}

func TestListFiltersPrincipalSubstringAndSortsStably(t *testing.T) {
	f := &fakeAclPort{list: []cluster.AclBinding{
		{Principal: "User:bob", Host: "*", ResourceType: "TOPIC", ResourceName: "z", PatternType: "LITERAL", Operation: "READ", Permission: "ALLOW"},
		{Principal: "User:alice", Host: "*", ResourceType: "TOPIC", ResourceName: "a", PatternType: "LITERAL", Operation: "READ", Permission: "ALLOW"},
	}}
	svc := newAclService(t, f)
	got, err := svc.List(context.Background(), "c1", cluster.AclFilter{Search: "alice"})
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, "User:alice", got[0].Principal)
	// 无 search 时按字段级稳定序（这里断言排序确定性：两次调用同序）
	all1, _ := svc.List(context.Background(), "c1", cluster.AclFilter{})
	all2, _ := svc.List(context.Background(), "c1", cluster.AclFilter{})
	require.Equal(t, all1, all2)
	require.Equal(t, []string{"User:alice", "User:bob"}, []string{all1[0].Principal, all1[1].Principal})
}

func TestListSearchIsCaseInsensitiveForPrincipalAndFts(t *testing.T) {
	f := &fakeAclPort{list: []cluster.AclBinding{
		{Principal: "User:Alice", Host: "HOST-A", ResourceType: "TOPIC", ResourceName: "Orders", PatternType: "LITERAL", Operation: "READ", Permission: "ALLOW"},
	}}
	svc := newAclService(t, f)
	principal, err := svc.List(context.Background(), "c1", cluster.AclFilter{Search: "ALICE"})
	require.NoError(t, err)
	require.Len(t, principal, 1)
	fts, err := svc.List(context.Background(), "c1", cluster.AclFilter{Search: "orders", Fts: true})
	require.NoError(t, err)
	require.Len(t, fts, 1)
}

func TestListMatchPatternBackstopUsesKafkaMatchSemantics(t *testing.T) {
	f := &fakeAclPort{list: []cluster.AclBinding{
		{Principal: "User:literal", Host: "*", ResourceType: "TOPIC", ResourceName: "orders", PatternType: "LITERAL", Operation: "READ", Permission: "ALLOW"},
		{Principal: "User:wildcard", Host: "*", ResourceType: "TOPIC", ResourceName: "*", PatternType: "LITERAL", Operation: "READ", Permission: "ALLOW"},
		{Principal: "User:prefix", Host: "*", ResourceType: "TOPIC", ResourceName: "ord", PatternType: "PREFIXED", Operation: "READ", Permission: "ALLOW"},
		{Principal: "User:other", Host: "*", ResourceType: "TOPIC", ResourceName: "payments", PatternType: "LITERAL", Operation: "READ", Permission: "ALLOW"},
		{Principal: "User:other-prefix", Host: "*", ResourceType: "TOPIC", ResourceName: "pay", PatternType: "PREFIXED", Operation: "READ", Permission: "ALLOW"},
	}}
	svc := newAclService(t, f)
	got, err := svc.List(context.Background(), "c1", cluster.AclFilter{
		ResourceType: "TOPIC", ResourceName: "orders", PatternType: "MATCH",
	})
	require.NoError(t, err)
	principals := make([]string, 0, len(got))
	for _, binding := range got {
		principals = append(principals, binding.Principal)
	}
	require.Equal(t, []string{"User:literal", "User:prefix", "User:wildcard"}, principals)
}

func TestListMatchPatternWithoutResourceNameKeepsStoredPatternsAndFiltersType(t *testing.T) {
	f := &fakeAclPort{list: []cluster.AclBinding{
		{Principal: "User:literal", Host: "*", ResourceType: "TOPIC", ResourceName: "orders", PatternType: "LITERAL", Operation: "READ", Permission: "ALLOW"},
		{Principal: "User:prefix", Host: "*", ResourceType: "TOPIC", ResourceName: "ord-", PatternType: "PREFIXED", Operation: "READ", Permission: "ALLOW"},
		{Principal: "User:group", Host: "*", ResourceType: "GROUP", ResourceName: "orders", PatternType: "LITERAL", Operation: "READ", Permission: "ALLOW"},
	}}
	svc := newAclService(t, f)

	got, err := svc.List(context.Background(), "c1", cluster.AclFilter{
		ResourceType: "TOPIC", PatternType: "MATCH",
	})

	require.NoError(t, err)
	require.Equal(t, []cluster.AclBinding{f.list[0], f.list[1]}, got)
}

func TestAclSortKeyDoesNotCollideOnEmbeddedNUL(t *testing.T) {
	left := cluster.AclBinding{Principal: "a\x00b", ResourceType: "c", PatternType: "LITERAL"}
	right := cluster.AclBinding{Principal: "a", ResourceType: "b\x00c", PatternType: "LITERAL"}
	require.NotEqual(t, appcluster.AclSortKey(left), appcluster.AclSortKey(right))
}

// TestListFiltersResourceTypeLocally is the ambiguity-resolution backstop:
// infra's ListAcls can degrade a type-only filter into an AnyResource() broad
// query (Task 2 review Minor#2), so AclService.List must locally narrow by
// ResourceType even though the fake port here ignores the filter entirely
// and always returns its full fixed list.
func TestListFiltersResourceTypeLocally(t *testing.T) {
	f := &fakeAclPort{list: []cluster.AclBinding{
		{Principal: "User:a", Host: "*", ResourceType: "TOPIC", ResourceName: "t1", PatternType: "LITERAL", Operation: "READ", Permission: "ALLOW"},
		{Principal: "User:b", Host: "*", ResourceType: "GROUP", ResourceName: "g1", PatternType: "LITERAL", Operation: "READ", Permission: "ALLOW"},
	}}
	svc := newAclService(t, f)
	got, err := svc.List(context.Background(), "c1", cluster.AclFilter{ResourceType: "TOPIC"})
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, "TOPIC", got[0].ResourceType)
	require.Equal(t, "t1", got[0].ResourceName)
}

// TestListFiltersResourceNameLocally is the ambiguity-resolution backstop's
// other half: a name-only filter must also be narrowed locally, since infra
// can likewise degrade it to a broad query.
func TestListFiltersResourceNameLocally(t *testing.T) {
	f := &fakeAclPort{list: []cluster.AclBinding{
		{Principal: "User:a", Host: "*", ResourceType: "TOPIC", ResourceName: "orders", PatternType: "LITERAL", Operation: "READ", Permission: "ALLOW"},
		{Principal: "User:b", Host: "*", ResourceType: "TOPIC", ResourceName: "payments", PatternType: "LITERAL", Operation: "READ", Permission: "ALLOW"},
	}}
	svc := newAclService(t, f)
	got, err := svc.List(context.Background(), "c1", cluster.AclFilter{ResourceName: "orders"})
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, "orders", got[0].ResourceName)
}

func TestDeleteAclZeroMatchesPropagates(t *testing.T) {
	f := &fakeAclPort{delCount: 0}
	svc := newAclService(t, f)
	n, err := svc.DeleteAcl(context.Background(), "c1", cluster.AclBinding{Principal: "User:x", Host: "*", ResourceType: "TOPIC", ResourceName: "t", PatternType: "LITERAL", Operation: "READ", Permission: "ALLOW"})
	require.NoError(t, err)
	require.Equal(t, 0, n)
}

func TestUnknownClusterIsErrUnknownCluster(t *testing.T) {
	svc := newAclService(t, &fakeAclPort{})
	_, err := svc.List(context.Background(), "nope", cluster.AclFilter{})
	require.ErrorIs(t, err, appcluster.ErrUnknownCluster)
}

func TestCreateAcl(t *testing.T) {
	f := &fakeAclPort{}
	svc := newAclService(t, f)
	binding := cluster.AclBinding{
		Principal: "User:alice", Host: "*", ResourceType: "TOPIC", ResourceName: "orders",
		PatternType: "LITERAL", Operation: "READ", Permission: "ALLOW",
	}
	require.NoError(t, svc.CreateAcl(context.Background(), "c1", binding))
	require.Len(t, f.created, 1)
	require.Equal(t, []cluster.AclBinding{binding}, f.created[0])
}

func TestCreateAclUnknownCluster(t *testing.T) {
	svc := newAclService(t, &fakeAclPort{})
	err := svc.CreateAcl(context.Background(), "nope", cluster.AclBinding{
		Principal: "User:alice", Host: "*", ResourceType: "TOPIC", ResourceName: "orders",
		PatternType: "LITERAL", Operation: "READ", Permission: "ALLOW",
	})
	require.ErrorIs(t, err, appcluster.ErrUnknownCluster)
}

func TestCreateAndDeleteAclRejectInvalidBindingsBeforePort(t *testing.T) {
	valid := cluster.AclBinding{
		Principal: "User:alice", Host: "*", ResourceType: "TOPIC", ResourceName: "orders",
		PatternType: "LITERAL", Operation: "READ", Permission: "ALLOW",
	}
	for _, tc := range []struct {
		name       string
		binding    cluster.AclBinding
		deleteMode bool
	}{
		{name: "blank principal", binding: withAclField(valid, "principal", " ")},
		{name: "malformed principal", binding: withAclField(valid, "principal", "alice")},
		{name: "blank host", binding: withAclField(valid, "host", " ")},
		{name: "blank resource name", binding: withAclField(valid, "resourceName", " ")},
		{name: "unsupported user resource", binding: withAclField(valid, "resourceType", "USER")},
		{name: "unknown resource", binding: withAclField(valid, "resourceType", "UNKNOWN")},
		{name: "unknown operation", binding: withAclField(valid, "operation", "UNKNOWN")},
		{name: "invalid permission", binding: withAclField(valid, "permission", "UNKNOWN")},
		{name: "create match pattern", binding: withAclField(valid, "patternType", "MATCH")},
		{name: "cluster wrong name", binding: cluster.AclBinding{Principal: "User:alice", Host: "*", ResourceType: "CLUSTER", ResourceName: "other", PatternType: "LITERAL", Operation: "ALTER", Permission: "ALLOW"}},
		{name: "cluster prefixed", binding: cluster.AclBinding{Principal: "User:alice", Host: "*", ResourceType: "CLUSTER", ResourceName: "kafka-cluster", PatternType: "PREFIXED", Operation: "ALTER", Permission: "ALLOW"}},
		{name: "delete invalid match cluster", deleteMode: true, binding: cluster.AclBinding{Principal: "User:alice", Host: "*", ResourceType: "CLUSTER", ResourceName: "kafka-cluster", PatternType: "MATCH", Operation: "ALTER", Permission: "ALLOW"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := &fakeAclPort{}
			svc := newAclService(t, f)
			var err error
			if tc.deleteMode {
				_, err = svc.DeleteAcl(context.Background(), "c1", tc.binding)
			} else {
				err = svc.CreateAcl(context.Background(), "c1", tc.binding)
			}
			require.ErrorContains(t, err, "invalid acl request")
			require.Empty(t, f.created)
			require.Empty(t, f.deleted)
		})
	}
}

func TestDeleteAclAllowsMatchForNonClusterResource(t *testing.T) {
	f := &fakeAclPort{delCount: 1}
	svc := newAclService(t, f)
	binding := cluster.AclBinding{
		Principal: "User:alice", Host: "*", ResourceType: "TOPIC", ResourceName: "orders",
		PatternType: "MATCH", Operation: "READ", Permission: "ALLOW",
	}
	deleted, err := svc.DeleteAcl(context.Background(), "c1", binding)
	require.NoError(t, err)
	require.Equal(t, 1, deleted)
	require.Equal(t, []cluster.AclBinding{binding}, f.deleted)
}

func TestAclHelpersRejectInvalidPrincipalHostAndEmptyExpansion(t *testing.T) {
	for _, tc := range []struct {
		name string
		call func(*appcluster.AclService) error
	}{
		{name: "consumer malformed principal", call: func(s *appcluster.AclService) error {
			return s.CreateConsumerAcl(context.Background(), "c1", appcluster.ConsumerAclSpec{Principal: "alice", Host: "*", Topics: []string{"t"}})
		}},
		{name: "producer blank host", call: func(s *appcluster.AclService) error {
			return s.CreateProducerAcl(context.Background(), "c1", appcluster.ProducerAclSpec{Principal: "User:a", Host: " ", Topics: []string{"t"}})
		}},
		{name: "consumer no bindings", call: func(s *appcluster.AclService) error {
			return s.CreateConsumerAcl(context.Background(), "c1", appcluster.ConsumerAclSpec{Principal: "User:a", Host: "*"})
		}},
		{name: "producer no bindings", call: func(s *appcluster.AclService) error {
			return s.CreateProducerAcl(context.Background(), "c1", appcluster.ProducerAclSpec{Principal: "User:a", Host: "*"})
		}},
		{name: "stream app no bindings", call: func(s *appcluster.AclService) error {
			return s.CreateStreamAppAcl(context.Background(), "c1", appcluster.StreamAppAclSpec{Principal: "User:a", Host: "*"})
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := &fakeAclPort{}
			err := tc.call(newAclService(t, f))
			require.ErrorContains(t, err, "invalid acl request")
			require.Empty(t, f.created)
		})
	}
}

func TestConsumerHelperFiltersEmptyResourcesAndDeduplicatesStably(t *testing.T) {
	f := &fakeAclPort{}
	svc := newAclService(t, f)
	require.NoError(t, svc.CreateConsumerAcl(context.Background(), "c1", appcluster.ConsumerAclSpec{
		Principal: "User:a", Host: "*", Topics: []string{"", "orders", "orders", " \t"},
		ConsumerGroups: []string{"group-a", "group-a"},
	}))
	require.Equal(t, []cluster.AclBinding{
		{Principal: "User:a", Host: "*", ResourceType: "TOPIC", ResourceName: "orders", PatternType: "LITERAL", Operation: "READ", Permission: "ALLOW"},
		{Principal: "User:a", Host: "*", ResourceType: "TOPIC", ResourceName: "orders", PatternType: "LITERAL", Operation: "DESCRIBE", Permission: "ALLOW"},
		{Principal: "User:a", Host: "*", ResourceType: "GROUP", ResourceName: "group-a", PatternType: "LITERAL", Operation: "READ", Permission: "ALLOW"},
		{Principal: "User:a", Host: "*", ResourceType: "GROUP", ResourceName: "group-a", PatternType: "LITERAL", Operation: "DESCRIBE", Permission: "ALLOW"},
	}, f.created[0])
}

func TestConsumerHelperTrimsActorResourcesAndPrefixesBeforeStableDedupe(t *testing.T) {
	f := &fakeAclPort{}
	svc := newAclService(t, f)
	require.NoError(t, svc.CreateConsumerAcl(context.Background(), "c1", appcluster.ConsumerAclSpec{
		Principal: " User:a ", Host: " * ",
		Topics: []string{" orders ", "orders"}, TopicsPrefix: " ord- ",
		ConsumerGroups: []string{" group-a ", "group-a"}, ConsumerGroupsPrefix: " grp- ",
	}))
	require.Equal(t, []cluster.AclBinding{
		{Principal: "User:a", Host: "*", ResourceType: "TOPIC", ResourceName: "orders", PatternType: "LITERAL", Operation: "READ", Permission: "ALLOW"},
		{Principal: "User:a", Host: "*", ResourceType: "TOPIC", ResourceName: "orders", PatternType: "LITERAL", Operation: "DESCRIBE", Permission: "ALLOW"},
		{Principal: "User:a", Host: "*", ResourceType: "TOPIC", ResourceName: "ord-", PatternType: "PREFIXED", Operation: "READ", Permission: "ALLOW"},
		{Principal: "User:a", Host: "*", ResourceType: "TOPIC", ResourceName: "ord-", PatternType: "PREFIXED", Operation: "DESCRIBE", Permission: "ALLOW"},
		{Principal: "User:a", Host: "*", ResourceType: "GROUP", ResourceName: "group-a", PatternType: "LITERAL", Operation: "READ", Permission: "ALLOW"},
		{Principal: "User:a", Host: "*", ResourceType: "GROUP", ResourceName: "group-a", PatternType: "LITERAL", Operation: "DESCRIBE", Permission: "ALLOW"},
		{Principal: "User:a", Host: "*", ResourceType: "GROUP", ResourceName: "grp-", PatternType: "PREFIXED", Operation: "READ", Permission: "ALLOW"},
		{Principal: "User:a", Host: "*", ResourceType: "GROUP", ResourceName: "grp-", PatternType: "PREFIXED", Operation: "DESCRIBE", Permission: "ALLOW"},
	}, f.created[0])
}

func TestStreamAppHelperTrimsApplicationIDAndResources(t *testing.T) {
	f := &fakeAclPort{}
	svc := newAclService(t, f)
	require.NoError(t, svc.CreateStreamAppAcl(context.Background(), "c1", appcluster.StreamAppAclSpec{
		Principal: " User:s ", Host: " * ",
		InputTopics: []string{" input ", "input"}, OutputTopics: []string{" output ", "output"},
		ApplicationID: " app-1 ",
	}))
	require.Equal(t, []cluster.AclBinding{
		{Principal: "User:s", Host: "*", ResourceType: "TOPIC", ResourceName: "input", PatternType: "LITERAL", Operation: "READ", Permission: "ALLOW"},
		{Principal: "User:s", Host: "*", ResourceType: "TOPIC", ResourceName: "output", PatternType: "LITERAL", Operation: "WRITE", Permission: "ALLOW"},
		{Principal: "User:s", Host: "*", ResourceType: "GROUP", ResourceName: "app-1", PatternType: "PREFIXED", Operation: "ALL", Permission: "ALLOW"},
		{Principal: "User:s", Host: "*", ResourceType: "TOPIC", ResourceName: "app-1", PatternType: "PREFIXED", Operation: "ALL", Permission: "ALLOW"},
	}, f.created[0])
}

func withAclField(binding cluster.AclBinding, field, value string) cluster.AclBinding {
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

// TestListFtsMatchesAnyFieldSubstring exercises the Fts:true branch of
// matchesSearch: with fts on, Search matches a substring in *any* of the
// seven fields (not just Principal), per the AclFilter domain doc
// ("fts=true 时按全字段子串").
func TestListFtsMatchesAnyFieldSubstring(t *testing.T) {
	f := &fakeAclPort{list: []cluster.AclBinding{
		{Principal: "User:bob", Host: "*", ResourceType: "TOPIC", ResourceName: "orders", PatternType: "LITERAL", Operation: "READ", Permission: "ALLOW"},
		{Principal: "User:carl", Host: "*", ResourceType: "GROUP", ResourceName: "g1", PatternType: "LITERAL", Operation: "WRITE", Permission: "DENY"},
	}}
	svc := newAclService(t, f)
	// "orders" only matches bob's ResourceName, not Principal -- proves fts
	// looks past Principal into the other fields.
	got, err := svc.List(context.Background(), "c1", cluster.AclFilter{Fts: true, Search: "orders"})
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, "User:bob", got[0].Principal)

	// "DENY" only matches carl's Permission field.
	got, err = svc.List(context.Background(), "c1", cluster.AclFilter{Fts: true, Search: "DENY"})
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, "User:carl", got[0].Principal)
}

func TestCSVRoundTrip(t *testing.T) {
	in := []cluster.AclBinding{
		{Principal: "User:alice", ResourceType: "TOPIC", PatternType: "LITERAL", ResourceName: "orders", Operation: "READ", Permission: "ALLOW", Host: "*"},
	}
	svc := newAclService(t, &fakeAclPort{})
	text, err := svc.FormatAclCSV(in) // 方法（不读 receiver 状态）
	require.NoError(t, err)
	out, err := appcluster.ParseAclCSV(text)
	require.NoError(t, err)
	require.Equal(t, in, out)
}

func TestParseCSVRejectsWrongColumnCount(t *testing.T) {
	_, err := appcluster.ParseAclCSV("Principal,ResourceType,PatternType,ResourceName,Operation,PermissionType,Host\nUser:a,TOPIC,LITERAL,t,READ\n")
	require.Error(t, err)
	require.ErrorIs(t, err, appcluster.ErrBadAclCSV) // 解析/校验失败 → sentinel（api 据此 400）
}

func TestParseCSVRequiresExactFirstNonEmptyHeader(t *testing.T) {
	const header = "Principal,ResourceType,PatternType,ResourceName,Operation,PermissionType,Host\n"
	for _, tc := range []struct {
		name string
		text string
	}{
		{name: "empty", text: ""},
		{name: "only blank records", text: "\n \n\t\n"},
		{name: "data without header", text: "User:a,TOPIC,LITERAL,t,READ,ALLOW,*\n"},
		{name: "wrong header value", text: "Principal,ResourceType,PatternType,ResourceName,Operation,Permission,Host\n"},
		{name: "missing header column", text: "Principal,ResourceType,PatternType,ResourceName,Operation,PermissionType\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := appcluster.ParseAclCSV(tc.text)
			require.ErrorIs(t, err, appcluster.ErrBadAclCSV)
		})
	}

	got, err := appcluster.ParseAclCSV("\n \n" + header)
	require.NoError(t, err)
	require.Empty(t, got, "header-only is the explicit clear payload even with leading blank records")
}

func TestParseCSVRejectsBlankDataFields(t *testing.T) {
	const header = "Principal,ResourceType,PatternType,ResourceName,Operation,PermissionType,Host\n"
	valid := []string{"User:a", "TOPIC", "LITERAL", "orders", "READ", "ALLOW", "*"}
	for field := range valid {
		t.Run(valid[field], func(t *testing.T) {
			row := append([]string(nil), valid...)
			row[field] = " \t "
			_, err := appcluster.ParseAclCSV(header + strings.Join(row, ",") + "\n")
			require.ErrorIs(t, err, appcluster.ErrBadAclCSV)
		})
	}
}

// TestParseCSVRejectsBadEnums covers all four enum columns (ResourceType/
// PatternType/Operation/PermissionType): a bad value in any one of them must
// be rejected at parse time with ErrBadAclCSV, not slip through and only
// surface as an unwrapped infra error later in SyncCSV.
func TestParseCSVRejectsBadEnums(t *testing.T) {
	const header = "Principal,ResourceType,PatternType,ResourceName,Operation,PermissionType,Host\n"
	cases := map[string]string{
		"ResourceType":   header + "User:a,BOGUS_TYPE,LITERAL,t,READ,ALLOW,*\n",
		"PatternType":    header + "User:a,TOPIC,BOGUS_PATTERN,t,READ,ALLOW,*\n",
		"Operation":      header + "User:a,TOPIC,LITERAL,t,BOGUS_OP,ALLOW,*\n",
		"PermissionType": header + "User:a,TOPIC,LITERAL,t,READ,BOGUS_PERM,*\n",
	}
	for name, csvText := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := appcluster.ParseAclCSV(csvText)
			require.ErrorIs(t, err, appcluster.ErrBadAclCSV)
		})
	}
}

// TestParseCSVAllowsUserResourceType asserts USER is a legal ResourceType at
// parse time -- kadm's lack of support for it is an infra sync-time concern,
// not something ParseAclCSV should reject.
func TestParseCSVAllowsUserResourceType(t *testing.T) {
	csvText := "Principal,ResourceType,PatternType,ResourceName,Operation,PermissionType,Host\nUser:a,USER,LITERAL,u,DESCRIBE,ALLOW,*\n"
	got, err := appcluster.ParseAclCSV(csvText)
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, "USER", got[0].ResourceType)
}

// TestParseCSVDedupesDuplicateRows mirrors upstream AclCsv.parseCsvLine
// collecting rows into a HashSet: identical rows (equal across all seven
// fields) collapse to one, keeping first-occurrence order.
func TestParseCSVDedupesDuplicateRows(t *testing.T) {
	csvText := "Principal,ResourceType,PatternType,ResourceName,Operation,PermissionType,Host\n" +
		"User:a,TOPIC,LITERAL,t,READ,ALLOW,*\n" +
		"User:b,TOPIC,LITERAL,t2,WRITE,ALLOW,*\n" +
		"User:a,TOPIC,LITERAL,t,READ,ALLOW,*\n" // duplicate of row 1
	got, err := appcluster.ParseAclCSV(csvText)
	require.NoError(t, err)
	require.Len(t, got, 2)
	require.Equal(t, "User:a", got[0].Principal)
	require.Equal(t, "User:b", got[1].Principal)
}

func TestParseCSVTrimsEveryDataCellBeforeValidationAndDedupe(t *testing.T) {
	csvText := "Principal,ResourceType,PatternType,ResourceName,Operation,PermissionType,Host\n" +
		" User:a , TOPIC , LITERAL , orders , READ , ALLOW , * \n" +
		"User:a,TOPIC,LITERAL,orders,READ,ALLOW,*\n"

	got, err := appcluster.ParseAclCSV(csvText)

	require.NoError(t, err)
	require.Equal(t, []cluster.AclBinding{{
		Principal: "User:a", ResourceType: "TOPIC", PatternType: "LITERAL", ResourceName: "orders",
		Operation: "READ", Permission: "ALLOW", Host: "*",
	}}, got)
}

func TestSyncCSVDoesNotMutateCanonicalCurrentForWhitespaceDesired(t *testing.T) {
	current := cluster.AclBinding{
		Principal: "User:a", ResourceType: "TOPIC", PatternType: "LITERAL", ResourceName: "orders",
		Operation: "READ", Permission: "ALLOW", Host: "*",
	}
	f := &fakeAclPort{list: []cluster.AclBinding{current}}
	svc := newAclService(t, f)
	desired := "Principal,ResourceType,PatternType,ResourceName,Operation,PermissionType,Host\n" +
		" User:a , TOPIC , LITERAL , orders , READ , ALLOW , * \n"

	require.NoError(t, svc.SyncCSV(context.Background(), "c1", desired))
	require.Empty(t, f.created)
	require.Empty(t, f.deleted)
}

func TestSyncCreatesBeforeDeletes(t *testing.T) {
	// current 有 X（会被删）；desired 有 Y（会被建）——断言先 create 再 delete。
	x := cluster.AclBinding{Principal: "User:x", ResourceType: "TOPIC", PatternType: "LITERAL", ResourceName: "old", Operation: "READ", Permission: "ALLOW", Host: "*"}
	f := &fakeAclPort{list: []cluster.AclBinding{x}, delCount: 1}
	svc := newAclService(t, f)
	desired := "Principal,ResourceType,PatternType,ResourceName,Operation,PermissionType,Host\nUser:y,TOPIC,LITERAL,new,WRITE,ALLOW,*\n"
	require.NoError(t, svc.SyncCSV(context.Background(), "c1", desired))
	require.Len(t, f.created, 1) // toAdd = {Y}
	require.Equal(t, "User:y", f.created[0][0].Principal)
	require.Len(t, f.deleted, 1) // toDelete = {X}
	require.Equal(t, "User:x", f.deleted[0].Principal)
}

func TestSyncCSVPrevalidatesFullDiffBeforeCreate(t *testing.T) {
	unsupportedCurrent := cluster.AclBinding{
		Principal: "User:legacy", Host: "*", ResourceType: "USER", ResourceName: "legacy",
		PatternType: "LITERAL", Operation: "READ", Permission: "ALLOW",
	}
	f := &fakeAclPort{list: []cluster.AclBinding{unsupportedCurrent}}
	svc := newAclService(t, f)
	desired := "Principal,ResourceType,PatternType,ResourceName,Operation,PermissionType,Host\n" +
		"User:new,TOPIC,LITERAL,new,WRITE,ALLOW,*\n"
	err := svc.SyncCSV(context.Background(), "c1", desired)
	require.ErrorContains(t, err, "invalid acl request")
	require.Empty(t, f.created, "invalid toDelete must be found before any toAdd is created")
	require.Empty(t, f.deleted)
}

type statefulAclPort struct {
	mu          sync.Mutex
	state       map[cluster.AclBinding]struct{}
	listCalls   int
	listEntered chan int
	releaseList chan struct{}
}

func (p *statefulAclPort) ListAcls(context.Context, cluster.Definition, cluster.AclFilter) ([]cluster.AclBinding, error) {
	p.mu.Lock()
	snapshot := make([]cluster.AclBinding, 0, len(p.state))
	for binding := range p.state {
		snapshot = append(snapshot, binding)
	}
	p.listCalls++
	call := p.listCalls
	p.mu.Unlock()
	if call <= 2 {
		p.listEntered <- call
		<-p.releaseList
	}
	return snapshot, nil
}

func (p *statefulAclPort) CreateAcls(_ context.Context, _ cluster.Definition, bindings []cluster.AclBinding) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, binding := range bindings {
		p.state[binding] = struct{}{}
	}
	return nil
}

func (p *statefulAclPort) DeleteAcls(_ context.Context, _ cluster.Definition, binding cluster.AclBinding) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, exists := p.state[binding]; !exists {
		return 0, nil
	}
	delete(p.state, binding)
	return 1, nil
}

func (p *statefulAclPort) snapshot() []cluster.AclBinding {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]cluster.AclBinding, 0, len(p.state))
	for binding := range p.state {
		out = append(out, binding)
	}
	return out
}

func TestSyncCSVSerializesByClusterSoConcurrentFinalStateIsNotUnion(t *testing.T) {
	port := &statefulAclPort{
		state: map[cluster.AclBinding]struct{}{}, listEntered: make(chan int, 2), releaseList: make(chan struct{}),
	}
	svc := newAclService(t, port)
	header := "Principal,ResourceType,PatternType,ResourceName,Operation,PermissionType,Host\n"
	csvA := header + "User:a,TOPIC,LITERAL,a,READ,ALLOW,*\n"
	csvB := header + "User:b,TOPIC,LITERAL,b,READ,ALLOW,*\n"

	errCh := make(chan error, 2)
	go func() { errCh <- svc.SyncCSV(context.Background(), "c1", csvA) }()
	require.Equal(t, 1, <-port.listEntered)
	go func() { errCh <- svc.SyncCSV(context.Background(), "c1", csvB) }()
	select {
	case <-port.listEntered:
		// Without the keyed lock both callers snapshot empty before either write.
	case <-time.After(100 * time.Millisecond):
		// With the keyed lock the second caller cannot enter ListAcls yet.
	}
	close(port.releaseList)
	require.NoError(t, <-errCh)
	require.NoError(t, <-errCh)

	got := port.snapshot()
	require.Len(t, got, 1, "serialized full replacements must finish as A or B, never their union")
	require.Contains(t, []string{"User:a", "User:b"}, got[0].Principal)
}
