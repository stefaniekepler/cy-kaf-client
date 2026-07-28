//go:build integration

package kafka

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	appcluster "github.com/cy-kaf/cy-kaf-client/internal/app/cluster"
	"github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
)

// TestAclsRoundTripAgainstRealAuthorizerBroker is P2c's ACL soul test. One
// StandardAuthorizer broker covers the atomic infra round-trip, all three app
// helper expansions, and both add/remove sides of CSV sync. Expected bindings
// are deliberately written out here instead of being produced by the
// expansion/formatting code under test.
func TestAclsRoundTripAgainstRealAuthorizerBroker(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	brokers := startAuthorizerBroker(t, ctx)

	pool := NewPool()
	defer pool.Close()
	def := cluster.Definition{Name: "it-acls", Conn: cluster.ConnectionSpec{BootstrapServers: brokers}}
	svc := appcluster.NewAclService(appcluster.NewResolver([]cluster.Definition{def}), pool)

	t.Run("atomic binding round trip", func(t *testing.T) {
		binding := cluster.AclBinding{
			Principal: "User:alice", Host: "*", ResourceName: "orders",
			ResourceType: "TOPIC", PatternType: "LITERAL", Operation: "READ", Permission: "ALLOW",
		}

		require.NoError(t, pool.CreateAcls(ctx, def, []cluster.AclBinding{binding}))
		requireAclSetEventually(t, ctx, svc, []cluster.AclBinding{binding})

		deleted, err := pool.DeleteAcls(ctx, def, binding)
		require.NoError(t, err)
		require.Equal(t, 1, deleted)

		deleted, err = pool.DeleteAcls(ctx, def, binding)
		require.NoError(t, err)
		require.Equal(t, 0, deleted)
		requireAclSetEventually(t, ctx, svc, nil)
	})

	t.Run("consumer helper exact expansion", func(t *testing.T) {
		expected := []cluster.AclBinding{
			acl("User:helper-consumer", "*", "TOPIC", "orders", "LITERAL", "READ"),
			acl("User:helper-consumer", "*", "TOPIC", "orders", "LITERAL", "DESCRIBE"),
			acl("User:helper-consumer", "*", "TOPIC", "orders-", "PREFIXED", "READ"),
			acl("User:helper-consumer", "*", "TOPIC", "orders-", "PREFIXED", "DESCRIBE"),
			acl("User:helper-consumer", "*", "GROUP", "orders-group", "LITERAL", "READ"),
			acl("User:helper-consumer", "*", "GROUP", "orders-group", "LITERAL", "DESCRIBE"),
			acl("User:helper-consumer", "*", "GROUP", "orders-group-", "PREFIXED", "READ"),
			acl("User:helper-consumer", "*", "GROUP", "orders-group-", "PREFIXED", "DESCRIBE"),
		}
		require.NoError(t, svc.CreateConsumerAcl(ctx, def.Name, appcluster.ConsumerAclSpec{
			Principal: "User:helper-consumer", Host: "*",
			Topics: []string{"orders"}, TopicsPrefix: "orders-",
			ConsumerGroups: []string{"orders-group"}, ConsumerGroupsPrefix: "orders-group-",
		}))
		requireAclSetEventually(t, ctx, svc, expected)
		deleteAclBindings(t, ctx, pool, def, expected)
		requireAclSetEventually(t, ctx, svc, nil)
	})

	t.Run("producer helper exact expansion", func(t *testing.T) {
		expected := []cluster.AclBinding{
			acl("User:helper-producer", "*", "TOPIC", "payments", "LITERAL", "WRITE"),
			acl("User:helper-producer", "*", "TOPIC", "payments", "LITERAL", "DESCRIBE"),
			acl("User:helper-producer", "*", "TOPIC", "payments", "LITERAL", "CREATE"),
			acl("User:helper-producer", "*", "TOPIC", "payments-", "PREFIXED", "WRITE"),
			acl("User:helper-producer", "*", "TOPIC", "payments-", "PREFIXED", "DESCRIBE"),
			acl("User:helper-producer", "*", "TOPIC", "payments-", "PREFIXED", "CREATE"),
			acl("User:helper-producer", "*", "TRANSACTIONAL_ID", "payments-tx", "LITERAL", "WRITE"),
			acl("User:helper-producer", "*", "TRANSACTIONAL_ID", "payments-tx", "LITERAL", "DESCRIBE"),
			acl("User:helper-producer", "*", "TRANSACTIONAL_ID", "payments-tx-", "PREFIXED", "WRITE"),
			acl("User:helper-producer", "*", "TRANSACTIONAL_ID", "payments-tx-", "PREFIXED", "DESCRIBE"),
			acl("User:helper-producer", "*", "CLUSTER", "kafka-cluster", "LITERAL", "IDEMPOTENT_WRITE"),
		}
		require.NoError(t, svc.CreateProducerAcl(ctx, def.Name, appcluster.ProducerAclSpec{
			Principal: "User:helper-producer", Host: "*",
			Topics: []string{"payments"}, TopicsPrefix: "payments-",
			TransactionalID: "payments-tx", TransactionsIDPrefix: "payments-tx-", Idempotent: true,
		}))
		requireAclSetEventually(t, ctx, svc, expected)
		deleteAclBindings(t, ctx, pool, def, expected)
		requireAclSetEventually(t, ctx, svc, nil)
	})

	t.Run("stream app helper exact expansion", func(t *testing.T) {
		expected := []cluster.AclBinding{
			acl("User:helper-stream", "*", "TOPIC", "events-in", "LITERAL", "READ"),
			acl("User:helper-stream", "*", "TOPIC", "events-out", "LITERAL", "WRITE"),
			acl("User:helper-stream", "*", "GROUP", "events-app", "PREFIXED", "ALL"),
			acl("User:helper-stream", "*", "TOPIC", "events-app", "PREFIXED", "ALL"),
		}
		require.NoError(t, svc.CreateStreamAppAcl(ctx, def.Name, appcluster.StreamAppAclSpec{
			Principal: "User:helper-stream", Host: "*", InputTopics: []string{"events-in"},
			OutputTopics: []string{"events-out"}, ApplicationID: "events-app",
		}))
		requireAclSetEventually(t, ctx, svc, expected)
		deleteAclBindings(t, ctx, pool, def, expected)
		requireAclSetEventually(t, ctx, svc, nil)
	})

	t.Run("csv sync adds target then removes to subset", func(t *testing.T) {
		target := []cluster.AclBinding{
			acl("User:csv-sync", "*", "TOPIC", "csv-orders", "LITERAL", "READ"),
			acl("User:csv-sync", "*", "TOPIC", "csv-orders-", "PREFIXED", "WRITE"),
			acl("User:csv-sync", "*", "GROUP", "csv-group", "LITERAL", "READ"),
		}
		subset := []cluster.AclBinding{target[1]}
		const targetCSV = "Principal,ResourceType,PatternType,ResourceName,Operation,PermissionType,Host\n" +
			"User:csv-sync,TOPIC,LITERAL,csv-orders,READ,ALLOW,*\n" +
			"User:csv-sync,TOPIC,PREFIXED,csv-orders-,WRITE,ALLOW,*\n" +
			"User:csv-sync,GROUP,LITERAL,csv-group,READ,ALLOW,*\n"
		const subsetCSV = "Principal,ResourceType,PatternType,ResourceName,Operation,PermissionType,Host\n" +
			"User:csv-sync,TOPIC,PREFIXED,csv-orders-,WRITE,ALLOW,*\n"

		requireAclSetEventually(t, ctx, svc, nil)
		require.NoError(t, svc.SyncCSV(ctx, def.Name, targetCSV))
		requireAclSetEventually(t, ctx, svc, target)
		require.NoError(t, svc.SyncCSV(ctx, def.Name, subsetCSV))
		requireAclSetEventually(t, ctx, svc, subset)
		require.NoError(t, svc.SyncCSV(ctx, def.Name,
			"Principal,ResourceType,PatternType,ResourceName,Operation,PermissionType,Host\n"))
		requireAclSetEventually(t, ctx, svc, nil)
	})

	t.Run("invalid csv leaves the existing set unchanged", func(t *testing.T) {
		existing := []cluster.AclBinding{
			acl("User:csv-existing", "*", "TOPIC", "must-survive", "LITERAL", "READ"),
		}
		require.NoError(t, pool.CreateAcls(ctx, def, existing))
		requireAclSetEventually(t, ctx, svc, existing)

		invalidInputs := []string{
			"",
			"Principal,ResourceType,PatternType,ResourceName,Operation,Host,PermissionType\n",
			"Principal,ResourceType,PatternType,ResourceName,Operation,PermissionType,Host\n" +
				"User:csv-existing,TOPIC,LITERAL, ,READ,ALLOW,*\n",
		}
		for _, input := range invalidInputs {
			require.ErrorIs(t, svc.SyncCSV(ctx, def.Name, input), appcluster.ErrBadAclCSV)
			requireAclSetEventually(t, ctx, svc, existing)
		}

		deleteAclBindings(t, ctx, pool, def, existing)
		requireAclSetEventually(t, ctx, svc, nil)
	})

	t.Run("invalid create batch has no partial writes", func(t *testing.T) {
		valid := acl("User:batch", "*", "TOPIC", "must-not-appear", "LITERAL", "READ")
		invalid := acl("User:batch", "*", "USER", "unsupported", "LITERAL", "READ")

		require.Error(t, pool.CreateAcls(ctx, def, []cluster.AclBinding{valid, invalid}))
		requireAclSetEventually(t, ctx, svc, nil)
	})
}

func acl(principal, host, resourceType, resourceName, patternType, operation string) cluster.AclBinding {
	return cluster.AclBinding{
		Principal: principal, Host: host, ResourceType: resourceType, ResourceName: resourceName,
		PatternType: patternType, Operation: operation, Permission: "ALLOW",
	}
}

func requireAclSetEventually(
	t *testing.T,
	ctx context.Context,
	svc *appcluster.AclService,
	expected []cluster.AclBinding,
) {
	t.Helper()
	deadline := time.NewTimer(30 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	var last []cluster.AclBinding
	var lastErr error
	for {
		last, lastErr = svc.List(ctx, "it-acls", cluster.AclFilter{})
		if lastErr == nil && aclSetsEqual(expected, last) {
			return
		}
		select {
		case <-ctx.Done():
			require.NoError(t, ctx.Err(), "context ended while waiting for exact ACL set; last read: %#v", last)
		case <-deadline.C:
			require.NoError(t, lastErr, "last ACL describe failed")
			require.ElementsMatch(t, expected, last, "exact ACL set did not become visible")
			return
		case <-ticker.C:
		}
	}
}

func aclSetsEqual(expected, actual []cluster.AclBinding) bool {
	if len(expected) != len(actual) {
		return false
	}
	counts := make(map[cluster.AclBinding]int, len(expected))
	for _, binding := range expected {
		counts[binding]++
	}
	for _, binding := range actual {
		counts[binding]--
		if counts[binding] < 0 {
			return false
		}
	}
	return true
}

func deleteAclBindings(
	t *testing.T,
	ctx context.Context,
	pool *Pool,
	def cluster.Definition,
	bindings []cluster.AclBinding,
) {
	t.Helper()
	for _, binding := range bindings {
		deleted, err := pool.DeleteAcls(ctx, def, binding)
		require.NoError(t, err)
		require.Equal(t, 1, deleted, "expected one exact ACL deletion for %#v", binding)
	}
}
