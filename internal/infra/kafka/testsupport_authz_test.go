//go:build integration

package kafka

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
)

// startAuthorizerBroker starts a confluent-local KRaft broker with the
// StandardAuthorizer enabled (P2c-D7): ANONYMOUS (the PLAINTEXT admin
// principal) is a super user so it may create/describe/delete ACLs and alter
// quotas, and ALLOW_EVERYONE_IF_NO_ACL_FOUND keeps every other operation open
// so the rest of the test suite isn't blocked. Returns the bootstrap servers.
func startAuthorizerBroker(t *testing.T, ctx context.Context) []string {
	t.Helper()
	kc, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.8.0",
		testcontainers.WithEnv(map[string]string{
			"KAFKA_AUTHORIZER_CLASS_NAME":          "org.apache.kafka.metadata.authorizer.StandardAuthorizer",
			"KAFKA_SUPER_USERS":                    "User:ANONYMOUS",
			"KAFKA_ALLOW_EVERYONE_IF_NO_ACL_FOUND": "true",
		}),
	)
	testcontainers.CleanupContainer(t, kc)
	require.NoError(t, err)
	brokers, err := kc.Brokers(ctx)
	require.NoError(t, err)
	return brokers
}
