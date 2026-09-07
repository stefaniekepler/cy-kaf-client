//go:build integration

package kafka

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"

	"github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
)

func TestProbeConnectivityReportsDeniedClusterDescribeAgainstAuthorizer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	kc, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.8.0",
		testcontainers.WithEnv(map[string]string{
			"KAFKA_LISTENER_SECURITY_PROTOCOL_MAP": "BROKER:PLAINTEXT,PLAINTEXT:SASL_PLAINTEXT,CONTROLLER:PLAINTEXT",
			"KAFKA_SASL_ENABLED_MECHANISMS":        "PLAIN",
			"KAFKA_LISTENER_NAME_PLAINTEXT_PLAIN_SASL_JAAS_CONFIG": `org.apache.kafka.common.security.plain.PlainLoginModule required ` +
				`username="admin" password="admin-secret" ` +
				`user_admin="admin-secret" user_limited="limited-secret";`,
			"KAFKA_AUTHORIZER_CLASS_NAME":          "org.apache.kafka.metadata.authorizer.StandardAuthorizer",
			"KAFKA_SUPER_USERS":                    "User:ANONYMOUS;User:admin",
			"KAFKA_ALLOW_EVERYONE_IF_NO_ACL_FOUND": "false",
		}),
	)
	testcontainers.CleanupContainer(t, kc)
	require.NoError(t, err)
	brokers, err := kc.Brokers(ctx)
	require.NoError(t, err)

	def := cluster.Definition{
		Name: "restricted",
		Conn: cluster.ConnectionSpec{
			BootstrapServers: brokers,
			Security: map[string]string{
				"security.protocol": "SASL_PLAINTEXT",
				"sasl.mechanism":    "PLAIN",
				"sasl.username":     "limited",
				"sasl.password":     "limited-secret",
			},
		},
	}

	err = ProbeConnectivity(ctx, def)

	require.Error(t, err)
	require.Contains(t, err.Error(), "Kafka 账号权限不足")
	require.Contains(t, err.Error(), "DESCRIBE")
}
