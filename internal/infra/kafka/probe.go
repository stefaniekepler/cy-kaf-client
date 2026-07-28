// probe.go is the config wizard's Kafka connectivity probe (P1c Task 13): a
// short-lived, never-pooled client that pings a cluster's seed brokers to test
// reachability without ever adding the cluster to the shared Pool.
package kafka

import (
	"context"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
)

// ProbeConnectivity opens a dedicated client to def's brokers and pings them,
// reporting the first connectivity/config failure (unsupported security,
// unreachable broker, ...). The caller bounds the attempt via ctx so an
// unreachable broker fails fast rather than hanging on kgo's dial retries.
func ProbeConnectivity(ctx context.Context, def cluster.Definition) error {
	opts, err := buildOpts(def.Conn)
	if err != nil {
		return err
	}
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return err
	}
	defer cl.Close()
	return cl.Ping(ctx)
}
