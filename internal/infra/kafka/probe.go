// probe.go is the config wizard's Kafka connectivity probe (P1c Task 13): a
// short-lived, never-pooled client that requests broker-only metadata to test
// reachability without ever adding the cluster to the shared Pool.
package kafka

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
)

// ProbeConnectivity opens a dedicated client to def's brokers and requests
// broker-only metadata, reporting the first connectivity/config failure
// (unsupported security, unreachable broker, authorization, ...). The caller
// bounds the attempt via ctx so an unreachable broker fails fast rather than
// hanging on kgo's dial retries.
func ProbeConnectivity(ctx context.Context, def cluster.Definition) error {
	if err := validateBootstrapServers(def.Conn.BootstrapServers); err != nil {
		return newProbeValidationError(def, err)
	}
	opts, err := buildOpts(def.Conn)
	if err != nil {
		return newProbeValidationError(def, err)
	}
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return newProbeValidationError(def, err)
	}
	defer cl.Close()
	return probeConnectedClient(ctx, def, cl)
}

type probeRequester interface {
	Request(context.Context, kmsg.Request) (kmsg.Response, error)
}

func probeConnectedClient(ctx context.Context, def cluster.Definition, cl probeRequester) error {
	req := kmsg.NewPtrMetadataRequest()
	req.Topics = []kmsg.MetadataRequestTopic{}
	resp, err := cl.Request(ctx, req)
	if err != nil {
		return newProbeValidationError(def, err)
	}
	metadata, ok := resp.(*kmsg.MetadataResponse)
	if !ok {
		return newProbeValidationError(def, fmt.Errorf("unexpected metadata response type %T", resp))
	}
	if err := probeMetadataResponseError(metadata); err != nil {
		return newProbeValidationError(def, err)
	}

	describeReq := kmsg.NewPtrDescribeClusterRequest()
	describeReq.IncludeClusterAuthorizedOperations = true
	resp, err = cl.Request(ctx, describeReq)
	if err != nil {
		return newProbeValidationError(def, normalizeDescribeClusterError(err))
	}
	describe, ok := resp.(*kmsg.DescribeClusterResponse)
	if !ok {
		return newProbeValidationError(def, fmt.Errorf("unexpected describe cluster response type %T", resp))
	}
	if err := kerr.ErrorForCode(describe.ErrorCode); err != nil {
		return newProbeValidationError(def, err)
	}
	return nil
}

func normalizeDescribeClusterError(err error) error {
	if errors.Is(err, kerr.UnsupportedVersion) {
		return err
	}
	// franz-go reports a broker that predates KIP-700 (Kafka 2.8) with
	// unexported sentinel errors. Normalize their stable messages so Validate
	// gives the dedicated version diagnostic instead of the unknown fallback.
	message := err.Error()
	if strings.Contains(message, "broker is too old") || strings.Contains(message, "request key is unknown") {
		return fmt.Errorf("%w: %v", kerr.UnsupportedVersion, err)
	}
	return err
}

func validateBootstrapServers(seeds []string) error {
	if len(seeds) == 0 {
		return &net.AddrError{Err: "no bootstrap server configured"}
	}
	for _, seed := range seeds {
		if err := validateBootstrapServer(seed); err != nil {
			return err
		}
	}
	return nil
}

func validateBootstrapServer(seed string) error {
	invalid := func(reason string) error {
		return &net.AddrError{Err: reason, Addr: seed}
	}
	if strings.TrimSpace(seed) == "" {
		return invalid("empty address")
	}

	// franz-go accepts a bare host or IP and supplies Kafka's default 9092
	// port. Preserve that behavior while rejecting malformed explicit ports.
	if strings.HasPrefix(seed, "[") {
		closing := strings.IndexByte(seed, ']')
		if closing < 0 || net.ParseIP(seed[1:closing]) == nil {
			return invalid("invalid bracketed IPv6 address")
		}
		remainder := seed[closing+1:]
		if remainder == "" {
			return nil
		}
		if !strings.HasPrefix(remainder, ":") || strings.Contains(remainder[1:], ":") {
			return invalid("invalid port separator")
		}
		return validateBootstrapPort(seed, remainder[1:])
	}

	switch strings.Count(seed, ":") {
	case 0:
		return nil
	case 1:
		host, port, err := net.SplitHostPort(seed)
		if err != nil || host == "" {
			return invalid("invalid host and port")
		}
		return validateBootstrapPort(seed, port)
	default:
		// An unbracketed IPv6 literal without an explicit port is valid and
		// also receives franz-go's default 9092 port.
		if net.ParseIP(seed) == nil {
			return invalid("invalid IPv6 address")
		}
		return nil
	}
}

func validateBootstrapPort(seed, rawPort string) error {
	port, err := strconv.Atoi(rawPort)
	if err != nil || port < 1 || port > 65535 {
		return &net.AddrError{Err: "port must be a number from 1 to 65535", Addr: seed}
	}
	return nil
}

func probeMetadataResponseError(resp *kmsg.MetadataResponse) error {
	if resp == nil {
		return errors.New("empty metadata response")
	}
	return kerr.ErrorForCode(resp.ErrorCode)
}
