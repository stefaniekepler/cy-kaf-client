package kafka

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
)

// TestBuildOptsSASLRequiresCredentials supersedes the P0-era
// TestBuildOptsRejectsSASLForNow. In P0, SASL_SSL was rejected unconditionally
// (SASL landed in P1). As of P1a Task 3 (security.go), SASL_PLAINTEXT/SASL_SSL
// are supported end-to-end, so the P0 assertion ("SASL_SSL always errors") is
// no longer true and is replaced by the narrower, still-true claim: SASL
// without resolvable credentials errors, SASL with credentials succeeds. This
// is a planned semantic upgrade, not a regression — see task-3-report.md and
// the full protocol/mechanism matrix in security_test.go's TestSecurityMatrix.
func TestBuildOptsSASLRequiresCredentials(t *testing.T) {
	_, err := buildOpts(cluster.ConnectionSpec{
		BootstrapServers: []string{"k:9092"},
		Security:         map[string]string{"security.protocol": "SASL_SSL"},
	})
	require.ErrorIs(t, err, ErrUnsupportedSecurity) // 无凭据：仍报错

	_, err = buildOpts(cluster.ConnectionSpec{
		BootstrapServers: []string{"k:9092"},
		Security: map[string]string{
			"security.protocol": "SASL_SSL",
			"sasl.mechanism":    "PLAIN",
			"sasl.username":     "u",
			"sasl.password":     "p",
		},
	})
	require.NoError(t, err) // 带凭据：P1a 新增能力，应成功
}

func TestBuildOptsPlaintextAndSSL(t *testing.T) {
	for _, proto := range []string{"", "PLAINTEXT", "SSL"} {
		_, err := buildOpts(cluster.ConnectionSpec{
			BootstrapServers: []string{"k:9092"},
			Security:         map[string]string{"security.protocol": proto},
		})
		require.NoError(t, err, proto)
	}
}
