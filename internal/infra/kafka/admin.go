// Package kafka adapts franz-go to the domain cluster ports (StateScraper,
// BrokerAdminPort, ClientLifecycle).
package kafka

import (
	"errors"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
)

// ErrUnsupportedSecurity marks security configurations not mapped by securityOpts
// (security.go): unsupported protocol, SASL mechanism, or credential format. P1a
// covers PLAINTEXT/SSL/SASL_PLAINTEXT/SASL_SSL with PLAIN/SCRAM-SHA-256/SCRAM-SHA-512;
// GSSAPI/OAUTHBEARER and any other configuration still lands here (parity matrix).
var ErrUnsupportedSecurity = errors.New("unsupported security configuration")

// buildOpts 把集群连接配置翻译为 kgo 选项；安全相关分支委托给 securityOpts
// （security.go），本函数只负责拼接 seed brokers。
func buildOpts(conn cluster.ConnectionSpec) ([]kgo.Opt, error) {
	sopts, err := securityOpts(conn.Security)
	if err != nil {
		return nil, err
	}
	return append([]kgo.Opt{kgo.SeedBrokers(conn.BootstrapServers...)}, sopts...), nil
}
