package kafka

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
)

func TestPoolReusesAndInvalidatesClients(t *testing.T) {
	p := NewPool()
	defer p.Close()
	def := cluster.Definition{Name: "x",
		Conn: cluster.ConnectionSpec{BootstrapServers: []string{"127.0.0.1:1"}}}
	c1, err := p.clientFor(def)
	require.NoError(t, err) // kgo.NewClient 惰性拨号，构造必须成功（ADR-0003 §2 已证）
	c2, err := p.clientFor(def)
	require.NoError(t, err)
	require.Same(t, c1, c2) // 同名集群复用同一客户端
	p.Invalidate("x")
	c3, err := p.clientFor(def)
	require.NoError(t, err)
	require.NotSame(t, c1, c3) // 失效后重建
}

func TestPoolRejectsUnsupportedSecurityEagerly(t *testing.T) {
	p := NewPool()
	defer p.Close()
	_, err := p.clientFor(cluster.Definition{Name: "bad",
		Conn: cluster.ConnectionSpec{BootstrapServers: []string{"k:9092"},
			Security: map[string]string{"security.protocol": "SASL_SSL",
				"sasl.mechanism": "GSSAPI"}}})
	// 无凭据的 SASL 配置在建池时被急切拒绝（带凭据的 GSSAPI 机制拒绝由 security_test.go 矩阵覆盖）
	require.Error(t, err)
}
