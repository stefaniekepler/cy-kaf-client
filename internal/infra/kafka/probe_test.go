package kafka

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
)

func TestProbeValidationErrorExplainsCommonFailuresInChinese(t *testing.T) {
	def := cluster.Definition{
		Name: "sampleCluster",
		Conn: cluster.ConnectionSpec{BootstrapServers: []string{"192.0.2.92:9093"}},
	}
	remote := &net.TCPAddr{IP: net.ParseIP("192.0.2.92"), Port: 9093}

	tests := []struct {
		name  string
		cause error
		want  []string
	}{
		{
			name:  "invalid address",
			cause: &net.AddrError{Err: "missing port in address", Addr: "kafka.internal"},
			want:  []string{"Kafka 地址格式不正确", "主机:端口", "1 到 65535"},
		},
		{
			name:  "dns lookup",
			cause: &net.DNSError{Err: "no such host", Name: "kafka.internal", IsNotFound: true},
			want:  []string{"无法解析 Kafka 主机名", "DNS", "kafka.internal"},
		},
		{
			name: "connection refused",
			cause: &net.OpError{Op: "dial", Net: "tcp", Addr: remote,
				Err: syscall.ECONNREFUSED},
			want: []string{"目标端口 192.0.2.92:9093 当前不接受 TCP 连接", "Kafka 未启动", "未监听 9093", "防火墙主动拒绝"},
		},
		{
			name: "network unreachable",
			cause: &net.OpError{Op: "dial", Net: "tcp", Addr: remote,
				Err: syscall.ENETUNREACH},
			want: []string{"目标网络不可达", "VPN", "路由"},
		},
		{
			name:  "timeout",
			cause: fmt.Errorf("unable to dial: %w", context.DeadlineExceeded),
			want:  []string{"连接 Kafka 超时", "网络丢包", "防火墙静默丢弃"},
		},
		{
			name: "connection reset",
			cause: &net.OpError{Op: "read", Net: "tcp", Addr: remote,
				Err: syscall.ECONNRESET},
			want: []string{"TCP 连接被中途关闭", "代理", "安全协议"},
		},
		{
			name:  "immediate eof",
			cause: &kgo.ErrFirstReadEOF{},
			want:  []string{"Broker 建立连接后立即断开", "SSL/PLAINTEXT", "SASL"},
		},
		{
			name:  "plain eof",
			cause: io.EOF,
			want:  []string{"Broker 建立连接后立即断开", "SSL/PLAINTEXT", "SASL"},
		},
		{
			name:  "tls sent to plaintext port",
			cause: tls.RecordHeaderError{Msg: "first record does not look like a TLS handshake"},
			want:  []string{"TLS 握手失败", "非 SSL 端口", "security.protocol"},
		},
		{
			name:  "unknown certificate authority",
			cause: x509.UnknownAuthorityError{Cert: &x509.Certificate{}},
			want:  []string{"服务端证书不受信任", "CA", "truststore"},
		},
		{
			name:  "certificate hostname mismatch",
			cause: x509.HostnameError{Certificate: &x509.Certificate{}, Host: "192.0.2.92"},
			want:  []string{"证书与连接地址不匹配", "SAN", "192.0.2.92"},
		},
		{
			name: "expired certificate",
			cause: x509.CertificateInvalidError{
				Cert: &x509.Certificate{}, Reason: x509.Expired,
			},
			want: []string{"证书已过期或尚未生效", "有效期", "系统时间"},
		},
		{
			name:  "truststore missing",
			cause: &os.PathError{Op: "open", Path: "/private/truststore.jks", Err: os.ErrNotExist},
			want:  []string{"无法读取 Truststore", "文件不存在", "访问权限"},
		},
		{
			name:  "truststore invalid",
			cause: errors.New("truststore /private/truststore.pem: 不含合法 PEM 证书"),
			want:  []string{"Truststore 内容无效", "PEM/JKS", "JKS 密码"},
		},
		{
			name:  "local sasl configuration",
			cause: fmt.Errorf("%w: SASL 需要用户名和密码", ErrUnsupportedSecurity),
			want:  []string{"Kafka 安全配置不完整或不受支持", "security.protocol", "SASL"},
		},
		{
			name:  "sasl authentication",
			cause: kerr.SaslAuthenticationFailed,
			want:  []string{"SASL 身份认证失败", "用户名、密码或 JAAS", "Broker"},
		},
		{
			name:  "unsupported sasl mechanism",
			cause: kerr.UnsupportedSaslMechanism,
			want:  []string{"SASL 机制不匹配", "PLAIN", "SCRAM"},
		},
		{
			name:  "cluster authorization",
			cause: kerr.ClusterAuthorizationFailed,
			want:  []string{"Kafka 账号权限不足", "DESCRIBE", "ACL"},
		},
		{
			name:  "broker unavailable",
			cause: kerr.BrokerNotAvailable,
			want:  []string{"Kafka Broker 当前不可用", "启动", "可用节点"},
		},
		{
			name:  "broker request timeout",
			cause: kerr.RequestTimedOut,
			want:  []string{"Kafka 请求处理超时", "Broker 负载", "网络"},
		},
		{
			name:  "unsupported kafka version",
			cause: kerr.UnsupportedVersion,
			want:  []string{"Kafka 协议版本不兼容", "Broker 版本", "客户端"},
		},
		{
			name:  "cancelled",
			cause: context.Canceled,
			want:  []string{"验证已取消", "重新点击 Validate"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := newProbeValidationError(def, tt.cause)

			require.ErrorIs(t, got, tt.cause)
			require.Contains(t, got.Error(), "现象：")
			require.Contains(t, got.Error(), "常见原因：")
			require.Contains(t, got.Error(), "建议检查：")
			for _, fragment := range tt.want {
				require.Contains(t, got.Error(), fragment)
			}
		})
	}
}

func TestProbeValidationErrorUsesSafeFallbackForUnknownFailure(t *testing.T) {
	cause := errors.New("unexpected failure containing password=super-secret")
	def := cluster.Definition{Conn: cluster.ConnectionSpec{BootstrapServers: []string{"broker:9092"}}}

	got := newProbeValidationError(def, cause)

	require.ErrorIs(t, got, cause)
	require.Contains(t, got.Error(), "暂时无法识别具体原因")
	require.NotContains(t, got.Error(), "super-secret")
}

func TestProbeMetadataResponseErrorRejectsBrokerLevelFailure(t *testing.T) {
	resp := kmsg.NewPtrMetadataResponse()
	resp.ErrorCode = kerr.ClusterAuthorizationFailed.Code

	err := probeMetadataResponseError(resp)

	require.ErrorIs(t, err, kerr.ClusterAuthorizationFailed)
}

type probeRequesterFunc func(context.Context, kmsg.Request) (kmsg.Response, error)

func (f probeRequesterFunc) Request(ctx context.Context, req kmsg.Request) (kmsg.Response, error) {
	return f(ctx, req)
}

func TestProbeConnectedClientRequiresClusterDescribePermission(t *testing.T) {
	def := cluster.Definition{
		Name: "restricted",
		Conn: cluster.ConnectionSpec{BootstrapServers: []string{"kafka.example.com:9092"}},
	}
	var requestKeys []int16
	client := probeRequesterFunc(func(_ context.Context, req kmsg.Request) (kmsg.Response, error) {
		requestKeys = append(requestKeys, req.Key())
		switch req.(type) {
		case *kmsg.MetadataRequest:
			return kmsg.NewPtrMetadataResponse(), nil
		case *kmsg.DescribeClusterRequest:
			resp := kmsg.NewPtrDescribeClusterResponse()
			resp.ErrorCode = kerr.ClusterAuthorizationFailed.Code
			return resp, nil
		default:
			return nil, fmt.Errorf("unexpected request %T", req)
		}
	})

	err := probeConnectedClient(context.Background(), def, client)

	require.Equal(t, []int16{3, 60}, requestKeys)
	require.ErrorIs(t, err, kerr.ClusterAuthorizationFailed)
	require.Contains(t, err.Error(), "Kafka 账号权限不足")
	require.Contains(t, err.Error(), "DESCRIBE")
}

func TestProbeConnectedClientExplainsBrokerTooOldForClusterDescribe(t *testing.T) {
	def := cluster.Definition{
		Name: "legacy",
		Conn: cluster.ConnectionSpec{BootstrapServers: []string{"legacy.example.com:9092"}},
	}
	client := probeRequesterFunc(func(_ context.Context, req kmsg.Request) (kmsg.Response, error) {
		if _, ok := req.(*kmsg.MetadataRequest); ok {
			return kmsg.NewPtrMetadataResponse(), nil
		}
		return nil, errors.New("broker is too old; the broker has already indicated it will not know how to handle the request")
	})

	err := probeConnectedClient(context.Background(), def, client)

	require.ErrorIs(t, err, kerr.UnsupportedVersion)
	require.Contains(t, err.Error(), "Kafka 协议版本不兼容")
}

func TestProbeConnectivityExplainsMalformedBootstrapServersInChinese(t *testing.T) {
	tests := []struct {
		name string
		seed string
	}{
		{name: "non numeric port", seed: "kafka.example.com:not-a-port"},
		{name: "port above tcp range", seed: "kafka.example.com:70000"},
		{name: "malformed bracketed ipv6", seed: "[2001:db8::1:9092"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
			defer cancel()
			def := cluster.Definition{
				Name: "invalid-address",
				Conn: cluster.ConnectionSpec{BootstrapServers: []string{tt.seed}},
			}

			err := ProbeConnectivity(ctx, def)

			require.Error(t, err)
			require.Contains(t, err.Error(), "Kafka 地址格式不正确")
			require.Contains(t, err.Error(), "主机:端口")
			require.Contains(t, err.Error(), "1 到 65535")
		})
	}
}

func TestValidateBootstrapServerPreservesFranzGoSupportedDefaults(t *testing.T) {
	for _, seed := range []string{
		"kafka.example.com",
		"127.0.0.1",
		"[2001:db8::1]",
		"[2001:db8::1]:9093",
		"2001:db8::1",
	} {
		t.Run(seed, func(t *testing.T) {
			require.NoError(t, validateBootstrapServer(seed))
		})
	}
}
