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
	"strings"
	"syscall"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
)

type probeValidationError struct {
	cause   error
	message string
}

func (e *probeValidationError) Error() string { return e.message }
func (e *probeValidationError) Unwrap() error { return e.cause }

func newProbeValidationError(def cluster.Definition, cause error) error {
	return &probeValidationError{
		cause:   cause,
		message: diagnoseProbeFailure(def, cause),
	}
}

func diagnoseProbeFailure(def cluster.Definition, cause error) string {
	var addrErr *net.AddrError
	if errors.As(cause, &addrErr) {
		return probeDiagnosis(
			"Kafka 地址格式不正确。",
			"Bootstrap Server 缺少主机或端口、端口不是数字，或端口超出 1 到 65535。",
			"请按“主机:端口”填写，例如 kafka.example.com:9092；IPv6 地址需要使用方括号。",
		)
	}

	var pathErr *os.PathError
	if errors.As(cause, &pathErr) {
		return probeDiagnosis(
			"无法读取 Truststore 文件。",
			"文件不存在、路径不可访问，或当前用户没有访问权限。",
			"请确认文件仍在原位置，并检查文件路径和访问权限。",
		)
	}

	lower := strings.ToLower(cause.Error())
	if strings.Contains(lower, "truststore") &&
		(strings.Contains(lower, "pem") || strings.Contains(lower, "jks") || strings.Contains(lower, "certificate")) {
		return probeDiagnosis(
			"Truststore 内容无效。",
			"文件不是合法的 PEM/JKS、JKS 密码错误，或其中没有受信任证书。",
			"请确认文件格式和 JKS 密码，并确保 Truststore 中包含 CA 或受信任证书。",
		)
	}

	if errors.Is(cause, ErrUnsupportedSecurity) {
		return probeDiagnosis(
			"Kafka 安全配置不完整或不受支持。",
			"security.protocol、SASL 机制、用户名密码或 JAAS 配置缺失或不匹配。",
			"请检查 security.protocol，并确认 SASL 使用受支持的 PLAIN、SCRAM-SHA-256 或 SCRAM-SHA-512 配置。",
		)
	}

	var dnsErr *net.DNSError
	if errors.As(cause, &dnsErr) {
		name := dnsErr.Name
		if name == "" {
			name = probeTarget(def, cause)
		}
		return probeDiagnosis(
			fmt.Sprintf("无法解析 Kafka 主机名 %s。", name),
			"主机名不存在、DNS 配置异常，或当前网络无法访问对应的 DNS 服务。",
			"请核对主机名，检查 DNS/VPN 配置，并确认本机能够解析该地址。",
		)
	}

	if errors.Is(cause, syscall.ECONNREFUSED) {
		target := probeTarget(def, cause)
		port := probePort(target)
		return probeDiagnosis(
			fmt.Sprintf("目标端口 %s 当前不接受 TCP 连接。", target),
			fmt.Sprintf("Kafka 未启动、未监听 %s、listeners 的监听地址或端口配置错误，或防火墙主动拒绝连接。", port),
			fmt.Sprintf("请确认 Broker 进程、listeners 配置及 %s 端口监听状态。", port),
		)
	}

	if errors.Is(cause, syscall.ENETUNREACH) || errors.Is(cause, syscall.EHOSTUNREACH) {
		return probeDiagnosis(
			"目标网络不可达。",
			"本机未连接所需 VPN、缺少目标网段路由、网关异常，或目标主机当前不可达。",
			"请检查 VPN 和路由表，并确认当前网络具备访问 Kafka 所在网段的权限。",
		)
	}

	if errors.Is(cause, context.Canceled) {
		return probeDiagnosis(
			"Kafka 连接验证已取消。",
			"页面离开、请求被主动取消，或客户端进程正在退出。",
			"请保持当前页面打开并重新点击 Validate。",
		)
	}

	var netErr net.Error
	if errors.Is(cause, context.DeadlineExceeded) || (errors.As(cause, &netErr) && netErr.Timeout()) {
		return probeDiagnosis(
			"连接 Kafka 超时。",
			"网络丢包、防火墙静默丢弃连接、目标地址不可达，或 Broker 长时间没有响应。",
			"请检查网络和防火墙策略，确认 Broker 负载，并从客户端所在网络测试目标端口。",
		)
	}

	if errors.Is(cause, syscall.ECONNRESET) || errors.Is(cause, syscall.EPIPE) {
		return probeDiagnosis(
			"TCP 连接被中途关闭。",
			"Broker 或代理主动重置了连接、网络设备中断会话，或客户端与 Broker 的安全协议不一致。",
			"请检查 Broker/代理日志，并核对 security.protocol、SSL 和 SASL 配置。",
		)
	}

	var firstReadEOF *kgo.ErrFirstReadEOF
	if errors.As(cause, &firstReadEOF) || errors.Is(cause, io.EOF) || errors.Is(cause, io.ErrUnexpectedEOF) {
		return probeDiagnosis(
			"Broker 建立连接后立即断开。",
			"SSL/PLAINTEXT 模式不匹配、Broker 要求 SASL 但客户端未配置，或目标端口并非 Kafka 服务。",
			"请核对目标端口用途以及 security.protocol、SSL 和 SASL 配置。",
		)
	}

	var recordErr tls.RecordHeaderError
	if errors.As(cause, &recordErr) {
		return probeDiagnosis(
			"TLS 握手失败。",
			"客户端可能使用 SSL 连接了非 SSL 端口，或目标端口运行的不是 Kafka 服务。",
			"请核对 Broker listener 的协议，并确认 security.protocol 与目标端口一致。",
		)
	}

	var unknownAuthority x509.UnknownAuthorityError
	if errors.As(cause, &unknownAuthority) {
		return probeDiagnosis(
			"Kafka 服务端证书不受信任。",
			"签发证书的 CA 不在系统证书库或所配置的 truststore 中，或服务端证书链不完整。",
			"请导入正确的 CA 证书到 truststore，并检查 Broker 是否发送了完整证书链。",
		)
	}

	var hostnameErr x509.HostnameError
	if errors.As(cause, &hostnameErr) {
		return probeDiagnosis(
			fmt.Sprintf("Kafka 证书与连接地址不匹配：%s。", hostnameErr.Host),
			"服务端证书的 SAN 未包含当前使用的主机名或 IP。",
			"请改用证书中声明的地址，或重新签发包含正确 SAN 的证书。",
		)
	}

	var certificateErr x509.CertificateInvalidError
	if errors.As(cause, &certificateErr) && certificateErr.Reason == x509.Expired {
		return probeDiagnosis(
			"Kafka 服务端证书已过期或尚未生效。",
			"证书有效期配置错误、证书尚未更新，或客户端系统时间不正确。",
			"请检查证书有效期和本机系统时间，必要时更新 Broker 证书。",
		)
	}

	if errors.Is(cause, kerr.SaslAuthenticationFailed) {
		return probeDiagnosis(
			"Kafka SASL 身份认证失败。",
			"用户名、密码或 JAAS 配置错误，账号被禁用，或 Broker 端认证配置不一致。",
			"请重新核对认证凭据、sasl.mechanism 和 Broker 的 SASL 配置。",
		)
	}

	if errors.Is(cause, kerr.UnsupportedSaslMechanism) {
		return probeDiagnosis(
			"客户端与 Broker 的 SASL 机制不匹配。",
			"Broker 未启用当前选择的 PLAIN、SCRAM-SHA-256 或 SCRAM-SHA-512 机制。",
			"请确认 Broker 启用的 SASL 机制，并在客户端选择相同机制。",
		)
	}

	if errors.Is(cause, kerr.ClusterAuthorizationFailed) ||
		errors.Is(cause, kerr.TopicAuthorizationFailed) ||
		errors.Is(cause, kerr.GroupAuthorizationFailed) {
		return probeDiagnosis(
			"Kafka 账号权限不足。",
			"身份认证可能已成功，但账号缺少读取集群 Metadata/DESCRIBE 所需的 ACL 权限。",
			"请让 Kafka 管理员检查账号 ACL，并授予连接验证所需的最小只读权限。",
		)
	}

	if errors.Is(cause, kerr.BrokerNotAvailable) {
		return probeDiagnosis(
			"Kafka Broker 当前不可用。",
			"Broker 正在启动或故障、控制器尚未完成选举，或集群暂时没有可用节点。",
			"请检查 Broker 和控制器状态，等待集群恢复后重新验证。",
		)
	}

	if errors.Is(cause, kerr.RequestTimedOut) || errors.Is(cause, kerr.NetworkException) {
		return probeDiagnosis(
			"Kafka 请求处理超时。",
			"Broker 负载过高、集群正在恢复，或客户端与 Broker 之间的网络不稳定。",
			"请检查 Broker 负载和集群健康状态，并确认网络没有持续丢包。",
		)
	}

	if errors.Is(cause, kerr.UnsupportedVersion) {
		return probeDiagnosis(
			"Kafka 协议版本不兼容。",
			"Broker 版本过旧，或代理返回了客户端不支持的 Kafka 协议响应。",
			"请确认 Broker 版本与客户端兼容，并检查连接是否经过不兼容的代理。",
		)
	}

	return probeDiagnosis(
		"Kafka 连接验证失败，暂时无法识别具体原因。",
		"可能涉及 Broker 状态、网络、安全协议、认证配置或中间代理。",
		"请依次检查连接地址、Broker 状态、security.protocol、SSL/SASL 配置和客户端日志。",
	)
}

func probeDiagnosis(observation, commonCauses, advice string) string {
	return fmt.Sprintf("现象：%s\n常见原因：%s\n建议检查：%s", observation, commonCauses, advice)
}

func probeTarget(def cluster.Definition, cause error) string {
	var opErr *net.OpError
	if errors.As(cause, &opErr) && opErr.Addr != nil && opErr.Addr.String() != "" {
		return opErr.Addr.String()
	}
	if len(def.Conn.BootstrapServers) > 0 && def.Conn.BootstrapServers[0] != "" {
		return def.Conn.BootstrapServers[0]
	}
	return "配置的 Kafka 地址"
}

func probePort(target string) string {
	_, port, err := net.SplitHostPort(target)
	if err == nil && port != "" {
		return port
	}
	return "目标"
}
