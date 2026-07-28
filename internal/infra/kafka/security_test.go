package kafka

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	keystore "github.com/pavlo-v-chernykh/keystore-go/v4"
	"github.com/stretchr/testify/require"

	"github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
)

// clusterConn 把 security 属性 map 包装成 buildOpts 期望的 ConnectionSpec，
// 固定一个占位 bootstrap（本文件测试均不实际拨号）。
func clusterConn(sec map[string]string) cluster.ConnectionSpec {
	return cluster.ConnectionSpec{BootstrapServers: []string{"k:9092"}, Security: sec}
}

func writeSelfSignedPEM(t *testing.T) string {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	tpl := x509.Certificate{SerialNumber: big.NewInt(1),
		Subject: pkix.Name{CommonName: "test-ca"}, NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true}
	der, err := x509.CreateCertificate(rand.Reader, &tpl, &tpl, &key.PublicKey, key)
	require.NoError(t, err)
	p := filepath.Join(t.TempDir(), "ca.pem")
	require.NoError(t, os.WriteFile(p,
		pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600))
	return p
}

func TestSecurityMatrix(t *testing.T) {
	pemPath := writeSelfSignedPEM(t)
	cases := []struct {
		name    string
		sec     map[string]string
		wantErr bool
	}{
		{"plaintext default", nil, false},
		{"ssl system roots", map[string]string{"security.protocol": "SSL"}, false},
		{"ssl custom pem truststore", map[string]string{
			"security.protocol": "SSL", "ssl.truststore.location": pemPath}, false},
		{"sasl plain via explicit props", map[string]string{
			"security.protocol": "SASL_PLAINTEXT", "sasl.mechanism": "PLAIN",
			"sasl.username": "u", "sasl.password": "p"}, false},
		{"sasl scram-256 via jaas", map[string]string{
			"security.protocol": "SASL_SSL", "sasl.mechanism": "SCRAM-SHA-256",
			"sasl.jaas.config": `org.apache.kafka.common.security.scram.ScramLoginModule required username="u2" password="p2";`}, false},
		{"sasl scram-512", map[string]string{
			"security.protocol": "SASL_PLAINTEXT", "sasl.mechanism": "SCRAM-SHA-512",
			"sasl.username": "u", "sasl.password": "p"}, false},
		{"sasl missing credentials", map[string]string{
			"security.protocol": "SASL_PLAINTEXT", "sasl.mechanism": "PLAIN"}, true},
		{"sasl unsupported mechanism (gssapi)", map[string]string{
			"security.protocol": "SASL_SSL", "sasl.mechanism": "GSSAPI",
			"sasl.username": "u", "sasl.password": "p"}, true},
		{"unknown protocol", map[string]string{"security.protocol": "QUIC"}, true},
		{"truststore file missing", map[string]string{
			"security.protocol": "SSL", "ssl.truststore.location": "/nope.pem"}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := buildOpts(clusterConn(tc.sec))
			if tc.wantErr {
				if tc.name == "sasl unsupported mechanism (gssapi)" {
					require.ErrorContains(t, err, "GSSAPI")
				} else {
					require.Error(t, err)
				}
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestParseJaasCredentials(t *testing.T) {
	// Standard order
	u, p, err := parseJaas(`org.apache.kafka.common.security.plain.PlainLoginModule required username="alice" password="s3cr=et";`)
	require.NoError(t, err)
	require.Equal(t, "alice", u)
	require.Equal(t, "s3cr=et", p)

	// Password before username
	u, p, err = parseJaas(`org.apache.kafka.common.security.plain.PlainLoginModule required password="s3cr=et" username="alice";`)
	require.NoError(t, err)
	require.Equal(t, "alice", u)
	require.Equal(t, "s3cr=et", p)

	// Username/password with intervening option (serviceName="kafka")
	u, p, err = parseJaas(`org.apache.kafka.common.security.scram.ScramLoginModule required username="user" serviceName="kafka" password="pass";`)
	require.NoError(t, err)
	require.Equal(t, "user", u)
	require.Equal(t, "pass", p)

	// Missing password
	_, _, err = parseJaas(`org.apache.kafka.common.security.plain.PlainLoginModule required username="alice";`)
	require.Error(t, err)

	// Garbage
	_, _, err = parseJaas(`garbage`)
	require.Error(t, err)
}

func TestHostnameVerificationSkip(t *testing.T) {
	cfg, err := tlsConfigFor(map[string]string{
		"security.protocol":                     "SSL",
		"ssl.endpoint.identification.algorithm": "",
	})
	require.NoError(t, err)
	require.True(t, cfg.InsecureSkipVerify)      // 停用内建校验……
	require.NotNil(t, cfg.VerifyPeerCertificate) // ……但保留自定义链校验（仅跳过主机名）
}

// TestHostnameVerificationSkipStillValidatesCertChain 实际调用（而不仅仅是判断
// 非 nil）hostname-skip 分支装配的 VerifyPeerCertificate 闭包，逐分支验证它的
// 链校验语义：跳过的只是主机名比对，证书链（签发者/信任根）校验必须依然生效。
func TestHostnameVerificationSkipStillValidatesCertChain(t *testing.T) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	tpl := x509.Certificate{SerialNumber: big.NewInt(3),
		Subject: pkix.Name{CommonName: "hostname-skip-ca"}, NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true}
	der, err := x509.CreateCertificate(rand.Reader, &tpl, &tpl, &key.PublicKey, key)
	require.NoError(t, err)
	pemPath := filepath.Join(t.TempDir(), "hostname-skip-ca.pem")
	require.NoError(t, os.WriteFile(pemPath,
		pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600))

	cfg, err := tlsConfigFor(map[string]string{
		"security.protocol":                     "SSL",
		"ssl.truststore.location":               pemPath,
		"ssl.endpoint.identification.algorithm": "",
	})
	require.NoError(t, err)
	require.True(t, cfg.InsecureSkipVerify)
	require.NotNil(t, cfg.VerifyPeerCertificate)

	// 正例：受信证书作为 leaf 呈递，外加一份自身副本充当 intermediate（走一遍
	// Intermediates 拼装分支）——应通过链校验，证明"跳过主机名 ≠ 跳过链校验"。
	require.NoError(t, cfg.VerifyPeerCertificate([][]byte{der, der}, nil))

	// 负例 1：呈递一个不在 truststore 内的证书——链校验必须失败。
	otherKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	otherTpl := x509.Certificate{SerialNumber: big.NewInt(4),
		Subject: pkix.Name{CommonName: "untrusted"}, NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true}
	otherDER, err := x509.CreateCertificate(rand.Reader, &otherTpl, &otherTpl, &otherKey.PublicKey, otherKey)
	require.NoError(t, err)
	require.Error(t, cfg.VerifyPeerCertificate([][]byte{otherDER}, nil))

	// 负例 2：没有呈递任何证书——必须明确报错，而不是静默放行。
	require.Error(t, cfg.VerifyPeerCertificate(nil, nil))

	// 负例 3：无法解析的证书字节。
	require.Error(t, cfg.VerifyPeerCertificate([][]byte{[]byte("not a certificate")}, nil))
}

// writeJKSTruststore 用 keystore-go 本身写出一个只含一条 TrustedCertificateEntry
// 的 JKS 文件，作为 loadTruststore 的往返 fixture（自洽：写入方与被测的读取方
// 共享同一份 keystore-go API 理解，不依赖外部 keytool）。
func writeJKSTruststore(t *testing.T, password string) string {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	tpl := x509.Certificate{SerialNumber: big.NewInt(2),
		Subject: pkix.Name{CommonName: "test-jks-ca"}, NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true}
	der, err := x509.CreateCertificate(rand.Reader, &tpl, &tpl, &key.PublicKey, key)
	require.NoError(t, err)

	ks := keystore.New()
	require.NoError(t, ks.SetTrustedCertificateEntry("test-ca-alias", keystore.TrustedCertificateEntry{
		CreationTime: time.Now(),
		Certificate:  keystore.Certificate{Type: "X509", Content: der},
	}))

	p := filepath.Join(t.TempDir(), "truststore.jks")
	f, err := os.Create(p)
	require.NoError(t, err)
	defer func() { _ = f.Close() }()
	require.NoError(t, ks.Store(f, []byte(password)))
	return p
}

func TestLoadTruststoreJKSRoundTrip(t *testing.T) {
	jksPath := writeJKSTruststore(t, "jks-test-pw")

	pool, err := LoadTruststore(jksPath, "jks-test-pw")
	require.NoError(t, err)
	require.NotNil(t, pool)
	require.False(t, pool.Equal(x509.NewCertPool())) // 非空：确实从 JKS 读回了证书

	// 也走一遍 securityOpts/buildOpts 的完整分发路径（.jks 后缀识别），
	// 而不只是单测 loadTruststore 本身。
	_, err = buildOpts(clusterConn(map[string]string{
		"security.protocol":       "SSL",
		"ssl.truststore.location": jksPath,
		"ssl.truststore.password": "jks-test-pw",
	}))
	require.NoError(t, err)
}

// TestSaslMechanismPropagatesJaasParseError 覆盖 saslMechanism 内调用 parseJaas
// 后错误传播的分支：TestParseJaasCredentials 只测了 parseJaas 本身，不经过
// buildOpts/securityOpts/saslMechanism 这条调用链。
func TestSaslMechanismPropagatesJaasParseError(t *testing.T) {
	_, err := buildOpts(clusterConn(map[string]string{
		"security.protocol": "SASL_PLAINTEXT",
		"sasl.mechanism":    "PLAIN",
		"sasl.jaas.config":  "not a valid jaas config string",
	}))
	require.Error(t, err)
	require.ErrorIs(t, err, ErrUnsupportedSecurity)
}

// TestLoadTruststoreJKSCorruptFileErrors 覆盖 loadTruststore 的 JKS 分支里
// ks.Load 失败的路径（文件存在、后缀是 .jks，但内容不是合法 JKS 编码）。
func TestLoadTruststoreJKSCorruptFileErrors(t *testing.T) {
	p := filepath.Join(t.TempDir(), "corrupt.jks")
	require.NoError(t, os.WriteFile(p, []byte("this is not a JKS keystore"), 0o600))
	_, err := LoadTruststore(p, "whatever")
	require.Error(t, err)
}

// TestLoadTruststorePEMInvalidContentErrors 覆盖 loadTruststore 的 PEM 分支里
// AppendCertsFromPEM 失败的路径（文件存在、可读，但内容不含合法 PEM 证书），
// 区别于「文件不存在」（TestSecurityMatrix 的 truststore file missing 用例）。
func TestLoadTruststorePEMInvalidContentErrors(t *testing.T) {
	p := filepath.Join(t.TempDir(), "not-a-cert.pem")
	require.NoError(t, os.WriteFile(p, []byte("this is not PEM content"), 0o600))
	_, err := LoadTruststore(p, "")
	require.Error(t, err)
}

// TestLoadTruststoreJKSEmptyErrors 覆盖 loadTruststore 的 JKS 分支在没有任何
// TrustedCertificateEntry 时报错的路径（防止返回空信任池导致静默接受所有证书）。
func TestLoadTruststoreJKSEmptyErrors(t *testing.T) {
	// 创建一个空 JKS keystore（没有任何条目）
	ks := keystore.New()
	p := filepath.Join(t.TempDir(), "empty.jks")
	f, err := os.Create(p)
	require.NoError(t, err)
	defer func() { _ = f.Close() }()
	require.NoError(t, ks.Store(f, []byte("test-pw")))

	_, err = LoadTruststore(p, "test-pw")
	require.Error(t, err)
	require.ErrorContains(t, err, "受信证书条目")
}
