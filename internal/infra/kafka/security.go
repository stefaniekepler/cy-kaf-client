package kafka

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strings"

	keystore "github.com/pavlo-v-chernykh/keystore-go/v4"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

// saslMech 是 sasl.Mechanism 的本地别名，避免在本包内到处写完整包名。
type saslMech = sasl.Mechanism

// securityOpts 把上游同形的 security 属性映射为 kgo 选项。
// 支持范围（P1a）：PLAINTEXT/SSL/SASL_PLAINTEXT/SASL_SSL；
// SASL 机制 PLAIN/SCRAM-SHA-256/SCRAM-SHA-512；truststore PEM/JKS。
func securityOpts(sec map[string]string) ([]kgo.Opt, error) {
	proto := sec["security.protocol"]
	var opts []kgo.Opt
	switch proto {
	case "", "PLAINTEXT", "SASL_PLAINTEXT":
	case "SSL", "SASL_SSL":
		cfg, err := tlsConfigFor(sec)
		if err != nil {
			return nil, err
		}
		opts = append(opts, kgo.DialTLSConfig(cfg))
	default:
		return nil, fmt.Errorf("%w: %s", ErrUnsupportedSecurity, proto)
	}
	if strings.HasPrefix(proto, "SASL_") {
		mech, err := saslMechanism(sec)
		if err != nil {
			return nil, err
		}
		opts = append(opts, kgo.SASL(mech))
	}
	return opts, nil
}

func saslMechanism(sec map[string]string) (saslMech, error) {
	user, pass := sec["sasl.username"], sec["sasl.password"]
	if user == "" && pass == "" {
		if jaas := sec["sasl.jaas.config"]; jaas != "" {
			var err error
			if user, pass, err = parseJaas(jaas); err != nil {
				return nil, err
			}
		}
	}
	if user == "" || pass == "" {
		return nil, fmt.Errorf("%w: SASL 需要 sasl.username/sasl.password 或可解析的 sasl.jaas.config", ErrUnsupportedSecurity)
	}
	switch m := sec["sasl.mechanism"]; m {
	case "PLAIN":
		return plain.Auth{User: user, Pass: pass}.AsMechanism(), nil
	case "SCRAM-SHA-256":
		return scram.Auth{User: user, Pass: pass}.AsSha256Mechanism(), nil
	case "SCRAM-SHA-512":
		return scram.Auth{User: user, Pass: pass}.AsSha512Mechanism(), nil
	default:
		return nil, fmt.Errorf("%w: sasl.mechanism=%q（支持 PLAIN/SCRAM-SHA-256/SCRAM-SHA-512）", ErrUnsupportedSecurity, m)
	}
}

var (
	jaasUserRe = regexp.MustCompile(`username="([^"]*)"`)
	jaasPassRe = regexp.MustCompile(`password="([^"]*)"`)
)

// parseJaas 从 JAAS 配置串提取用户名/密码。限制（如实注释）：不支持含转义引号(\")的值。
func parseJaas(jaas string) (user, pass string, err error) {
	u := jaasUserRe.FindStringSubmatch(jaas)
	p := jaasPassRe.FindStringSubmatch(jaas)
	if u == nil || p == nil {
		return "", "", fmt.Errorf("%w: 无法从 sasl.jaas.config 解析凭据（需要 username=\"...\" 与 password=\"...\"）", ErrUnsupportedSecurity)
	}
	return u[1], p[1], nil
}

func tlsConfigFor(sec map[string]string) (*tls.Config, error) {
	cfg := &tls.Config{MinVersion: tls.VersionTLS12}
	if loc := sec["ssl.truststore.location"]; loc != "" {
		pool, err := LoadTruststore(loc, sec["ssl.truststore.password"])
		if err != nil {
			return nil, err
		}
		cfg.RootCAs = pool
	}
	// 上游语义：ssl.endpoint.identification.algorithm="" 表示跳过主机名校验，
	// 但证书链校验保留（不同于整体 InsecureSkipVerify）。
	if v, ok := sec["ssl.endpoint.identification.algorithm"]; ok && v == "" {
		roots := cfg.RootCAs
		cfg.InsecureSkipVerify = true
		cfg.VerifyPeerCertificate = func(raw [][]byte, _ [][]*x509.Certificate) error {
			certs := make([]*x509.Certificate, 0, len(raw))
			for _, rc := range raw {
				c, err := x509.ParseCertificate(rc)
				if err != nil {
					return err
				}
				certs = append(certs, c)
			}
			if len(certs) == 0 {
				return errors.New("no peer certificate")
			}
			opts := x509.VerifyOptions{Roots: roots, Intermediates: x509.NewCertPool()}
			for _, c := range certs[1:] {
				opts.Intermediates.AddCert(c)
			}
			_, err := certs[0].Verify(opts) // 无 DNSName：跳过主机名，保留链校验
			return err
		}
	}
	return cfg, nil
}

func LoadTruststore(loc, password string) (*x509.CertPool, error) {
	raw, err := os.ReadFile(loc)
	if err != nil {
		return nil, fmt.Errorf("read truststore: %w", err)
	}
	pool := x509.NewCertPool()
	if strings.HasSuffix(strings.ToLower(loc), ".jks") {
		ks := keystore.New()
		if err := ks.Load(strings.NewReader(string(raw)), []byte(password)); err != nil {
			return nil, fmt.Errorf("parse JKS truststore: %w", err)
		}
		added := 0
		for _, alias := range ks.Aliases() {
			if e, err := ks.GetTrustedCertificateEntry(alias); err == nil {
				if c, err := x509.ParseCertificate(e.Certificate.Content); err == nil {
					pool.AddCert(c)
					added++
				}
			}
		}
		if added == 0 {
			return nil, fmt.Errorf("truststore %s: JKS 中没有任何受信证书条目（TrustedCertificateEntry）——是否误配了 keystore 而非 truststore？", loc)
		}
		return pool, nil
	}
	if !pool.AppendCertsFromPEM(raw) {
		return nil, fmt.Errorf("truststore %s: 不含合法 PEM 证书", loc)
	}
	return pool, nil
}
