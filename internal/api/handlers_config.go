// handlers_config.go implements the config wizard's read + validate endpoints
// (P1c Task 13): getCurrentConfig (GET /api/config) and validateConfig (PUT
// /api/config/validated). Both bridge the generated.ApplicationConfig tree and
// the domain ConfigSnapshot/ConfigValidation via the mapping helpers below; the
// probing itself lives behind ConfigServicer (*appcluster.ConfigService).
package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/cy-kaf/cy-kaf-client/internal/api/generated"
	"github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
)

// maxUploadBytes caps a config-related-file upload (truststores/keystores are
// small; this keeps a hostile client from buffering an unbounded body).
const maxUploadBytes = 10 << 20 // 10 MiB

// ConfigServicer is the narrow interface the config handlers consume;
// *appcluster.ConfigService satisfies it. Restart (the reload half) is a
// separate interface (ReloaderServicer) so the two app types stay decoupled.
type ConfigServicer interface {
	Current() (cluster.ConfigSnapshot, error)
	Validate(ctx context.Context, snap cluster.ConfigSnapshot) (cluster.ConfigValidation, error)
	SaveRelatedFile(ctx context.Context, name string, content []byte) (location string, err error)
}

// ReloaderServicer is the narrow interface RestartWithConfig consumes to apply
// a new config in-process; *appcluster.Reloader satisfies it. Apply validates +
// persists + swaps atomically-or-rolls-back (see the app Reloader's doc).
// Import applies a raw config document (schema-validated, probe-free).
type ReloaderServicer interface {
	Apply(ctx context.Context, snap cluster.ConfigSnapshot) error
	Import(ctx context.Context, content []byte) error
}

// GetCurrentConfig serves GET /api/config: returns the running configuration as
// {properties: <raw tree>}. The Raw tree is emitted verbatim rather than
// funneled through the typed generated.ApplicationConfig struct, so fields this
// codebase doesn't model (auth/rbac/webclient/...) pass through untouched --
// exactly what a subsequent Save (Task 14) needs to avoid dropping them. A read
// failure is a 500 (serverError, no leaked detail).
func (s *apiServer) GetCurrentConfig(w http.ResponseWriter, r *http.Request) {
	snap, err := s.deps.Config.Current()
	if err != nil {
		serverError(w, "GetCurrentConfig", "failed to read configuration", err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"properties": snap.Raw})
}

// ValidateConfig serves PUT /api/config/validated: decodes an ApplicationConfig,
// probes each of its clusters, and returns the per-cluster verdict as
// ApplicationConfigValidation. A body that won't parse is a 400; an unexpected
// probe-layer error is a 500 (per-cluster reachability failures are NOT errors
// here -- they surface as Kafka.Error=true inside the 200 verdict).
func (s *apiServer) ValidateConfig(w http.ResponseWriter, r *http.Request) {
	var body generated.ApplicationConfig
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse(http.StatusBadRequest, "invalid request body"))
		return
	}
	val, err := s.deps.Config.Validate(r.Context(), snapshotFromApplicationConfig(body))
	if err != nil {
		serverError(w, "ValidateConfig", "failed to validate configuration", err)
		return
	}
	writeJSON(w, http.StatusOK, applicationConfigValidationFromDomain(val))
}

// snapshotFromApplicationConfig maps every cluster setting supported by the
// current runtime into its domain Definition. Validate currently probes Kafka,
// but RestartWithConfig reuses this snapshot for the live Resolver; keeping the
// complete mapping here is what makes SR/Connect/KSQL/serde/masking/read-only
// edits take effect immediately instead of only after a process restart.
// Raw is left nil: restartSnapshotFromRequest attaches the lossless properties
// tree used for persistence.
func snapshotFromApplicationConfig(body generated.ApplicationConfig) cluster.ConfigSnapshot {
	var defs []cluster.Definition
	if body.Properties.Kafka != nil && body.Properties.Kafka.Clusters != nil {
		for _, c := range *body.Properties.Kafka.Clusters {
			def := cluster.Definition{
				SerdeConfigs: []cluster.SerdeConfig{},
				Maskings:     []cluster.MaskingRule{},
				Connects:     []cluster.ConnectSpec{},
			}
			if c.Name != nil {
				def.Name = *c.Name
			}
			if c.ReadOnly != nil {
				def.ReadOnly = *c.ReadOnly
			}
			if c.BootstrapServers != nil {
				for _, part := range strings.Split(*c.BootstrapServers, ",") {
					if part = strings.TrimSpace(part); part != "" {
						def.Conn.BootstrapServers = append(def.Conn.BootstrapServers, part)
					}
				}
			}
			if c.Properties != nil {
				sec := make(map[string]string, len(*c.Properties))
				for k, v := range *c.Properties {
					sec[k] = fmt.Sprint(v)
				}
				def.Conn.Security = sec
			}
			if def.Conn.Security == nil {
				def.Conn.Security = map[string]string{}
			}
			if c.Ssl != nil {
				if c.Ssl.TruststoreLocation != nil {
					def.Conn.Security["ssl.truststore.location"] = *c.Ssl.TruststoreLocation
				}
				if c.Ssl.TruststorePassword != nil {
					def.Conn.Security["ssl.truststore.password"] = *c.Ssl.TruststorePassword
				}
				if c.Ssl.Verify != nil && !*c.Ssl.Verify {
					def.Conn.Security["ssl.endpoint.identification.algorithm"] = ""
				}
			}

			if c.SchemaRegistry != nil {
				def.SchemaRegistry.URL = *c.SchemaRegistry
			}
			if c.SchemaRegistryAuth != nil {
				def.SchemaRegistry.Auth = &cluster.SRAuth{}
				if c.SchemaRegistryAuth.Username != nil {
					def.SchemaRegistry.Auth.Username = *c.SchemaRegistryAuth.Username
				}
				if c.SchemaRegistryAuth.Password != nil {
					def.SchemaRegistry.Auth.Password = *c.SchemaRegistryAuth.Password
				}
			}
			if c.SchemaRegistrySsl != nil {
				def.SchemaRegistry.SSL = &cluster.SRSSL{}
				if c.SchemaRegistrySsl.KeystoreLocation != nil {
					def.SchemaRegistry.SSL.KeystoreLocation = *c.SchemaRegistrySsl.KeystoreLocation
				}
				if c.SchemaRegistrySsl.KeystorePassword != nil {
					def.SchemaRegistry.SSL.KeystorePassword = *c.SchemaRegistrySsl.KeystorePassword
				}
			}

			if c.KafkaConnect != nil {
				for _, configured := range *c.KafkaConnect {
					var connect cluster.ConnectSpec
					if configured.Name != nil {
						connect.Name = *configured.Name
					}
					if configured.Address != nil {
						connect.Address = *configured.Address
					}
					if configured.Username != nil || configured.Password != nil {
						connect.Auth = &cluster.ConnectAuth{}
						if configured.Username != nil {
							connect.Auth.Username = *configured.Username
						}
						if configured.Password != nil {
							connect.Auth.Password = *configured.Password
						}
					}
					if configured.KeystoreLocation != nil || configured.KeystorePassword != nil {
						connect.SSL = &cluster.ConnectSSL{}
						if configured.KeystoreLocation != nil {
							connect.SSL.KeystoreLocation = *configured.KeystoreLocation
						}
						if configured.KeystorePassword != nil {
							connect.SSL.KeystorePassword = *configured.KeystorePassword
						}
					}
					def.Connects = append(def.Connects, connect)
				}
			}

			if c.KsqldbServer != nil {
				def.KsqlURL = *c.KsqldbServer
			}
			if c.KsqldbServerAuth != nil {
				def.KsqlAuth = &cluster.KsqlAuth{}
				if c.KsqldbServerAuth.Username != nil {
					def.KsqlAuth.Username = *c.KsqldbServerAuth.Username
				}
				if c.KsqldbServerAuth.Password != nil {
					def.KsqlAuth.Password = *c.KsqldbServerAuth.Password
				}
			}
			if c.KsqldbServerSsl != nil {
				def.KsqlSSL = &cluster.KsqlSSL{}
				if c.KsqldbServerSsl.KeystoreLocation != nil {
					def.KsqlSSL.KeystoreLocation = *c.KsqldbServerSsl.KeystoreLocation
				}
				if c.KsqldbServerSsl.KeystorePassword != nil {
					def.KsqlSSL.KeystorePassword = *c.KsqldbServerSsl.KeystorePassword
				}
			}

			if c.Serde != nil {
				for _, configured := range *c.Serde {
					var serdeConfig cluster.SerdeConfig
					if configured.Name != nil {
						serdeConfig.Name = *configured.Name
					}
					if configured.TopicKeysPattern != nil {
						serdeConfig.TopicKeysPattern = *configured.TopicKeysPattern
					}
					if configured.TopicValuesPattern != nil {
						serdeConfig.TopicValuesPattern = *configured.TopicValuesPattern
					}
					if configured.Properties != nil {
						serdeConfig.Properties = make(map[string]any, len(*configured.Properties))
						for key, value := range *configured.Properties {
							serdeConfig.Properties[key] = value
						}
					}
					def.SerdeConfigs = append(def.SerdeConfigs, serdeConfig)
				}
			}
			if c.DefaultKeySerde != nil {
				def.DefaultKeySerde = *c.DefaultKeySerde
			}
			if c.DefaultValueSerde != nil {
				def.DefaultValueSerde = *c.DefaultValueSerde
			}
			if c.PollingThrottleRate != nil {
				def.PollingThrottleRate = *c.PollingThrottleRate
			}
			if c.Masking != nil {
				for _, configured := range *c.Masking {
					var rule cluster.MaskingRule
					if configured.Type != nil {
						switch *configured.Type {
						case generated.MASK:
							rule.Type = cluster.MaskMask
						case generated.REPLACE:
							rule.Type = cluster.MaskReplace
						default:
							rule.Type = cluster.MaskRemove
						}
					}
					if configured.Fields != nil {
						rule.Fields = append([]string(nil), (*configured.Fields)...)
					}
					if configured.FieldsNamePattern != nil {
						rule.FieldsNamePattern = *configured.FieldsNamePattern
					}
					if configured.MaskingCharsReplacement != nil {
						rule.MaskingCharsReplacement = append([]string(nil), (*configured.MaskingCharsReplacement)...)
					}
					if configured.Replacement != nil {
						rule.Replacement = *configured.Replacement
					}
					if configured.TopicKeysPattern != nil {
						rule.TopicKeysPattern = *configured.TopicKeysPattern
					}
					if configured.TopicValuesPattern != nil {
						rule.TopicValuesPattern = *configured.TopicValuesPattern
					}
					def.Maskings = append(def.Maskings, rule)
				}
			}
			defs = append(defs, def)
		}
	}
	return cluster.ConfigSnapshot{Clusters: defs}
}

// applicationConfigValidationFromDomain maps the domain per-cluster verdict onto
// the contract's ApplicationConfigValidation. Kafka is always present; the P2
// subsystems are emitted only when the domain side chose to populate them (nil
// pointer -> omitted, matching "not configured / not probed").
func applicationConfigValidationFromDomain(val cluster.ConfigValidation) generated.ApplicationConfigValidation {
	clusters := make(map[string]generated.ClusterConfigValidation, len(val.Clusters))
	for name, cv := range val.Clusters {
		gcv := generated.ClusterConfigValidation{Kafka: propertyValidation(cv.Kafka)}
		if cv.SchemaRegistry != nil {
			p := propertyValidation(*cv.SchemaRegistry)
			gcv.SchemaRegistry = &p
		}
		if cv.Ksqldb != nil {
			p := propertyValidation(*cv.Ksqldb)
			gcv.Ksqldb = &p
		}
		if cv.PrometheusStorage != nil {
			p := propertyValidation(*cv.PrometheusStorage)
			gcv.PrometheusStorage = &p
		}
		if len(cv.KafkaConnects) > 0 {
			connects := make(map[string]generated.ApplicationPropertyValidation, len(cv.KafkaConnects))
			for k, pv := range cv.KafkaConnects {
				connects[k] = propertyValidation(pv)
			}
			gcv.KafkaConnects = &connects
		}
		clusters[name] = gcv
	}
	return generated.ApplicationConfigValidation{Clusters: &clusters}
}

// propertyValidation maps one domain PropertyValidation onto the contract's
// ApplicationPropertyValidation, emitting errorMessage only when non-empty.
func propertyValidation(pv cluster.PropertyValidation) generated.ApplicationPropertyValidation {
	out := generated.ApplicationPropertyValidation{Error: pv.Error}
	if pv.ErrorMessage != "" {
		msg := pv.ErrorMessage
		out.ErrorMessage = &msg
	}
	return out
}

// RestartWithConfig serves PUT /api/config: applies a RestartRequest's config
// in-process (validate + persist + smooth reload, all in Reloader.Apply) and
// reports 204. A malformed body or a missing config object is a 400; a reload
// failure (unreachable new config -> rollback, or a persist failure) is a 500.
//
// The snapshot handed to Apply carries BOTH the parsed clusters (for the
// reload's runtime defs) and the raw properties tree pulled from the untyped
// JSON (so Save persists fields this codebase doesn't model, e.g. auth/rbac).
func (s *apiServer) RestartWithConfig(w http.ResponseWriter, r *http.Request) {
	raw, err := io.ReadAll(io.LimitReader(r.Body, maxUploadBytes))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse(http.StatusBadRequest, "invalid request body"))
		return
	}
	var req generated.RestartRequest
	if err := json.Unmarshal(raw, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse(http.StatusBadRequest, "invalid request body"))
		return
	}
	if req.Config == nil {
		writeJSON(w, http.StatusBadRequest, errorResponse(http.StatusBadRequest, "config is required"))
		return
	}
	if err := s.deps.Reloader.Apply(r.Context(), restartSnapshotFromRequest(raw, *req.Config)); err != nil {
		serverError(w, "RestartWithConfig", "failed to apply configuration", err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// restartSnapshotFromRequest builds the reload snapshot from a RestartRequest:
// Clusters come from the typed ApplicationConfig (via the same helper
// validateConfig uses), and Raw is the config.properties subtree pulled straight
// from the untyped JSON so Save can round-trip unmodeled fields. Raw stays nil
// if the body has no config.properties object -- Save then refuses rather than
// truncating config.yaml.
func restartSnapshotFromRequest(raw []byte, cfg generated.ApplicationConfig) cluster.ConfigSnapshot {
	snap := snapshotFromApplicationConfig(cfg)
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err == nil {
		if c, ok := m["config"].(map[string]any); ok {
			if props, ok := c["properties"].(map[string]any); ok {
				snap.Raw = props
			}
		}
	}
	return snap
}

// UploadConfigRelatedFile serves POST /api/config/relatedfiles: stores the
// multipart "file" part via SaveRelatedFile and returns its location. A missing
// file part, an unreadable part, or a store rejection (e.g. a path-traversal
// filename) is a 400; success is 200 UploadedFileInfo.
func (s *apiServer) UploadConfigRelatedFile(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseMultipartForm(maxUploadBytes); err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse(http.StatusBadRequest, "invalid multipart form"))
		return
	}
	f, hdr, err := r.FormFile("file")
	if err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse(http.StatusBadRequest, "missing file part"))
		return
	}
	defer func() { _ = f.Close() }()
	content, err := io.ReadAll(f)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse(http.StatusBadRequest, "unreadable file part"))
		return
	}
	loc, err := s.deps.Config.SaveRelatedFile(r.Context(), hdr.Filename, content)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse(http.StatusBadRequest, "could not save file"))
		return
	}
	writeJSON(w, http.StatusOK, generated.UploadedFileInfo{Location: loc})
}

// ImportConfig serves POST /api/config/import: reads the multipart "file" part
// (the new config.yaml) and applies it via Reloader.Import (parse+validate,
// backup, persist, probe-free reload). A schema violation is a 400; an
// unexpected backend failure is a 500.
func (s *apiServer) ImportConfig(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseMultipartForm(maxUploadBytes); err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse(http.StatusBadRequest, "invalid multipart form"))
		return
	}
	f, _, err := r.FormFile("file")
	if err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse(http.StatusBadRequest, "missing file part"))
		return
	}
	defer func() { _ = f.Close() }()
	content, err := io.ReadAll(f)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse(http.StatusBadRequest, "unreadable file part"))
		return
	}
	if err := s.deps.Reloader.Import(r.Context(), content); err != nil {
		if errors.Is(err, cluster.ErrInvalidConfig) {
			writeJSON(w, http.StatusBadRequest, errorResponse(http.StatusBadRequest, "invalid configuration file"))
			return
		}
		serverError(w, "ImportConfig", "failed to import configuration", err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
