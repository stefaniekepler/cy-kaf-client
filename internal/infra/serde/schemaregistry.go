package serde

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/hamba/avro/v2"

	"github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
	"github.com/cy-kaf/cy-kaf-client/internal/domain/serde"
)

const (
	// SchemaRegistryName is the SR-backed serde's stable Name(), matching
	// upstream kafka-ui's single "SchemaRegistry" serde (which likewise
	// dispatches Avro/JSON/Protobuf on the fetched schema's type rather than
	// exposing one entry per format).
	SchemaRegistryName = "SchemaRegistry"

	confluentMagic = 0x00
	wireHeaderLen  = 5 // 1-byte magic + 4-byte big-endian schema id
)

// SchemaRegistrySerde is the Schema-Registry-backed serde: it renders/parses
// Confluent-wire-format payloads (magic 0x00 + 4-byte big-endian schema id +
// body) by fetching schemas from the cluster's Schema Registry — by id on
// Deserialize, by <topic>-key/-value subject on Serialize. Unlike the 8
// built-in codecs it is stateful (it carries a SchemaRegistryPort and the
// owning Definition) and does I/O; the Provider only offers it as a candidate
// when def.SchemaRegistry.URL != "".
//
// It dispatches on the fetched schema's type: AVRO decodes via hamba/avro to
// JSON text (and encodes JSON text back to Avro); JSON Schema payloads are
// already JSON text (the body after the 5-byte header, passed through);
// PROTOBUF is not supported (returns an error — P2a defers it, ADR-0007). Any
// error out of Deserialize lets the message engine's decodeField fall back to
// rendering the raw bytes, per the serde fallback rule.
type SchemaRegistrySerde struct {
	port cluster.SchemaRegistryPort
	def  cluster.Definition
}

// NewSchemaRegistrySerde builds an SR serde bound to one cluster's Definition
// (for its SchemaRegistry connection) and the port that reaches it.
func NewSchemaRegistrySerde(port cluster.SchemaRegistryPort, def cluster.Definition) SchemaRegistrySerde {
	return SchemaRegistrySerde{port: port, def: def}
}

var _ serde.Serde = SchemaRegistrySerde{}

func (SchemaRegistrySerde) Name() string { return SchemaRegistryName }

func (SchemaRegistrySerde) Description() string {
	return "Confluent Schema Registry (Avro / JSON Schema): wire format magic byte + 4-byte schema id + payload"
}

// CanDeserialize/CanSerialize report true whenever this cluster has a Schema
// Registry configured — which, since the Provider only constructs this serde
// in that case, is always. The actual per-record viability (right magic byte,
// fetchable schema) is decided at Deserialize time, with graceful fallback.
func (s SchemaRegistrySerde) CanDeserialize(_ string, _ serde.Target) bool {
	return s.def.SchemaRegistry.URL != ""
}

func (s SchemaRegistrySerde) CanSerialize(_ string, _ serde.Target) bool {
	return s.def.SchemaRegistry.URL != ""
}

// Schema returns the latest registered schema for topic's <topic>-key/-value
// subject, when one exists — letting the Provider prefer this serde for topics
// that actually carry schemas. A missing subject or any fetch error yields
// ("", false), never an error out of the suggestion path.
func (s SchemaRegistrySerde) Schema(topic string, t serde.Target) (string, bool) {
	sv, err := s.port.SchemaByVersion(context.Background(), s.def, subjectFor(topic, t), "latest")
	if err != nil || sv.Schema == "" {
		return "", false
	}
	return sv.Schema, true
}

// Deserialize renders one Confluent-wire-format record as text: validate the
// 5-byte header, fetch the schema by its embedded id, then dispatch on schema
// type (AVRO → hamba/avro → JSON text; JSON → the body verbatim).
func (s SchemaRegistrySerde) Deserialize(_ string, _ serde.Target, data []byte) (string, error) {
	if len(data) < wireHeaderLen {
		return "", fmt.Errorf("schema registry: payload too short for confluent wire header (%d bytes)", len(data))
	}
	if data[0] != confluentMagic {
		return "", fmt.Errorf("schema registry: unexpected magic byte 0x%02x (want 0x00)", data[0])
	}
	id := int(binary.BigEndian.Uint32(data[1:wireHeaderLen]))
	raw, err := s.port.SchemaByID(context.Background(), s.def, id)
	if err != nil {
		return "", fmt.Errorf("schema registry: fetch schema id %d: %w", id, err)
	}
	body := data[wireHeaderLen:]
	switch normalizeSchemaType(raw.SchemaType) {
	case "JSON":
		return string(body), nil // JSON Schema payloads are already JSON text
	case "AVRO":
		sch, err := avro.Parse(raw.Schema)
		if err != nil {
			return "", fmt.Errorf("schema registry: parse avro schema id %d: %w", id, err)
		}
		var native any
		if err := avro.Unmarshal(sch, body, &native); err != nil {
			return "", fmt.Errorf("schema registry: decode avro id %d: %w", id, err)
		}
		out, err := json.Marshal(native)
		if err != nil {
			return "", fmt.Errorf("schema registry: render avro id %d as json: %w", id, err)
		}
		return string(out), nil
	default:
		return "", fmt.Errorf("schema registry: schema type %q not supported", raw.SchemaType)
	}
}

// Serialize turns text into a Confluent-wire-format record: resolve the
// subject's latest schema for its id, then prepend the 5-byte header to the
// encoded body (AVRO ← hamba/avro from JSON text; JSON ← the text verbatim).
func (s SchemaRegistrySerde) Serialize(topic string, t serde.Target, input string) ([]byte, error) {
	subject := subjectFor(topic, t)
	sv, err := s.port.SchemaByVersion(context.Background(), s.def, subject, "latest")
	if err != nil {
		return nil, fmt.Errorf("schema registry: fetch schema for subject %q: %w", subject, err)
	}
	header := make([]byte, wireHeaderLen)
	header[0] = confluentMagic
	binary.BigEndian.PutUint32(header[1:wireHeaderLen], uint32(sv.ID))

	switch normalizeSchemaType(sv.SchemaType) {
	case "JSON":
		return append(header, []byte(input)...), nil
	case "AVRO":
		sch, err := avro.Parse(sv.Schema)
		if err != nil {
			return nil, fmt.Errorf("schema registry: parse avro schema for %q: %w", subject, err)
		}
		var native any
		if err := json.Unmarshal([]byte(input), &native); err != nil {
			return nil, fmt.Errorf("schema registry: parse input as json for %q: %w", subject, err)
		}
		body, err := avro.Marshal(sch, native)
		if err != nil {
			return nil, fmt.Errorf("schema registry: encode avro for %q: %w", subject, err)
		}
		return append(header, body...), nil
	default:
		return nil, fmt.Errorf("schema registry: schema type %q not supported", sv.SchemaType)
	}
}

// subjectFor is the TopicNameStrategy subject: <topic>-key / <topic>-value.
func subjectFor(topic string, t serde.Target) string {
	if t == serde.TargetKey {
		return topic + "-key"
	}
	return topic + "-value"
}

// normalizeSchemaType upper-cases the registry's schema-type string and maps
// the empty/omitted value to AVRO (Confluent's default when a schema is
// registered without an explicit type).
func normalizeSchemaType(s string) string {
	if s == "" {
		return "AVRO"
	}
	return strings.ToUpper(s)
}
