package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"strings"

	"github.com/cy-kaf/cy-kaf-client/internal/api/generated"
	appcluster "github.com/cy-kaf/cy-kaf-client/internal/app/cluster"
	"github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
)

// TopicServicer is the narrow interface api's topic handlers consume;
// app.TopicService satisfies it (same "resolve name -> Definition, delegate"
// shape as BrokerAdmin/LogDirser above, plus List/Details which additionally
// read the periodically-refreshed cache — mirrors ClusterStater.Get). Its
// List/Details signatures reference appcluster's own TopicListQuery/TopicPage
// types directly rather than duplicating them in this package: api importing
// app is the allowed dependency direction (only api->infra is denied), and
// these two types only exist as an app-layer concept (the contract's query
// params/response shape, not a domain type).
type TopicServicer interface {
	List(ctx context.Context, name string, q appcluster.TopicListQuery) (appcluster.TopicPage, error)
	Details(ctx context.Context, name, topic string) (cluster.TopicState, []cluster.ConfigEntry, error)
	Configs(ctx context.Context, name, topic string) ([]cluster.ConfigEntry, error)
	Acls(ctx context.Context, name, topic string) ([]cluster.AclBinding, error)
	ActiveProducers(ctx context.Context, name, topic string) ([]cluster.ProducerState, error)

	// Connectors is P1b Task 8a's empty getTopicConnectors stub (see
	// docs/superpowers/plans/2026-07-04-p1b-topics-groups.md's 2026-07-04
	// revision): resolves name so an unknown cluster still 404s like every
	// other topic endpoint, but there is no Kafka Connect-backed data source
	// behind it yet (a real query is P2's scope) — nothing to return beyond
	// success/failure, hence error-only rather than a slice of some
	// not-yet-justified domain type. GetTopicConnectors always turns a nil
	// error into the contract's non-nil empty array.
	Connectors(ctx context.Context, name, topic string) error

	// Create/Delete/UpdateConfigs/Recreate/Clone/IncreasePartitions/
	// ChangeReplicationFactor are P1b Task 5's write surface — same
	// resolve-name-then-delegate shape as the read methods above, all
	// backed by app.TopicService.
	Create(ctx context.Context, name string, spec cluster.TopicSpec) (cluster.TopicState, error)
	Delete(ctx context.Context, name, topic string) error
	UpdateConfigs(ctx context.Context, name, topic string, desired map[string]string) (cluster.TopicState, error)
	Recreate(ctx context.Context, name, topic string) (cluster.TopicState, error)
	Clone(ctx context.Context, name, sourceTopic, newTopic string) (cluster.TopicState, error)
	IncreasePartitions(ctx context.Context, name, topic string, total int32) error
	ChangeReplicationFactor(ctx context.Context, name, topic string, target int16) error
}

// GetTopics serves /api/clusters/{clusterName}/topics: the paged/filtered/
// sorted topic list (TopicService.List -> filterSortPage does the actual
// work; this handler only translates params in and the result out).
func (s *apiServer) GetTopics(w http.ResponseWriter, r *http.Request, clusterName string, params generated.GetTopicsParams) {
	page, err := s.deps.Topics.List(r.Context(), clusterName, topicListQueryFrom(params))
	if err != nil {
		if errors.Is(err, appcluster.ErrUnknownCluster) {
			writeJSON(w, http.StatusNotFound, errorResponse(http.StatusNotFound, "cluster not found"))
			return
		}
		serverError(w, "GetTopics", "failed to list topics", err)
		return
	}
	rows := topicsToGeneratedRows(page.Topics)
	writeJSON(w, http.StatusOK, generated.TopicsResponse{
		Topics:    &rows,
		PageCount: ptr(int32(page.PageCount)),
	})
}

// GetTopicsCsv serves /api/clusters/{clusterName}/topics/csv: the same rows
// as GetTopics, rendered as CSV via rowsToCsv (csv.go). Unlike GetTopics, the
// contract declares no page/perPage params here — CSV export means "every
// topic", not one page — see topicListQueryFromCsv.
func (s *apiServer) GetTopicsCsv(w http.ResponseWriter, r *http.Request, clusterName string, params generated.GetTopicsCsvParams) {
	page, err := s.deps.Topics.List(r.Context(), clusterName, topicListQueryFromCsv(params))
	if err != nil {
		if errors.Is(err, appcluster.ErrUnknownCluster) {
			writeJSON(w, http.StatusNotFound, errorResponse(http.StatusNotFound, "cluster not found"))
			return
		}
		serverError(w, "GetTopicsCsv", "failed to list topics", err)
		return
	}
	body, err := rowsToCsv(topicsToGeneratedRows(page.Topics))
	if err != nil {
		// Unreachable today (topicsToGeneratedRows always returns a
		// []generated.Topic, which rowsToCsv always accepts) — guarded
		// anyway, same defensive shape as GetBrokersCsv.
		serverError(w, "GetTopicsCsv", "failed to render topics csv", err)
		return
	}
	w.Header().Set("Content-Type", "text/csv")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(body))
}

// baseTopicListQuery maps the four query params GetTopics and GetTopicsCsv
// share onto an appcluster.TopicListQuery; each caller layers its own
// page/perPage handling on top (GetTopics has real ones, GetTopicsCsv
// doesn't — see topicListQueryFromCsv).
func baseTopicListQuery(showInternal *bool, search *string, orderBy *generated.TopicColumnsToSort, sortOrder *generated.SortOrder) appcluster.TopicListQuery {
	var q appcluster.TopicListQuery
	if showInternal != nil {
		q.ShowInternal = *showInternal
	}
	if search != nil {
		q.Search = *search
	}
	if orderBy != nil {
		q.OrderBy = string(*orderBy)
	}
	if sortOrder != nil {
		q.SortOrder = string(*sortOrder)
	}
	return q
}

func topicListQueryFrom(p generated.GetTopicsParams) appcluster.TopicListQuery {
	q := baseTopicListQuery(p.ShowInternal, p.Search, p.OrderBy, p.SortOrder)
	if p.Page != nil {
		q.Page = int(*p.Page)
	}
	if p.PerPage != nil {
		q.PerPage = int(*p.PerPage)
	}
	return q
}

// topicListQueryFromCsv sets PerPage to a value no real topic list will ever
// reach, so filterSortPage's single default page (Page defaults to 1) always
// contains every row — the contract's getTopicsCsv declares no page/perPage
// params at all, so "all of it" is the only sensible reading.
func topicListQueryFromCsv(p generated.GetTopicsCsvParams) appcluster.TopicListQuery {
	q := baseTopicListQuery(p.ShowInternal, p.Search, p.OrderBy, p.SortOrder)
	q.PerPage = math.MaxInt32
	return q
}

// GetTopicDetails serves /api/clusters/{clusterName}/topics/{topicName}: the
// contract's Topic-superset TopicDetails shape (adds partitions detail,
// cleanUpPolicy, key/value serde — serde is out of scope for P1b Task 4 and
// stays unset/nil, same "no data source yet" degrade as Topic's
// bytesInPerSec/bytesOutPerSec). Unlike Topic, the contract's TopicDetails
// shape has no messagesCount field at all (confirmed against
// generated/models.gen.go — P1b Task 8c), so there is nothing to compute or
// leave unset here.
func (s *apiServer) GetTopicDetails(w http.ResponseWriter, r *http.Request, clusterName, topicName string) {
	ts, cfgs, err := s.deps.Topics.Details(r.Context(), clusterName, topicName)
	if err != nil {
		if errors.Is(err, appcluster.ErrUnknownCluster) {
			writeJSON(w, http.StatusNotFound, errorResponse(http.StatusNotFound, "cluster not found"))
			return
		}
		serverError(w, "GetTopicDetails", "failed to get topic details", err)
		return
	}
	writeJSON(w, http.StatusOK, topicDetailsToGenerated(ts, cfgs))
}

// GetTopicConfigs serves /api/clusters/{clusterName}/topics/{topicName}/config:
// the topic's full configuration entry list (dynamic/static/default, with
// synonyms), fetched live on every call (TopicService.Configs, no caching —
// same as GetBrokerConfig).
func (s *apiServer) GetTopicConfigs(w http.ResponseWriter, r *http.Request, clusterName, topicName string) {
	cfgs, err := s.deps.Topics.Configs(r.Context(), clusterName, topicName)
	if err != nil {
		if errors.Is(err, appcluster.ErrUnknownCluster) {
			writeJSON(w, http.StatusNotFound, errorResponse(http.StatusNotFound, "cluster not found"))
			return
		}
		serverError(w, "GetTopicConfigs", "failed to describe topic configs", err)
		return
	}
	out := make([]generated.TopicConfig, 0, len(cfgs))
	for _, c := range cfgs {
		out = append(out, topicConfigToGenerated(c))
	}
	writeJSON(w, http.StatusOK, out)
}

// ListTopicAcls serves /api/clusters/{clusterName}/topics/{topicName}/acls:
// every ACL bound to the topic (TopicService.Acls, live on every call).
func (s *apiServer) ListTopicAcls(w http.ResponseWriter, r *http.Request, clusterName, topicName string) {
	acls, err := s.deps.Topics.Acls(r.Context(), clusterName, topicName)
	if err != nil {
		if errors.Is(err, appcluster.ErrUnknownCluster) {
			writeJSON(w, http.StatusNotFound, errorResponse(http.StatusNotFound, "cluster not found"))
			return
		}
		serverError(w, "ListTopicAcls", "failed to list topic acls", err)
		return
	}
	out := make([]generated.KafkaAcl, 0, len(acls))
	for _, a := range acls {
		out = append(out, aclToGenerated(a))
	}
	writeJSON(w, http.StatusOK, out)
}

// GetActiveProducerStates serves
// /api/clusters/{clusterName}/topics/{topicName}/activeproducers: the
// topic's active (in-flight transactional) producers (TopicService.
// ActiveProducers, live on every call).
func (s *apiServer) GetActiveProducerStates(w http.ResponseWriter, r *http.Request, clusterName, topicName string) {
	states, err := s.deps.Topics.ActiveProducers(r.Context(), clusterName, topicName)
	if err != nil {
		if errors.Is(err, appcluster.ErrUnknownCluster) {
			writeJSON(w, http.StatusNotFound, errorResponse(http.StatusNotFound, "cluster not found"))
			return
		}
		serverError(w, "GetActiveProducerStates", "failed to describe active producers", err)
		return
	}
	out := make([]generated.TopicProducerState, 0, len(states))
	for _, p := range states {
		out = append(out, producerStateToGenerated(p))
	}
	writeJSON(w, http.StatusOK, out)
}

// GetTopicConnectors serves
// /api/clusters/{clusterName}/topics/{topicName}/connectors: P1b Task 8a's
// empty stub (see docs/superpowers/plans/2026-07-04-p1b-topics-groups.md's
// 2026-07-04 revision) for the contract's array-of-FullConnectorInfo
// response. Still resolves clusterName through TopicServicer.Connectors so
// an unknown cluster 404s like every other topic endpoint, but a known
// cluster always answers 200 with a non-nil empty slice — [] on the wire,
// never null (the vendored frontend's Topic.tsx unconditionally reads
// connectors.length, which throws on null; an empty array is what makes it
// hide the Connect tab). A real Kafka Connect-backed query stays out of
// scope until P2.
func (s *apiServer) GetTopicConnectors(w http.ResponseWriter, r *http.Request, clusterName, topicName string) {
	if err := s.deps.Topics.Connectors(r.Context(), clusterName, topicName); err != nil {
		if errors.Is(err, appcluster.ErrUnknownCluster) {
			writeJSON(w, http.StatusNotFound, errorResponse(http.StatusNotFound, "cluster not found"))
			return
		}
		serverError(w, "GetTopicConnectors", "failed to get topic connectors", err)
		return
	}
	writeJSON(w, http.StatusOK, []generated.FullConnectorInfo{})
}

// --- P1b Task 5: write endpoints ---

// --- P1b Task 8d: lenient configs decoding ---
//
// CreateTopic/UpdateTopic used to json.Decode straight into generated.
// TopicCreation/TopicUpdate, whose Configs field is *map[string]string — a
// strict decode that rejects the *entire* request body the instant any one
// configs value isn't a JSON string. Task 9's acceptance smoke run found that
// the vendored frontend's Edit dialog (formatTopicUpdate,
// frontend/src/lib/hooks/api/topics.ts:204-224) sends numeric configs like
// retentionBytes as a bare JSON number, never calling .toString() on it —
// every "Edit settings -> Update" submission 400ed as a result ("Edit
// settings -> Update" is the only UI path that edits topic configs at all).
// Upstream's Java/Jackson backend tolerates this (numbers/booleans coerce to
// String on deserialize); our Go decode didn't. Task 9's e2e suite never
// caught it because the vendored e2e feature set has no Edit/Update
// scenario. User ruling: fix the backend to match upstream's leniency
// (frontend is off-limits/vendored).
//
// topicCreationLenient/topicUpdateLenient below decode configs as
// map[string]json.RawMessage instead of *map[string]string, and
// coerceConfigValue/coerceConfigMap convert each raw value to a string the
// same lenient way Jackson would. Every other field keeps its normal strict
// decode (name/partitions/replicationFactor's shapes aren't in question
// here).
//
// This makes our REQUEST side deliberately more permissive than the OpenAPI
// contract (configs: additionalProperties: type: string) — the contract
// remains the single source of truth for the RESPONSE shape and for what a
// well-formed client is documented to send; we simply also accept a
// slightly wider set of inputs to match upstream's real behaviour and
// unblock the vendored frontend. See handlers_topic_test.go's
// TestUpdateTopicNumericConfigValueCoercesToString and its neighbors for how
// the tests document this intentional divergence (they deliberately skip
// request-side contract validation for exactly this reason).

// topicCreationLenient mirrors generated.TopicCreation's JSON shape field for
// field, except Configs: map[string]json.RawMessage instead of
// *map[string]string, so a non-string configs value doesn't fail the whole
// decode before coerceConfigMap gets a chance to convert it.
type topicCreationLenient struct {
	Name              string                     `json:"name"`
	Partitions        int32                      `json:"partitions"`
	ReplicationFactor *int32                     `json:"replicationFactor,omitempty"`
	Configs           map[string]json.RawMessage `json:"configs,omitempty"`
}

// topicUpdateLenient is generated.TopicUpdate's only field (Configs),
// decoded leniently the same way as topicCreationLenient.
type topicUpdateLenient struct {
	Configs map[string]json.RawMessage `json:"configs,omitempty"`
}

// coerceConfigValue converts one decoded JSON configs value to the string
// Kafka configs are always represented as, matching upstream kafka-ui's
// Jackson leniency (numeric/boolean configs values coerce to their string
// form rather than rejecting the request). skip reports a JSON null value:
// the vendored frontend never actually sends one, but treating it as "key
// omitted" rather than the string "" is the conservative reading — an empty
// string is itself a real, different configs value ("set to empty") from
// "don't touch this key", and null is ambiguous between the two.
//
// object/array values (s[0] == '{' || s[0] == '[') are a genuine error, not
// leniency's remit: there is no sensible string coercion for a nested JSON
// structure, so those still fail the request (400), same as before this
// task.
//
// ⚠️ Numbers must NEVER be json.Unmarshal'd into a float64 and then
// fmt.Sprint'd back to a string: 604800000 (a common retention.ms value)
// would round-trip through float64 as the scientific-notation string
// "6.048e+08", silently corrupting the config — retention.bytes and other
// large-integer configs are just as exposed, and some would also lose
// precision outright. The raw JSON token text (TrimSpace'd, nothing more) IS
// the correct string form for every scalar case here. A JSON string value
// still needs unmarshaling into a Go string to strip its quotes/unescape it
// — that's the one case where reparsing is safe, since it can't lose
// precision — but numbers and booleans are returned as their literal source
// text, untouched.
func coerceConfigValue(raw json.RawMessage) (value string, skip bool, err error) {
	s := strings.TrimSpace(string(raw))
	switch {
	case s == "" || s == "null":
		return "", true, nil
	case s[0] == '"':
		var str string
		if err := json.Unmarshal(raw, &str); err != nil {
			return "", false, err
		}
		return str, false, nil
	case s[0] == '{' || s[0] == '[':
		return "", false, fmt.Errorf("config value must be a string, number, or boolean, not an object/array: %s", s)
	default:
		// Number or boolean literal: the raw JSON token text is already the
		// desired string form (see the float64 warning above) — a plain
		// string(raw) assignment, no numeric type ever involved.
		return s, false, nil
	}
}

// coerceConfigMap runs coerceConfigValue over every entry, preserving the
// nil-vs-empty-map distinction generated.TopicCreation/TopicUpdate's
// *map[string]string had: a request with no "configs" key at all decodes raw
// to a nil map, which coerceConfigMap passes straight through as nil (->
// callers leave TopicSpec.Configs/UpdateConfigs' desired at its own zero
// value, same as the old `if tc.Configs != nil` guard this replaces);
// "configs":{} decodes to a non-nil empty map, which coerceConfigMap
// likewise returns non-nil-but-empty.
func coerceConfigMap(raw map[string]json.RawMessage) (map[string]string, error) {
	if raw == nil {
		return nil, nil
	}
	out := make(map[string]string, len(raw))
	for k, v := range raw {
		val, skip, err := coerceConfigValue(v)
		if err != nil {
			return nil, fmt.Errorf("config %q: %w", k, err)
		}
		if skip {
			continue
		}
		out[k] = val
	}
	return out, nil
}

// CreateTopic serves POST /api/clusters/{clusterName}/topics: creates a
// topic per the request body's TopicCreation shape. The contract declares
// 201 + a Topic body (topicToGenerated — same shape GetTopics/GetTopicsCsv
// rows use, no per-partition detail). Decodes via topicCreationLenient/
// coerceConfigMap rather than generated.TopicCreation directly — see the
// "P1b Task 8d" comment block above.
func (s *apiServer) CreateTopic(w http.ResponseWriter, r *http.Request, clusterName string) {
	var body topicCreationLenient
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse(http.StatusBadRequest, "invalid request body"))
		return
	}
	configs, err := coerceConfigMap(body.Configs)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse(http.StatusBadRequest, "invalid request body"))
		return
	}
	ts, err := s.deps.Topics.Create(r.Context(), clusterName, topicSpecFromCreation(body, configs))
	if err != nil {
		if errors.Is(err, appcluster.ErrUnknownCluster) {
			writeJSON(w, http.StatusNotFound, errorResponse(http.StatusNotFound, "cluster not found"))
			return
		}
		serverError(w, "CreateTopic", "failed to create topic", err)
		return
	}
	writeJSON(w, http.StatusCreated, topicToGenerated(ts))
}

// topicSpecFromCreation maps the decoded request body plus its
// already-coerced configs (coerceConfigMap, run by the caller so a bad
// configs value can still 400 before this ever gets called) onto a domain
// TopicSpec. ReplicationFactor defaults to -1 ("cluster default") when
// omitted — the contract marks it optional but TopicSpec's sentinel
// convention needs an explicit value either way.
func topicSpecFromCreation(body topicCreationLenient, configs map[string]string) cluster.TopicSpec {
	spec := cluster.TopicSpec{Name: body.Name, Partitions: body.Partitions, ReplicationFactor: -1, Configs: configs}
	if body.ReplicationFactor != nil {
		spec.ReplicationFactor = int16(*body.ReplicationFactor)
	}
	return spec
}

// DeleteTopic serves DELETE /api/clusters/{clusterName}/topics/{topicName}:
// 204 on success, no body. A disabled TOPIC_DELETION feature
// (ErrTopicDeletionDisabled) maps to a fixed-text 403 — defense in depth
// alongside upstream's own UI-hides-the-entry-point behaviour when the
// feature is off.
func (s *apiServer) DeleteTopic(w http.ResponseWriter, r *http.Request, clusterName, topicName string) {
	if err := s.deps.Topics.Delete(r.Context(), clusterName, topicName); err != nil {
		if errors.Is(err, appcluster.ErrUnknownCluster) {
			writeJSON(w, http.StatusNotFound, errorResponse(http.StatusNotFound, "cluster not found"))
			return
		}
		if errors.Is(err, appcluster.ErrTopicDeletionDisabled) {
			writeJSON(w, http.StatusForbidden, errorResponse(http.StatusForbidden, "topic deletion is disabled for this cluster"))
			return
		}
		serverError(w, "DeleteTopic", "failed to delete topic", err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// UpdateTopic serves PATCH /api/clusters/{clusterName}/topics/{topicName}:
// reconciles the topic's dynamic configs to the request body's TopicUpdate.
// configs full desired set (TopicService.UpdateConfigs' diff, never a full
// replace — ADR-0003 §6.1). Contract declares 200 + a Topic body. Decodes
// via topicUpdateLenient/coerceConfigMap rather than generated.TopicUpdate
// directly — see the "P1b Task 8d" comment block above CreateTopic (this is
// the endpoint Task 9's smoke run actually caught 400ing: the vendored
// frontend's only Edit/Update UI path sends a numeric retentionBytes here).
func (s *apiServer) UpdateTopic(w http.ResponseWriter, r *http.Request, clusterName, topicName string) {
	var body topicUpdateLenient
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse(http.StatusBadRequest, "invalid request body"))
		return
	}
	desired, err := coerceConfigMap(body.Configs)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse(http.StatusBadRequest, "invalid request body"))
		return
	}
	ts, err := s.deps.Topics.UpdateConfigs(r.Context(), clusterName, topicName, desired)
	if err != nil {
		if errors.Is(err, appcluster.ErrUnknownCluster) {
			writeJSON(w, http.StatusNotFound, errorResponse(http.StatusNotFound, "cluster not found"))
			return
		}
		serverError(w, "UpdateTopic", "failed to update topic configs", err)
		return
	}
	writeJSON(w, http.StatusOK, topicToGenerated(ts))
}

// RecreateTopic serves POST /api/clusters/{clusterName}/topics/{topicName}:
// no request body (the contract declares none) — wipes the topic's data
// while preserving its shape (TopicService.Recreate). Contract declares 201
// + a Topic body; a disabled TOPIC_DELETION feature maps to the same
// fixed-text 403 as DeleteTopic (Recreate deletes internally).
func (s *apiServer) RecreateTopic(w http.ResponseWriter, r *http.Request, clusterName, topicName string) {
	ts, err := s.deps.Topics.Recreate(r.Context(), clusterName, topicName)
	if err != nil {
		if errors.Is(err, appcluster.ErrUnknownCluster) {
			writeJSON(w, http.StatusNotFound, errorResponse(http.StatusNotFound, "cluster not found"))
			return
		}
		if errors.Is(err, appcluster.ErrTopicDeletionDisabled) {
			writeJSON(w, http.StatusForbidden, errorResponse(http.StatusForbidden, "topic deletion is disabled for this cluster"))
			return
		}
		serverError(w, "RecreateTopic", "failed to recreate topic", err)
		return
	}
	writeJSON(w, http.StatusCreated, topicToGenerated(ts))
}

// CloneTopic serves POST
// /api/clusters/{clusterName}/topics/{topicName}/clone?newTopicName=...:
// creates a new topic with topicName's shape/dynamic configs
// (TopicService.Clone). Contract declares 201 + a Topic body.
func (s *apiServer) CloneTopic(w http.ResponseWriter, r *http.Request, clusterName, topicName string, params generated.CloneTopicParams) {
	ts, err := s.deps.Topics.Clone(r.Context(), clusterName, topicName, params.NewTopicName)
	if err != nil {
		if errors.Is(err, appcluster.ErrUnknownCluster) {
			writeJSON(w, http.StatusNotFound, errorResponse(http.StatusNotFound, "cluster not found"))
			return
		}
		serverError(w, "CloneTopic", "failed to clone topic", err)
		return
	}
	writeJSON(w, http.StatusCreated, topicToGenerated(ts))
}

// IncreaseTopicPartitions serves PATCH
// /api/clusters/{clusterName}/topics/{topicName}/partitions: sets the
// topic's total partition count (contract's PartitionsIncrease.
// totalPartitionsCount — a target total, not an incremental add; see
// cluster.TopicAdminPort.CreatePartitions' doc comment). Contract declares
// 200 + a PartitionsIncreaseResponse body, built directly from the request
// (the resulting total is exactly what was requested on success — no extra
// live describe needed).
func (s *apiServer) IncreaseTopicPartitions(w http.ResponseWriter, r *http.Request, clusterName, topicName string) {
	var body generated.PartitionsIncrease
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse(http.StatusBadRequest, "invalid request body"))
		return
	}
	var total int32
	if body.TotalPartitionsCount != nil {
		total = int32(*body.TotalPartitionsCount)
	}
	if err := s.deps.Topics.IncreasePartitions(r.Context(), clusterName, topicName, total); err != nil {
		if errors.Is(err, appcluster.ErrUnknownCluster) {
			writeJSON(w, http.StatusNotFound, errorResponse(http.StatusNotFound, "cluster not found"))
			return
		}
		serverError(w, "IncreaseTopicPartitions", "failed to increase topic partitions", err)
		return
	}
	writeJSON(w, http.StatusOK, generated.PartitionsIncreaseResponse{TopicName: topicName, TotalPartitionsCount: int(total)})
}

// ChangeReplicationFactor serves PATCH
// /api/clusters/{clusterName}/topics/{topicName}/replications: reassigns
// the topic's partitions to a new replication factor
// (TopicService.ChangeReplicationFactor -> reassignForFactor). Contract
// declares 200 + a ReplicationFactorChangeResponse body, plus 400 for an
// invalid target (ErrInvalidReplicationFactor — out of the achievable
// range, see reassignForFactor's doc comment), distinct from a genuine
// backend failure (500).
func (s *apiServer) ChangeReplicationFactor(w http.ResponseWriter, r *http.Request, clusterName, topicName string) {
	var body generated.ReplicationFactorChange
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse(http.StatusBadRequest, "invalid request body"))
		return
	}
	var target int16
	if body.TotalReplicationFactor != nil {
		target = int16(*body.TotalReplicationFactor)
	}
	if err := s.deps.Topics.ChangeReplicationFactor(r.Context(), clusterName, topicName, target); err != nil {
		if errors.Is(err, appcluster.ErrUnknownCluster) {
			writeJSON(w, http.StatusNotFound, errorResponse(http.StatusNotFound, "cluster not found"))
			return
		}
		if errors.Is(err, appcluster.ErrInvalidReplicationFactor) {
			writeJSON(w, http.StatusBadRequest, errorResponse(http.StatusBadRequest, "invalid target replication factor"))
			return
		}
		serverError(w, "ChangeReplicationFactor", "failed to change replication factor", err)
		return
	}
	writeJSON(w, http.StatusOK, generated.ReplicationFactorChangeResponse{TopicName: topicName, TotalReplicationFactor: int(target)})
}

// --- domain -> contract mapping helpers ---

// topicTallies sums the per-topic totals the contract's Topic/TopicDetails
// both derive from partition-level replica/ISR data: Replicas and
// InSyncReplicas are cluster-wide *counts* (sum across every partition, not
// a per-partition value — confirmed against the vendored frontend's
// Overview.tsx, which compares inSyncReplicas directly against replicas as
// two topic-wide totals), and UnderReplicatedPartitions counts partitions
// whose ISR set is smaller than its replica set.
func topicTallies(ts cluster.TopicState) (replicas, inSync, underReplicated int32) {
	for _, p := range ts.Partitions {
		replicas += int32(len(p.Replicas))
		inSync += int32(len(p.ISR))
		if len(p.ISR) < len(p.Replicas) {
			underReplicated++
		}
	}
	return replicas, inSync, underReplicated
}

// topicCountPtrs returns the PartitionCount/ReplicationFactor pointer pair
// shared verbatim by the Topic (topicToGenerated) and TopicDetails
// (topicDetailsToGenerated) mappers — one implementation of the same rule so
// the two never diverge. Each is nil (→ omitted) unless genuinely known
// (> 0): a real topic always has >= 1 partition and >= 1 replica, so a Go
// zero here only ever means "unknown" — the write-path synthesized state for
// a spec value of -1 ("cluster default", synthesizeTopicState), the
// updateTopic not-yet-cached fallback (topicStateFromCache's name-only
// TopicState{}), or the GetTopicDetails not-yet-cached degrade
// (TopicService.Details returning cluster.TopicState{Name: topic}). Both
// generated fields are *int32 with `omitempty`, but omitempty only
// suppresses a nil pointer, not a pointer-to-zero — so unconditionally
// wrapping the domain value in ptr(...) would put a misleading literal 0 on
// the wire ("this topic has 0 partitions") instead of just omitting the
// field. Only these two fields get the <=0→nil treatment: SegmentSize/
// SegmentCount legitimately read 0 for an empty topic, and the tally fields
// (Replicas/InSyncReplicas/UnderReplicatedPartitions) are out of scope.
func topicCountPtrs(ts cluster.TopicState) (partitionCount, replicationFactor *int32) {
	if n := len(ts.Partitions); n > 0 {
		partitionCount = ptr(int32(n))
	}
	if ts.ReplicationFactor > 0 {
		replicationFactor = ptr(int32(ts.ReplicationFactor))
	}
	return partitionCount, replicationFactor
}

// topicMessagesCount estimates a topic's message count as Σ per-partition
// max(0, EndOffset-StartOffset) — the same approximation upstream kafka-ui
// uses (a compacted topic's true live message count is lower than this raw
// end-minus-start span, since compaction removes superseded keys; upstream
// accepts the same over-estimate rather than trying to correct for it). The
// data is already in hand: FetchState's cluster-wide ListStartOffsets/
// ListEndOffsets scrape (infra/kafka/state.go) fills every cached topic's
// partition offsets, so this is a pure sum over the same TopicState List/
// Details already share — no new scan, no new scrape.
//
// PartitionState.StartOffset/EndOffset are -1 when that partition's offsets
// fetch failed or wasn't attempted (see PartitionState's doc comment); such
// partitions are skipped rather than treated as a real 0.
//
// Returns nil (-> omitted, an honest "unknown") only when not a single
// partition's offsets are known. Otherwise returns a pointer to the sum,
// even when that sum is 0: an empty topic's partitions are known at 0/0 —
// a real, reportable message count of zero, not "unknown". This is the
// opposite direction from topicCountPtrs' <=0->nil rule (there, a Go zero
// value always means "never scraped"; here, a known 0 is itself the
// answer) — deliberately so: the frontend's Topics list row locator
// (TopicsLocators.ts) matches on a literal "0" messages cell for a
// freshly-created empty topic, and would never find the row if this
// returned nil instead of &0 (see P1b Task 8c's brief).
func topicMessagesCount(ts cluster.TopicState) *int64 {
	n, known := ts.MessagesCount()
	if !known {
		return nil
	}
	return ptr(n)
}

// topicToGenerated maps one domain TopicState onto the contract's Topic
// shape (the GetTopics/GetTopicsCsv row shape). Partitions detail and
// cleanUpPolicy are deliberately left unset here: List doesn't fetch live
// configs per topic (would be N describe calls per page), and per-partition
// detail belongs to the single-topic GetTopicDetails view, not the list
// (topicDetailsToGenerated adds both on top of this same tally). Throughput
// (bytesInPerSec/bytesOutPerSec) still has no data source and stays unset.
// messagesCount, unlike throughput, DOES have a data source (see
// topicMessagesCount) despite both once being lumped together as "no data
// source" — that framing was stale (Task 4 wrote it before Task 8c noticed
// the scrape already fills partition offsets) and has been corrected here.
// PartitionCount/ReplicationFactor use the shared "known-only" rule — see
// topicCountPtrs.
func topicToGenerated(ts cluster.TopicState) generated.Topic {
	replicas, inSync, underReplicated := topicTallies(ts)
	partitionCount, replicationFactor := topicCountPtrs(ts)
	return generated.Topic{
		Name:                      ts.Name,
		Internal:                  ptr(ts.Internal),
		PartitionCount:            partitionCount,
		ReplicationFactor:         replicationFactor,
		Replicas:                  ptr(replicas),
		InSyncReplicas:            ptr(inSync),
		SegmentSize:               ptr(ts.SegmentSize),
		SegmentCount:              ptr(int32(ts.SegmentCount)),
		UnderReplicatedPartitions: ptr(underReplicated),
		MessagesCount:             topicMessagesCount(ts),
	}
}

func topicsToGeneratedRows(ts []cluster.TopicState) []generated.Topic {
	out := make([]generated.Topic, 0, len(ts))
	for _, t := range ts {
		out = append(out, topicToGenerated(t))
	}
	return out
}

// topicDetailsToGenerated maps one domain TopicState plus its live configs
// onto the contract's TopicDetails shape: the same tallies topicToGenerated
// computes, plus full per-partition detail (partitionsToGenerated) and
// cleanUpPolicy (derived from the "cleanup.policy" config entry — Details is
// the only place that combination of cached state + live configs exists).
// KeySerde/ValueSerde have no data source yet (P1c's serde work) and stay
// unset. PartitionCount/ReplicationFactor use the same shared "known-only"
// rule as topicToGenerated (see topicCountPtrs) — GetTopicDetails on a
// not-yet-cached topic degrades to cluster.TopicState{Name: topic}
// (TopicService.Details), which must serialize as absent, not literal 0.
func topicDetailsToGenerated(ts cluster.TopicState, cfgs []cluster.ConfigEntry) generated.TopicDetails {
	replicas, inSync, underReplicated := topicTallies(ts)
	partitionCount, replicationFactor := topicCountPtrs(ts)
	d := generated.TopicDetails{
		Name:                      ts.Name,
		Internal:                  ptr(ts.Internal),
		PartitionCount:            partitionCount,
		ReplicationFactor:         replicationFactor,
		Replicas:                  ptr(replicas),
		InSyncReplicas:            ptr(inSync),
		SegmentSize:               ptr(ts.SegmentSize),
		SegmentCount:              ptr(int32(ts.SegmentCount)),
		UnderReplicatedPartitions: ptr(underReplicated),
	}
	if len(ts.Partitions) > 0 {
		parts := partitionsToGenerated(ts.Partitions)
		d.Partitions = &parts
	}
	if cp := cleanUpPolicyFromConfigs(cfgs); cp != "" {
		d.CleanUpPolicy = ptr(cp)
	}
	return d
}

// partitionsToGenerated maps domain PartitionState entries onto the
// contract's Partition/Replica shapes: each replica broker ID gets its own
// Replica entry, flagged Leader (== the partition's leader) and InSync
// (member of the partition's ISR set).
func partitionsToGenerated(ps []cluster.PartitionState) []generated.Partition {
	out := make([]generated.Partition, 0, len(ps))
	for _, p := range ps {
		isr := make(map[int32]bool, len(p.ISR))
		for _, id := range p.ISR {
			isr[id] = true
		}
		reps := make([]generated.Replica, 0, len(p.Replicas))
		for _, r := range p.Replicas {
			reps = append(reps, generated.Replica{
				Broker: ptr(r),
				Leader: ptr(r == p.Leader),
				InSync: ptr(isr[r]),
			})
		}
		out = append(out, generated.Partition{
			Partition: p.ID,
			Leader:    ptr(p.Leader),
			OffsetMin: p.StartOffset,
			OffsetMax: p.EndOffset,
			Replicas:  &reps,
		})
	}
	return out
}

// cleanUpPolicyFromConfigs derives the contract's CleanUpPolicy enum from
// the topic's "cleanup.policy" config value (Kafka's own literal values:
// "delete", "compact", or a comma-joined combination of both — order isn't
// guaranteed by the broker, so both orderings map to COMPACT_DELETE).
// Returns "" (not a valid enum value) when the config entry isn't present,
// which the caller treats as "leave the contract field unset" rather than
// serializing an empty string.
func cleanUpPolicyFromConfigs(cfgs []cluster.ConfigEntry) generated.CleanUpPolicy {
	for _, c := range cfgs {
		if c.Name != "cleanup.policy" {
			continue
		}
		switch c.Value {
		case "delete":
			return generated.CleanUpPolicyDELETE
		case "compact":
			return generated.CleanUpPolicyCOMPACT
		case "compact,delete", "delete,compact":
			return generated.CleanUpPolicyCOMPACTDELETE
		default:
			return generated.CleanUpPolicyUNKNOWN
		}
	}
	return ""
}

// topicConfigToGenerated maps one domain ConfigEntry onto the contract's
// TopicConfig shape (same Source translation as configEntryToGenerated/
// handlers_broker.go's sourceToGenerated), plus TopicConfig's extra
// defaultValue field: upstream derives it from the DEFAULT_CONFIG-sourced
// synonym (defaultValueFromSynonyms), since Kafka's DescribeConfigs response
// carries the cluster/broker default as a synonym entry, not as a separate
// top-level field.
func topicConfigToGenerated(c cluster.ConfigEntry) generated.TopicConfig {
	tc := generated.TopicConfig{
		Name: c.Name, Value: ptr(c.Value), Source: ptr(sourceToGenerated(c.Source)),
		IsSensitive: ptr(c.IsSensitive), IsReadOnly: ptr(c.IsReadOnly),
	}
	if dv := defaultValueFromSynonyms(c.Synonyms); dv != nil {
		tc.DefaultValue = dv
	}
	if len(c.Synonyms) > 0 {
		syns := make([]generated.ConfigSynonym, 0, len(c.Synonyms))
		for _, syn := range c.Synonyms {
			syns = append(syns, generated.ConfigSynonym{
				Name: ptr(syn.Name), Value: ptr(syn.Value), Source: ptr(sourceToGenerated(syn.Source)),
			})
		}
		tc.Synonyms = &syns
	}
	return tc
}

// defaultValueFromSynonyms returns the value of the synonym sourced from
// DEFAULT_CONFIG (the raw driver string, same vocabulary as
// ConfigEntry.Source — see sourceToGenerated's doc comment), or nil if no
// such synonym exists (e.g. a key with no cluster-wide default).
func defaultValueFromSynonyms(syns []cluster.ConfigSynonym) *string {
	for _, syn := range syns {
		if syn.Source == "DEFAULT_CONFIG" {
			v := syn.Value
			return &v
		}
	}
	return nil
}

// aclToGenerated maps one domain AclBinding onto the contract's KafkaAcl
// shape. Unlike topicConfigToGenerated's Source translation, this is a
// direct cast, not a lookup table: AclBinding's four enum-ish fields are
// already contract-shaped strings (infra/kafka/topics.go's
// aclXxxToContract functions did that translation before the domain ever
// saw these values — see AclBinding's doc comment in domain/cluster).
func aclToGenerated(a cluster.AclBinding) generated.KafkaAcl {
	return generated.KafkaAcl{
		Principal:       a.Principal,
		Host:            a.Host,
		ResourceType:    generated.KafkaAclResourceType(a.ResourceType),
		ResourceName:    a.ResourceName,
		NamePatternType: generated.KafkaAclNamePatternType(a.PatternType),
		Operation:       generated.KafkaAclOperation(a.Operation),
		Permission:      generated.KafkaAclPermission(a.Permission),
	}
}

// producerStateToGenerated maps one domain ProducerState onto the
// contract's TopicProducerState shape.
func producerStateToGenerated(p cluster.ProducerState) generated.TopicProducerState {
	return generated.TopicProducerState{
		Partition:                     ptr(p.Partition),
		ProducerId:                    ptr(p.ProducerID),
		ProducerEpoch:                 ptr(p.ProducerEpoch),
		LastSequence:                  ptr(p.LastSequence),
		LastTimestampMs:               ptr(p.LastTimestamp),
		CoordinatorEpoch:              ptr(p.CoordinatorEpoch),
		CurrentTransactionStartOffset: ptr(p.CurrentTransactionStartOffset),
	}
}
