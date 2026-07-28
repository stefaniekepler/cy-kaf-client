// handlers_message.go implements GetTopicMessagesV2: this repo's SSE
// message-browsing endpoint, and (per this task's brief) the vendored
// frontend's actual main path for reading topic messages -- its
// topicMessages.tsx hook drives @microsoft/fetch-event-source straight at
// this URL, not through the generated OpenAPI client. It overrides the
// generated 501 stub for GetTopicMessagesV2 (ADR-0004 §1: same-name method
// override, no manual routing) and does two jobs: map
// generated.GetTopicMessagesV2Params onto app's appcluster.BrowseSpec, and
// stream app's MessageService.Browse callback events out as SSE frames via
// sse.go's writeSSE, mapping each appcluster.BrowseEvent onto the contract's
// generated.TopicMessageEvent as it goes.
package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/cy-kaf/cy-kaf-client/internal/api/generated"
	appcluster "github.com/cy-kaf/cy-kaf-client/internal/app/cluster"
)

// GetTopicMessagesV2 serves GET .../topics/{topicName}/messages/v2.
//
// Error handling has three buckets, in this order:
//  1. an unrecognized PollingMode value -- the contract's enum is not
//     enforced at query-param bind time (generated.GetTopicMessagesV2Params.
//     Mode binds as a bare string, see models.gen.go), so this handler is
//     the only place that can catch it. This is always pre-stream (nothing
//     has been written yet), so it's an ordinary 400 JSON response.
//  2. appcluster.ErrUnknownCluster from Browse -- Browse's very first
//     statement is res.Lookup, before its first EventPhase emit, so this
//     always surfaces before writeSSE's first send ever runs (see writeSSE's
//     doc comment): a normal 404 JSON response.
//  3. any other error writeSSE returns -- also always pre-stream, by the
//     same writeSSE contract (post-stream errors make writeSSE return nil,
//     not an error) -- a normal 500 JSON response, same two-bucket
//     convention as every other handler in this package.
func (s *apiServer) GetTopicMessagesV2(w http.ResponseWriter, r *http.Request, clusterName, topicName string, params generated.GetTopicMessagesV2Params) {
	mode, ok := browseModeFromPollingMode(params.Mode)
	if !ok {
		writeJSON(w, http.StatusBadRequest, errorResponse(http.StatusBadRequest, "unknown polling mode"))
		return
	}

	spec := appcluster.BrowseSpec{Mode: mode}
	if params.Partitions != nil {
		spec.Partitions = *params.Partitions
	}
	if params.Limit != nil {
		spec.Limit = int(*params.Limit)
	}
	if params.Offset != nil {
		spec.Offset = *params.Offset
	}
	if params.Timestamp != nil {
		spec.TimestampMs = *params.Timestamp
	}
	if params.StringFilter != nil {
		spec.StringFilter = *params.StringFilter
	}
	if params.SmartFilterId != nil {
		spec.SmartFilterID = *params.SmartFilterId
	}
	if params.KeySerde != nil {
		spec.KeySerde = *params.KeySerde
	}
	if params.ValueSerde != nil {
		spec.ValueSerde = *params.ValueSerde
	}
	if params.Cursor != nil {
		spec.Cursor = *params.Cursor
	}

	err := writeSSE(w, r, func(send func(v any) error) error {
		return s.deps.Messages.Browse(r.Context(), clusterName, topicName, spec, func(ev appcluster.BrowseEvent) error {
			return send(browseEventToGenerated(ev))
		})
	})
	if err == nil {
		return
	}
	if errors.Is(err, appcluster.ErrUnknownCluster) {
		writeJSON(w, http.StatusNotFound, errorResponse(http.StatusNotFound, "cluster not found"))
		return
	}
	serverError(w, "GetTopicMessagesV2", "failed to browse topic messages", err)
}

// browseModeFromPollingMode maps the contract's PollingMode enum onto app's
// BrowseMode (api's job, not app's -- app never imports internal/api/
// generated, see message.go's BrowseMode doc comment). mode == nil (the
// query param was absent) defaults to ModeLatest, matching upstream
// kafka-ui's own default polling mode. Any other value this switch doesn't
// recognize (the contract's enum isn't validated at query-param bind time,
// see GetTopicMessagesV2's doc comment) reports ok == false for the caller
// to turn into a 400.
func browseModeFromPollingMode(mode *generated.PollingMode) (appcluster.BrowseMode, bool) {
	if mode == nil {
		return appcluster.ModeLatest, true
	}
	switch *mode {
	case generated.PollingModeLATEST:
		return appcluster.ModeLatest, true
	case generated.PollingModeEARLIEST:
		return appcluster.ModeEarliest, true
	case generated.PollingModeTAILING:
		return appcluster.ModeTailing, true
	case generated.PollingModeFROMOFFSET:
		return appcluster.ModeFromOffset, true
	case generated.PollingModeTOOFFSET:
		return appcluster.ModeToOffset, true
	case generated.PollingModeFROMTIMESTAMP:
		return appcluster.ModeFromTimestamp, true
	case generated.PollingModeTOTIMESTAMP:
		return appcluster.ModeToTimestamp, true
	default:
		return 0, false
	}
}

// GetTopicMessages serves GET .../topics/{topicName}/messages -- the
// contract's older v1 poll shape (SeekType/SeekDirection/SeekTo), kept for
// contract + parity-matrix completeness only: the vendored frontend's real
// message-browsing path is v2 (GetTopicMessagesV2 above, Task 9's
// topicMessages.tsx hook), so this handler has no production caller. It is
// a THIN adapter, deliberately: it translates v1's seek query params onto
// the exact same appcluster.BrowseSpec v2 builds, then reuses v2's own
// writeSSE + Deps.Messages.Browse + browseEventToGenerated pipeline
// verbatim -- no engine logic is duplicated here, and GetTopicMessagesV2
// itself is untouched by this handler's existence (ADR-0004 §1: same-name
// method override, ordinary Go method dispatch, ships independently).
//
// Field mapping, in order:
//
//   - SeekDirection x SeekType -> BrowseMode, via browseModeFromSeek (see
//     its own doc comment for the full 9-row table this task's brief
//     specifies). Unlike GetTopicMessagesV2's PollingMode mapping, this is a
//     total function -- there is no 400 for an unrecognized enum value here,
//     matching upstream's own tolerant seek handling (the brief's own
//     words: "照上游宽容").
//   - SeekTo ([]string of "partition::value", kafka-ui's format) -> Partitions
//     (every entry's partition index) plus, when SeekType is OFFSET or
//     TIMESTAMP, Offset/TimestampMs from the FIRST entry's value only --
//     BrowseSpec.Offset/TimestampMs are single scalars, so v1's
//     per-partition seek granularity is intentionally lossy here (this
//     endpoint has no real caller to notice; see this task's report for the
//     full rationale). A malformed entry (missing "::", or a non-integer
//     partition index) is a pre-stream 400 via parseSeekTo's error.
//   - Limit, KeySerde, ValueSerde -> straight passthrough, same as v2.
//   - Q + FilterQueryType: STRING_CONTAINS -> StringFilter; CEL_SCRIPT ->
//     FilterCode (P1c Task 10's app-layer inline-CEL hook, message.go --
//     RegisterFilter/smartFilterId isn't exposed over HTTP until Task 12, so
//     v1's raw CEL source has nowhere else to compile through). Neither set,
//     or Q nil, means no filter of that kind.
//
// Error handling is the same two-bucket convention as GetTopicMessagesV2 (an
// unrecognized seekTo entry is the one extra pre-stream 400 v1 has that v2
// doesn't): appcluster.ErrUnknownCluster -> 404, anything else writeSSE
// returns -> 500 (a FilterCode compile error included -- see message.go's
// Browse doc comment on exactly why that's still always pre-stream).
func (s *apiServer) GetTopicMessages(w http.ResponseWriter, r *http.Request, clusterName, topicName string, params generated.GetTopicMessagesParams) {
	spec := appcluster.BrowseSpec{Mode: browseModeFromSeek(params.SeekDirection, params.SeekType)}

	if params.SeekTo != nil {
		partitions, firstValue, err := parseSeekTo(*params.SeekTo)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, errorResponse(http.StatusBadRequest, err.Error()))
			return
		}
		spec.Partitions = partitions
		if firstValue != "" && params.SeekType != nil {
			switch *params.SeekType {
			case generated.OFFSET:
				n, convErr := strconv.ParseInt(firstValue, 10, 64)
				if convErr != nil {
					writeJSON(w, http.StatusBadRequest, errorResponse(http.StatusBadRequest, fmt.Sprintf("invalid seekTo offset value %q", firstValue)))
					return
				}
				spec.Offset = n
			case generated.TIMESTAMP:
				n, convErr := strconv.ParseInt(firstValue, 10, 64)
				if convErr != nil {
					writeJSON(w, http.StatusBadRequest, errorResponse(http.StatusBadRequest, fmt.Sprintf("invalid seekTo timestamp value %q", firstValue)))
					return
				}
				spec.TimestampMs = n
			}
		}
	}

	if params.Limit != nil {
		spec.Limit = int(*params.Limit)
	}
	if params.KeySerde != nil {
		spec.KeySerde = *params.KeySerde
	}
	if params.ValueSerde != nil {
		spec.ValueSerde = *params.ValueSerde
	}
	if params.FilterQueryType != nil && params.Q != nil {
		switch *params.FilterQueryType {
		case generated.STRINGCONTAINS:
			spec.StringFilter = *params.Q
		case generated.CELSCRIPT:
			spec.FilterCode = *params.Q
		}
	}

	err := writeSSE(w, r, func(send func(v any) error) error {
		return s.deps.Messages.Browse(r.Context(), clusterName, topicName, spec, func(ev appcluster.BrowseEvent) error {
			return send(browseEventToGenerated(ev))
		})
	})
	if err == nil {
		return
	}
	if errors.Is(err, appcluster.ErrUnknownCluster) {
		writeJSON(w, http.StatusNotFound, errorResponse(http.StatusNotFound, "cluster not found"))
		return
	}
	serverError(w, "GetTopicMessages", "failed to browse topic messages", err)
}

// browseModeFromSeek maps v1's SeekDirection x SeekType pair onto app's
// BrowseMode, per this task's brief table verbatim:
//
//	SeekDirection  SeekType     BrowseMode
//	TAILING        (any)        ModeTailing
//	FORWARD        BEGINNING    ModeEarliest
//	FORWARD        OFFSET       ModeFromOffset
//	FORWARD        TIMESTAMP    ModeFromTimestamp
//	FORWARD        LATEST       ModeTailing   (seek to end, forward = collect new messages)
//	BACKWARD       LATEST       ModeLatest
//	BACKWARD       OFFSET       ModeToOffset
//	BACKWARD       TIMESTAMP    ModeToTimestamp
//	BACKWARD       BEGINNING    ModeEarliest  (degenerate combo, no real caller -- reasonable default)
//
// direction == nil defaults to FORWARD, seekType == nil defaults to LATEST
// (this task's brief, verbatim: "照上游宽容"). Any value neither nil nor a
// recognized enum member (query params bind as bare strings with no
// enum/format validation at bind time, same situation as
// GetTopicMessagesV2Params.Mode) is folded into the nearest table row's
// tolerant default rather than rejected -- FORWARD's default row (unrecognized
// SeekType, or an unrecognized SeekDirection falling through to FORWARD's own
// switch) resolves to ModeTailing, BACKWARD's default row resolves to
// ModeLatest -- so, unlike GetTopicMessagesV2's browseModeFromPollingMode,
// this function never reports a 400: v1 has no real caller to protect from a
// mistyped seek param, so leniency (matching upstream kafka-ui's own
// behavior) was judged preferable to inventing a new error path this
// endpoint's tests would be the only ones ever exercising.
func browseModeFromSeek(direction *generated.SeekDirection, seekType *generated.SeekType) appcluster.BrowseMode {
	dir := generated.FORWARD
	if direction != nil {
		dir = *direction
	}
	st := generated.LATEST
	if seekType != nil {
		st = *seekType
	}

	if dir == generated.TAILING {
		return appcluster.ModeTailing
	}
	if dir == generated.BACKWARD {
		switch st {
		case generated.OFFSET:
			return appcluster.ModeToOffset
		case generated.TIMESTAMP:
			return appcluster.ModeToTimestamp
		case generated.BEGINNING:
			return appcluster.ModeEarliest
		default: // LATEST, or any unrecognized SeekType value
			return appcluster.ModeLatest
		}
	}
	// dir == FORWARD, or any unrecognized SeekDirection value (folded into
	// FORWARD's tolerant handling -- see doc comment above).
	switch st {
	case generated.BEGINNING:
		return appcluster.ModeEarliest
	case generated.OFFSET:
		return appcluster.ModeFromOffset
	case generated.TIMESTAMP:
		return appcluster.ModeFromTimestamp
	default: // LATEST, or any unrecognized SeekType value
		return appcluster.ModeTailing
	}
}

// parseSeekTo parses v1's SeekTo entries (kafka-ui's "partition::value"
// format, e.g. ["0::100","1::100"]) into a partition index list plus the
// FIRST entry's raw value string -- the only part of SeekTo
// GetTopicMessages actually consults beyond partitions, since
// appcluster.BrowseSpec.Offset/TimestampMs are single scalars, not
// per-partition (v1's per-partition seek is therefore lossy here by
// necessity; see GetTopicMessages' own doc comment). An empty entries slice
// reports a nil partitions list (caller leaves BrowseSpec.Partitions at its
// zero value, meaning "all partitions", same as v2's omitted `partitions`
// query param). A malformed entry -- missing the "::" separator, or a
// partition index that isn't a valid int32 -- reports an error the caller
// turns into a pre-stream 400.
func parseSeekTo(entries []string) (partitions []int32, firstValue string, err error) {
	for i, entry := range entries {
		sep := strings.Index(entry, "::")
		if sep < 0 {
			return nil, "", fmt.Errorf("invalid seekTo entry %q: expected \"partition::value\"", entry)
		}
		partStr, value := entry[:sep], entry[sep+2:]
		part, convErr := strconv.ParseInt(partStr, 10, 32)
		if convErr != nil {
			return nil, "", fmt.Errorf("invalid seekTo entry %q: partition must be an integer: %w", entry, convErr)
		}
		partitions = append(partitions, int32(part))
		if i == 0 {
			firstValue = value
		}
	}
	return partitions, firstValue, nil
}

// browseEventToGenerated maps one appcluster.BrowseEvent onto the contract's
// TopicMessageEvent shape. Type is always set to one of the 4 bare
// generated.TopicMessageEventType constants (PHASE/MESSAGE/CONSUMING/DONE
// -- there is no 5th "CURSOR" kind, on either side of this mapping: a
// resume cursor is the Cursor field below, riding on whichever event
// happened to carry a non-empty CursorID). Cursor is independent of Type:
// it's attached whenever ev.CursorID is non-empty, which today is only ever
// EventDone (see Browse), but this mapping doesn't hardcode that
// assumption.
func browseEventToGenerated(ev appcluster.BrowseEvent) generated.TopicMessageEvent {
	out := generated.TopicMessageEvent{}
	switch ev.Kind {
	case appcluster.EventPhase:
		out.Type = ptr(generated.PHASE)
		out.Phase = &generated.TopicMessagePhase{Name: &ev.Phase}
	case appcluster.EventConsuming:
		out.Type = ptr(generated.CONSUMING)
		if ev.Consuming != nil {
			out.Consuming = consumingToGenerated(ev.Consuming)
		}
	case appcluster.EventMessage:
		out.Type = ptr(generated.MESSAGE)
		if ev.Message != nil {
			out.Message = decodedMessageToGenerated(ev.Message)
		}
	case appcluster.EventDone:
		out.Type = ptr(generated.DONE)
	}
	if ev.CursorID != "" {
		id := ev.CursorID
		out.Cursor = &generated.TopicMessageNextPageCursor{Id: &id}
	}
	return out
}

// consumingToGenerated maps one appcluster.ConsumingStats onto the
// contract's TopicMessageConsuming shape -- a straight field-for-field
// copy, all pointer fields per the contract.
func consumingToGenerated(c *appcluster.ConsumingStats) *generated.TopicMessageConsuming {
	return &generated.TopicMessageConsuming{
		BytesConsumed:     ptr(c.BytesConsumed),
		ElapsedMs:         ptr(c.ElapsedMs),
		FilterApplyErrors: ptr(c.FilterApplyErrors),
		MessagesConsumed:  ptr(c.MessagesConsumed),
		IsCancelled:       ptr(c.IsCancelled),
	}
}

// decodedMessageToGenerated maps one appcluster.DecodedMessage onto the
// contract's TopicMessage shape. Offset/Partition/Timestamp are the
// schema's only required (non-pointer) fields -- Timestamp in particular
// must always be set via time.UnixMilli(m.TimestampMs), never left at its
// zero value, or contract validation fails (m.TimestampMs == 0 is itself a
// legitimate epoch timestamp, not "absent": DecodedMessage has no separate
// "no timestamp" signal, so there is nothing to omit here). Headers is only
// set when non-empty: the contract's headers field isn't nullable, so "no
// headers" is represented by omitting the field entirely (same convention
// as GetClusterStats' DiskUsage, handlers_cluster.go), not by sending an
// explicit null. TimestampType is only set when non-empty (Task 8's
// MessageService always sets "CREATE_TIME" today, see message.go, but this
// mapping doesn't assume that won't change).
func decodedMessageToGenerated(m *appcluster.DecodedMessage) *generated.TopicMessage {
	tm := &generated.TopicMessage{
		Offset:      m.Offset,
		Partition:   m.Partition,
		Timestamp:   time.UnixMilli(m.TimestampMs).UTC(),
		Key:         ptr(m.Key),
		Value:       ptr(m.Value),
		KeySerde:    ptr(m.KeySerde),
		ValueSerde:  ptr(m.ValueSerde),
		KeySize:     ptr(m.KeySize),
		ValueSize:   ptr(m.ValueSize),
		HeadersSize: ptr(m.HeadersSize),
	}
	if len(m.Headers) > 0 {
		h := m.Headers
		tm.Headers = &h
	}
	if m.TimestampType != "" {
		tt := generated.TopicMessageTimestampType(m.TimestampType)
		tm.TimestampType = &tt
	}
	return tm
}

// SendTopicMessages serves POST .../topics/{topicName}/messages (P1c Task
// 11): decodes the request body's CreateTopicMessage, maps it onto
// appcluster.SendSpec (Key/Value's *string nil-ness passed straight through
// -- see SendSpec's own doc comment on why nil vs pointer-to-empty-string is
// load-bearing), and calls MessageService.Send. Success is 204, no body
// (the contract declares no response schema for the 204 here -- see
// contract/openapi.yaml's sendTopicMessages operation).
//
// Error handling, in order:
//  1. a malformed JSON body -- pre-Send, ordinary 400 JSON response, same
//     decode->400 pattern as CreateTopic/UpdateTopic (handlers_topic.go).
//  2. appcluster.ErrUnknownCluster from Send -- 404 JSON response, same
//     two-bucket convention as every other handler in this package.
//  3. appcluster.ErrSerialize from Send -- a client input error (bad text
//     for the resolved serde), not a server one -- 400 JSON response. This
//     is this endpoint's one addition to the usual two-bucket convention
//     (unknown-cluster -> 404, everything else -> 500): ErrSerialize gets
//     its own 400 bucket, checked before the generic else.
//  4. any other error -- 500 JSON response via serverError, same as
//     everywhere else in this package.
func (s *apiServer) SendTopicMessages(w http.ResponseWriter, r *http.Request, clusterName, topicName string) {
	var body generated.CreateTopicMessage
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse(http.StatusBadRequest, "invalid request body"))
		return
	}

	spec := sendSpecFromGenerated(body)
	if err := s.deps.Messages.Send(r.Context(), clusterName, topicName, spec); err != nil {
		if errors.Is(err, appcluster.ErrUnknownCluster) {
			writeJSON(w, http.StatusNotFound, errorResponse(http.StatusNotFound, "cluster not found"))
			return
		}
		if errors.Is(err, appcluster.ErrSerialize) {
			writeJSON(w, http.StatusBadRequest, errorResponse(http.StatusBadRequest, "failed to serialize message"))
			return
		}
		serverError(w, "SendTopicMessages", "failed to send message", err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// sendSpecFromGenerated maps one decoded CreateTopicMessage request body
// onto appcluster.SendSpec -- a straight field-for-field copy, Key/Value's
// *string nil-ness passed through untouched (SendSpec.Key/Value are
// themselves *string with the identical nil convention, so no translation
// is needed, unlike most other decode->domain mappings in this package).
// Headers/KeySerdeProperties/ValueSerdeProperties default to Go's own nil
// map when the request body omits them, which SendSpec and downstream
// (serializeField, cluster.ProduceRecord) already treat as "none" -- no
// extra normalization needed here either.
func sendSpecFromGenerated(body generated.CreateTopicMessage) appcluster.SendSpec {
	spec := appcluster.SendSpec{
		Partition: body.Partition,
		Key:       body.Key,
		Value:     body.Value,
	}
	if body.Headers != nil {
		spec.Headers = *body.Headers
	}
	if body.KeySerde != nil {
		spec.KeySerde = *body.KeySerde
	}
	if body.ValueSerde != nil {
		spec.ValueSerde = *body.ValueSerde
	}
	if body.KeySerdeProperties != nil {
		spec.KeySerdeProps = *body.KeySerdeProperties
	}
	if body.ValueSerdeProperties != nil {
		spec.ValueSerdeProps = *body.ValueSerdeProperties
	}
	return spec
}

// DeleteTopicMessages serves DELETE .../topics/{topicName}/messages (P1c
// Task 11's deleteRecords): purges params.Partitions (nil/omitted = every
// partition of topicName, cluster.MessageWriterPort.DeleteRecords' own
// empty-means-all convention, passed straight through unchanged). Success
// is 204, no body. Error handling is the ordinary two-bucket convention
// (appcluster.ErrUnknownCluster -> 404, everything else -> 500) -- unlike
// SendTopicMessages, Delete has no serialize step and so no ErrSerialize
// bucket to add.
func (s *apiServer) DeleteTopicMessages(w http.ResponseWriter, r *http.Request, clusterName, topicName string, params generated.DeleteTopicMessagesParams) {
	var partitions []int32
	if params.Partitions != nil {
		partitions = *params.Partitions
	}
	if err := s.deps.Messages.Delete(r.Context(), clusterName, topicName, partitions); err != nil {
		if errors.Is(err, appcluster.ErrUnknownCluster) {
			writeJSON(w, http.StatusNotFound, errorResponse(http.StatusNotFound, "cluster not found"))
			return
		}
		serverError(w, "DeleteTopicMessages", "failed to delete messages", err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
