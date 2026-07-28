package api

import (
	"context"
	"errors"
	"net/http"

	"github.com/cy-kaf/cy-kaf-client/internal/api/generated"
	appcluster "github.com/cy-kaf/cy-kaf-client/internal/app/cluster"
	"github.com/cy-kaf/cy-kaf-client/internal/domain/serde"
)

// SerdesServicer is the narrow interface api's GetSerdes handler consumes;
// app.SerdeService satisfies it (same "resolve name -> Definition, delegate"
// shape as GroupServicer/TopicServicer).
type SerdesServicer interface {
	Suggest(ctx context.Context, name, topic string, use serde.Usage) (serde.Suggestion, error)
}

// GetSerdes serves /api/clusters/{clusterName}/topics/{topicName}/serdes:
// which built-in serdes can handle topicName's key/value, and which one
// cluster config prefers (SerdesServicer.Suggest -> a live serde.Provider
// call every time, no caching). This overrides the generated 501 stub --
// doing so is this task's whole point, since the vendored frontend's
// useSerdes hook (frontend/src/lib/hooks/api/topicMessages.tsx) calls this
// endpoint unconditionally via useSuspenseQuery, from both Filters.tsx
// (DESERIALIZE) and SendMessage.tsx (SERIALIZE) -- a 501 here previously
// crashed the whole Messages tab and Produce side panel (see this task's
// brief).
//
// The contract declares only a 200 response for this operation (no 404/500
// schema) -- same "over-contract, established error convention wins" shape
// as GetBrokerConfig/GetConsumerGroup etc: unknown cluster still reports 404,
// everything else still reports 500, per this repo's fixed two-bucket error
// rule (CLAUDE.md).
func (s *apiServer) GetSerdes(w http.ResponseWriter, r *http.Request, clusterName, topicName string, params generated.GetSerdesParams) {
	use := serde.UsageSerialize
	if params.Use == generated.DESERIALIZE {
		use = serde.UsageDeserialize
	}
	suggestion, err := s.deps.Serdes.Suggest(r.Context(), clusterName, topicName, use)
	if err != nil {
		if errors.Is(err, appcluster.ErrUnknownCluster) {
			writeJSON(w, http.StatusNotFound, errorResponse(http.StatusNotFound, "cluster not found"))
			return
		}
		serverError(w, "GetSerdes", "failed to compute serde suggestions", err)
		return
	}
	writeJSON(w, http.StatusOK, suggestionToGenerated(suggestion))
}

// suggestionToGenerated maps one domain serde.Suggestion onto the contract's
// TopicSerdeSuggestion shape.
func suggestionToGenerated(sg serde.Suggestion) generated.TopicSerdeSuggestion {
	key := descriptionsToGenerated(sg.Key)
	value := descriptionsToGenerated(sg.Value)
	return generated.TopicSerdeSuggestion{Key: &key, Value: &value}
}

// descriptionsToGenerated maps a []serde.Description slice onto the
// contract's []SerdeDescription shape, preserving order (Registry.All's
// fixed display order).
func descriptionsToGenerated(descs []serde.Description) []generated.SerdeDescription {
	out := make([]generated.SerdeDescription, 0, len(descs))
	for _, d := range descs {
		out = append(out, descriptionToGenerated(d))
	}
	return out
}

// descriptionToGenerated maps one domain serde.Description onto the
// contract's SerdeDescription shape. Name/Description/Preferred are always
// set (never omitted, even Preferred==false: "not preferred" is as much a
// real signal to the UI as "preferred" is). Schema carries straight through
// (already *string, nil when the serde has none for this topic/target).
// Parameters is only set when Params is non-empty -- every built-in serde
// today has none (see infra/serde.describeCandidate's doc comment), so this
// stays nil/omitted in practice, but the mapping is still written out fully
// for whenever a non-built-in serde with real Params exists.
func descriptionToGenerated(d serde.Description) generated.SerdeDescription {
	gd := generated.SerdeDescription{
		Name:        ptr(d.Name),
		Description: ptr(d.Description),
		Preferred:   ptr(d.Preferred),
		Schema:      d.Schema,
	}
	if len(d.Params) == 0 {
		return gd
	}
	params := make([]generated.SerdeParameter, 0, len(d.Params))
	for _, p := range d.Params {
		params = append(params, paramToGenerated(p))
	}
	gd.Parameters = &params
	return gd
}

// paramToGenerated maps one domain serde.Param onto the contract's
// SerdeParameter shape.
func paramToGenerated(p serde.Param) generated.SerdeParameter {
	sp := generated.SerdeParameter{Name: p.Name}
	if p.VisibleName != "" {
		sp.VisibleName = ptr(p.VisibleName)
	}
	if len(p.AllowedValues) > 0 {
		av := p.AllowedValues
		sp.AllowedValues = &av
	}
	return sp
}
