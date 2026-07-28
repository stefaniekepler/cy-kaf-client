package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/cy-kaf/cy-kaf-client/internal/api/generated"
	appcluster "github.com/cy-kaf/cy-kaf-client/internal/app/cluster"
	"github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
)

// ConnectServicer is the narrow interface api's Kafka Connect handlers
// consume; *appcluster.ConnectService satisfies it (same "resolve name ->
// Definition, delegate" shape as SchemaServicer/GroupServicer). P2b Task 3
// declared the four read-only endpoints' methods; Task 4 (connector reads)
// and Task 5 (connector writes/actions) grew this same interface alongside
// their handlers, up to the full Connect surface KafkaConnectPort (domain)
// exposes -- all 16 P2b endpoints as of Task 5.
type ConnectServicer interface {
	ListConnects(ctx context.Context, name string) ([]cluster.ConnectCluster, error)
	Plugins(ctx context.Context, name, connectName string) ([]cluster.ConnectorPlugin, error)
	ValidatePlugin(ctx context.Context, name, connectName, pluginName string, cfg map[string]any) (cluster.PluginValidation, error)
	AllConnectors(ctx context.Context, name string) ([]cluster.ConnectorRef, error)
	Connectors(ctx context.Context, name, connectName string) ([]string, error)
	Connector(ctx context.Context, name, connectName, connectorName string) (cluster.Connector, error)
	ConnectorConfig(ctx context.Context, name, connectName, connectorName string) (map[string]any, error)
	ConnectorTasks(ctx context.Context, name, connectName, connectorName string) ([]cluster.ConnectorTask, error)
	CreateConnector(ctx context.Context, name, connectName, connectorName string, cfg map[string]any) (cluster.Connector, error)
	DeleteConnector(ctx context.Context, name, connectName, connectorName string) error
	SetConnectorConfig(ctx context.Context, name, connectName, connectorName string, cfg map[string]any) (cluster.Connector, error)
	UpdateConnectorState(ctx context.Context, name, connectName, connectorName, action string) error
	ResetConnectorOffsets(ctx context.Context, name, connectName, connectorName string) error
	RestartConnectorTask(ctx context.Context, name, connectName, connectorName string, taskID int) error
}

// connectErrStatus maps ConnectServicer's two "not found" sentinels
// (ErrUnknownCluster: cluster name; ErrUnknownConnect: connectName within a
// known cluster) onto their shared 404 status+message; ok is false for
// anything else, so the caller falls back to serverError -> 500.
func connectErrStatus(err error) (status int, message string, ok bool) {
	switch {
	case errors.Is(err, appcluster.ErrUnknownCluster):
		return http.StatusNotFound, "cluster not found", true
	case errors.Is(err, appcluster.ErrUnknownConnect):
		return http.StatusNotFound, "connect not found", true
	default:
		return 0, "", false
	}
}

// GetConnects serves /api/clusters/{clusterName}/connects: every Kafka
// Connect worker configured on the cluster (skip-bad aggregated -- see
// ConnectService.ListConnects). The contract's withStats query param is
// accepted (params) but unused: domain ConnectCluster carries only
// name+address, no per-worker connector/task counts to report, so a
// stats-augmented response isn't yet possible at this layer.
func (s *apiServer) GetConnects(w http.ResponseWriter, r *http.Request, clusterName string, _ generated.GetConnectsParams) {
	connects, err := s.deps.Connects.ListConnects(r.Context(), clusterName)
	if err != nil {
		if status, msg, ok := connectErrStatus(err); ok {
			writeJSON(w, status, errorResponse(status, msg))
			return
		}
		serverError(w, "GetConnects", "failed to list connects", err)
		return
	}
	writeJSON(w, http.StatusOK, connectsToGenerated(connects))
}

// GetConnectsCsv serves /api/clusters/{clusterName}/connects/csv: the same
// data as GetConnects, rendered as CSV via rowsToCsv (csv.go; upstream's
// "export as CSV" affordance, same pattern as GetBrokersCsv).
func (s *apiServer) GetConnectsCsv(w http.ResponseWriter, r *http.Request, clusterName string, _ generated.GetConnectsCsvParams) {
	connects, err := s.deps.Connects.ListConnects(r.Context(), clusterName)
	if err != nil {
		if status, msg, ok := connectErrStatus(err); ok {
			writeJSON(w, status, errorResponse(status, msg))
			return
		}
		serverError(w, "GetConnectsCsv", "failed to list connects", err)
		return
	}
	body, err := rowsToCsv(connectsToGenerated(connects))
	if err != nil {
		serverError(w, "GetConnectsCsv", "failed to render connects csv", err)
		return
	}
	w.Header().Set("Content-Type", "text/csv")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(body))
}

// GetConnectorPlugins serves
// /api/clusters/{clusterName}/connects/{connectName}/plugins: connectName's
// available connector plugin classes.
func (s *apiServer) GetConnectorPlugins(w http.ResponseWriter, r *http.Request, clusterName, connectName string) {
	plugins, err := s.deps.Connects.Plugins(r.Context(), clusterName, connectName)
	if err != nil {
		if status, msg, ok := connectErrStatus(err); ok {
			writeJSON(w, status, errorResponse(status, msg))
			return
		}
		serverError(w, "GetConnectorPlugins", "failed to list connector plugins", err)
		return
	}
	out := make([]generated.ConnectorPlugin, 0, len(plugins))
	for _, p := range plugins {
		out = append(out, generated.ConnectorPlugin{Class: ptr(p.Class)})
	}
	writeJSON(w, http.StatusOK, out)
}

// ValidateConnectorPluginConfig serves PUT
// /api/clusters/{clusterName}/connects/{connectName}/plugins/{pluginName}/config/validate:
// a dry-run validation of the request body's connector config against
// pluginName's config definitions on connectName (never creates/alters a
// connector). Read-only-in-effect -- whitelisted past readOnlyGuard
// (middleware.go's readOnlyWhitelistPatterns, P2b-D6) so a read-only cluster
// can still run it.
func (s *apiServer) ValidateConnectorPluginConfig(w http.ResponseWriter, r *http.Request, clusterName, connectName, pluginName string) {
	var body generated.ConnectorConfig
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse(http.StatusBadRequest, "invalid request body"))
		return
	}
	pv, err := s.deps.Connects.ValidatePlugin(r.Context(), clusterName, connectName, pluginName, body)
	if err != nil {
		if status, msg, ok := connectErrStatus(err); ok {
			writeJSON(w, status, errorResponse(status, msg))
			return
		}
		serverError(w, "ValidateConnectorPluginConfig", "failed to validate connector plugin config", err)
		return
	}
	writeJSON(w, http.StatusOK, pluginValidationToGenerated(pv))
}

// GetAllConnectors serves /api/clusters/{clusterName}/connectors: every
// connector across every configured Connect worker, with real per-connector
// status/type/task-counts (skip-bad aggregated -- see
// ConnectService.AllConnectors and infra/connect.Pool.AllConnectors's
// KIP-465 bulk expand doc comment). The contract's search/orderBy/sortOrder/
// fts query params are accepted (params) but unused: filtering/sorting the
// aggregated list isn't implemented at this layer yet (mirrors GetConnects'
// withStats -- accepted, out of scope).
func (s *apiServer) GetAllConnectors(w http.ResponseWriter, r *http.Request, clusterName string, params generated.GetAllConnectorsParams) {
	refs, err := s.deps.Connects.AllConnectors(r.Context(), clusterName)
	if err != nil {
		if status, msg, ok := connectErrStatus(err); ok {
			writeJSON(w, status, errorResponse(status, msg))
			return
		}
		serverError(w, "GetAllConnectors", "failed to list connectors", err)
		return
	}
	refs = filterConnectorRefs(refs, params.Search)
	writeJSON(w, http.StatusOK, connectorRefsToGenerated(refs))
}

// GetAllConnectorsCsv serves /api/clusters/{clusterName}/connectors/csv: the
// same data as GetAllConnectors, rendered as CSV via rowsToCsv (csv.go; same
// pattern as GetConnectsCsv).
func (s *apiServer) GetAllConnectorsCsv(w http.ResponseWriter, r *http.Request, clusterName string, params generated.GetAllConnectorsCsvParams) {
	refs, err := s.deps.Connects.AllConnectors(r.Context(), clusterName)
	if err != nil {
		if status, msg, ok := connectErrStatus(err); ok {
			writeJSON(w, status, errorResponse(status, msg))
			return
		}
		serverError(w, "GetAllConnectorsCsv", "failed to list connectors", err)
		return
	}
	refs = filterConnectorRefs(refs, params.Search)
	body, err := rowsToCsv(connectorRefsToGenerated(refs))
	if err != nil {
		serverError(w, "GetAllConnectorsCsv", "failed to render connectors csv", err)
		return
	}
	w.Header().Set("Content-Type", "text/csv")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(body))
}

// GetConnectors serves
// /api/clusters/{clusterName}/connects/{connectName}/connectors:
// connectName's connector names.
func (s *apiServer) GetConnectors(w http.ResponseWriter, r *http.Request, clusterName, connectName string) {
	names, err := s.deps.Connects.Connectors(r.Context(), clusterName, connectName)
	if err != nil {
		if status, msg, ok := connectErrStatus(err); ok {
			writeJSON(w, status, errorResponse(status, msg))
			return
		}
		serverError(w, "GetConnectors", "failed to list connectors", err)
		return
	}
	out := make([]string, 0, len(names))
	out = append(out, names...)
	writeJSON(w, http.StatusOK, out)
}

// GetConnector serves
// /api/clusters/{clusterName}/connects/{connectName}/connectors/{connectorName}:
// connectorName's full assembled detail on connectName.
func (s *apiServer) GetConnector(w http.ResponseWriter, r *http.Request, clusterName, connectName, connectorName string) {
	c, err := s.deps.Connects.Connector(r.Context(), clusterName, connectName, connectorName)
	if err != nil {
		if status, msg, ok := connectErrStatus(err); ok {
			writeJSON(w, status, errorResponse(status, msg))
			return
		}
		serverError(w, "GetConnector", "failed to get connector", err)
		return
	}
	writeJSON(w, http.StatusOK, connectorToGenerated(c))
}

// GetConnectorConfig serves
// /api/clusters/{clusterName}/connects/{connectName}/connectors/{connectorName}/config:
// connectorName's current config on connectName (a plain config KV map --
// contract's ConnectorConfig).
func (s *apiServer) GetConnectorConfig(w http.ResponseWriter, r *http.Request, clusterName, connectName, connectorName string) {
	cfg, err := s.deps.Connects.ConnectorConfig(r.Context(), clusterName, connectName, connectorName)
	if err != nil {
		if status, msg, ok := connectErrStatus(err); ok {
			writeJSON(w, status, errorResponse(status, msg))
			return
		}
		serverError(w, "GetConnectorConfig", "failed to get connector config", err)
		return
	}
	if cfg == nil {
		cfg = map[string]any{}
	}
	writeJSON(w, http.StatusOK, generated.ConnectorConfig(cfg))
}

// GetConnectorTasks serves
// /api/clusters/{clusterName}/connects/{connectName}/connectors/{connectorName}/tasks:
// connectorName's tasks (assembled config + status) on connectName.
func (s *apiServer) GetConnectorTasks(w http.ResponseWriter, r *http.Request, clusterName, connectName, connectorName string) {
	tasks, err := s.deps.Connects.ConnectorTasks(r.Context(), clusterName, connectName, connectorName)
	if err != nil {
		if status, msg, ok := connectErrStatus(err); ok {
			writeJSON(w, status, errorResponse(status, msg))
			return
		}
		serverError(w, "GetConnectorTasks", "failed to get connector tasks", err)
		return
	}
	writeJSON(w, http.StatusOK, connectorTasksToGenerated(connectorName, tasks))
}

// CreateConnector serves POST
// /api/clusters/{clusterName}/connects/{connectName}/connectors: creates a
// new connector from the request body's NewConnector (name + config) on
// connectName, returning its assembled detail. Genuine cluster-scoped write
// -> readOnlyGuard 403's it on a read-only cluster before it ever reaches
// here.
func (s *apiServer) CreateConnector(w http.ResponseWriter, r *http.Request, clusterName, connectName string) {
	var body generated.NewConnector
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse(http.StatusBadRequest, "invalid request body"))
		return
	}
	c, err := s.deps.Connects.CreateConnector(r.Context(), clusterName, connectName, body.Name, body.Config)
	if err != nil {
		if status, msg, ok := connectErrStatus(err); ok {
			writeJSON(w, status, errorResponse(status, msg))
			return
		}
		serverError(w, "CreateConnector", "failed to create connector", err)
		return
	}
	writeJSON(w, http.StatusOK, connectorToGenerated(c))
}

// DeleteConnector serves DELETE
// /api/clusters/{clusterName}/connects/{connectName}/connectors/{connectorName}:
// deletes connectorName from connectName. 204 on success, no body. Genuine
// write -> readOnlyGuard 403's it on a read-only cluster.
func (s *apiServer) DeleteConnector(w http.ResponseWriter, r *http.Request, clusterName, connectName, connectorName string) {
	if err := s.deps.Connects.DeleteConnector(r.Context(), clusterName, connectName, connectorName); err != nil {
		if status, msg, ok := connectErrStatus(err); ok {
			writeJSON(w, status, errorResponse(status, msg))
			return
		}
		serverError(w, "DeleteConnector", "failed to delete connector", err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// SetConnectorConfig serves PUT
// /api/clusters/{clusterName}/connects/{connectName}/connectors/{connectorName}/config:
// replaces connectorName's whole config on connectName with the request
// body (Connect's own PUT .../config semantics -- a full replace, not an
// incremental merge; see KafkaConnectPort.SetConnectorConfig's doc
// comment), returning its assembled detail. Genuine write -> readOnlyGuard
// 403's it on a read-only cluster.
func (s *apiServer) SetConnectorConfig(w http.ResponseWriter, r *http.Request, clusterName, connectName, connectorName string) {
	var body generated.ConnectorConfig
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse(http.StatusBadRequest, "invalid request body"))
		return
	}
	c, err := s.deps.Connects.SetConnectorConfig(r.Context(), clusterName, connectName, connectorName, body)
	if err != nil {
		if status, msg, ok := connectErrStatus(err); ok {
			writeJSON(w, status, errorResponse(status, msg))
			return
		}
		serverError(w, "SetConnectorConfig", "failed to set connector config", err)
		return
	}
	writeJSON(w, http.StatusOK, connectorToGenerated(c))
}

// UpdateConnectorState serves POST
// /api/clusters/{clusterName}/connects/{connectName}/connectors/{connectorName}/action/{action}:
// applies action (the contract's ConnectorAction path enum -- PAUSE/RESUME/
// STOP/RESTART/RESTART_ALL_TASKS/RESTART_FAILED_TASKS, P2b-D4) to
// connectorName on connectName. action is forwarded verbatim via
// string(action); the six-value -> REST mapping lives entirely in the
// KafkaConnectPort implementation. 204 on success, no body. Genuine write
// -> readOnlyGuard 403's it on a read-only cluster.
func (s *apiServer) UpdateConnectorState(w http.ResponseWriter, r *http.Request, clusterName, connectName, connectorName string, action generated.ConnectorAction) {
	if err := s.deps.Connects.UpdateConnectorState(r.Context(), clusterName, connectName, connectorName, string(action)); err != nil {
		if status, msg, ok := connectErrStatus(err); ok {
			writeJSON(w, status, errorResponse(status, msg))
			return
		}
		serverError(w, "UpdateConnectorState", "failed to update connector state", err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// ResetConnectorOffsets serves DELETE
// /api/clusters/{clusterName}/connects/{connectName}/connectors/{connectorName}/offsets:
// resets connectorName's committed offsets on connectName. 204 on success,
// no body. Genuine write -> readOnlyGuard 403's it on a read-only cluster.
func (s *apiServer) ResetConnectorOffsets(w http.ResponseWriter, r *http.Request, clusterName, connectName, connectorName string) {
	if err := s.deps.Connects.ResetConnectorOffsets(r.Context(), clusterName, connectName, connectorName); err != nil {
		if status, msg, ok := connectErrStatus(err); ok {
			writeJSON(w, status, errorResponse(status, msg))
			return
		}
		serverError(w, "ResetConnectorOffsets", "failed to reset connector offsets", err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// RestartConnectorTask serves POST
// /api/clusters/{clusterName}/connects/{connectName}/connectors/{connectorName}/tasks/{taskId}/action/restart:
// restarts one task (by index) of connectorName on connectName. 204 on
// success, no body. Genuine write -> readOnlyGuard 403's it on a read-only
// cluster.
func (s *apiServer) RestartConnectorTask(w http.ResponseWriter, r *http.Request, clusterName, connectName, connectorName string, taskId int32) {
	if err := s.deps.Connects.RestartConnectorTask(r.Context(), clusterName, connectName, connectorName, int(taskId)); err != nil {
		if status, msg, ok := connectErrStatus(err); ok {
			writeJSON(w, status, errorResponse(status, msg))
			return
		}
		serverError(w, "RestartConnectorTask", "failed to restart connector task", err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// connectsToGenerated maps a slice of domain ConnectCluster onto the
// contract's Connect wire shape; shared by GetConnects and GetConnectsCsv so
// the two stay in lockstep (mirrors brokersFor's role for
// GetBrokers/GetBrokersCsv).
func connectsToGenerated(connects []cluster.ConnectCluster) []generated.Connect {
	out := make([]generated.Connect, 0, len(connects))
	for _, c := range connects {
		out = append(out, generated.Connect{
			Name:    c.Name,
			Address: ptr(c.Address),
		})
	}
	return out
}

// filterConnectorRefs implements GetAllConnectors/GetAllConnectorsCsv's
// `search` query param (contract's GetAllConnectorsParams.Search): a
// case-insensitive substring match against connector name, connect name,
// type, or state. The frontend's ListPage.tsx sends the search box's value
// straight through to this endpoint and renders whatever comes back with no
// client-side re-filtering of its own (its Table's filterPersister is for
// per-column filters, not this free-text box) -- so an unfiltered response
// here means the search box silently does nothing, caught live by
// e2e-p2b's "KafkaConnect search is working" scenario. A nil/empty search
// is a no-op (matches every ref), same "absent param -> unfiltered"
// convention as the rest of this API.
func filterConnectorRefs(refs []cluster.ConnectorRef, search *string) []cluster.ConnectorRef {
	if search == nil || *search == "" {
		return refs
	}
	q := strings.ToLower(*search)
	out := make([]cluster.ConnectorRef, 0, len(refs))
	for _, ref := range refs {
		if strings.Contains(strings.ToLower(ref.Name), q) ||
			strings.Contains(strings.ToLower(ref.ConnectName), q) ||
			strings.Contains(strings.ToLower(ref.Type), q) ||
			strings.Contains(strings.ToLower(ref.State), q) {
			out = append(out, ref)
		}
	}
	return out
}

// pluginValidationToGenerated maps a domain PluginValidation to the
// contract's ConnectorPluginConfigValidationResponse wire shape.
// Groups/Configs are omitted (nil pointer) when empty, matching
// schemaVersionToGenerated's References convention.
func pluginValidationToGenerated(pv cluster.PluginValidation) generated.ConnectorPluginConfigValidationResponse {
	out := generated.ConnectorPluginConfigValidationResponse{
		Name:       ptr(pv.Name),
		ErrorCount: ptr(int32(pv.ErrorCount)),
	}
	if len(pv.Groups) > 0 {
		out.Groups = ptr(pv.Groups)
	}
	if len(pv.Configs) > 0 {
		configs := make([]generated.ConnectorPluginConfig, len(pv.Configs))
		for i, c := range pv.Configs {
			configs[i] = generated.ConnectorPluginConfig{
				Definition: pluginConfigDefToGenerated(c.Definition),
				Value:      pluginConfigValueToGenerated(c.Value),
			}
		}
		out.Configs = &configs
	}
	return out
}

// pluginConfigDefToGenerated maps a domain PluginConfigDef to the contract's
// ConnectorPluginConfigDefinition wire shape.
func pluginConfigDefToGenerated(d cluster.PluginConfigDef) *generated.ConnectorPluginConfigDefinition {
	out := generated.ConnectorPluginConfigDefinition{
		Name:          ptr(d.Name),
		Type:          ptr(generated.ConnectorPluginConfigDefinitionType(d.Type)),
		Required:      ptr(d.Required),
		DefaultValue:  ptr(d.DefaultValue),
		Importance:    ptr(generated.ConnectorPluginConfigDefinitionImportance(d.Importance)),
		Documentation: ptr(d.Documentation),
		Group:         ptr(d.Group),
		Width:         ptr(generated.ConnectorPluginConfigDefinitionWidth(d.Width)),
		DisplayName:   ptr(d.DisplayName),
		Order:         ptr(int32(d.Order)),
	}
	if len(d.Dependents) > 0 {
		out.Dependents = ptr(d.Dependents)
	}
	return &out
}

// pluginConfigValueToGenerated maps a domain PluginConfigValue to the
// contract's ConnectorPluginConfigValue wire shape.
func pluginConfigValueToGenerated(v cluster.PluginConfigValue) *generated.ConnectorPluginConfigValue {
	out := generated.ConnectorPluginConfigValue{
		Name:    ptr(v.Name),
		Value:   ptr(v.Value),
		Visible: ptr(v.Visible),
	}
	if len(v.RecommendedValues) > 0 {
		out.RecommendedValues = ptr(v.RecommendedValues)
	}
	if len(v.Errors) > 0 {
		out.Errors = ptr(v.Errors)
	}
	return &out
}

// connectorRefsToGenerated maps a slice of domain ConnectorRef (the
// AllConnectors aggregate -- see KafkaConnectPort.AllConnectors's doc
// comment) onto the contract's FullConnectorInfo wire shape. ConnectorRef's
// State/WorkerID/Type/TasksCount/FailedTasksCount are populated from Kafka
// Connect's bulk expand=status&expand=info endpoint (infra/connect.Pool.
// AllConnectors), so this carries real per-connector status, not a
// placeholder. State falls back to UNASSIGNED (Connect's own meaning for "no
// worker currently assigned") only in the defensive case of an empty string
// (e.g. a malformed/partial bulk response entry) -- FullConnectorInfo.status
// is a required field with a required enum State that has no "unknown"
// member, so an empty string is never a valid value to send over the wire.
// ConnectorClass/Consumer/Topics still stay nil: the bulk endpoint doesn't
// carry a plugin class name, consumer group, or topic list -- a caller
// wanting those calls GetConnector for one connector's full detail.
func connectorRefsToGenerated(refs []cluster.ConnectorRef) []generated.FullConnectorInfo {
	out := make([]generated.FullConnectorInfo, 0, len(refs))
	for _, ref := range refs {
		info := generated.FullConnectorInfo{
			Connect: ref.ConnectName,
			Name:    ref.Name,
			Status: generated.ConnectorStatus{
				State:    connectorRefStateToGenerated(ref.State),
				WorkerId: ptr(ref.WorkerID),
			},
			TasksCount:       ptr(ref.TasksCount),
			FailedTasksCount: ptr(ref.FailedTasksCount),
		}
		if ref.Type != "" {
			t := generated.ConnectorType(strings.ToUpper(ref.Type))
			info.Type = &t
		}
		out = append(out, info)
	}
	return out
}

// connectorRefStateToGenerated maps ConnectorRef.State onto the contract's
// ConnectorState enum, falling back to UNASSIGNED for an empty string (see
// connectorRefsToGenerated's doc comment) -- every other value is passed
// through as-is, same direct-cast convention connectorToGenerated already
// uses for GetConnector's real (always-populated) state.
func connectorRefStateToGenerated(state string) generated.ConnectorState {
	if state == "" {
		return generated.ConnectorStateUNASSIGNED
	}
	return generated.ConnectorState(state)
}

// connectorTypeToGenerated maps a domain Connector.Type string (Kafka
// Connect's wire "type" field -- lowercase in practice, e.g. "source"/"sink";
// see infra/connect.Pool.toConnector, which passes it through verbatim) onto
// the contract's ConnectorType enum, which is UPPERCASE-ONLY
// (SOURCE/SINK/UNKNOWN). Connector.Type is a required, non-nullable field per
// the contract (unlike FullConnectorInfo.Type, which connectorRefsToGenerated
// leaves nil for an empty ref.Type), so an empty or otherwise unrecognized
// value falls back to UNKNOWN rather than emitting an invalid enum value onto
// the wire.
func connectorTypeToGenerated(t string) generated.ConnectorType {
	ct := generated.ConnectorType(strings.ToUpper(t))
	if !ct.Valid() {
		return generated.ConnectorTypeUNKNOWN
	}
	return ct
}

// connectorToGenerated maps a domain Connector (GetConnector's assembled
// three-REST-call detail -- see cluster.go's Connector doc comment) onto the
// contract's Connector wire shape. Config is never left nil (a nil
// map[string]any marshals to JSON null, but Connector.Config is a required,
// non-nullable object per the contract). Tasks reconstructs the contract's
// per-entry TaskId{Connector,Task} pairs from domain's flat TaskIDs -- the
// Connector name is intentionally stripped in the domain type as redundant
// (every entry repeats it) but the wire shape still carries it, so it's
// added back here.
func connectorToGenerated(c cluster.Connector) generated.Connector {
	cfg := c.Config
	if cfg == nil {
		cfg = map[string]any{}
	}
	out := generated.Connector{
		Config:  generated.ConnectorConfig(cfg),
		Connect: c.ConnectName,
		Name:    c.Name,
		Status: generated.ConnectorStatus{
			State:    generated.ConnectorState(c.State),
			Trace:    ptr(c.Trace),
			WorkerId: ptr(c.WorkerID),
		},
		Type: connectorTypeToGenerated(c.Type),
	}
	if len(c.TaskIDs) > 0 {
		tasks := make([]generated.TaskId, len(c.TaskIDs))
		for i, id := range c.TaskIDs {
			tasks[i] = generated.TaskId{Connector: ptr(c.Name), Task: ptr(int32(id))}
		}
		out.Tasks = &tasks
	}
	if len(c.Topics) > 0 {
		out.Topics = ptr(c.Topics)
	}
	return out
}

// connectorTasksToGenerated maps a slice of domain ConnectorTask (one
// connector's tasks -- see cluster.go's ConnectorTask doc comment) onto the
// contract's []Task wire shape, reconstructing each entry's TaskId (domain
// keeps only the bare int ID; connectorName is the caller-supplied name all
// these tasks belong to, not carried per task in the domain type).
func connectorTasksToGenerated(connectorName string, tasks []cluster.ConnectorTask) []generated.Task {
	out := make([]generated.Task, 0, len(tasks))
	for _, t := range tasks {
		task := generated.Task{
			Id: &generated.TaskId{Connector: ptr(connectorName), Task: ptr(int32(t.ID))},
			Status: generated.TaskStatus{
				Id:       int32(t.ID),
				State:    generated.ConnectorTaskStatus(t.State),
				Trace:    ptr(t.Trace),
				WorkerId: t.WorkerID,
			},
		}
		if len(t.Config) > 0 {
			cfg := generated.ConnectorConfig(t.Config)
			task.Config = &cfg
		}
		out = append(out, task)
	}
	return out
}
