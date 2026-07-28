package api

import (
	"context"
	"errors"
	"io"
	"net/http"

	"github.com/cy-kaf/cy-kaf-client/internal/api/generated"
	appcluster "github.com/cy-kaf/cy-kaf-client/internal/app/cluster"
	"github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
)

// AclServicer is the narrow interface api's ACL handlers consume;
// *appcluster.AclService satisfies it (same "resolve name -> Definition,
// delegate" shape as SchemaServicer/ConnectServicer).
type AclServicer interface {
	List(ctx context.Context, name string, filter cluster.AclFilter) ([]cluster.AclBinding, error)
	FormatAclCSV(bindings []cluster.AclBinding) (string, error)
	CreateAcl(ctx context.Context, name string, binding cluster.AclBinding) error
	DeleteAcl(ctx context.Context, name string, binding cluster.AclBinding) (int, error)
	CreateConsumerAcl(ctx context.Context, name string, spec appcluster.ConsumerAclSpec) error
	CreateProducerAcl(ctx context.Context, name string, spec appcluster.ProducerAclSpec) error
	CreateStreamAppAcl(ctx context.Context, name string, spec appcluster.StreamAppAclSpec) error
	SyncCSV(ctx context.Context, name string, csvText string) error
}

// aclFilterFrom builds a domain AclFilter from listAcls/getAclAsCsv query
// params (all optional pointers; nil => "").
func aclFilterFrom(resourceType *generated.KafkaAclResourceType, resourceName *string, namePatternType *generated.KafkaAclNamePatternType, search *string, fts *bool) cluster.AclFilter {
	f := cluster.AclFilter{}
	if resourceType != nil {
		f.ResourceType = string(*resourceType)
	}
	if resourceName != nil {
		f.ResourceName = *resourceName
	}
	if namePatternType != nil {
		f.PatternType = string(*namePatternType)
	}
	if search != nil {
		f.Search = *search
	}
	if fts != nil {
		f.Fts = *fts
	}
	return f
}

// aclBindingToGenerated maps a domain AclBinding to the contract's KafkaAcl.
func aclBindingToGenerated(b cluster.AclBinding) generated.KafkaAcl {
	return generated.KafkaAcl{
		ResourceType:    generated.KafkaAclResourceType(b.ResourceType),
		ResourceName:    b.ResourceName,
		NamePatternType: generated.KafkaAclNamePatternType(b.PatternType),
		Principal:       b.Principal,
		Host:            b.Host,
		Operation:       generated.KafkaAclOperation(b.Operation),
		Permission:      generated.KafkaAclPermission(b.Permission),
	}
}

// aclBindingFrom maps a decoded KafkaAcl request body onto a domain AclBinding.
func aclBindingFrom(k generated.KafkaAcl) cluster.AclBinding {
	return cluster.AclBinding{
		Principal:    k.Principal,
		Host:         k.Host,
		ResourceName: k.ResourceName,
		ResourceType: string(k.ResourceType),
		PatternType:  string(k.NamePatternType),
		Operation:    string(k.Operation),
		Permission:   string(k.Permission),
	}
}

// ListAcls serves GET /api/clusters/{clusterName}/acls.
func (s *apiServer) ListAcls(w http.ResponseWriter, r *http.Request, clusterName string, params generated.ListAclsParams) {
	filter := aclFilterFrom(params.ResourceType, params.ResourceName, params.NamePatternType, params.Search, params.Fts)
	bindings, err := s.deps.Acls.List(r.Context(), clusterName, filter)
	if err != nil {
		if errors.Is(err, appcluster.ErrUnknownCluster) {
			writeJSON(w, http.StatusNotFound, errorResponse(http.StatusNotFound, "cluster not found"))
			return
		}
		serverError(w, "ListAcls", "failed to list acls", err)
		return
	}
	out := make([]generated.KafkaAcl, 0, len(bindings))
	for _, b := range bindings {
		out = append(out, aclBindingToGenerated(b))
	}
	writeJSON(w, http.StatusOK, out)
}

// GetAclAsCsv serves GET /api/clusters/{clusterName}/acls/csv (text/csv, fixed
// 7-column order). CSV rendering is the app layer's FormatAclCSV (single source
// of the CSV shape); the handler only resolves + lists + delegates formatting.
func (s *apiServer) GetAclAsCsv(w http.ResponseWriter, r *http.Request, clusterName string, params generated.GetAclAsCsvParams) {
	filter := aclFilterFrom(params.ResourceType, params.ResourceName, params.NamePatternType, params.Search, params.Fts)
	bindings, err := s.deps.Acls.List(r.Context(), clusterName, filter)
	if err != nil {
		if errors.Is(err, appcluster.ErrUnknownCluster) {
			writeJSON(w, http.StatusNotFound, errorResponse(http.StatusNotFound, "cluster not found"))
			return
		}
		serverError(w, "GetAclAsCsv", "failed to list acls", err)
		return
	}
	text, err := s.deps.Acls.FormatAclCSV(bindings)
	if err != nil {
		serverError(w, "GetAclAsCsv", "failed to render csv", err)
		return
	}
	w.Header().Set("Content-Type", "text/csv")
	w.WriteHeader(http.StatusOK)
	_, _ = io.WriteString(w, text)
}

// CreateAcl serves POST /api/clusters/{clusterName}/acls (204). Genuine write ->
// readOnlyGuard 403's it on a read-only cluster before it reaches here.
func (s *apiServer) CreateAcl(w http.ResponseWriter, r *http.Request, clusterName string) {
	body, ok := decodeStrictJSON[generated.KafkaAcl](w, r)
	if !ok {
		return
	}
	if err := s.deps.Acls.CreateAcl(r.Context(), clusterName, aclBindingFrom(*body)); err != nil {
		writeAclWriteErr(w, "CreateAcl", err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// DeleteAcl serves DELETE /api/clusters/{clusterName}/acls (JSON body; 204 on
// match, 404 when nothing matched). Genuine write -> readOnlyGuard 403.
func (s *apiServer) DeleteAcl(w http.ResponseWriter, r *http.Request, clusterName string) {
	body, ok := decodeStrictJSON[generated.KafkaAcl](w, r)
	if !ok {
		return
	}
	deleted, err := s.deps.Acls.DeleteAcl(r.Context(), clusterName, aclBindingFrom(*body))
	if err != nil {
		writeAclWriteErr(w, "DeleteAcl", err)
		return
	}
	if deleted == 0 {
		writeJSON(w, http.StatusNotFound, errorResponse(http.StatusNotFound, "no matching acl"))
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// CreateConsumerAcl serves POST /api/clusters/{clusterName}/acls/consumer (204).
func (s *apiServer) CreateConsumerAcl(w http.ResponseWriter, r *http.Request, clusterName string) {
	body, ok := decodeStrictJSON[generated.CreateConsumerAcl](w, r)
	if !ok {
		return
	}
	if body.Principal == nil {
		writeJSON(w, http.StatusBadRequest, errorResponse(http.StatusBadRequest, "invalid acl request"))
		return
	}
	spec := appcluster.ConsumerAclSpec{
		Principal: derefStr(body.Principal), Host: hostOrWildcard(body.Host),
		Topics: derefSlice(body.Topics), TopicsPrefix: derefStr(body.TopicsPrefix),
		ConsumerGroups: derefSlice(body.ConsumerGroups), ConsumerGroupsPrefix: derefStr(body.ConsumerGroupsPrefix),
	}
	if err := s.deps.Acls.CreateConsumerAcl(r.Context(), clusterName, spec); err != nil {
		writeAclWriteErr(w, "CreateConsumerAcl", err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// CreateProducerAcl serves POST /api/clusters/{clusterName}/acls/producer (204).
func (s *apiServer) CreateProducerAcl(w http.ResponseWriter, r *http.Request, clusterName string) {
	body, ok := decodeStrictJSON[generated.CreateProducerAcl](w, r)
	if !ok {
		return
	}
	if body.Principal == nil {
		writeJSON(w, http.StatusBadRequest, errorResponse(http.StatusBadRequest, "invalid acl request"))
		return
	}
	spec := appcluster.ProducerAclSpec{
		Principal: derefStr(body.Principal), Host: hostOrWildcard(body.Host),
		Topics: derefSlice(body.Topics), TopicsPrefix: derefStr(body.TopicsPrefix),
		TransactionalID: derefStr(body.TransactionalId), TransactionsIDPrefix: derefStr(body.TransactionsIdPrefix),
		Idempotent: body.Idempotent != nil && *body.Idempotent,
	}
	if err := s.deps.Acls.CreateProducerAcl(r.Context(), clusterName, spec); err != nil {
		writeAclWriteErr(w, "CreateProducerAcl", err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// CreateStreamAppAcl serves POST /api/clusters/{clusterName}/acls/streamapp (204).
func (s *apiServer) CreateStreamAppAcl(w http.ResponseWriter, r *http.Request, clusterName string) {
	body, ok := decodeStrictJSON[generated.CreateStreamAppAcl](w, r)
	if !ok {
		return
	}
	if body.Principal == nil {
		writeJSON(w, http.StatusBadRequest, errorResponse(http.StatusBadRequest, "invalid acl request"))
		return
	}
	spec := appcluster.StreamAppAclSpec{
		Principal: derefStr(body.Principal), Host: hostOrWildcard(body.Host),
		InputTopics: derefSlice(body.InputTopics), OutputTopics: derefSlice(body.OutputTopics),
		ApplicationID: derefStr(body.ApplicationId),
	}
	if err := s.deps.Acls.CreateStreamAppAcl(r.Context(), clusterName, spec); err != nil {
		writeAclWriteErr(w, "CreateStreamAppAcl", err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// SyncAclsCsv serves POST /api/clusters/{clusterName}/acls/csv (text/plain body;
// 204). Error routing (P2c-D4/Fix 2): ErrBadAclCSV (parse/validation, e.g. rows
// != 7 columns, unparseable enum) -> 400; ErrUnknownCluster -> 404; any other
// (backend ListAcls/CreateAcls/DeleteAcls failure during sync) -> 500 (no longer
// swallowed as 400).
func (s *apiServer) SyncAclsCsv(w http.ResponseWriter, r *http.Request, clusterName string) {
	r.Body = http.MaxBytesReader(w, r.Body, requestMaxBodyBytes)
	raw, err := io.ReadAll(r.Body)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse(http.StatusBadRequest, "invalid request body"))
		return
	}
	if _, err := appcluster.ParseAclCSV(string(raw)); err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse(http.StatusBadRequest, "invalid acl csv: "+err.Error()))
		return
	}
	if err := s.deps.Acls.SyncCSV(r.Context(), clusterName, string(raw)); err != nil {
		switch {
		case errors.Is(err, appcluster.ErrBadAclCSV):
			writeJSON(w, http.StatusBadRequest, errorResponse(http.StatusBadRequest, "invalid acl csv: "+err.Error()))
		case errors.Is(err, appcluster.ErrUnknownCluster):
			writeJSON(w, http.StatusNotFound, errorResponse(http.StatusNotFound, "cluster not found"))
		default:
			serverError(w, "SyncAclsCsv", "failed to sync acls", err)
		}
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// writeAclWriteErr routes a helper-endpoint write error: unknown cluster 404,
// else 500. (Body decode already 400'd upstream.)
func writeAclWriteErr(w http.ResponseWriter, op string, err error) {
	if errors.Is(err, appcluster.ErrBadAclRequest) {
		writeJSON(w, http.StatusBadRequest, errorResponse(http.StatusBadRequest, "invalid acl request: "+err.Error()))
		return
	}
	if errors.Is(err, appcluster.ErrUnknownCluster) {
		writeJSON(w, http.StatusNotFound, errorResponse(http.StatusNotFound, "cluster not found"))
		return
	}
	serverError(w, op, "failed to create acl", err)
}

func derefStr(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}

func derefSlice(p *[]string) []string {
	if p == nil {
		return nil
	}
	return *p
}

// hostOrWildcard defaults an unset host to "*" (upstream Create*Acl treats a
// missing host as "any host").
func hostOrWildcard(p *string) string {
	if p == nil {
		return "*"
	}
	return *p
}
