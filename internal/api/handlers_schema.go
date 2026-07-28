package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	"github.com/cy-kaf/cy-kaf-client/internal/api/generated"
	appcluster "github.com/cy-kaf/cy-kaf-client/internal/app/cluster"
	"github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
)

// SchemaServicer is the narrow interface api's Schema Registry handlers
// consume; *appcluster.SchemaService satisfies it (same "resolve name ->
// Definition, delegate" shape as GroupServicer/SerdesServicer). P2a Task 3
// declares only the four read methods; Task 4 (writes) and Task 5
// (compatibility) grow this same interface alongside their handlers.
type SchemaServicer interface {
	ListSchemas(ctx context.Context, name string, q appcluster.SchemaListQuery) (appcluster.SchemaPage, error)
	LatestSchema(ctx context.Context, name, subject string) (cluster.SchemaVersion, error)
	SchemaByVersion(ctx context.Context, name, subject, version string) (cluster.SchemaVersion, error)
	AllVersions(ctx context.Context, name, subject string) ([]cluster.SchemaVersion, error)
	Register(ctx context.Context, name, subject string, ns cluster.NewSchema) (cluster.SchemaVersion, error)
	DeleteSubject(ctx context.Context, name, subject string) ([]int, error)
	DeleteVersion(ctx context.Context, name, subject, version string) (int, error)
	GlobalCompat(ctx context.Context, name string) (string, error)
	SetGlobalCompat(ctx context.Context, name, level string) error
	SetSubjectCompat(ctx context.Context, name, subject, level string) error
	CheckCompat(ctx context.Context, name, subject string, ns cluster.NewSchema) (bool, error)
}

// schemaErrStatus maps the two expected resolution failures shared by every
// Schema handler. Anything else remains a real backend failure and falls
// through to the handler's existing serverError path.
func schemaErrStatus(err error) (status int, message string, ok bool) {
	switch {
	case errors.Is(err, appcluster.ErrUnknownCluster):
		return http.StatusNotFound, "cluster not found", true
	case errors.Is(err, appcluster.ErrSchemaRegistryNotConfigured):
		return http.StatusNotFound, "schema registry not configured", true
	default:
		return 0, "", false
	}
}

// GetSchemas serves /api/clusters/{clusterName}/schemas: the paged/filtered/
// sorted subject list, each subject resolved to its latest schema version
// (SchemaService.ListSchemas does the actual work; this handler only
// translates params in and the SchemaSubjectsResponse out — same shape as
// GetTopics/GetConsumerGroupsPage).
func (s *apiServer) GetSchemas(w http.ResponseWriter, r *http.Request, clusterName string, params generated.GetSchemasParams) {
	page, err := s.deps.Schemas.ListSchemas(r.Context(), clusterName, schemaListQueryFrom(params))
	if err != nil {
		if status, msg, ok := schemaErrStatus(err); ok {
			writeJSON(w, status, errorResponse(status, msg))
			return
		}
		serverError(w, "GetSchemas", "failed to list schemas", err)
		return
	}
	schemas := make([]generated.SchemaSubject, 0, len(page.Schemas))
	for _, sv := range page.Schemas {
		schemas = append(schemas, schemaVersionToGenerated(sv))
	}
	pageCount := int32(page.PageCount)
	writeJSON(w, http.StatusOK, generated.SchemaSubjectsResponse{
		PageCount: &pageCount,
		Schemas:   &schemas,
	})
}

// GetLatestSchema serves /api/clusters/{clusterName}/schemas/{subject}/latest:
// subject's latest registered schema version.
func (s *apiServer) GetLatestSchema(w http.ResponseWriter, r *http.Request, clusterName, subject string) {
	sv, err := s.deps.Schemas.LatestSchema(r.Context(), clusterName, subject)
	if err != nil {
		if status, msg, ok := schemaErrStatus(err); ok {
			writeJSON(w, status, errorResponse(status, msg))
			return
		}
		serverError(w, "GetLatestSchema", "failed to get latest schema", err)
		return
	}
	writeJSON(w, http.StatusOK, schemaVersionToGenerated(sv))
}

// GetSchemaByVersion serves
// /api/clusters/{clusterName}/schemas/{subject}/versions/{version}: subject's
// schema at one concrete version. The {version} path param is an int32 in the
// contract; the port takes it as a string (its "latest"-or-numeric
// convention), so it's forwarded as a plain decimal string here.
func (s *apiServer) GetSchemaByVersion(w http.ResponseWriter, r *http.Request, clusterName, subject string, version int32) {
	sv, err := s.deps.Schemas.SchemaByVersion(r.Context(), clusterName, subject, strconv.FormatInt(int64(version), 10))
	if err != nil {
		if status, msg, ok := schemaErrStatus(err); ok {
			writeJSON(w, status, errorResponse(status, msg))
			return
		}
		serverError(w, "GetSchemaByVersion", "failed to get schema version", err)
		return
	}
	writeJSON(w, http.StatusOK, schemaVersionToGenerated(sv))
}

// GetAllVersionsBySubject serves
// /api/clusters/{clusterName}/schemas/{subject}/versions: every registered
// version of subject, each as a full SchemaSubject (the contract 200 is an
// array of SchemaSubject, not bare version numbers).
func (s *apiServer) GetAllVersionsBySubject(w http.ResponseWriter, r *http.Request, clusterName, subject string) {
	svs, err := s.deps.Schemas.AllVersions(r.Context(), clusterName, subject)
	if err != nil {
		if status, msg, ok := schemaErrStatus(err); ok {
			writeJSON(w, status, errorResponse(status, msg))
			return
		}
		serverError(w, "GetAllVersionsBySubject", "failed to list schema versions", err)
		return
	}
	out := make([]generated.SchemaSubject, 0, len(svs))
	for _, sv := range svs {
		out = append(out, schemaVersionToGenerated(sv))
	}
	writeJSON(w, http.StatusOK, out)
}

// CreateNewSchema serves POST /api/clusters/{clusterName}/schemas: registers
// the request body's schema under its subject, then returns the resulting
// SchemaSubject (SchemaService.Register reads back the now-latest version).
// A genuine cluster-scoped write — readOnlyGuard 403's it on a read-only
// cluster before it ever reaches here.
func (s *apiServer) CreateNewSchema(w http.ResponseWriter, r *http.Request, clusterName string) {
	var body generated.NewSchemaSubject
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse(http.StatusBadRequest, "invalid request body"))
		return
	}
	sv, err := s.deps.Schemas.Register(r.Context(), clusterName, body.Subject, newSchemaFrom(body))
	if err != nil {
		if status, msg, ok := schemaErrStatus(err); ok {
			writeJSON(w, status, errorResponse(status, msg))
			return
		}
		serverError(w, "CreateNewSchema", "failed to create schema", err)
		return
	}
	writeJSON(w, http.StatusOK, schemaVersionToGenerated(sv))
}

// DeleteSchema serves DELETE /api/clusters/{clusterName}/schemas/{subject}:
// soft-deletes every version of subject. 204 on success, no body. Genuine
// write → readOnlyGuard 403's it on a read-only cluster.
func (s *apiServer) DeleteSchema(w http.ResponseWriter, r *http.Request, clusterName, subject string) {
	if _, err := s.deps.Schemas.DeleteSubject(r.Context(), clusterName, subject); err != nil {
		if status, msg, ok := schemaErrStatus(err); ok {
			writeJSON(w, status, errorResponse(status, msg))
			return
		}
		serverError(w, "DeleteSchema", "failed to delete schema", err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// DeleteLatestSchema serves DELETE
// /api/clusters/{clusterName}/schemas/{subject}/latest: soft-deletes
// subject's latest version (DeleteVersion with the "latest" sentinel). 204 on
// success. Genuine write → readOnlyGuard 403's it on a read-only cluster.
func (s *apiServer) DeleteLatestSchema(w http.ResponseWriter, r *http.Request, clusterName, subject string) {
	if _, err := s.deps.Schemas.DeleteVersion(r.Context(), clusterName, subject, "latest"); err != nil {
		if status, msg, ok := schemaErrStatus(err); ok {
			writeJSON(w, status, errorResponse(status, msg))
			return
		}
		serverError(w, "DeleteLatestSchema", "failed to delete latest schema", err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// DeleteSchemaByVersion serves DELETE
// /api/clusters/{clusterName}/schemas/{subject}/versions/{version}:
// soft-deletes one concrete version of subject. 204 on success. Genuine write
// → readOnlyGuard 403's it on a read-only cluster.
func (s *apiServer) DeleteSchemaByVersion(w http.ResponseWriter, r *http.Request, clusterName, subject string, version int32) {
	if _, err := s.deps.Schemas.DeleteVersion(r.Context(), clusterName, subject, strconv.FormatInt(int64(version), 10)); err != nil {
		if status, msg, ok := schemaErrStatus(err); ok {
			writeJSON(w, status, errorResponse(status, msg))
			return
		}
		serverError(w, "DeleteSchemaByVersion", "failed to delete schema version", err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// GetGlobalSchemaCompatibilityLevel serves GET
// /api/clusters/{clusterName}/schemas/compatibility: the registry-wide default
// compatibility level.
func (s *apiServer) GetGlobalSchemaCompatibilityLevel(w http.ResponseWriter, r *http.Request, clusterName string) {
	level, err := s.deps.Schemas.GlobalCompat(r.Context(), clusterName)
	if err != nil {
		if status, msg, ok := schemaErrStatus(err); ok {
			writeJSON(w, status, errorResponse(status, msg))
			return
		}
		serverError(w, "GetGlobalSchemaCompatibilityLevel", "failed to get global compatibility level", err)
		return
	}
	writeJSON(w, http.StatusOK, generated.CompatibilityLevel{
		Compatibility: generated.CompatibilityLevelCompatibility(level),
	})
}

// UpdateGlobalSchemaCompatibilityLevel serves PUT
// /api/clusters/{clusterName}/schemas/compatibility: writes the registry-wide
// default compatibility level. 204 on success. Genuine write → readOnlyGuard
// 403's it on a read-only cluster.
func (s *apiServer) UpdateGlobalSchemaCompatibilityLevel(w http.ResponseWriter, r *http.Request, clusterName string) {
	var body generated.CompatibilityLevel
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse(http.StatusBadRequest, "invalid request body"))
		return
	}
	if err := s.deps.Schemas.SetGlobalCompat(r.Context(), clusterName, string(body.Compatibility)); err != nil {
		if status, msg, ok := schemaErrStatus(err); ok {
			writeJSON(w, status, errorResponse(status, msg))
			return
		}
		serverError(w, "UpdateGlobalSchemaCompatibilityLevel", "failed to set global compatibility level", err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// UpdateSchemaCompatibilityLevel serves PUT
// /api/clusters/{clusterName}/schemas/{subject}/compatibility: writes
// subject's own compatibility-level override. 204 on success. Genuine write →
// readOnlyGuard 403's it on a read-only cluster.
func (s *apiServer) UpdateSchemaCompatibilityLevel(w http.ResponseWriter, r *http.Request, clusterName, subject string) {
	var body generated.CompatibilityLevel
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse(http.StatusBadRequest, "invalid request body"))
		return
	}
	if err := s.deps.Schemas.SetSubjectCompat(r.Context(), clusterName, subject, string(body.Compatibility)); err != nil {
		if status, msg, ok := schemaErrStatus(err); ok {
			writeJSON(w, status, errorResponse(status, msg))
			return
		}
		serverError(w, "UpdateSchemaCompatibilityLevel", "failed to set subject compatibility level", err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// CheckSchemaCompatibility serves POST
// /api/clusters/{clusterName}/schemas/{subject}/check: a dry-run check of
// whether the request body's schema would be compatible with subject's latest
// version. Read-only-in-effect (registers nothing) — whitelisted past
// readOnlyGuard (middleware.go's readOnlyWhitelistPatterns) so a read-only
// cluster can still run it.
func (s *apiServer) CheckSchemaCompatibility(w http.ResponseWriter, r *http.Request, clusterName, subject string) {
	var body generated.NewSchemaSubject
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse(http.StatusBadRequest, "invalid request body"))
		return
	}
	ok, err := s.deps.Schemas.CheckCompat(r.Context(), clusterName, subject, newSchemaFrom(body))
	if err != nil {
		if status, msg, matched := schemaErrStatus(err); matched {
			writeJSON(w, status, errorResponse(status, msg))
			return
		}
		serverError(w, "CheckSchemaCompatibility", "failed to check schema compatibility", err)
		return
	}
	writeJSON(w, http.StatusOK, generated.CompatibilityCheckResponse{IsCompatible: ok})
}

// newSchemaFrom maps createNewSchema's decoded request body onto a domain
// NewSchema (schema text + type + optional references). The body's Subject is
// consumed by the caller (it's the registration target, not part of the
// schema payload), so it isn't copied here.
func newSchemaFrom(body generated.NewSchemaSubject) cluster.NewSchema {
	ns := cluster.NewSchema{Schema: body.Schema, SchemaType: string(body.SchemaType)}
	if body.References != nil {
		refs := make([]cluster.SchemaReference, len(*body.References))
		for i, ref := range *body.References {
			refs[i] = cluster.SchemaReference{Name: ref.Name, Subject: ref.Subject, Version: int(ref.Version)}
		}
		ns.References = refs
	}
	return ns
}

// schemaListQueryFrom translates getSchemas' generated query params into the
// app-layer SchemaListQuery. OrderBy/Fts are intentionally dropped: see
// SchemaListQuery's doc comment (only subject-name ordering + a plain
// substring Search are supported).
func schemaListQueryFrom(p generated.GetSchemasParams) appcluster.SchemaListQuery {
	q := appcluster.SchemaListQuery{}
	if p.Page != nil {
		q.Page = int(*p.Page)
	}
	if p.PerPage != nil {
		q.PerPage = int(*p.PerPage)
	}
	if p.Search != nil {
		q.Search = *p.Search
	}
	if p.SortOrder != nil {
		q.SortOrder = string(*p.SortOrder)
	}
	return q
}

// schemaVersionToGenerated maps a domain SchemaVersion to the contract's
// SchemaSubject wire shape. Version is emitted as a decimal string (the
// contract types SchemaSubject.version as a string, not an integer);
// References is omitted (nil pointer) when the schema declares none.
func schemaVersionToGenerated(sv cluster.SchemaVersion) generated.SchemaSubject {
	out := generated.SchemaSubject{
		CompatibilityLevel: sv.CompatLevel,
		Id:                 int32(sv.ID),
		Schema:             sv.Schema,
		SchemaType:         generated.SchemaType(sv.SchemaType),
		Subject:            sv.Subject,
		Version:            strconv.Itoa(sv.Version),
	}
	if len(sv.References) > 0 {
		refs := make([]generated.SchemaReference, len(sv.References))
		for i, r := range sv.References {
			refs[i] = generated.SchemaReference{Name: r.Name, Subject: r.Subject, Version: int32(r.Version)}
		}
		out.References = &refs
	}
	return out
}
