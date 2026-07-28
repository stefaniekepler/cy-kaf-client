package mcpserver

import (
	"context"
	"math"
	"sort"
	"strconv"
	"strings"

	"github.com/cy-kaf/cy-kaf-client/internal/api/generated"
	appcluster "github.com/cy-kaf/cy-kaf-client/internal/app/cluster"
	domaincluster "github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
)

const (
	defaultMCPSchemaPage    = 1
	defaultMCPSchemaPerPage = 25
	maxMCPSchemaPerPage     = 100
	maxMCPSchemaTextBytes   = 256 << 10
	maxMCPSchemaNameBytes   = 1024
	maxMCPSchemaReferences  = 100
)

type schemaDeleteVersionResult struct {
	Status         string `json:"status"`
	DeletedVersion int    `json:"deletedVersion"`
}

type schemaDeleteSubjectResult struct {
	Status          string `json:"status"`
	DeletedVersions []int  `json:"deletedVersions"`
}

type schemaCompatibilityUpdateResult struct {
	Status        string `json:"status"`
	Compatibility string `json:"compatibility"`
}

func schemaTool(meta ToolMeta) ToolSpec {
	switch meta.Name {
	case "checkSchemaCompatibility":
		return schemaReadTool(meta, schemaSubjectBodyInputSelector, checkSchemaCompatibility)
	case "getAllVersionsBySubject":
		return schemaReadTool(
			meta,
			schemaSubjectInputSelector,
			func(ctx context.Context, executor *Executor, input schemaSubjectInput) (any, error) {
				return getAllVersionsBySubject(ctx, executor, input, meta.MaxItems)
			},
		)
	case "getGlobalSchemaCompatibilityLevel":
		return schemaReadTool(meta, clusterInputSelector, getGlobalSchemaCompatibilityLevel)
	case "getLatestSchema":
		return schemaReadTool(meta, schemaSubjectInputSelector, getLatestSchema)
	case "getSchemaByVersion":
		return schemaReadTool(meta, schemaVersionInputSelector, getSchemaByVersion)
	case "getSchemas":
		return schemaReadTool(meta, schemaListInputSelector, getSchemas)
	case "createNewSchema":
		return schemaWriteTool(meta, schemaCreateInputSelector, createNewSchema)
	case "deleteLatestSchema":
		return schemaWriteTool(meta, schemaSubjectInputSelector, deleteLatestSchema)
	case "deleteSchema":
		return schemaWriteTool(
			meta,
			schemaSubjectInputSelector,
			func(ctx context.Context, executor *Executor, input schemaSubjectInput) (any, error) {
				return deleteSchema(ctx, executor, input, meta.MaxItems)
			},
		)
	case "deleteSchemaByVersion":
		return schemaWriteTool(meta, schemaVersionInputSelector, deleteSchemaByVersion)
	case "updateGlobalSchemaCompatibilityLevel":
		return schemaWriteTool(meta, schemaGlobalCompatibilityInputSelector, updateGlobalSchemaCompatibilityLevel)
	case "updateSchemaCompatibilityLevel":
		return schemaWriteTool(meta, schemaSubjectCompatibilityInputSelector, updateSchemaCompatibilityLevel)
	default:
		panic("unsupported Schemas MCP tool: " + meta.Name)
	}
}

func schemaReadTool[In any](
	meta ToolMeta,
	cluster clusterSelector[In],
	call toolCall[In],
) ToolSpec {
	return newTool(
		meta,
		cluster,
		func(context.Context, *Executor, In) (AccessClass, error) {
			return AccessReadOnly, nil
		},
		call,
	)
}

func schemaWriteTool[In any](
	meta ToolMeta,
	cluster clusterSelector[In],
	call toolCall[In],
) ToolSpec {
	return newTool(
		meta,
		cluster,
		func(context.Context, *Executor, In) (AccessClass, error) {
			return AccessWrite, nil
		},
		call,
	)
}

func schemaSubjectInputSelector(input schemaSubjectInput) string {
	return input.ClusterName
}

func schemaVersionInputSelector(input schemaVersionInput) string {
	return input.ClusterName
}

func schemaSubjectBodyInputSelector(input schemaSubjectBodyInput[generated.NewSchemaSubject]) string {
	return input.ClusterName
}

func schemaListInputSelector(input clusterOptionalQueryInput[generated.GetSchemasParams]) string {
	return input.ClusterName
}

func schemaCreateInputSelector(input clusterBodyInput[generated.NewSchemaSubject]) string {
	return input.ClusterName
}

func schemaGlobalCompatibilityInputSelector(input clusterBodyInput[generated.CompatibilityLevel]) string {
	return input.ClusterName
}

func schemaSubjectCompatibilityInputSelector(input schemaSubjectBodyInput[generated.CompatibilityLevel]) string {
	return input.ClusterName
}

func checkSchemaCompatibility(
	ctx context.Context,
	executor *Executor,
	input schemaSubjectBodyInput[generated.NewSchemaSubject],
) (any, error) {
	if err := validateSchemaSubjectInput(schemaSubjectInput{
		ClusterName: input.ClusterName,
		Subject:     input.Subject,
	}); err != nil {
		return nil, err
	}
	schema, err := newSchemaFromGenerated(input.Body)
	if err != nil {
		return nil, err
	}
	if input.Subject != input.Body.Subject {
		return nil, errInvalidRequest
	}
	if executor.deps.Schemas == nil {
		return nil, errOperationFailed
	}
	compatible, err := executor.deps.Schemas.CheckCompat(
		ctx,
		input.ClusterName,
		input.Subject,
		schema,
	)
	if err != nil {
		return nil, err
	}
	return generated.CompatibilityCheckResponse{IsCompatible: compatible}, nil
}

func getAllVersionsBySubject(
	ctx context.Context,
	executor *Executor,
	input schemaSubjectInput,
	maxItems int,
) (any, error) {
	if err := validateSchemaSubjectInput(input); err != nil {
		return nil, err
	}
	if executor.deps.Schemas == nil {
		return nil, errOperationFailed
	}
	versions, err := executor.deps.Schemas.AllVersions(ctx, input.ClusterName, input.Subject)
	if err != nil {
		return nil, err
	}
	versions = append([]domaincluster.SchemaVersion(nil), versions...)
	sort.SliceStable(versions, func(left, right int) bool {
		if versions[left].Version == versions[right].Version {
			return versions[left].ID < versions[right].ID
		}
		return versions[left].Version < versions[right].Version
	})
	if maxItems > 0 && len(versions) > maxItems {
		versions = versions[:maxItems]
	}
	output := make([]generated.SchemaSubject, 0, len(versions))
	for _, version := range versions {
		converted, err := schemaVersionToContract(version)
		if err != nil {
			return nil, err
		}
		output = append(output, converted)
	}
	return output, nil
}

func getGlobalSchemaCompatibilityLevel(
	ctx context.Context,
	executor *Executor,
	input clusterInput,
) (any, error) {
	if err := validateClusterName(input.ClusterName); err != nil {
		return nil, err
	}
	if executor.deps.Schemas == nil {
		return nil, errOperationFailed
	}
	level, err := executor.deps.Schemas.GlobalCompat(ctx, input.ClusterName)
	if err != nil {
		return nil, err
	}
	compatibility := generated.CompatibilityLevelCompatibility(level)
	if !compatibility.Valid() {
		return nil, errOperationFailed
	}
	return generated.CompatibilityLevel{Compatibility: compatibility}, nil
}

func getLatestSchema(
	ctx context.Context,
	executor *Executor,
	input schemaSubjectInput,
) (any, error) {
	if err := validateSchemaSubjectInput(input); err != nil {
		return nil, err
	}
	if executor.deps.Schemas == nil {
		return nil, errOperationFailed
	}
	version, err := executor.deps.Schemas.LatestSchema(ctx, input.ClusterName, input.Subject)
	if err != nil {
		return nil, err
	}
	return schemaVersionToContract(version)
}

func getSchemaByVersion(
	ctx context.Context,
	executor *Executor,
	input schemaVersionInput,
) (any, error) {
	version, err := validatedSchemaVersionInput(input)
	if err != nil {
		return nil, err
	}
	if executor.deps.Schemas == nil {
		return nil, errOperationFailed
	}
	schema, err := executor.deps.Schemas.SchemaByVersion(
		ctx,
		input.ClusterName,
		input.Subject,
		version,
	)
	if err != nil {
		return nil, err
	}
	return schemaVersionToContract(schema)
}

func getSchemas(
	ctx context.Context,
	executor *Executor,
	input clusterOptionalQueryInput[generated.GetSchemasParams],
) (any, error) {
	if err := validateClusterName(input.ClusterName); err != nil {
		return nil, err
	}
	query, err := schemaListQuery(input.Query)
	if err != nil {
		return nil, err
	}
	if executor.deps.Schemas == nil {
		return nil, errOperationFailed
	}
	page, err := executor.deps.Schemas.ListSchemas(ctx, input.ClusterName, query)
	if err != nil {
		return nil, err
	}
	if page.PageCount < 0 {
		return nil, errOperationFailed
	}
	if int64(page.PageCount) > math.MaxInt32 {
		return nil, errResultTooLarge
	}
	schemas := append([]domaincluster.SchemaVersion(nil), page.Schemas...)
	sort.SliceStable(schemas, func(left, right int) bool {
		if query.SortOrder == string(generated.DESC) {
			return schemas[left].Subject > schemas[right].Subject
		}
		return schemas[left].Subject < schemas[right].Subject
	})
	if len(schemas) > query.PerPage {
		schemas = schemas[:query.PerPage]
	}
	output := make([]generated.SchemaSubject, 0, len(schemas))
	for _, schema := range schemas {
		converted, err := schemaVersionToContract(schema)
		if err != nil {
			return nil, err
		}
		output = append(output, converted)
	}
	pageCount := int32(page.PageCount)
	return generated.SchemaSubjectsResponse{PageCount: &pageCount, Schemas: &output}, nil
}

func createNewSchema(
	ctx context.Context,
	executor *Executor,
	input clusterBodyInput[generated.NewSchemaSubject],
) (any, error) {
	if err := validateClusterName(input.ClusterName); err != nil {
		return nil, err
	}
	schema, err := newSchemaFromGenerated(input.Body)
	if err != nil {
		return nil, err
	}
	if executor.deps.Schemas == nil {
		return nil, errOperationFailed
	}
	version, err := executor.deps.Schemas.Register(
		ctx,
		input.ClusterName,
		input.Body.Subject,
		schema,
	)
	if err != nil {
		return nil, err
	}
	return schemaVersionToContract(version)
}

func deleteLatestSchema(
	ctx context.Context,
	executor *Executor,
	input schemaSubjectInput,
) (any, error) {
	if err := validateSchemaSubjectInput(input); err != nil {
		return nil, err
	}
	if executor.deps.Schemas == nil {
		return nil, errOperationFailed
	}
	version, err := executor.deps.Schemas.DeleteVersion(
		ctx,
		input.ClusterName,
		input.Subject,
		"latest",
	)
	if err != nil {
		return nil, err
	}
	if !validSchemaVersionNumber(version) {
		return nil, errOperationFailed
	}
	return schemaDeleteVersionResult{Status: "deleted", DeletedVersion: version}, nil
}

func deleteSchema(
	ctx context.Context,
	executor *Executor,
	input schemaSubjectInput,
	maxItems int,
) (any, error) {
	if err := validateSchemaSubjectInput(input); err != nil {
		return nil, err
	}
	if executor.deps.Schemas == nil {
		return nil, errOperationFailed
	}
	versions, err := executor.deps.Schemas.DeleteSubject(ctx, input.ClusterName, input.Subject)
	if err != nil {
		return nil, err
	}
	versions = append([]int{}, versions...)
	for _, version := range versions {
		if !validSchemaVersionNumber(version) {
			return nil, errOperationFailed
		}
	}
	sort.Ints(versions)
	if maxItems > 0 && len(versions) > maxItems {
		versions = versions[:maxItems]
	}
	return schemaDeleteSubjectResult{
		Status: "deleted", DeletedVersions: versions,
	}, nil
}

func deleteSchemaByVersion(
	ctx context.Context,
	executor *Executor,
	input schemaVersionInput,
) (any, error) {
	version, err := validatedSchemaVersionInput(input)
	if err != nil {
		return nil, err
	}
	if executor.deps.Schemas == nil {
		return nil, errOperationFailed
	}
	deleted, err := executor.deps.Schemas.DeleteVersion(
		ctx,
		input.ClusterName,
		input.Subject,
		version,
	)
	if err != nil {
		return nil, err
	}
	if !validSchemaVersionNumber(deleted) {
		return nil, errOperationFailed
	}
	return schemaDeleteVersionResult{Status: "deleted", DeletedVersion: deleted}, nil
}

func updateGlobalSchemaCompatibilityLevel(
	ctx context.Context,
	executor *Executor,
	input clusterBodyInput[generated.CompatibilityLevel],
) (any, error) {
	if err := validateClusterName(input.ClusterName); err != nil {
		return nil, err
	}
	level, err := validatedCompatibility(input.Body)
	if err != nil {
		return nil, err
	}
	if executor.deps.Schemas == nil {
		return nil, errOperationFailed
	}
	if err := executor.deps.Schemas.SetGlobalCompat(ctx, input.ClusterName, level); err != nil {
		return nil, err
	}
	return schemaCompatibilityUpdateResult{Status: "updated", Compatibility: level}, nil
}

func updateSchemaCompatibilityLevel(
	ctx context.Context,
	executor *Executor,
	input schemaSubjectBodyInput[generated.CompatibilityLevel],
) (any, error) {
	if err := validateSchemaSubjectInput(schemaSubjectInput{
		ClusterName: input.ClusterName,
		Subject:     input.Subject,
	}); err != nil {
		return nil, err
	}
	level, err := validatedCompatibility(input.Body)
	if err != nil {
		return nil, err
	}
	if executor.deps.Schemas == nil {
		return nil, errOperationFailed
	}
	if err := executor.deps.Schemas.SetSubjectCompat(
		ctx,
		input.ClusterName,
		input.Subject,
		level,
	); err != nil {
		return nil, err
	}
	return schemaCompatibilityUpdateResult{Status: "updated", Compatibility: level}, nil
}

func schemaListQuery(params *generated.GetSchemasParams) (appcluster.SchemaListQuery, error) {
	query := appcluster.SchemaListQuery{
		Page: defaultMCPSchemaPage, PerPage: defaultMCPSchemaPerPage,
	}
	if params == nil {
		return query, nil
	}
	if params.Page != nil {
		if *params.Page < 1 {
			return appcluster.SchemaListQuery{}, errInvalidRequest
		}
		query.Page = int(*params.Page)
	}
	if params.PerPage != nil {
		if *params.PerPage < 1 || *params.PerPage > maxMCPSchemaPerPage {
			return appcluster.SchemaListQuery{}, errInvalidRequest
		}
		query.PerPage = int(*params.PerPage)
	}
	if params.Search != nil {
		if len(*params.Search) > maxMCPSchemaNameBytes {
			return appcluster.SchemaListQuery{}, errInvalidRequest
		}
		query.Search = *params.Search
	}
	if params.OrderBy != nil && !params.OrderBy.Valid() {
		return appcluster.SchemaListQuery{}, errInvalidRequest
	}
	if params.SortOrder != nil {
		if !params.SortOrder.Valid() {
			return appcluster.SchemaListQuery{}, errInvalidRequest
		}
		query.SortOrder = string(*params.SortOrder)
	}
	// SchemaListQuery has no full-text-search mode. An explicit false is the
	// same plain substring search as omission; true must not silently degrade.
	if params.Fts != nil && *params.Fts {
		return appcluster.SchemaListQuery{}, errInvalidRequest
	}
	return query, nil
}

func newSchemaFromGenerated(body generated.NewSchemaSubject) (domaincluster.NewSchema, error) {
	if err := validateSchemaSubject(body.Subject); err != nil {
		return domaincluster.NewSchema{}, err
	}
	if strings.TrimSpace(body.Schema) == "" || len(body.Schema) > maxMCPSchemaTextBytes {
		return domaincluster.NewSchema{}, errInvalidRequest
	}
	if !body.SchemaType.Valid() {
		return domaincluster.NewSchema{}, errInvalidRequest
	}
	output := domaincluster.NewSchema{
		Schema: body.Schema, SchemaType: string(body.SchemaType),
	}
	if body.References == nil {
		return output, nil
	}
	if len(*body.References) > maxMCPSchemaReferences {
		return domaincluster.NewSchema{}, errInvalidRequest
	}
	output.References = make([]domaincluster.SchemaReference, 0, len(*body.References))
	for _, reference := range *body.References {
		if err := validateSchemaReference(
			reference.Name,
			reference.Subject,
			int(reference.Version),
		); err != nil {
			return domaincluster.NewSchema{}, err
		}
		output.References = append(output.References, domaincluster.SchemaReference{
			Name: reference.Name, Subject: reference.Subject, Version: int(reference.Version),
		})
	}
	return output, nil
}

func schemaVersionToContract(version domaincluster.SchemaVersion) (generated.SchemaSubject, error) {
	if !validSchemaVersionNumber(version.ID) ||
		!validSchemaVersionNumber(version.Version) ||
		validateSchemaSubject(version.Subject) != nil ||
		strings.TrimSpace(version.Schema) == "" {
		return generated.SchemaSubject{}, errOperationFailed
	}
	if len(version.Schema) > maxMCPSchemaTextBytes ||
		len(version.References) > maxMCPSchemaReferences {
		return generated.SchemaSubject{}, errResultTooLarge
	}
	schemaType := generated.SchemaType(version.SchemaType)
	compatibility := generated.CompatibilityLevelCompatibility(version.CompatLevel)
	if !schemaType.Valid() || !compatibility.Valid() {
		return generated.SchemaSubject{}, errOperationFailed
	}

	var references *[]generated.SchemaReference
	if version.References != nil {
		sorted := append([]domaincluster.SchemaReference(nil), version.References...)
		sort.SliceStable(sorted, func(left, right int) bool {
			if sorted[left].Name != sorted[right].Name {
				return sorted[left].Name < sorted[right].Name
			}
			if sorted[left].Subject != sorted[right].Subject {
				return sorted[left].Subject < sorted[right].Subject
			}
			return sorted[left].Version < sorted[right].Version
		})
		converted := make([]generated.SchemaReference, 0, len(sorted))
		for _, reference := range sorted {
			if err := validateSchemaReference(
				reference.Name,
				reference.Subject,
				reference.Version,
			); err != nil {
				return generated.SchemaSubject{}, errOperationFailed
			}
			converted = append(converted, generated.SchemaReference{
				Name: reference.Name, Subject: reference.Subject, Version: int32(reference.Version),
			})
		}
		references = &converted
	}

	return generated.SchemaSubject{
		CompatibilityLevel: version.CompatLevel,
		Id:                 int32(version.ID),
		References:         references,
		Schema:             version.Schema,
		SchemaType:         schemaType,
		Subject:            version.Subject,
		Version:            strconv.Itoa(version.Version),
	}, nil
}

func validateSchemaSubjectInput(input schemaSubjectInput) error {
	if err := validateClusterName(input.ClusterName); err != nil {
		return err
	}
	return validateSchemaSubject(input.Subject)
}

func validateSchemaSubject(subject string) error {
	if strings.TrimSpace(subject) == "" || len(subject) > maxMCPSchemaNameBytes {
		return errInvalidRequest
	}
	return nil
}

func validatedSchemaVersionInput(input schemaVersionInput) (string, error) {
	if err := validateSchemaSubjectInput(schemaSubjectInput{
		ClusterName: input.ClusterName,
		Subject:     input.Subject,
	}); err != nil {
		return "", err
	}
	version, err := strconv.ParseInt(input.Version, 10, 32)
	if err != nil || version < 1 {
		return "", errInvalidRequest
	}
	return strconv.FormatInt(version, 10), nil
}

func validateSchemaReference(name, subject string, version int) error {
	if strings.TrimSpace(name) == "" ||
		len(name) > maxMCPSchemaNameBytes ||
		validateSchemaSubject(subject) != nil ||
		!validSchemaVersionNumber(version) {
		return errInvalidRequest
	}
	return nil
}

func validSchemaVersionNumber(version int) bool {
	return version >= 1 && int64(version) <= math.MaxInt32
}

func validatedCompatibility(body generated.CompatibilityLevel) (string, error) {
	if !body.Compatibility.Valid() {
		return "", errInvalidRequest
	}
	return string(body.Compatibility), nil
}
