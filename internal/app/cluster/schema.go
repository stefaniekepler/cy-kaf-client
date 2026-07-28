package cluster

import (
	"context"
	"errors"
	"sort"
	"strconv"
	"strings"

	"github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
)

const (
	defaultSchemaPage                         = 1
	defaultSchemaPerPage                      = 25
	maxSchemaRegistrationMaterializationCalls = 500 // includes the mandatory "latest" read
	maxSchemaRegistrationVersionEntries       = 500
)

// ErrSchemaRegistryNotConfigured reports that a known cluster has no Schema
// Registry integration configured. It is distinct from ErrUnknownCluster and
// is detected before any SchemaRegistryPort call, so an empty URL can never
// become a malformed outbound request.
var ErrSchemaRegistryNotConfigured = errors.New("schema registry not configured")

// ErrSchemaRegistrationChanged reports that the ID returned by Register could
// not be materialized from the subject's bounded version history. Callers must
// not return an unrelated latest or raw-by-ID schema as this mutation's result.
var ErrSchemaRegistrationChanged = errors.New("schema registration changed before read-back")

// SchemaListQuery is getSchemas' query params (page/perPage/search/sortOrder),
// same "already contract-enum-shaped, api handler casts straight across"
// convention as GroupPageQuery. Page/PerPage default to 1/25 when zero.
//
// The contract also carries orderBy (SchemaColumnsToSort: SUBJECT/ID/TYPE/
// VERSION/COMPATIBILITY) and fts, neither of which has a field here: the
// getSchemas list is built by paginating subject *names* (cheap) and fetching
// each page slice's latest schema only, so ID/TYPE/VERSION/COMPATIBILITY
// orderings — which would need every subject's full schema pulled up front,
// defeating that optimization — degrade to subject-name ordering (SortOrder
// direction still honored). This matches upstream kafka-ui's own getSchemas,
// whose subject list is likewise ordered by name, and the general "no
// separate full-text engine" stance (parity matrix exemption #7/#12) covers
// the absent fts mode: Search is always a plain case-insensitive substring
// match on the subject name.
type SchemaListQuery struct {
	Page, PerPage int
	Search        string
	SortOrder     string
}

// SchemaPage is one filtered/sorted/paginated page of a cluster's subjects,
// each resolved to its latest registered schema version.
type SchemaPage struct {
	Schemas   []cluster.SchemaVersion
	PageCount int
}

// SchemaService performs Schema-Registry-scoped operations (P2a's 12-endpoint
// surface) by resolving a cluster name to its Definition and delegating to the
// cluster's SchemaRegistryPort — same "resolve name -> Definition, delegate"
// shape as GroupService, with no StateCache dependency (nothing here reads the
// periodically-refreshed kafka state cache; every call is a live Schema
// Registry request).
type SchemaService struct {
	res *Resolver
	sr  cluster.SchemaRegistryPort
}

func NewSchemaService(res *Resolver, sr cluster.SchemaRegistryPort) *SchemaService {
	return &SchemaService{res: res, sr: sr}
}

// resolve finds name and verifies that its optional Schema Registry integration
// is configured before a caller can reach the port.
func (s *SchemaService) resolve(name string) (cluster.Definition, error) {
	def, err := s.res.Lookup(name)
	if err != nil {
		return cluster.Definition{}, err
	}
	if strings.TrimSpace(def.SchemaRegistry.URL) == "" {
		return cluster.Definition{}, ErrSchemaRegistryNotConfigured
	}
	return def, nil
}

// ListSchemas lists every subject, filters by q.Search (case-insensitive
// substring on the subject name), sorts by name (q.SortOrder direction),
// paginates, then fetches the latest schema for only the resulting page slice
// — bounding SchemaByVersion round-trips to at most perPage regardless of how
// many subjects the registry holds. PageCount for an empty (post-filter)
// result is 0, not 1 (same convention as filterSortPageGroups).
func (s *SchemaService) ListSchemas(ctx context.Context, name string, q SchemaListQuery) (SchemaPage, error) {
	def, err := s.resolve(name)
	if err != nil {
		return SchemaPage{}, err
	}
	subjects, err := s.sr.Subjects(ctx, def)
	if err != nil {
		return SchemaPage{}, err
	}

	search := strings.ToLower(q.Search)
	filtered := make([]string, 0, len(subjects))
	for _, subj := range subjects {
		if search != "" && !strings.Contains(strings.ToLower(subj), search) {
			continue
		}
		filtered = append(filtered, subj)
	}
	sort.SliceStable(filtered, func(i, j int) bool {
		if q.SortOrder == "DESC" {
			return filtered[i] > filtered[j]
		}
		return filtered[i] < filtered[j]
	})

	perPage := q.PerPage
	if perPage <= 0 {
		perPage = defaultSchemaPerPage
	}
	page := q.Page
	if page <= 0 {
		page = defaultSchemaPage
	}
	total := len(filtered)
	pageCount := 0
	if total > 0 {
		pageCount = (total + perPage - 1) / perPage
		if page > pageCount {
			page = pageCount
		}
	}
	start := (page - 1) * perPage
	if start < 0 || start > total {
		start = total
	}
	end := start + perPage
	if end > total {
		end = total
	}

	slice := filtered[start:end]
	out := make([]cluster.SchemaVersion, 0, len(slice))
	for _, subj := range slice {
		sv, err := s.sr.SchemaByVersion(ctx, def, subj, "latest")
		if err != nil {
			return SchemaPage{}, err
		}
		out = append(out, sv)
	}
	return SchemaPage{Schemas: out, PageCount: pageCount}, nil
}

// LatestSchema resolves name then fetches subject's latest registered schema
// version (SchemaByVersion with the "latest" sentinel).
func (s *SchemaService) LatestSchema(ctx context.Context, name, subject string) (cluster.SchemaVersion, error) {
	def, err := s.resolve(name)
	if err != nil {
		return cluster.SchemaVersion{}, err
	}
	return s.sr.SchemaByVersion(ctx, def, subject, "latest")
}

// SchemaByVersion resolves name then fetches subject's schema at version (a
// numeric string, or "latest").
func (s *SchemaService) SchemaByVersion(ctx context.Context, name, subject, version string) (cluster.SchemaVersion, error) {
	def, err := s.resolve(name)
	if err != nil {
		return cluster.SchemaVersion{}, err
	}
	return s.sr.SchemaByVersion(ctx, def, subject, version)
}

// Versions is intentionally not exposed at the app layer — no endpoint needs
// bare version numbers; AllVersions (above) is the version-list surface.

// GlobalCompat resolves name then reads the registry-wide default
// compatibility level (SchemaRegistryPort.GlobalCompat).
func (s *SchemaService) GlobalCompat(ctx context.Context, name string) (string, error) {
	def, err := s.resolve(name)
	if err != nil {
		return "", err
	}
	return s.sr.GlobalCompat(ctx, def)
}

// SetGlobalCompat resolves name then writes the registry-wide default
// compatibility level (SchemaRegistryPort.SetGlobalCompat).
func (s *SchemaService) SetGlobalCompat(ctx context.Context, name, level string) error {
	def, err := s.resolve(name)
	if err != nil {
		return err
	}
	return s.sr.SetGlobalCompat(ctx, def, level)
}

// SetSubjectCompat resolves name then writes subject's own compatibility-level
// override (SchemaRegistryPort.SetSubjectCompat).
func (s *SchemaService) SetSubjectCompat(ctx context.Context, name, subject, level string) error {
	def, err := s.resolve(name)
	if err != nil {
		return err
	}
	return s.sr.SetSubjectCompat(ctx, def, subject, level)
}

// CheckCompat resolves name then reports whether ns would be compatible with
// subject's latest registered version under subject's effective rule — a dry
// run (SchemaRegistryPort.CheckCompat; never registers ns). checkCompatibility
// is a read-only-in-effect POST: it's whitelisted past readOnlyGuard (see
// middleware.go's readOnlyWhitelistPatterns) so a read-only cluster can still
// run compatibility checks.
func (s *SchemaService) CheckCompat(ctx context.Context, name, subject string, ns cluster.NewSchema) (bool, error) {
	def, err := s.resolve(name)
	if err != nil {
		return false, err
	}
	return s.sr.CheckCompat(ctx, def, subject, ns)
}

// (SchemaRegistryPort.Register), then materializes that exact registry-global
// ID as a full subject version — createNewSchema's contract 200 is a complete
// SchemaSubject, not a bare ID. The common case reads latest once. If a
// concurrent writer advanced latest, or registry-native idempotency returned
// an older ID, the bounded fallback searches concrete subject versions from
// highest to lowest.
func (s *SchemaService) Register(ctx context.Context, name, subject string, ns cluster.NewSchema) (cluster.SchemaVersion, error) {
	def, err := s.resolve(name)
	if err != nil {
		return cluster.SchemaVersion{}, err
	}
	registeredID, err := s.sr.Register(ctx, def, subject, ns)
	if err != nil {
		return cluster.SchemaVersion{}, err
	}
	latest, err := s.sr.SchemaByVersion(ctx, def, subject, "latest")
	if err != nil {
		return cluster.SchemaVersion{}, err
	}
	if latest.ID == registeredID {
		return latest, nil
	}

	versions, err := s.sr.Versions(ctx, def, subject)
	if err != nil {
		return cluster.SchemaVersion{}, err
	}
	if len(versions) > maxSchemaRegistrationVersionEntries {
		return cluster.SchemaVersion{}, ErrSchemaRegistrationChanged
	}
	ordered := append([]int(nil), versions...)
	sort.Sort(sort.Reverse(sort.IntSlice(ordered)))

	seen := make(map[int]struct{}, min(len(ordered), maxSchemaRegistrationVersionEntries))
	materializationCalls := 1
	for processedEntries, version := range ordered {
		if processedEntries >= maxSchemaRegistrationVersionEntries {
			break
		}
		if version <= 0 || version == latest.Version {
			continue
		}
		if _, duplicate := seen[version]; duplicate {
			continue
		}
		seen[version] = struct{}{}
		if materializationCalls >= maxSchemaRegistrationMaterializationCalls {
			break
		}
		materializationCalls++

		candidate, err := s.sr.SchemaByVersion(ctx, def, subject, strconv.Itoa(version))
		if err != nil {
			return cluster.SchemaVersion{}, err
		}
		if candidate.ID == registeredID {
			return candidate, nil
		}
	}
	return cluster.SchemaVersion{}, ErrSchemaRegistrationChanged
}

// DeleteSubject resolves name then soft-deletes every version of subject
// (SchemaRegistryPort.DeleteSubject, permanent=false), returning the deleted
// version numbers. The deleteSchema endpoint carries no hard/permanent param,
// so this is always a soft delete (matching upstream's UI delete semantics).
func (s *SchemaService) DeleteSubject(ctx context.Context, name, subject string) ([]int, error) {
	def, err := s.resolve(name)
	if err != nil {
		return nil, err
	}
	return s.sr.DeleteSubject(ctx, def, subject, false)
}

// DeleteVersion resolves name then soft-deletes one version (a numeric
// string, or "latest" — forwarded verbatim, the port resolves the sentinel)
// of subject, returning the concrete version number that was deleted.
// deleteLatestSchema is this with version="latest"; deleteSchemaByVersion is
// this with the concrete version. Neither endpoint carries a hard/permanent
// param, so this is always a soft delete.
func (s *SchemaService) DeleteVersion(ctx context.Context, name, subject, version string) (int, error) {
	def, err := s.resolve(name)
	if err != nil {
		return 0, err
	}
	return s.sr.DeleteVersion(ctx, def, subject, version, false)
}

// (SchemaRegistryPort.Versions), then materializes each into a full
// SchemaVersion (SchemaByVersion per number) — getAllVersionsBySubject's
// contract 200 is an array of complete SchemaSubject objects, not bare
// version numbers, so every version is fetched in full and returned in the
// order Versions reported them.
func (s *SchemaService) AllVersions(ctx context.Context, name, subject string) ([]cluster.SchemaVersion, error) {
	def, err := s.resolve(name)
	if err != nil {
		return nil, err
	}
	versions, err := s.sr.Versions(ctx, def, subject)
	if err != nil {
		return nil, err
	}
	out := make([]cluster.SchemaVersion, 0, len(versions))
	for _, v := range versions {
		sv, err := s.sr.SchemaByVersion(ctx, def, subject, strconv.Itoa(v))
		if err != nil {
			return nil, err
		}
		out = append(out, sv)
	}
	return out, nil
}
