package cluster

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
)

var ErrBadQuotaRequest = errors.New("invalid quota request")

// QuotaService performs client-quota read/write by resolving a cluster name to
// its Definition and delegating to QuotaPort. The full-replace diff (which keys
// to Set vs Remove) lives in the infra implementation (P2c-D5); QuotaService is
// a thin resolve-and-forward layer, same shape as SchemaService.
type QuotaService struct {
	res  *Resolver
	port cluster.QuotaPort
}

func NewQuotaService(res *Resolver, port cluster.QuotaPort) *QuotaService {
	return &QuotaService{res: res, port: port}
}

func (s *QuotaService) ListQuotas(ctx context.Context, name string) ([]cluster.ClientQuota, error) {
	def, err := s.res.Lookup(name)
	if err != nil {
		return nil, err
	}
	return s.port.ListQuotas(ctx, def)
}

func (s *QuotaService) UpsertQuotas(ctx context.Context, name string, quota cluster.ClientQuota) error {
	if err := validateQuotaEntity(quota); err != nil {
		return err
	}
	def, err := s.res.Lookup(name)
	if err != nil {
		return err
	}
	return s.port.UpsertQuotas(ctx, def, quota)
}

func validateQuotaEntity(quota cluster.ClientQuota) error {
	dimensions := []string{quota.User, quota.ClientID, quota.IP}
	present := false
	for _, dimension := range dimensions {
		if dimension == "" {
			continue
		}
		present = true
		if strings.TrimSpace(dimension) == "" {
			return fmt.Errorf("%w: entity dimensions must not be blank", ErrBadQuotaRequest)
		}
	}
	if !present {
		return fmt.Errorf("%w: at least one entity dimension is required", ErrBadQuotaRequest)
	}
	return nil
}
