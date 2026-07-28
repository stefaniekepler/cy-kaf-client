package cluster

import (
	"context"
	"errors"
	"fmt"

	"github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
)

// ErrUnknownConnect is returned by ConnectService's per-Connect methods
// (Plugins, ValidatePlugin, ...) when connectName doesn't name any entry in
// the resolved Definition's Connects list. This is a distinct app-layer
// sentinel, not a re-export of infra/connect.ErrUnknownConnect: api may not
// import internal/infra (api-no-infra depguard rule) and app may only import
// domain+stdlib (app-only-domain depguard rule), so neither layer can match
// against the infra sentinel directly. ConnectService instead pre-validates
// connectName against def.Connects itself, before ever calling the port --
// same "resolve name -> Definition, (validate,) delegate" shape as
// ErrUnknownCluster/Resolver.Lookup, one level deeper (cluster name, then
// connectName within that cluster).
var ErrUnknownConnect = errors.New("unknown connect")

// ConnectService performs Kafka-Connect-scoped operations (P2b's Connect
// surface) by resolving a cluster name to its Definition and delegating to
// the cluster's KafkaConnectPort -- same "resolve name -> Definition,
// delegate" shape as SchemaService, with an added connectName
// pre-validation step (see ErrUnknownConnect) that SchemaService doesn't
// need: Schema Registry is one-per-cluster, but a cluster can configure
// several named Kafka Connect workers (Definition.Connects), so per-Connect
// methods need a second name resolved within the first.
type ConnectService struct {
	res *Resolver
	cn  cluster.KafkaConnectPort
}

func NewConnectService(res *Resolver, cn cluster.KafkaConnectPort) *ConnectService {
	return &ConnectService{res: res, cn: cn}
}

// ListConnects resolves name then lists every Kafka Connect worker
// configured on the cluster. Aggregation is skip-bad (an unreachable Connect
// worker is silently dropped, not surfaced as an error) -- that behavior
// lives entirely in the KafkaConnectPort implementation
// (infra/connect.Pool.Connects, P2b Task 1-2); ListConnects itself does no
// filtering, it only resolves the cluster name and forwards the port's
// result verbatim.
func (s *ConnectService) ListConnects(ctx context.Context, name string) ([]cluster.ConnectCluster, error) {
	def, err := s.res.Lookup(name)
	if err != nil {
		return nil, err
	}
	return s.cn.Connects(ctx, def)
}

// lookupConnect resolves name then validates that connectName names one of
// the resolved Definition's configured Connect workers, returning
// ErrUnknownConnect if not. Every per-Connect method (Plugins,
// ValidatePlugin, ...) routes through this first.
func (s *ConnectService) lookupConnect(name, connectName string) (cluster.Definition, error) {
	def, err := s.res.Lookup(name)
	if err != nil {
		return cluster.Definition{}, err
	}
	for _, c := range def.Connects {
		if c.Name == connectName {
			return def, nil
		}
	}
	return cluster.Definition{}, fmt.Errorf("%w: %q", ErrUnknownConnect, connectName)
}

// Plugins resolves name, validates connectName, then lists connectName's
// available connector plugin classes.
func (s *ConnectService) Plugins(ctx context.Context, name, connectName string) ([]cluster.ConnectorPlugin, error) {
	def, err := s.lookupConnect(name, connectName)
	if err != nil {
		return nil, err
	}
	return s.cn.Plugins(ctx, def, connectName)
}

// ValidatePlugin resolves name, validates connectName, then dry-runs cfg
// against pluginName's config definitions on connectName (never
// creates/alters a connector).
func (s *ConnectService) ValidatePlugin(ctx context.Context, name, connectName, pluginName string, cfg map[string]any) (cluster.PluginValidation, error) {
	def, err := s.lookupConnect(name, connectName)
	if err != nil {
		return cluster.PluginValidation{}, err
	}
	return s.cn.ValidatePlugin(ctx, def, connectName, pluginName, cfg)
}

// AllConnectors resolves name then lists every connector across every
// configured Connect worker on the cluster. Aggregation is skip-bad (an
// unreachable Connect worker is silently dropped, not surfaced as an error)
// -- same "no filtering of its own, port already did it" shape as
// ListConnects; AllConnectors just resolves the cluster name and forwards
// KafkaConnectPort.AllConnectors's result verbatim.
func (s *ConnectService) AllConnectors(ctx context.Context, name string) ([]cluster.ConnectorRef, error) {
	def, err := s.res.Lookup(name)
	if err != nil {
		return nil, err
	}
	return s.cn.AllConnectors(ctx, def)
}

// Connectors resolves name, validates connectName, then lists connectName's
// connector names.
func (s *ConnectService) Connectors(ctx context.Context, name, connectName string) ([]string, error) {
	def, err := s.lookupConnect(name, connectName)
	if err != nil {
		return nil, err
	}
	return s.cn.Connectors(ctx, def, connectName)
}

// Connector resolves name, validates connectName, then fetches
// connectorName's full assembled detail on connectName.
func (s *ConnectService) Connector(ctx context.Context, name, connectName, connectorName string) (cluster.Connector, error) {
	def, err := s.lookupConnect(name, connectName)
	if err != nil {
		return cluster.Connector{}, err
	}
	return s.cn.Connector(ctx, def, connectName, connectorName)
}

// ConnectorConfig resolves name, validates connectName, then fetches
// connectorName's current config on connectName.
func (s *ConnectService) ConnectorConfig(ctx context.Context, name, connectName, connectorName string) (map[string]any, error) {
	def, err := s.lookupConnect(name, connectName)
	if err != nil {
		return nil, err
	}
	return s.cn.ConnectorConfig(ctx, def, connectName, connectorName)
}

// ConnectorTasks resolves name, validates connectName, then fetches
// connectorName's tasks (assembled config + status) on connectName.
func (s *ConnectService) ConnectorTasks(ctx context.Context, name, connectName, connectorName string) ([]cluster.ConnectorTask, error) {
	def, err := s.lookupConnect(name, connectName)
	if err != nil {
		return nil, err
	}
	return s.cn.ConnectorTasks(ctx, def, connectName, connectorName)
}

// CreateConnector resolves name, validates connectName, then creates a new
// connector called connectorName with cfg on connectName, returning its
// assembled detail.
func (s *ConnectService) CreateConnector(ctx context.Context, name, connectName, connectorName string, cfg map[string]any) (cluster.Connector, error) {
	def, err := s.lookupConnect(name, connectName)
	if err != nil {
		return cluster.Connector{}, err
	}
	return s.cn.CreateConnector(ctx, def, connectName, connectorName, cfg)
}

// DeleteConnector resolves name, validates connectName, then deletes
// connectorName from connectName.
func (s *ConnectService) DeleteConnector(ctx context.Context, name, connectName, connectorName string) error {
	def, err := s.lookupConnect(name, connectName)
	if err != nil {
		return err
	}
	return s.cn.DeleteConnector(ctx, def, connectName, connectorName)
}

// SetConnectorConfig resolves name, validates connectName, then replaces
// connectorName's whole config on connectName (Connect's own PUT .../config
// semantics -- a full replace, not an incremental merge; see
// KafkaConnectPort.SetConnectorConfig's doc comment), returning its
// assembled detail.
func (s *ConnectService) SetConnectorConfig(ctx context.Context, name, connectName, connectorName string, cfg map[string]any) (cluster.Connector, error) {
	def, err := s.lookupConnect(name, connectName)
	if err != nil {
		return cluster.Connector{}, err
	}
	return s.cn.SetConnectorConfig(ctx, def, connectName, connectorName, cfg)
}

// UpdateConnectorState resolves name, validates connectName, then applies
// action (the contract's ConnectorAction enum string) to connectorName on
// connectName.
func (s *ConnectService) UpdateConnectorState(ctx context.Context, name, connectName, connectorName, action string) error {
	def, err := s.lookupConnect(name, connectName)
	if err != nil {
		return err
	}
	return s.cn.UpdateConnectorState(ctx, def, connectName, connectorName, action)
}

// ResetConnectorOffsets resolves name, validates connectName, then resets
// connectorName's committed offsets on connectName.
func (s *ConnectService) ResetConnectorOffsets(ctx context.Context, name, connectName, connectorName string) error {
	def, err := s.lookupConnect(name, connectName)
	if err != nil {
		return err
	}
	return s.cn.ResetConnectorOffsets(ctx, def, connectName, connectorName)
}

// RestartConnectorTask resolves name, validates connectName, then restarts
// one task (by index) of connectorName on connectName.
func (s *ConnectService) RestartConnectorTask(ctx context.Context, name, connectName, connectorName string, taskID int) error {
	def, err := s.lookupConnect(name, connectName)
	if err != nil {
		return err
	}
	return s.cn.RestartConnectorTask(ctx, def, connectName, connectorName, taskID)
}
