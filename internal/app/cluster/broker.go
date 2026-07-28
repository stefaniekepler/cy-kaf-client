package cluster

import (
	"context"

	"github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
)

// BrokerService performs broker-scoped admin operations (log dirs, broker
// config read/write) against a resolved cluster, delegating to a
// cluster.BrokerAdminPort. Unlike StateCache, it never caches: every call is
// a fresh describe/alter against the live client pool. P1b's
// TopicService/GroupService follow this same shape (Resolver + a narrow
// domain port), not more methods hung off StateCache.
type BrokerService struct {
	res  *Resolver
	port cluster.BrokerAdminPort
}

func NewBrokerService(res *Resolver, port cluster.BrokerAdminPort) *BrokerService {
	return &BrokerService{res: res, port: port}
}

// LogDirs 按集群名找到配置的 Definition 后透传给 port（与 StateCache.Refresh 同样的
// name->Definition 查找方式，经由 Resolver；未知集群返回明确错误，不静默返回空）。不经过
// 缓存：log dirs 是按需查询，不像 FetchState 那样有周期性刷新基线。
func (s *BrokerService) LogDirs(ctx context.Context, name string, brokers []int32) ([]cluster.BrokerLogDirs, error) {
	d, err := s.res.Lookup(name)
	if err != nil {
		return nil, err
	}
	return s.port.LogDirs(ctx, d, brokers)
}

// BrokerConfigs 按集群名找到配置的 Definition 后透传给 port（与 LogDirs 同样的 name->Definition
// 查找方式；未知集群返回明确错误）。不经过缓存：与 LogDirs 一样是按需查询。
func (s *BrokerService) BrokerConfigs(ctx context.Context, name string, broker int32) ([]cluster.ConfigEntry, error) {
	d, err := s.res.Lookup(name)
	if err != nil {
		return nil, err
	}
	return s.port.BrokerConfigs(ctx, d, broker)
}

// AlterBrokerConfig 按集群名找到配置的 Definition 后透传给 port，做增量单键配置修改
// （严禁全量替换语义，见 domain BrokerAdminPort.AlterBrokerConfig 与 ADR-0003 §6.1）。
func (s *BrokerService) AlterBrokerConfig(ctx context.Context, name string, broker int32, cfgName, value string) error {
	d, err := s.res.Lookup(name)
	if err != nil {
		return err
	}
	return s.port.AlterBrokerConfig(ctx, d, broker, cfgName, value)
}

// MoveReplicaLogDir 按集群名找到配置的 Definition 后透传给 port，迁移一个 topic-partition
// 副本的落盘目录。
func (s *BrokerService) MoveReplicaLogDir(ctx context.Context, name string, broker int32, topic string, partition int32, dir string) error {
	d, err := s.res.Lookup(name)
	if err != nil {
		return err
	}
	return s.port.MoveReplicaLogDir(ctx, d, broker, topic, partition, dir)
}
