package kafka

import (
	"sync"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
)

// Pool 为每个集群维护长生命周期的管理客户端，以及全局有界的空闲消息读取
// 客户端（均惰性创建、显式失效）。复用连接避免每次请求重复建连，同时读取
// session 在租期内仍由单个调用方独占。
type Pool struct {
	mu                sync.Mutex
	clients           map[string]*pooled
	idleReaders       map[readerPoolKey][]*kgo.Client
	readerGenerations map[string]uint64
	idleReaderCount   int
	quotaLocks        keyedLocker[quotaLockKey]
}

type pooled struct {
	cl  *kgo.Client
	adm *kadm.Client
}

func NewPool() *Pool {
	return &Pool{
		clients:           map[string]*pooled{},
		idleReaders:       map[readerPoolKey][]*kgo.Client{},
		readerGenerations: map[string]uint64{},
	}
}

func (p *Pool) clientFor(def cluster.Definition) (*pooled, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if c, ok := p.clients[def.Name]; ok {
		return c, nil
	}
	opts, err := buildOpts(def.Conn)
	if err != nil {
		return nil, err
	}
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, err
	}
	c := &pooled{cl: cl, adm: kadm.NewClient(cl)}
	p.clients[def.Name] = c
	return c, nil
}

func (p *Pool) Invalidate(name string) {
	p.mu.Lock()
	var closing []*kgo.Client
	if c, ok := p.clients[name]; ok {
		closing = append(closing, c.cl)
		delete(p.clients, name)
	}
	p.readerGenerations[name]++
	for key, readers := range p.idleReaders {
		if key.clusterName != name {
			continue
		}
		closing = append(closing, readers...)
		p.idleReaderCount -= len(readers)
		delete(p.idleReaders, key)
	}
	p.mu.Unlock()

	for _, client := range closing {
		client.Close()
	}
}

func (p *Pool) Close() {
	p.mu.Lock()
	closing := make([]*kgo.Client, 0, len(p.clients)+p.idleReaderCount)
	for _, c := range p.clients {
		closing = append(closing, c.cl)
	}
	p.clients = map[string]*pooled{}
	for name := range p.readerGenerations {
		p.readerGenerations[name]++
	}
	for _, readers := range p.idleReaders {
		closing = append(closing, readers...)
	}
	p.idleReaders = map[readerPoolKey][]*kgo.Client{}
	p.idleReaderCount = 0
	p.mu.Unlock()

	for _, client := range closing {
		client.Close()
	}
}

// 编译期证明：Pool 一个具体类型满足全部拆分后的端口接口（FetchState 实现见
// state.go；LogDirs/BrokerConfigs/AlterBrokerConfig/MoveReplicaLogDir 同样在
// state.go；TopicConfigs/TopicAcls/ActiveProducers 见 topics.go（P1b Task 4）；
// ListGroups/DescribeGroup/GroupsForTopic/ResetOffsets/DeleteGroup/
// DeleteGroupOffsets 见 groups.go（P1b Task 6）；Invalidate/Close 见本文件上方）。
var (
	_ cluster.StateScraper    = (*Pool)(nil)
	_ cluster.BrokerAdminPort = (*Pool)(nil)
	_ cluster.ClientLifecycle = (*Pool)(nil)
	_ cluster.TopicAdminPort  = (*Pool)(nil)
	_ cluster.GroupAdminPort  = (*Pool)(nil)
)
