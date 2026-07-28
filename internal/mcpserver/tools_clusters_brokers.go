package mcpserver

import (
	"bytes"
	"context"
	"encoding/csv"
	"math"
	"sort"
	"strconv"
	"strings"

	"github.com/cy-kaf/cy-kaf-client/internal/api/generated"
	appcluster "github.com/cy-kaf/cy-kaf-client/internal/app/cluster"
	domaincluster "github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
)

const (
	maxClusterBrokerNameBytes = 1024
	maxBrokerLogDirBytes      = 4096
	maxBrokerConfigValueBytes = maxResultBytes
)

var brokerCSVHeader = []string{
	"bytesInPerSec",
	"bytesOutPerSec",
	"host",
	"id",
	"inSyncPartitions",
	"leadersSkew",
	"partitions",
	"partitionsLeader",
	"partitionsSkew",
	"port",
}

func clusterBrokerTool(meta ToolMeta) ToolSpec {
	switch meta.Name {
	case "getClusters":
		return clusterBrokerReadTool(meta, nil, getClusters)
	case "getClusterMetrics":
		return clusterBrokerReadTool(meta, clusterInputSelector, getClusterMetrics)
	case "getClusterStats":
		return clusterBrokerReadTool(meta, clusterInputSelector, getClusterStats)
	case "updateClusterInfo":
		return clusterBrokerReadTool(meta, clusterInputSelector, updateClusterInfo)
	case "getBrokers":
		return clusterBrokerReadTool(meta, clusterInputSelector, getBrokers)
	case "getBrokersCsv":
		return clusterBrokerReadTool(meta, clusterInputSelector, func(ctx context.Context, executor *Executor, input clusterInput) (any, error) {
			return getBrokersCSV(ctx, executor, input, meta.MaxItems)
		})
	case "getBrokersMetrics":
		return clusterBrokerReadTool(meta, brokerInputSelector, getBrokerMetrics)
	case "getAllBrokersLogdirs":
		return clusterBrokerReadTool(meta,
			brokerLogDirsInputSelector,
			getAllBrokerLogDirs,
		)
	case "getBrokerConfig":
		return clusterBrokerReadTool(meta, brokerInputSelector, getBrokerConfig)
	case "updateBrokerTopicPartitionLogDir":
		return clusterBrokerWriteTool(meta, moveLogDirInputSelector, updateBrokerTopicPartitionLogDir)
	case "updateBrokerConfigByName":
		return clusterBrokerWriteTool(meta, updateBrokerConfigInputSelector, updateBrokerConfigByName)
	default:
		panic("unsupported Clusters/Brokers MCP tool: " + meta.Name)
	}
}

func clusterBrokerReadTool[In any](
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

func clusterBrokerWriteTool[In any](
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

func clusterInputSelector(input clusterInput) string {
	return input.ClusterName
}

func brokerInputSelector(input brokerInput) string {
	return input.ClusterName
}

func brokerLogDirsInputSelector(
	input clusterOptionalQueryInput[generated.GetAllBrokersLogdirsParams],
) string {
	return input.ClusterName
}

func updateBrokerConfigInputSelector(input updateBrokerConfigInput) string {
	return input.ClusterName
}

func moveLogDirInputSelector(input moveLogDirInput) string {
	return input.ClusterName
}

func getClusters(ctx context.Context, executor *Executor, _ noInput) (any, error) {
	if executor.deps.States == nil {
		return nil, errOperationFailed
	}
	snapshots := executor.deps.States.List(ctx)
	clusters := make([]generated.Cluster, 0, len(snapshots))
	for _, snapshot := range snapshots {
		clusters = append(clusters, clusterFromSnapshot(snapshot))
	}
	return clusters, nil
}

func getClusterMetrics(ctx context.Context, executor *Executor, input clusterInput) (any, error) {
	state, err := cachedClusterState(ctx, executor, input.ClusterName)
	if err != nil {
		return nil, err
	}
	return generated.ClusterMetrics{Items: clusterMetricsFrom(state)}, nil
}

func getClusterStats(ctx context.Context, executor *Executor, input clusterInput) (any, error) {
	state, err := cachedClusterState(ctx, executor, input.ClusterName)
	if err != nil {
		return nil, err
	}
	return clusterStatsFrom(state), nil
}

func updateClusterInfo(ctx context.Context, executor *Executor, input clusterInput) (any, error) {
	if err := validateBoundedName(input.ClusterName); err != nil {
		return nil, err
	}
	if executor.deps.States == nil {
		return nil, errOperationFailed
	}
	state, err := executor.deps.States.Refresh(ctx, input.ClusterName)
	if err != nil {
		return nil, err
	}
	return clusterFromSnapshot(state.Snapshot()), nil
}

func getBrokers(ctx context.Context, executor *Executor, input clusterInput) (any, error) {
	state, err := cachedClusterState(ctx, executor, input.ClusterName)
	if err != nil {
		return nil, err
	}
	return brokersFrom(state), nil
}

func getBrokersCSV(
	ctx context.Context,
	executor *Executor,
	input clusterInput,
	maxItems int,
) (any, error) {
	state, err := cachedClusterState(ctx, executor, input.ClusterName)
	if err != nil {
		return nil, err
	}
	brokers := append([]domaincluster.BrokerInfo(nil), state.Brokers...)
	sort.Slice(brokers, func(left, right int) bool {
		return brokers[left].ID < brokers[right].ID
	})
	if maxItems > 0 && len(brokers) > maxItems {
		brokers = brokers[:maxItems]
	}

	var buffer bytes.Buffer
	writer := csv.NewWriter(&buffer)
	if err := writer.Write(brokerCSVHeader); err != nil {
		return nil, errOperationFailed
	}
	for _, broker := range brokers {
		if err := writer.Write([]string{
			"",
			"",
			broker.Host,
			strconv.FormatInt(int64(broker.ID), 10),
			strconv.Itoa(broker.InSyncPartitions),
			"",
			strconv.Itoa(broker.Partitions),
			strconv.Itoa(broker.PartitionsLeader),
			"",
			strconv.FormatInt(int64(broker.Port), 10),
		}); err != nil {
			return nil, errOperationFailed
		}
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		return nil, errOperationFailed
	}
	return buffer.String(), nil
}

func getBrokerMetrics(ctx context.Context, executor *Executor, input brokerInput) (any, error) {
	if err := validateBrokerInput(input); err != nil {
		return nil, err
	}
	state, err := cachedClusterState(ctx, executor, input.ClusterName)
	if err != nil {
		return nil, err
	}
	return brokerMetricsFrom(state, input.ID), nil
}

func getAllBrokerLogDirs(
	ctx context.Context,
	executor *Executor,
	input clusterOptionalQueryInput[generated.GetAllBrokersLogdirsParams],
) (any, error) {
	if err := validateBoundedName(input.ClusterName); err != nil {
		return nil, err
	}
	var brokerIDs []int32
	if input.Query != nil && input.Query.Broker != nil {
		brokerIDs = *input.Query.Broker
	}
	if len(brokerIDs) > maxListItems {
		return nil, errInvalidRequest
	}
	for _, brokerID := range brokerIDs {
		if brokerID < 0 {
			return nil, errInvalidRequest
		}
	}
	if executor.deps.Brokers == nil {
		return nil, errOperationFailed
	}
	dirs, err := executor.deps.Brokers.LogDirs(ctx, input.ClusterName, brokerIDs)
	if err != nil {
		return nil, err
	}
	if err := validateBrokerLogDirsResult(ctx, dirs); err != nil {
		return nil, err
	}
	output := make([]generated.BrokersLogdirs, 0, len(dirs))
	for _, dir := range dirs {
		output = append(output, brokerLogDirsFrom(dir))
	}
	return output, nil
}

const (
	maxBrokerLogDirBrokers     = maxListItems
	maxBrokerLogDirDirectories = maxListItems
	maxBrokerLogDirTopicGroups = maxListItems * 4
	maxBrokerLogDirPartitions  = maxListItems * 16
)

func validateBrokerLogDirsResult(
	ctx context.Context,
	directories []domaincluster.BrokerLogDirs,
) error {
	if ctx == nil {
		return context.Canceled
	}
	brokers := make(map[int32]struct{})
	var topicGroups, partitions, estimatedBytes int
	for _, directory := range directories {
		if err := ctx.Err(); err != nil {
			return err
		}
		brokers[directory.Broker] = struct{}{}
		if len(brokers) > maxBrokerLogDirBrokers ||
			len(directories) > maxBrokerLogDirDirectories ||
			!addBrokerLogDirEstimate(&estimatedBytes, 96+len(directory.Dir)+len(directory.Error)) {
			return errResultTooLarge
		}
		for _, topic := range directory.Topics {
			if err := ctx.Err(); err != nil {
				return err
			}
			topicGroups++
			if topicGroups > maxBrokerLogDirTopicGroups ||
				!addBrokerLogDirEstimate(&estimatedBytes, 64+len(topic.Topic)) {
				return errResultTooLarge
			}
			for range topic.Partitions {
				if err := ctx.Err(); err != nil {
					return err
				}
				partitions++
				if partitions > maxBrokerLogDirPartitions ||
					!addBrokerLogDirEstimate(&estimatedBytes, 96) {
					return errResultTooLarge
				}
			}
		}
	}
	return nil
}

func addBrokerLogDirEstimate(total *int, size int) bool {
	if size < 0 || *total > maxResultBytes-size {
		return false
	}
	*total += size
	return true
}

func getBrokerConfig(ctx context.Context, executor *Executor, input brokerInput) (any, error) {
	if err := validateBrokerInput(input); err != nil {
		return nil, err
	}
	if executor.deps.Brokers == nil {
		return nil, errOperationFailed
	}
	configs, err := executor.deps.Brokers.BrokerConfigs(ctx, input.ClusterName, input.ID)
	if err != nil {
		return nil, err
	}
	output := make([]generated.BrokerConfig, 0, len(configs))
	for _, config := range configs {
		output = append(output, brokerConfigFrom(config))
	}
	redactBrokerConfigValues(output)
	// Configuration-bearing tools opt in explicitly. This recursive pass also
	// covers any credential-shaped JSON fields added to the DTO in the future.
	return redactCredentials(output), nil
}

func updateBrokerTopicPartitionLogDir(
	ctx context.Context,
	executor *Executor,
	input moveLogDirInput,
) (any, error) {
	if err := validateMoveLogDirInput(input); err != nil {
		return nil, err
	}
	if executor.deps.Brokers == nil {
		return nil, errOperationFailed
	}
	if err := executor.deps.Brokers.MoveReplicaLogDir(
		ctx,
		input.ClusterName,
		input.ID,
		input.Topic,
		input.Partition,
		input.LogDir,
	); err != nil {
		return nil, err
	}
	return nil, nil
}

func updateBrokerConfigByName(
	ctx context.Context,
	executor *Executor,
	input updateBrokerConfigInput,
) (any, error) {
	if err := validateUpdateBrokerConfigInput(input); err != nil {
		return nil, err
	}
	if executor.deps.Brokers == nil {
		return nil, errOperationFailed
	}
	if err := executor.deps.Brokers.AlterBrokerConfig(
		ctx,
		input.ClusterName,
		input.ID,
		input.Name,
		input.Value,
	); err != nil {
		return nil, err
	}
	return nil, nil
}

func cachedClusterState(
	ctx context.Context,
	executor *Executor,
	clusterName string,
) (domaincluster.RuntimeState, error) {
	if err := validateBoundedName(clusterName); err != nil {
		return domaincluster.RuntimeState{}, err
	}
	if executor.deps.States == nil {
		return domaincluster.RuntimeState{}, errOperationFailed
	}
	state, ok := executor.deps.States.Get(ctx, clusterName)
	if !ok {
		return domaincluster.RuntimeState{}, appcluster.ErrUnknownCluster
	}
	return state, nil
}

func validateBrokerInput(input brokerInput) error {
	if err := validateBoundedName(input.ClusterName); err != nil {
		return err
	}
	if input.ID < 0 {
		return errInvalidRequest
	}
	return nil
}

func validateUpdateBrokerConfigInput(input updateBrokerConfigInput) error {
	if err := validateBoundedName(input.ClusterName); err != nil {
		return err
	}
	if input.ID < 0 {
		return errInvalidRequest
	}
	if err := validateBoundedName(input.Name); err != nil {
		return err
	}
	if len(input.Value) > maxBrokerConfigValueBytes {
		return errInvalidRequest
	}
	return nil
}

func validateMoveLogDirInput(input moveLogDirInput) error {
	if err := validateBoundedName(input.ClusterName); err != nil {
		return err
	}
	if input.ID < 0 || input.Partition < 0 {
		return errInvalidRequest
	}
	if err := validateBoundedName(input.Topic); err != nil {
		return err
	}
	if strings.TrimSpace(input.LogDir) == "" || len(input.LogDir) > maxBrokerLogDirBytes {
		return errInvalidRequest
	}
	return nil
}

func validateBoundedName(name string) error {
	if strings.TrimSpace(name) == "" || len(name) > maxClusterBrokerNameBytes {
		return errInvalidRequest
	}
	return nil
}

func clusterFromSnapshot(snapshot domaincluster.Snapshot) generated.Cluster {
	features := make([]generated.ClusterFeatures, 0, len(snapshot.Features))
	for _, feature := range snapshot.Features {
		features = append(features, generated.ClusterFeatures(feature))
	}
	output := generated.Cluster{
		Name:     snapshot.Definition.Name,
		ReadOnly: clusterBrokerPointer(snapshot.Definition.ReadOnly),
		Features: &features,
		Status:   generated.OFFLINE,
	}
	if snapshot.Status == domaincluster.StatusOnline {
		output.Status = generated.ONLINE
		output.BrokerCount = clusterBrokerPointer(boundedInt32Count(snapshot.BrokerCount))
	}
	return output
}

func clusterMetricsFrom(state domaincluster.RuntimeState) []generated.Metric {
	metrics := []generated.Metric{
		{
			Name:  clusterBrokerPointer("broker_count"),
			Value: clusterBrokerPointer(float32(len(state.Brokers))),
		},
		{
			Name:  clusterBrokerPointer("topic_count"),
			Value: clusterBrokerPointer(float32(state.TopicCount)),
		},
		{
			Name:   clusterBrokerPointer("kafka_topic_partitions"),
			Value:  clusterBrokerPointer(float32(state.Partitions.Online)),
			Labels: clusterBrokerPointer(map[string]string{"status": "online"}),
		},
		{
			Name:   clusterBrokerPointer("kafka_topic_partitions"),
			Value:  clusterBrokerPointer(float32(state.Partitions.Offline)),
			Labels: clusterBrokerPointer(map[string]string{"status": "offline"}),
		},
	}
	for _, disk := range state.Disk {
		metrics = append(metrics, generated.Metric{
			Name:  clusterBrokerPointer("broker_bytes_disk"),
			Value: clusterBrokerPointer(float32(disk.SegmentSize)),
			Labels: clusterBrokerPointer(map[string]string{
				"broker": strconv.FormatInt(int64(disk.Broker), 10),
			}),
		})
	}
	return metrics
}

func clusterStatsFrom(state domaincluster.RuntimeState) generated.ClusterStats {
	stats := generated.ClusterStats{
		BrokerCount:                   clusterBrokerPointer(boundedInt32Count(len(state.Brokers))),
		ActiveControllers:             clusterBrokerPointer(activeControllerCount(state.Controller)),
		OnlinePartitionCount:          clusterBrokerPointer(boundedInt32Count(state.Partitions.Online)),
		OfflinePartitionCount:         clusterBrokerPointer(boundedInt32Count(state.Partitions.Offline)),
		InSyncReplicasCount:           clusterBrokerPointer(boundedInt32Count(state.Partitions.InSync)),
		OutOfSyncReplicasCount:        clusterBrokerPointer(boundedInt32Count(state.Partitions.OutOfSync)),
		UnderReplicatedPartitionCount: clusterBrokerPointer(boundedInt32Count(state.Partitions.UnderReplicated)),
	}
	if len(state.Disk) > 0 {
		disk := make([]generated.BrokerDiskUsage, 0, len(state.Disk))
		for _, usage := range state.Disk {
			disk = append(disk, generated.BrokerDiskUsage{
				BrokerId:     usage.Broker,
				SegmentSize:  clusterBrokerPointer(usage.SegmentSize),
				SegmentCount: clusterBrokerPointer(boundedInt32Count(usage.SegmentCount)),
			})
		}
		stats.DiskUsage = &disk
	}
	if state.Version != "" {
		stats.Version = clusterBrokerPointer(state.Version)
	}
	return stats
}

func brokersFrom(state domaincluster.RuntimeState) []generated.Broker {
	brokers := make([]generated.Broker, 0, len(state.Brokers))
	for _, broker := range state.Brokers {
		brokers = append(brokers, generated.Broker{
			Id:               broker.ID,
			Host:             clusterBrokerPointer(broker.Host),
			Port:             clusterBrokerPointer(broker.Port),
			PartitionsLeader: clusterBrokerPointer(boundedInt32Count(broker.PartitionsLeader)),
			Partitions:       clusterBrokerPointer(boundedInt32Count(broker.Partitions)),
			InSyncPartitions: clusterBrokerPointer(boundedInt32Count(broker.InSyncPartitions)),
		})
	}
	return brokers
}

func brokerMetricsFrom(state domaincluster.RuntimeState, brokerID int32) generated.BrokerMetrics {
	var metrics generated.BrokerMetrics
	for _, disk := range state.Disk {
		if disk.Broker == brokerID {
			metrics.SegmentSize = clusterBrokerPointer(disk.SegmentSize)
			metrics.SegmentCount = clusterBrokerPointer(boundedInt32Count(disk.SegmentCount))
			break
		}
	}
	for _, broker := range state.Brokers {
		if broker.ID == brokerID {
			items := []generated.Metric{
				{
					Name:  clusterBrokerPointer("partitions_leader"),
					Value: clusterBrokerPointer(float32(broker.PartitionsLeader)),
				},
				{
					Name:  clusterBrokerPointer("partitions"),
					Value: clusterBrokerPointer(float32(broker.Partitions)),
				},
				{
					Name:  clusterBrokerPointer("in_sync_partitions"),
					Value: clusterBrokerPointer(float32(broker.InSyncPartitions)),
				},
			}
			metrics.Metrics = &items
			break
		}
	}
	return metrics
}

func brokerLogDirsFrom(input domaincluster.BrokerLogDirs) generated.BrokersLogdirs {
	output := generated.BrokersLogdirs{Name: clusterBrokerPointer(input.Dir)}
	if input.Error != "" {
		output.Error = clusterBrokerPointer(input.Error)
	}
	topics := make([]generated.BrokerTopicLogdirs, 0, len(input.Topics))
	for _, topic := range input.Topics {
		partitions := make([]generated.BrokerTopicPartitionLogdir, 0, len(topic.Partitions))
		for _, partition := range topic.Partitions {
			partitions = append(partitions, generated.BrokerTopicPartitionLogdir{
				Broker:    clusterBrokerPointer(input.Broker),
				Partition: clusterBrokerPointer(partition.Partition),
				Size:      clusterBrokerPointer(partition.Size),
				OffsetLag: clusterBrokerPointer(partition.OffsetLag),
			})
		}
		topics = append(topics, generated.BrokerTopicLogdirs{
			Name:       clusterBrokerPointer(topic.Topic),
			Partitions: &partitions,
		})
	}
	output.Topics = &topics
	return output
}

func brokerConfigFrom(input domaincluster.ConfigEntry) generated.BrokerConfig {
	output := generated.BrokerConfig{
		Name:        input.Name,
		Value:       input.Value,
		Source:      brokerConfigSourceFrom(input.Source),
		IsSensitive: input.IsSensitive,
		IsReadOnly:  input.IsReadOnly,
	}
	if len(input.Synonyms) > 0 {
		synonyms := make([]generated.ConfigSynonym, 0, len(input.Synonyms))
		for _, synonym := range input.Synonyms {
			source := brokerConfigSourceFrom(synonym.Source)
			synonyms = append(synonyms, generated.ConfigSynonym{
				Name:   clusterBrokerPointer(synonym.Name),
				Value:  clusterBrokerPointer(synonym.Value),
				Source: &source,
			})
		}
		output.Synonyms = &synonyms
	}
	return output
}

func brokerConfigSourceFrom(source string) generated.ConfigSource {
	switch source {
	case "DYNAMIC_TOPIC_CONFIG":
		return generated.ConfigSourceDYNAMICTOPICCONFIG
	case "DYNAMIC_BROKER_CONFIG":
		return generated.ConfigSourceDYNAMICBROKERCONFIG
	case "DYNAMIC_DEFAULT_BROKER_CONFIG":
		return generated.ConfigSourceDYNAMICDEFAULTBROKERCONFIG
	case "STATIC_BROKER_CONFIG":
		return generated.ConfigSourceSTATICBROKERCONFIG
	case "DEFAULT_CONFIG":
		return generated.ConfigSourceDEFAULTCONFIG
	case "DYNAMIC_BROKER_LOGGER_CONFIG":
		return generated.ConfigSourceDYNAMICBROKERLOGGERCONFIG
	case "CLIENT_METRICS_CONFIG":
		return generated.ConfigSourceDYNAMICCLIENTMETRICSCONFIG
	default:
		return generated.ConfigSourceUNKNOWN
	}
}

func redactBrokerConfigValues(configs []generated.BrokerConfig) {
	for index := range configs {
		sensitive := configs[index].IsSensitive || isCredentialKey(configs[index].Name)
		if sensitive {
			configs[index].Value = redactedValue
		}
		if configs[index].Synonyms == nil {
			continue
		}
		for synonymIndex := range *configs[index].Synonyms {
			synonym := &(*configs[index].Synonyms)[synonymIndex]
			if synonym.Value == nil {
				continue
			}
			if sensitive || (synonym.Name != nil && isCredentialKey(*synonym.Name)) {
				*synonym.Value = redactedValue
			}
		}
	}
}

func clusterBrokerPointer[Value any](value Value) *Value {
	return &value
}

func activeControllerCount(controllerID int32) int32 {
	if controllerID < 0 {
		return 0
	}
	return 1
}

func boundedInt32Count(value int) int32 {
	switch {
	case value <= 0:
		return 0
	case value > math.MaxInt32:
		return math.MaxInt32
	default:
		return int32(value)
	}
}
