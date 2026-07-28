//go:build integration

package kafka

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	"github.com/twmb/franz-go/pkg/kgo"

	appcluster "github.com/cy-kaf/cy-kaf-client/internal/app/cluster"
	"github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
	infrafilter "github.com/cy-kaf/cy-kaf-client/internal/infra/filter"
	infraserde "github.com/cy-kaf/cy-kaf-client/internal/infra/serde"
)

// TestMessagesReaderRealRoundTripAgainstRealKafka locks down P1c Task 6's
// MessageReaderPort against a *real* single-partition topic: a kgo producer
// seeds 5 records with distinct key/value/header bytes and strictly
// increasing, explicit (client-set, not broker-assigned) CreateTime
// timestamps one second apart — see the recTS helper below for why an
// explicit, widely-spaced timestamp per record matters here. It then
// exercises all three MessageReaderPort methods against that fixture:
//   - PartitionRanges (no partition filter) must report the single
//     partition's watermark as {Start:0, End:5} — 5 records written from an
//     empty topic.
//   - Open seeked to {0:2} + repeated Poll must yield exactly the 3 records
//     at offsets 2,3,4, with Key/Value/Headers/TimestampMs matching what was
//     produced byte-for-byte, and Close must not panic.
//   - OffsetsForTimestamp queried at the 3rd record's (offset 2) own
//     timestamp must resolve back to offset 2 (kadm's ListOffsetsAfterMilli
//     "first offset with timestamp >= ts" contract, and since every record's
//     timestamp here is unique this is an exact, unambiguous check — no
//     "or the one after" fuzziness needed).
func TestMessagesReaderRealRoundTripAgainstRealKafka(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	kc, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.8.0")
	testcontainers.CleanupContainer(t, kc)
	require.NoError(t, err)
	brokers, err := kc.Brokers(ctx)
	require.NoError(t, err)

	pool := NewPool()
	defer pool.Close()
	def := cluster.Definition{Name: "it-messages-read", Conn: cluster.ConnectionSpec{BootstrapServers: brokers}}

	const topic = "msg-topic"
	require.NoError(t, pool.CreateTopic(ctx, def, cluster.TopicSpec{Name: topic, Partitions: 1, ReplicationFactor: 1}))

	// recTS gives record i (0-based) a unique, one-second-spaced timestamp,
	// truncated to millisecond precision up front (via time.UnixMilli) so
	// there is no sub-millisecond rounding to reason about when comparing
	// against RawRecord.TimestampMs (an int64 millisecond value) later.
	// Spacing them a full second apart (rather than relying on kgo's default
	// "each record gets its own time.Now()" behavior, which for a single
	// ProduceSync batch can collapse multiple records into the same
	// millisecond) is what makes the OffsetsForTimestamp assertion below
	// exact rather than "offset 2 or the record after it".
	baseMs := time.Now().UnixMilli()
	recTS := func(i int) time.Time { return time.UnixMilli(baseMs + int64(i)*1000) }

	producer, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	require.NoError(t, err)
	defer producer.Close()

	const n = 5
	recs := make([]*kgo.Record, n)
	for i := range recs {
		recs[i] = &kgo.Record{
			Topic:     topic,
			Key:       []byte(fmt.Sprintf("k%d", i)),
			Value:     []byte(fmt.Sprintf("v%d", i)),
			Headers:   []kgo.RecordHeader{{Key: "h", Value: []byte(fmt.Sprintf("hv%d", i))}},
			Timestamp: recTS(i),
		}
	}
	results := producer.ProduceSync(ctx, recs...)
	require.NoError(t, results.FirstErr())

	// --- PartitionRanges: fresh topic, 5 records written -> {0:{0,5}} ---
	ranges, err := pool.PartitionRanges(ctx, def, topic, nil)
	require.NoError(t, err)
	require.Equal(t, map[int32]cluster.OffsetRange{0: {Start: 0, End: 5}}, ranges)

	// --- Open seeked to offset 2 + Poll -> records at offsets 2,3,4 ---
	session, err := pool.Open(ctx, def, topic, map[int32]int64{0: 2})
	require.NoError(t, err)
	// Release the dedicated client even if a require/FailNow between here and
	// the explicit NotPanics(Close) below fires mid-scenario — a bare
	// end-of-scenario Close would leak the client on any failing assertion.
	// Wrapped in sync.OnceFunc so the guaranteed-cleanup registration and the
	// explicit "Close doesn't panic" assertion below both route through a
	// single real Close (kgo.Client.Close has no double-close guard of its
	// own, so we don't rely on calling it twice being safe).
	closeSession := sync.OnceFunc(session.Close)
	t.Cleanup(closeSession)

	var got []cluster.RawRecord
	deadline := time.Now().Add(30 * time.Second)
	for len(got) < 3 && time.Now().Before(deadline) {
		batch, pollErr := session.Poll(ctx)
		require.NoError(t, pollErr)
		got = append(got, batch...)
	}
	require.Len(t, got, 3, "expected exactly 3 records (offsets 2,3,4) before the deadline")
	sort.Slice(got, func(i, j int) bool { return got[i].Offset < got[j].Offset })

	for i, r := range got {
		idx := i + 2 // got[0] is offset 2, got[1] is offset 3, got[2] is offset 4
		require.Equal(t, int32(0), r.Partition)
		require.Equal(t, int64(idx), r.Offset)
		require.Equal(t, []byte(fmt.Sprintf("k%d", idx)), r.Key)
		require.Equal(t, []byte(fmt.Sprintf("v%d", idx)), r.Value)
		require.Equal(t, map[string]string{"h": fmt.Sprintf("hv%d", idx)}, r.Headers)
		require.Equal(t, recTS(idx).UnixMilli(), r.TimestampMs)
		require.Equal(t, len(r.Key), r.KeySize)
		require.Equal(t, len(r.Value), r.ValueSize)
		require.Equal(t, len("h")+len(fmt.Sprintf("hv%d", idx)), r.HeadersSize)
	}
	require.NotPanics(t, func() { closeSession() })

	// --- OffsetsForTimestamp: query at the 3rd record's (offset 2) own ts -> offset 2 ---
	offsets, err := pool.OffsetsForTimestamp(ctx, def, topic, nil, recTS(2).UnixMilli())
	require.NoError(t, err)
	require.Equal(t, map[int32]int64{0: 2}, offsets)
}

// TestMessagesWriterRealRoundTripAgainstRealKafka locks down P1c Task 11's
// MessageWriterPort (Produce/DeleteRecords) against a real 3-partition
// topic:
//   - Produce a record to partition 0, one to partition 1 -- each read back
//     via MessageReaderPort.Open (Task 6, already proven above) and asserted
//     byte-for-byte AND on the exact partition requested. This is the
//     fixed-partition regression test the brief calls for: kgo's default
//     partitioner re-hashes by key and would very likely NOT put these on
//     the partitions requested (a plain string key has no guaranteed
//     hash-to-partition mapping across 3 partitions) -- if Produce ever
//     regresses to a client without kgo.RecordPartitioner(kgo.
//     ManualPartitioner()), this test either reads back the wrong partition
//     or times out waiting on the requested one, either way failing loudly.
//   - DeleteRecords([0]) then purges partition 0 to its own end offset (i.e.
//     empties it -- PartitionRanges' Start catches up to End) while
//     partition 1's range is untouched, proving DeleteRecords' delete-to-end
//     semantics and its partition-scoping (not "delete every partition of
//     topic" when the caller names a subset).
func TestMessagesWriterRealRoundTripAgainstRealKafka(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	kc, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.8.0")
	testcontainers.CleanupContainer(t, kc)
	require.NoError(t, err)
	brokers, err := kc.Brokers(ctx)
	require.NoError(t, err)

	pool := NewPool()
	defer pool.Close()
	def := cluster.Definition{Name: "it-messages-write", Conn: cluster.ConnectionSpec{BootstrapServers: brokers}}

	const topic = "msg-write-topic"
	require.NoError(t, pool.CreateTopic(ctx, def, cluster.TopicSpec{Name: topic, Partitions: 3, ReplicationFactor: 1}))

	// --- Produce to partition 0, then partition 1 -- fixed-partition proof ---
	recA := cluster.ProduceRecord{
		Partition: 0,
		Key:       []byte("kA"),
		Value:     []byte("vA"),
		Headers:   map[string]string{"h": "hvA"},
	}
	require.NoError(t, pool.Produce(ctx, def, topic, recA))

	recB := cluster.ProduceRecord{
		Partition: 1,
		Key:       []byte("kB"),
		Value:     []byte("vB"),
		Headers:   map[string]string{"h": "hvB"},
	}
	require.NoError(t, pool.Produce(ctx, def, topic, recB))

	pollOne := func(partition int32) cluster.RawRecord {
		session, openErr := pool.Open(ctx, def, topic, map[int32]int64{partition: 0})
		require.NoError(t, openErr)
		defer session.Close()

		var got []cluster.RawRecord
		deadline := time.Now().Add(30 * time.Second)
		for len(got) < 1 && time.Now().Before(deadline) {
			batch, pollErr := session.Poll(ctx)
			require.NoError(t, pollErr)
			got = append(got, batch...)
		}
		require.Len(t, got, 1, "expected exactly 1 record on partition %d before the deadline", partition)
		return got[0]
	}

	gotA := pollOne(0)
	require.Equal(t, int32(0), gotA.Partition, "recA must land on the REQUESTED partition 0, not wherever a hash partitioner would put it")
	require.Equal(t, recA.Key, gotA.Key)
	require.Equal(t, recA.Value, gotA.Value)
	require.Equal(t, recA.Headers, gotA.Headers)

	gotB := pollOne(1)
	require.Equal(t, int32(1), gotB.Partition, "recB must land on the REQUESTED partition 1, not wherever a hash partitioner would put it")
	require.Equal(t, recB.Key, gotB.Key)
	require.Equal(t, recB.Value, gotB.Value)
	require.Equal(t, recB.Headers, gotB.Headers)

	// --- DeleteRecords([0]): partition 0 emptied, partition 1 untouched ---
	before, err := pool.PartitionRanges(ctx, def, topic, []int32{0, 1})
	require.NoError(t, err)
	require.Equal(t, cluster.OffsetRange{Start: 0, End: 1}, before[0])
	require.Equal(t, cluster.OffsetRange{Start: 0, End: 1}, before[1])

	require.NoError(t, pool.DeleteRecords(ctx, def, topic, []int32{0}))

	after, err := pool.PartitionRanges(ctx, def, topic, []int32{0, 1})
	require.NoError(t, err)
	require.Equal(t, cluster.OffsetRange{Start: 1, End: 1}, after[0], "partition 0 must be purged to its own end offset (emptied)")
	require.Equal(t, cluster.OffsetRange{Start: 0, End: 1}, after[1], "partition 1 must be untouched by a delete scoped to partition 0 only")
}

// --- P1c Task 15: full message-engine end-to-end against real Kafka ---
//
// These tests wire the REAL app-layer MessageService (real serde Provider,
// real CEL filter Engine, real masking, real CursorCache) over the REAL infra
// Pool (as both MessageReaderPort and MessageWriterPort) against a
// testcontainers Kafka -- the whole produce -> serde -> mask -> filter ->
// cursor -> browse -> delete path stitched together, not the per-layer unit
// slices. Importing app/cluster + infra/serde + infra/filter here is
// acyclic (app/cluster imports only domain), and mirrors cmd/main.go's wiring.

func e2eStr(s string) *string { return &s }

// e2eMessageService builds a production-shaped MessageService for def over pool.
func e2eMessageService(pool *Pool, def cluster.Definition) *appcluster.MessageService {
	res := appcluster.NewResolver([]cluster.Definition{def})
	serdeProv := infraserde.NewProvider(infraserde.NewRegistry())
	filterEng := infrafilter.NewEngine()
	cursors := appcluster.NewCursorCache(10*time.Minute, 1000)
	return appcluster.NewMessageService(res, pool, pool, serdeProv, filterEng, cursors, nil)
}

// e2eBrowse runs one Browse to completion, collecting decoded messages and the
// terminal cursor id.
func e2eBrowse(t *testing.T, ctx context.Context, svc *appcluster.MessageService, name, topic string, spec appcluster.BrowseSpec) ([]appcluster.DecodedMessage, string) {
	t.Helper()
	var msgs []appcluster.DecodedMessage
	var cursor string
	err := svc.Browse(ctx, name, topic, spec, func(ev appcluster.BrowseEvent) error {
		switch ev.Kind {
		case appcluster.EventMessage:
			msgs = append(msgs, *ev.Message)
		case appcluster.EventDone:
			cursor = ev.CursorID
		}
		return nil
	})
	require.NoError(t, err)
	return msgs, cursor
}

// TestMessageEngineEndToEndAgainstRealKafka is the Task 15 soul suite: one
// container, several topics, the real stack. Subsections cover forward browse +
// masking + byte fidelity, backward (LATEST) browse, numeric-serde round trip,
// CEL filtering of real data, cursor pagination, tailing of newly produced
// records, and deleteRecords emptying a partition.
func TestMessageEngineEndToEndAgainstRealKafka(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	kc, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.8.0")
	testcontainers.CleanupContainer(t, kc)
	require.NoError(t, err)
	brokers, err := kc.Brokers(ctx)
	require.NoError(t, err)

	pool := NewPool()
	defer pool.Close()
	// The masking rule REMOVE("secret") is scoped to the JSON browse topic
	// only (TopicValuesPattern "e2e-browse"): the masking subsection proves it
	// runs on that topic's real decoded JSON, while the numeric-serde topic's
	// scalar values are deliberately left out of its scope.
	def := cluster.Definition{
		Name: "it-msg-e2e",
		Conn: cluster.ConnectionSpec{BootstrapServers: brokers},
		Maskings: []cluster.MaskingRule{
			{Type: cluster.MaskRemove, Fields: []string{"secret"}, TopicValuesPattern: "e2e-browse"},
		},
	}
	svc := e2eMessageService(pool, def)

	// ---- fixture: 6 JSON records on a String-serde topic ----
	const browseTopic = "e2e-browse"
	require.NoError(t, pool.CreateTopic(ctx, def, cluster.TopicSpec{Name: browseTopic, Partitions: 1, ReplicationFactor: 1}))
	for i := 0; i < 6; i++ {
		tag := "keep"
		if i%2 == 1 {
			tag = "skip"
		}
		val := fmt.Sprintf(`{"secret":"top%d","seq":%d,"tag":"%s"}`, i, i, tag)
		require.NoError(t, svc.Send(ctx, def.Name, browseTopic, appcluster.SendSpec{
			Partition: 0,
			Key:       e2eStr(fmt.Sprintf("k%d", i)),
			Value:     e2eStr(val),
			Headers:   map[string]string{"h": fmt.Sprintf("v%d", i)},
		}))
	}

	t.Run("forward browse reads all in order, masks secret, keeps bytes/headers", func(t *testing.T) {
		msgs, _ := e2eBrowse(t, ctx, svc, def.Name, browseTopic, appcluster.BrowseSpec{Mode: appcluster.ModeEarliest, Limit: 100})
		require.Len(t, msgs, 6)
		for i, m := range msgs {
			require.Equal(t, int64(i), m.Offset, "records must arrive in offset order")
			require.Equal(t, fmt.Sprintf("k%d", i), m.Key)
			require.NotContains(t, m.Value, "secret", "masking must strip the secret field from the real value")
			require.NotContains(t, m.Value, fmt.Sprintf("top%d", i))
			require.Contains(t, m.Value, fmt.Sprintf(`"seq":%d`, i), "non-masked fields must survive")
			require.Equal(t, map[string]string{"h": fmt.Sprintf("v%d", i)}, m.Headers)
		}
	})

	t.Run("backward LATEST browse returns the last N", func(t *testing.T) {
		msgs, _ := e2eBrowse(t, ctx, svc, def.Name, browseTopic, appcluster.BrowseSpec{Mode: appcluster.ModeLatest, Limit: 2})
		require.Len(t, msgs, 2)
		offsets := []int64{msgs[0].Offset, msgs[1].Offset}
		sort.Slice(offsets, func(a, b int) bool { return offsets[a] < offsets[b] })
		require.Equal(t, []int64{4, 5}, offsets, "LATEST limit=2 must yield the final two records")
	})

	t.Run("CEL filter selects matching real records", func(t *testing.T) {
		msgs, _ := e2eBrowse(t, ctx, svc, def.Name, browseTopic, appcluster.BrowseSpec{
			Mode: appcluster.ModeEarliest, Limit: 100, FilterCode: `record.value.tag == "keep"`,
		})
		require.Len(t, msgs, 3, "3 of 6 records are tagged keep")
		for _, m := range msgs {
			require.Contains(t, m.Value, "keep")
		}
	})

	t.Run("cursor pagination stitches two pages back into the full set", func(t *testing.T) {
		page1, cursor := e2eBrowse(t, ctx, svc, def.Name, browseTopic, appcluster.BrowseSpec{Mode: appcluster.ModeEarliest, Limit: 3})
		require.Len(t, page1, 3)
		require.NotEmpty(t, cursor, "a limited forward browse must hand back a resume cursor")
		page2, _ := e2eBrowse(t, ctx, svc, def.Name, browseTopic, appcluster.BrowseSpec{Mode: appcluster.ModeEarliest, Limit: 3, Cursor: cursor})
		require.Len(t, page2, 3)

		var offsets []int64
		for _, m := range append(append([]appcluster.DecodedMessage{}, page1...), page2...) {
			offsets = append(offsets, m.Offset)
		}
		sort.Slice(offsets, func(a, b int) bool { return offsets[a] < offsets[b] })
		require.Equal(t, []int64{0, 1, 2, 3, 4, 5}, offsets, "the two pages must cover the full set exactly once, no gap/overlap")
	})

	t.Run("numeric serde round trips through produce and browse", func(t *testing.T) {
		const intTopic = "e2e-int64"
		require.NoError(t, pool.CreateTopic(ctx, def, cluster.TopicSpec{Name: intTopic, Partitions: 1, ReplicationFactor: 1}))
		require.NoError(t, svc.Send(ctx, def.Name, intTopic, appcluster.SendSpec{
			Partition: 0, Value: e2eStr("42"), ValueSerde: "Int64",
		}))
		msgs, _ := e2eBrowse(t, ctx, svc, def.Name, intTopic, appcluster.BrowseSpec{Mode: appcluster.ModeEarliest, Limit: 10, ValueSerde: "Int64"})
		require.Len(t, msgs, 1)
		require.Equal(t, "42", msgs[0].Value, "Int64 serde must decode the 8-byte big-endian value back to text")
		require.Equal(t, "Int64", msgs[0].ValueSerde)
	})

	t.Run("tailing receives records produced after the browse starts", func(t *testing.T) {
		const tailTopic = "e2e-tail"
		require.NoError(t, pool.CreateTopic(ctx, def, cluster.TopicSpec{Name: tailTopic, Partitions: 1, ReplicationFactor: 1}))
		// Seed one record so tailing has a defined end to seek past.
		require.NoError(t, svc.Send(ctx, def.Name, tailTopic, appcluster.SendSpec{Partition: 0, Value: e2eStr(`{"seq":-1}`)}))

		tctx, tcancel := context.WithCancel(ctx)
		defer tcancel()
		var mu sync.Mutex
		var got []appcluster.DecodedMessage
		done := make(chan struct{})
		go func() {
			defer close(done)
			_ = svc.Browse(tctx, def.Name, tailTopic, appcluster.BrowseSpec{Mode: appcluster.ModeTailing, Limit: 100}, func(ev appcluster.BrowseEvent) error {
				if ev.Kind == appcluster.EventMessage {
					mu.Lock()
					got = append(got, *ev.Message)
					mu.Unlock()
				}
				return nil
			})
		}()

		// Let tailing seek to the current end before producing the records it
		// must observe (produced-before-seek records would be skipped).
		time.Sleep(2 * time.Second)
		for i := 0; i < 2; i++ {
			require.NoError(t, svc.Send(ctx, def.Name, tailTopic, appcluster.SendSpec{Partition: 0, Value: e2eStr(fmt.Sprintf(`{"seq":%d}`, i))}))
		}
		require.Eventually(t, func() bool {
			mu.Lock()
			defer mu.Unlock()
			return len(got) >= 2
		}, 20*time.Second, 200*time.Millisecond, "tailing must deliver the two newly produced records")
		tcancel()
		<-done

		mu.Lock()
		defer mu.Unlock()
		require.GreaterOrEqual(t, len(got), 2)
		require.Contains(t, got[len(got)-1].Value, `"seq":1`)
	})

	t.Run("deleteRecords empties a partition so browse returns nothing", func(t *testing.T) {
		const purgeTopic = "e2e-purge"
		require.NoError(t, pool.CreateTopic(ctx, def, cluster.TopicSpec{Name: purgeTopic, Partitions: 1, ReplicationFactor: 1}))
		for i := 0; i < 3; i++ {
			require.NoError(t, svc.Send(ctx, def.Name, purgeTopic, appcluster.SendSpec{Partition: 0, Value: e2eStr(fmt.Sprintf(`{"seq":%d}`, i))}))
		}
		before, _ := e2eBrowse(t, ctx, svc, def.Name, purgeTopic, appcluster.BrowseSpec{Mode: appcluster.ModeEarliest, Limit: 100})
		require.Len(t, before, 3)

		require.NoError(t, svc.Delete(ctx, def.Name, purgeTopic, []int32{0}))

		after, _ := e2eBrowse(t, ctx, svc, def.Name, purgeTopic, appcluster.BrowseSpec{Mode: appcluster.ModeEarliest, Limit: 100})
		require.Empty(t, after, "a purged partition must browse empty")
	})
}
