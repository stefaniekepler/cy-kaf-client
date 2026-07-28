package api

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cy-kaf/cy-kaf-client/internal/api/generated"
)

// Whitebox unit tests for rowsToCsv/csvCell (package api, not api_test) —
// mirrors handlers_broker_internal_test.go's approach of exercising
// unexported functions directly. TestRowsToCsv/TestRowsToCsvEmptyIsJustHeader
// are the former TestBrokersToCsv/TestBrokersToCsvEmptyIsJustHeader
// (handlers_broker_internal_test.go), migrated here now that brokersToCsv has
// been generalized into rowsToCsv and collected into csv.go.

// TestRowsToCsv locks in the column-order contract: rowsToCsv reflects the
// element type's json tag declaration order rather than a hand-maintained
// column list, so a future contract field addition/rename can't silently
// desync the CSV from the JSON shape — exercised here against
// generated.Broker, GetBrokersCsv's actual row type (handlers_broker.go).
// Also covers nil-pointer (all-optional-fields-absent) and populated-float-
// field cell rendering.
func TestRowsToCsv(t *testing.T) {
	brokers := []generated.Broker{
		{Id: 1, Host: ptr("h1"), Port: ptr(int32(9092)), PartitionsLeader: ptr(int32(3)),
			Partitions: ptr(int32(6)), InSyncPartitions: ptr(int32(6)), BytesInPerSec: ptr(1.5)},
		{Id: 2}, // every optional (pointer) field nil; Id is the only required/value field
	}
	out, err := rowsToCsv(brokers)
	require.NoError(t, err)
	lines := strings.Split(strings.TrimSpace(out), "\n")
	require.Len(t, lines, 3) // header + 2 data rows

	require.Equal(t, []string{
		"bytesInPerSec", "bytesOutPerSec", "host", "id", "inSyncPartitions",
		"leadersSkew", "partitions", "partitionsLeader", "partitionsSkew", "port",
	}, strings.Split(lines[0], ","))

	require.Equal(t, []string{"1.5", "", "h1", "1", "6", "", "6", "3", "", "9092"},
		strings.Split(lines[1], ","))

	require.Equal(t, []string{"", "", "", "2", "", "", "", "", "", ""},
		strings.Split(lines[2], ","))
}

func TestRowsToCsvEmptyIsJustHeader(t *testing.T) {
	out, err := rowsToCsv([]generated.Broker(nil))
	require.NoError(t, err)
	lines := strings.Split(strings.TrimSpace(out), "\n")
	require.Len(t, lines, 1)
	require.Contains(t, lines[0], "id")
}

// float32Row is a test-only struct exercising rowsToCsv's float32 bitSize
// fix: generated.Broker has no float32 fields of its own (its float columns
// are all float64), so the bug this locks in needs its own row type.
type float32Row struct {
	Name  string  `json:"name"`
	Value float32 `json:"value"`
}

// TestRowsToCsvFloat32UsesBitSize32 pins down the bitSize fix: 0.1 as a
// float32 must render as "0.1", not the 17-digit noise
// ("0.10000000149011612") strconv.FormatFloat would produce at bitSize 64 for
// that same float32 value's exact float64 conversion.
func TestRowsToCsvFloat32UsesBitSize32(t *testing.T) {
	out, err := rowsToCsv([]float32Row{{Name: "n", Value: 0.1}})
	require.NoError(t, err)
	lines := strings.Split(strings.TrimSpace(out), "\n")
	require.Len(t, lines, 2)
	require.Equal(t, []string{"name", "value"}, strings.Split(lines[0], ","))
	require.Equal(t, []string{"n", "0.1"}, strings.Split(lines[1], ","))
}

// TestRowsToCsvRejectsNonSlice covers rowsToCsv's input-shape guard: a caller
// passing something other than a []struct{...} (Task 4/6's topics/groups CSV
// endpoints will call this with their own row types) gets a clear error
// instead of a reflect panic.
func TestRowsToCsvRejectsNonSlice(t *testing.T) {
	_, err := rowsToCsv(42)
	require.Error(t, err)

	_, err = rowsToCsv([]int{1, 2, 3})
	require.Error(t, err)
}

// TestRowsToCsvStructAndSliceFields locks in csvCell's struct/slice fallback
// (P2b Task 4 review Fix 1): before this fix, a nested struct field (e.g.
// FullConnectorInfo.Status, a ConnectorStatus{State,Trace,WorkerId}) and a
// string-slice field (Topics, a *[]string) both fell through to the
// %v-formats-a-Go-struct-literal default branch, leaking Go's own internal
// representation ("{RUNNING <nil> <nil>}", "[a b c]") into the exported CSV
// cell instead of a value a human (or a spreadsheet) could read. This
// asserts the ACTUAL rendered cell values, not just that some substring is
// present in the body.
func TestRowsToCsvStructAndSliceFields(t *testing.T) {
	rows := []generated.FullConnectorInfo{
		{
			Connect: "connect-1",
			Name:    "jdbc-sink",
			Status:  generated.ConnectorStatus{State: generated.ConnectorStateRUNNING, WorkerId: ptr("worker-1")},
			Topics:  ptr([]string{"topic-a", "topic-b", "topic-c"}),
		},
	}
	out, err := rowsToCsv(rows)
	require.NoError(t, err)
	lines := strings.Split(strings.TrimSpace(out), "\n")
	require.Len(t, lines, 2) // header + 1 data row

	cols := strings.Split(lines[0], ",")
	statusIdx, topicsIdx := -1, -1
	for i, c := range cols {
		switch c {
		case "status":
			statusIdx = i
		case "topics":
			topicsIdx = i
		}
	}
	require.GreaterOrEqual(t, statusIdx, 0, "status column must be present")
	require.GreaterOrEqual(t, topicsIdx, 0, "topics column must be present")

	cells := strings.Split(lines[1], ",")
	require.Equal(t, "RUNNING", cells[statusIdx], "status cell must render ConnectorStatus.State, not a Go struct literal")
	require.Equal(t, "topic-a;topic-b;topic-c", cells[topicsIdx], "topics cell must render a joined string list, not a Go slice literal")
	require.NotContains(t, cells[statusIdx], "{")
	require.NotContains(t, cells[topicsIdx], "[")
}

// TestCsvCellStructFallsBackToPercentVWhenNoStateField covers csvCell's
// struct case when the struct has no State field at all: it must still fall
// back to the pre-existing %v rendering rather than panicking (FieldByName
// on a struct with no such field returns an invalid, zero Value — csvCell
// must check IsValid() before touching it).
func TestCsvCellStructFallsBackToPercentVWhenNoStateField(t *testing.T) {
	type noStateField struct {
		Name string               `json:"name"`
		Foo  struct{ Bar string } `json:"foo"`
	}
	out, err := rowsToCsv([]noStateField{{Name: "n", Foo: struct{ Bar string }{Bar: "baz"}}})
	require.NoError(t, err)
	lines := strings.Split(strings.TrimSpace(out), "\n")
	require.Len(t, lines, 2)
	// No panic and some rendering happened (exact %v format isn't the point
	// here, just that the struct-without-State case is handled safely).
	require.NotEmpty(t, lines[1])
}
