package api

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

// rowsToCsv reflects rows (which must be a []T where T is a struct, e.g.
// []generated.Broker) into CSV text: header row from T's exported fields'
// json tag names (struct declaration order, ",omitempty" stripped — a
// hand-maintained column list would risk silently drifting from the JSON
// shape as the contract evolves), one data row per element. Shared by every
// "export as CSV" endpoint (brokers today via GetBrokersCsv; Task 4/6 add
// topics/groups on top of this same function) so the reflection machinery
// isn't duplicated per DTO.
func rowsToCsv(rows any) (string, error) {
	v := reflect.ValueOf(rows)
	if v.Kind() != reflect.Slice {
		return "", fmt.Errorf("rowsToCsv: %T is not a slice", rows)
	}
	t := v.Type().Elem()
	if t.Kind() != reflect.Struct {
		return "", fmt.Errorf("rowsToCsv: element type %s is not a struct", t)
	}

	var cols []string
	var fields []int
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if f.PkgPath != "" { // unexported: no json tag ever applies to it
			continue
		}
		tag := f.Tag.Get("json")
		if tag == "-" {
			continue
		}
		name := strings.Split(tag, ",")[0]
		if name == "" {
			name = f.Name
		}
		cols = append(cols, name)
		fields = append(fields, i)
	}

	var buf bytes.Buffer
	cw := csv.NewWriter(&buf)
	if err := cw.Write(cols); err != nil {
		return "", err
	}
	for i := 0; i < v.Len(); i++ {
		row := v.Index(i)
		cells := make([]string, len(fields))
		for j, idx := range fields {
			cells[j] = csvCell(row.Field(idx))
		}
		if err := cw.Write(cells); err != nil {
			return "", err
		}
	}
	cw.Flush()
	if err := cw.Error(); err != nil {
		return "", err
	}
	return buf.String(), nil
}

// csvCell renders one reflected struct field as a CSV cell: nil pointers
// become "" (contract fields are mostly optional/pointer), everything else is
// dereferenced (if a pointer) and formatted by kind. float32 is formatted at
// bitSize 32, not 64: a float32 value's exact float64 conversion is usually
// not itself exactly representable in decimal, so formatting it at 64-bit
// precision reintroduces noise nobody typed — e.g. float32(0.1) would render
// as "0.10000000149011612" instead of "0.1" (see TestRowsToCsvFloat32UsesBitSize32).
//
// Struct and slice/array fields (e.g. FullConnectorInfo.Status, a nested
// ConnectorStatus{State,Trace,WorkerId} struct; Topics, a *[]string) used to
// fall through to the default %v branch below, which renders Go's own
// struct/slice syntax verbatim into the CSV cell (e.g. "{RUNNING <nil>
// <nil>}", "[a b c]") — meaningless to a human reading the exported CSV and a
// leak of Go-internal representation into a user-facing artifact. Struct
// renders its State field if it has one (every nested status-shaped struct
// in this contract does); slice/array renders each element (recursively,
// so a nested pointer/struct/slice element still gets this same treatment)
// joined by ";" (a plain "," would collide with a multi-value cell needing
// its own CSV-level quoting, and csv.Writer already quotes a cell containing
// "," for us when needed elsewhere in this file — ";" avoids that ambiguity
// entirely for the common case of a handful of short string tokens).
func csvCell(v reflect.Value) string {
	if v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return ""
		}
		v = v.Elem()
	}
	switch v.Kind() {
	case reflect.String:
		return v.String()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return strconv.FormatInt(v.Int(), 10)
	case reflect.Float32:
		return strconv.FormatFloat(v.Float(), 'f', -1, 32)
	case reflect.Float64:
		return strconv.FormatFloat(v.Float(), 'f', -1, 64)
	case reflect.Bool:
		return strconv.FormatBool(v.Bool())
	case reflect.Struct:
		if state := v.FieldByName("State"); state.IsValid() && state.Kind() == reflect.String {
			return state.String()
		}
		return fmt.Sprintf("%v", v.Interface())
	case reflect.Slice, reflect.Array:
		if v.Len() == 0 {
			return ""
		}
		parts := make([]string, v.Len())
		for i := 0; i < v.Len(); i++ {
			parts[i] = csvCell(v.Index(i))
		}
		return strings.Join(parts, ";")
	default:
		return fmt.Sprintf("%v", v.Interface())
	}
}
