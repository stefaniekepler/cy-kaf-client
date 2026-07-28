package cluster

import "context"

// KsqlStatementKind identifies which KSQL REST endpoint executes a statement.
// SELECT queries use /query; all other supported statements use /ksql.
type KsqlStatementKind string

const (
	KsqlQuery     KsqlStatementKind = "query"
	KsqlStatement KsqlStatementKind = "ksql"
)

// KsqlCommand is a validated, classified statement ready for the KSQL REST
// adapter.  SQL is kept in its caller-supplied form so comments and quoted
// values are not rewritten between classification and execution.
type KsqlCommand struct {
	SQL               string
	StreamsProperties map[string]string
	Kind              KsqlStatementKind
}

// KsqlTable is one result frame emitted by a KsqlPort.  IsError distinguishes
// a KSQL execution error frame from a normal result frame without coupling the
// application layer to the wire protocol.
type KsqlTable struct {
	Header      string
	ColumnNames []string
	Values      [][]any
	IsError     bool
}

// KsqlStreamDescription mirrors the optional fields returned by LIST STREAMS.
// Pointers preserve the distinction between an absent field and an empty
// string for the generated API mapping layer.
type KsqlStreamDescription struct {
	Name, Topic, KeyFormat, ValueFormat *string
}

// KsqlTableDescription mirrors the optional fields returned by LIST TABLES.
type KsqlTableDescription struct {
	Name, Topic, KeyFormat, ValueFormat *string
	IsWindowed                          *bool
}

// KsqlPort is the narrow application-to-infrastructure boundary for ksqlDB.
// Implementations choose the REST resource from KsqlCommand.Kind and emit
// normalized result frames through emit in wire order.
type KsqlPort interface {
	ListStreams(context.Context, Definition) ([]KsqlStreamDescription, error)
	ListTables(context.Context, Definition) ([]KsqlTableDescription, error)
	Execute(context.Context, Definition, KsqlCommand, func(KsqlTable) error) error
}
