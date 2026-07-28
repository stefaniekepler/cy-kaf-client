package ksql

import (
	"errors"
	"strings"
	"testing"
)

func TestClassifierAcceptsSingleStatements(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want StatementKind
	}{
		{"select", "select * from ORDERS emit changes;", StatementQuery},
		{"pull query", "SELECT * FROM ORDERS LIMIT 1", StatementQuery},
		{"create stream", "CREATE STREAM S AS SELECT * FROM T;", StatementKSQL},
		{"comment and quoted semicolon", "/* ; */ SELECT 'a;b' FROM T;", StatementQuery},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := (Classifier{}).Classify(tt.sql)
			if err != nil {
				t.Fatalf("Classify() error = %v", err)
			}
			if got != tt.want {
				t.Fatalf("Classify() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestClassifierRejectsInvalidOrForbiddenStatements(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want error
	}{
		{"empty", "", ErrEmptyStatement},
		{"semicolon only", "   ;", ErrParseStatement},
		{"multiple", "SELECT * FROM A; SELECT * FROM B;", ErrMultipleStatements},
		{"print", "PRINT 'topic';", ErrUnsupportedStatement},
		{"define", "DEFINE x = 'y';", ErrUnsupportedStatement},
		{"undefine", "UNDEFINE x;", ErrUnsupportedStatement},
		{"syntax", "SELECT FROM", ErrParseStatement},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := (Classifier{}).Classify(tt.sql); !errors.Is(err, tt.want) {
				t.Fatalf("Classify(%q) error = %v, want %v", tt.sql, err, tt.want)
			}
		})
	}
}

func TestClassifierHandlesCommentsAndDelimiterBoundaries(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{"line comment without delimiter", "SELECT * FROM T -- keep this comment"},
		{"block comment after delimiter", "SELECT * FROM T; /* trailing comment */"},
		{"comment only", "-- only a comment"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			kind, err := (Classifier{}).Classify(tt.sql)
			if tt.name == "comment only" {
				if !errors.Is(err, ErrEmptyStatement) {
					t.Fatalf("Classify() error = %v, want ErrEmptyStatement", err)
				}
				return
			}
			if err != nil || kind != StatementQuery {
				t.Fatalf("Classify() = (%q, %v), want (%q, nil)", kind, err, StatementQuery)
			}
		})
	}
}

func TestClassifierErrorsDoNotEchoSQL(t *testing.T) {
	secret := "SELECT 'do-not-log-me' FROM"
	_, err := (Classifier{}).Classify(secret)
	if err == nil {
		t.Fatal("Classify() returned nil error")
	}
	if strings.Contains(err.Error(), "do-not-log-me") {
		t.Fatalf("Classify() error contains SQL text: %q", err)
	}
}
