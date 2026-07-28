package ksql

import (
	"errors"
	"strings"
	"unicode"

	"github.com/antlr4-go/antlr/v4"
	"github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
	"github.com/cy-kaf/cy-kaf-client/internal/infra/ksql/grammar"
)

var (
	ErrEmptyStatement       = errors.New("ksql statement is empty")
	ErrMultipleStatements   = errors.New("multiple ksql statements are not allowed")
	ErrParseStatement       = errors.New("ksql statement could not be parsed")
	ErrUnsupportedStatement = errors.New("ksql statement is not supported")
)

// StatementKind is kept as an infrastructure alias so callers can use the
// domain contract without depending on generated parser types.
type StatementKind = cluster.KsqlStatementKind

const (
	StatementQuery = cluster.KsqlQuery
	StatementKSQL  = cluster.KsqlStatement
)

// Classifier validates one KSQL statement against the pinned ANTLR grammar.
// It intentionally has no mutable state and is safe to reuse concurrently.
type Classifier struct{}

// Classify accepts exactly one statement, with an optional final semicolon.
// PRINT, DEFINE and UNDEFINE parse successfully in the upstream grammar but
// are intentionally rejected because the KSQL REST API does not expose them
// through the UI command endpoint.
func (Classifier) Classify(sql string) (StatementKind, error) {
	if strings.TrimSpace(sql) == "" {
		return "", ErrEmptyStatement
	}

	// Lex once to distinguish a comment-only input from a statement and to
	// locate a trailing semicolon without inspecting SQL text with a regexp.
	stream, lexErrors := newTokenStream(sql)
	stream.Fill()
	if lexErrors.count != 0 {
		return "", ErrParseStatement
	}
	defaultTokens := defaultChannelTokens(stream.GetAllTokens())
	if len(defaultTokens) == 0 {
		return "", ErrEmptyStatement
	}

	// The upstream grammar requires a semicolon. Add one on a separate line
	// when it is omitted so a trailing -- comment cannot consume the delimiter.
	parseSQL := sql
	if defaultTokens[len(defaultTokens)-1].GetTokenType() != grammar.KsqlGrammarLexerT__0 {
		parseSQL += "\n;"
	}

	parseStream, parseLexErrors := newTokenStream(parseSQL)
	parser := grammar.NewKsqlGrammarParser(parseStream)
	parser.RemoveErrorListeners()
	parseStream.Fill()
	if parseLexErrors.count != 0 {
		return "", ErrParseStatement
	}
	parseErrors := &syntaxErrorCollector{DefaultErrorListener: antlr.NewDefaultErrorListener()}
	parser.AddErrorListener(parseErrors)
	root := parser.Statements()
	if parseErrors.count != 0 || parser.HasError() {
		return "", ErrParseStatement
	}

	statements := root.AllSingleStatement()
	if len(statements) == 0 {
		return "", ErrEmptyStatement
	}
	if len(statements) != 1 {
		return "", ErrMultipleStatements
	}

	switch statements[0].Statement().(type) {
	case *grammar.PrintTopicContext,
		*grammar.DefineVariableContext,
		*grammar.UndefineVariableContext:
		return "", ErrUnsupportedStatement
	case *grammar.QueryStatementContext:
		return StatementQuery, nil
	default:
		return StatementKSQL, nil
	}
}

// syntaxErrorCollector deliberately discards ANTLR's diagnostic text. Some
// diagnostics contain token text, which could include SQL or credentials.
type syntaxErrorCollector struct {
	*antlr.DefaultErrorListener
	count int
}

func (c *syntaxErrorCollector) SyntaxError(_ antlr.Recognizer, _ interface{}, _ int, _ int, _ string, _ antlr.RecognitionException) {
	c.count++
}

func newTokenStream(sql string) (*antlr.CommonTokenStream, *syntaxErrorCollector) {
	input := caseInsensitiveCharStream{CharStream: antlr.NewInputStream(sql)}
	lexer := grammar.NewKsqlGrammarLexer(input)
	lexer.RemoveErrorListeners()
	lexErrors := &syntaxErrorCollector{DefaultErrorListener: antlr.NewDefaultErrorListener()}
	lexer.AddErrorListener(lexErrors)
	return antlr.NewCommonTokenStream(lexer, antlr.TokenDefaultChannel), lexErrors
}

// caseInsensitiveCharStream mirrors KSQL's CaseInsensitiveStream: only
// lookahead is upper-cased, while GetText still returns the caller's original
// SQL. This keeps quoted identifiers and values intact for downstream clients.
type caseInsensitiveCharStream struct {
	antlr.CharStream
}

func (s caseInsensitiveCharStream) LA(offset int) int {
	value := s.CharStream.LA(offset)
	if value == 0 || value == antlr.TokenEOF {
		return value
	}
	return int(unicode.ToUpper(rune(value)))
}

func defaultChannelTokens(tokens []antlr.Token) []antlr.Token {
	filtered := make([]antlr.Token, 0, len(tokens))
	for _, token := range tokens {
		if token.GetTokenType() == antlr.TokenEOF || token.GetChannel() != antlr.TokenDefaultChannel {
			continue
		}
		filtered = append(filtered, token)
	}
	return filtered
}
