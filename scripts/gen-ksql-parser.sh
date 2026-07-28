#!/usr/bin/env bash
set -euo pipefail

# Regenerate the checked-in parser with ANTLR 4.13.1. The generated files are
# committed so normal Go builds do not require Java or network access.
repo_root="$(cd "$(dirname "$0")/.." && pwd)"
grammar_dir="$repo_root/internal/infra/ksql/grammar"
java_bin="${JAVA_BIN:-${JAVA_HOME:+$JAVA_HOME/bin/java}}"
java_bin="${java_bin:-java}"
tmp_jar="$(mktemp)"
trap 'rm -f "$tmp_jar"' EXIT
curl -fsSL "https://www.antlr.org/download/antlr-4.13.1-complete.jar" -o "$tmp_jar"
rm -f "$grammar_dir"/ksqlgrammar_*.go
"$java_bin" -jar "$tmp_jar" -Dlanguage=Go -visitor -package grammar \
  -o "$grammar_dir" "$grammar_dir/KsqlGrammar.g4"

# KsqlGrammar.g4 is shared with Kafka UI's Java parser and therefore carries
# its Java package header. Remove that one line from Go output while retaining
# the grammar source verbatim and the package selected above.
for generated in "$grammar_dir"/ksqlgrammar_*.go; do
	awk '$0 != "package ksql;" && $0 !~ /^[[:space:]]*public static final int (COMMENTS|WHITESPACE|DIRECTIVES) =/ { print }' "$generated" > "$generated.tmp"
	mv "$generated.tmp" "$generated"
	if [[ "$(basename "$generated")" == "ksqlgrammar_parser.go" ]]; then
		LC_ALL=C perl -pi -e 's/(?<!antlr\.)\bParserRuleContext\b/antlr.ParserRuleContext/g' "$generated"
	fi
	LC_ALL=C perl -pi -e 's#^// Code generated from .*/KsqlGrammar\.g4 by ANTLR#// Code generated from KsqlGrammar.g4 by ANTLR#' "$generated"
done

# ANTLR's interpreter/token sidecars are useful during generation but are not
# consumed by the Go runtime and would make the checked-in output non-minimal.
rm -f "$grammar_dir"/KsqlGrammar.interp "$grammar_dir"/KsqlGrammar.tokens \
	"$grammar_dir"/KsqlGrammarLexer.interp "$grammar_dir"/KsqlGrammarLexer.tokens

gofmt -w "$grammar_dir"/ksqlgrammar_*.go
