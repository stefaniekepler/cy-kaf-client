#!/usr/bin/env bash
# 覆盖率门禁：domain+app ≥90%，全仓(排除生成代码/cmd/scripts) ≥85%（规格 D13）
# 引导期豁免：domain/app 尚无代码时跳过 core 门禁（首个 domain 代码在 P0-T7 落地）。
set -euo pipefail
PROFILE="${1:-coverage.out}"
pct() { go tool cover -func="$1" | awk '/^total:/ {gsub(/%/,""); print $3}'; }

# Keep generated ANTLR grammar out of the aggregate, alongside the existing
# *.gen.go policy; handwritten KSQL code remains covered by the gate.
grep -v -e '\.gen\.go' -e '/internal/infra/ksql/grammar/' -e '^github.com/cy-kaf/cy-kaf-client/cmd/' -e '/scripts/' "$PROFILE" > coverage.filtered || true
TOTAL=$(pct coverage.filtered)
awk -v t="$TOTAL" 'BEGIN{exit !(t>=85)}' || { echo "FAIL: total coverage ${TOTAL}% < 85%"; exit 1; }

if grep -q -e '/internal/domain/' -e '/internal/app/' "$PROFILE"; then
  head -1 "$PROFILE" > coverage.core
  grep -e '/internal/domain/' -e '/internal/app/' "$PROFILE" >> coverage.core
  CORE=$(pct coverage.core)
  echo "coverage: total=${TOTAL}% core(domain+app)=${CORE}%"
  awk -v c="$CORE" 'BEGIN{exit !(c>=90)}' || { echo "FAIL: domain+app coverage ${CORE}% < 90%"; exit 1; }
else
  echo "coverage: total=${TOTAL}% core(domain+app)=skipped (no domain/app code yet)"
fi
