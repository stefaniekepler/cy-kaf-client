#!/usr/bin/env bash
# 从 kafbat/kafka-ui v1.5.0（固定 commit）vendoring 前端/契约/e2e/compose。
# 一次性脚本：仅在初始 vendoring 或上游基线升级时运行。需要 pnpm + java>=11。
set -euo pipefail
command -v java >/dev/null 2>&1 || { echo "FATAL: java 不在 PATH——先按 docs/adr/0001-dev-environment.md 导出 JAVA_HOME"; exit 1; }
PIN_COMMIT="afc9c918e13c4422268a3a5b7933c7b448746c82"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

git clone --depth 1 --branch v1.5.0 https://github.com/kafbat/kafka-ui.git "$WORK/upstream"
ACTUAL=$(git -C "$WORK/upstream" rev-parse HEAD)
[ "$ACTUAL" = "$PIN_COMMIT" ] || { echo "FATAL: tag v1.5.0 指向 $ACTUAL，与固定 commit 不符"; exit 1; }

echo "==> 编译 TypeSpec 契约并生成前端 TS client（上游自带脚本）"
(cd "$WORK/upstream/frontend" && pnpm install --frozen-lockfile && pnpm gen:sources)
CONTRACT="$WORK/upstream/contract-typespec/build/tsp/api/openapi.yaml"
[ -f "$CONTRACT" ] || { echo "FATAL: TypeSpec 编译产物缺失 $CONTRACT"; exit 1; }
[ -d "$WORK/upstream/frontend/src/generated-sources" ] || { echo "FATAL: generated-sources 未生成"; exit 1; }

echo "==> 拷贝到本仓库"
mkdir -p "$ROOT/contract" "$ROOT/deploy"
rsync -a --delete --exclude node_modules --exclude build --exclude coverage \
  "$WORK/upstream/frontend/" "$ROOT/frontend/"
cp "$CONTRACT" "$ROOT/contract/openapi.yaml"
rsync -a --delete "$WORK/upstream/e2e-playwright/" "$ROOT/e2e/"
rsync -a --delete "$WORK/upstream/documentation/compose/" "$ROOT/deploy/compose/"
cp "$WORK/upstream/LICENSE" "$ROOT/LICENSE"

# 上游 frontend/.gitignore 忽略 generated-sources；本仓库策略是入库（规格 §5.1）
sed -i '' '/generated-sources/d' "$ROOT/frontend/.gitignore" 2>/dev/null || \
  sed -i '/generated-sources/d' "$ROOT/frontend/.gitignore"
if grep -q 'generated-sources' "$ROOT/frontend/.gitignore"; then
  echo "FATAL: frontend/.gitignore 仍含 generated-sources 忽略行（sed 未生效，TS client 将无法入库）"; exit 1
fi

echo "==> 完成。请核对后 git add。"
