#!/usr/bin/env bash
# Intelligence 워크스페이스가 벡터/임베딩 provider를 의존성으로 고정하지 못하게 막는다.
#
# 위협 모델(루트 ADR-0027): 이것은 honest-mistake detection이다. Intelligence
# ADR-0002가 승인 조건으로 둔 "승인되지 않은 embedding provider 또는 vector index를
# 코드에 고정 금지"를 별도 ADR 없이 어기는 정직한 실수를 막는다. 이름을 바꿔 우회하는
# 유지보수자, vendored 소스, 런타임 HTTP 호출은 막지 못한다.
#
# 매니페스트의 의존성 키만 본다. 산문·주석·문자열 리터럴은 보지 않는다 —
# chat_policy.rs의 한국어 맞춤법 허용 목록(ALLOWED_LATIN_TERMS)에 qdrant·pgvector가
# 들어 있고 그것은 결정이 아니라 출력 검증기의 데이터다. 이름을 언급했다는 이유로
# 실패하면 가드가 거짓 양성을 만든다.
set -euo pipefail

ROOT="${1:-platforms/intelligence-platform}"

FORBIDDEN='^(qdrant|qdrant-client|pgvector|lancedb|lance|milvus|weaviate|weaviate-client|meilisearch|tantivy|opensearch|elasticsearch|fastembed|candle-core|candle-transformers|tiktoken-rs|async-openai|openai-api-rs)$'

violations=0
while IFS= read -r manifest; do
  # [dependencies] 계열 테이블 안의 왼쪽 키만 뽑는다.
  keys="$(awk '
    /^\[/ { intable = ($0 ~ /^\[(workspace\.)?(dependencies|dev-dependencies|build-dependencies)\]/) ; next }
    intable && /^[A-Za-z0-9_-]+[[:space:]]*=/ { sub(/[[:space:]]*=.*/, ""); print }
  ' "$manifest")"

  while IFS= read -r key; do
    [ -n "$key" ] || continue
    if printf '%s' "$key" | grep -Eq "$FORBIDDEN"; then
      echo "FAIL: $manifest declares forbidden dependency '$key'" >&2
      echo "      Intelligence ADR-0002 forbids pinning an unapproved embedding provider or vector index." >&2
      echo "      Write the engine ADR first, then update docs/technology-stack.md and this guard in the same commit." >&2
      violations=$((violations + 1))
    fi
  done <<< "$keys"
done < <(find "$ROOT" -name Cargo.toml -not -path '*/target/*')

if [ "$violations" -gt 0 ]; then
  exit 2
fi
echo "OK: no vector or embedding provider is pinned under $ROOT"
