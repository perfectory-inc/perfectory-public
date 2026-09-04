#!/usr/bin/env bash
# 공짱 로컬 데모 한 방 기동 (2026-09-04 의 검문소 전부 반영).
#
# 세우는 것: Zitadel(18443, 첫 부팅 시 PAT 볼륨 권한까지) + valkey(6379) + 데모 DB(15436,
# 마이그레이션 포함) + gongzzang-api(8090) + web(3010). 사용자 파일은 복사하지 않고
# 실행 시점 env 주입만 한다. 데모 명부 시드의 id 는 반드시 유효한 ULID 여야 한다
# (Crockford base32 — I·L·O·U 금지; 오늘 이걸로 전 화면 500 을 맞았다).
#
# 선행: Docker 데몬 살아 있을 것(죽어 있으면 좀비 백엔드 정리 + 잠긴 로그 rename 후
# 한 번만 재기동 — this-host 메모리), 3010/8090/18443/15433/15436/6379 비어 있을 것.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
USER_TREE="${GONGZZANG_USER_TREE:-C:/Users/admin/Desktop/perfectory/products/gongzzang}"
Z="$USER_TREE/infra/zitadel"

echo "==> [1/6] Zitadel 스택 (기존 .env 재사용; 없으면 먼저 만들 것 — README 의 .env.example)"
[ -f "$Z/.env" ] || { echo "ERROR: $Z/.env 없음 — ZITADEL_* 값을 먼저 채워라"; exit 1; }
docker volume create zitadel_zitadel-init-pat >/dev/null
# 첫 부팅 함정: zitadel(uid 1000)이 PAT 볼륨에 못 쓰면 셋업 전체가 조용히 치명 실패한다.
docker run --rm -v zitadel_zitadel-init-pat://p alpine chown -R 1000:1000 //p
( cd "$Z" && docker compose --env-file .env up -d )

echo "==> [2/6] 데모 DB (15436)"
docker rm -f gz-dev-db 2>/dev/null || true
docker run -d --name gz-dev-db -p 127.0.0.1:15436:5432 \
  -e POSTGRES_USER=gongzzang -e POSTGRES_PASSWORD=gz-dev-only -e POSTGRES_DB=gongzzang \
  postgis/postgis:17-3.5-alpine >/dev/null
for i in $(seq 1 20); do docker exec gz-dev-db pg_isready -U gongzzang >/dev/null 2>&1 && break; sleep 2; done
( cd "$ROOT" && DATABASE_URL="postgres://gongzzang:gz-dev-only@127.0.0.1:15436/gongzzang" \
    bash scripts/sqlx-migrate.sh >/dev/null )
echo "    migrations applied"

echo "==> [3/6] Zitadel 준비 대기 + 프로젝트/앱 등록 (멱등)"
for i in $(seq 1 60); do curl -sf http://localhost:18443/debug/healthz >/dev/null 2>&1 && break; sleep 3; done
bash "$Z/init-zitadel.sh" | tail -3
CLIENT_ID="$(bash "$Z/init-zitadel.sh" 2>/dev/null | grep -oE "CLIENT_ID=[0-9]+" | head -1 | cut -d= -f2)"
echo "    CLIENT_ID=$CLIENT_ID"

echo "==> [4/6] 데모 명부 시드 (관리자 sub → user 행; id 는 유효 ULID)"
SUB="$(docker run --rm -v zitadel_zitadel-init-pat://p alpine cat //p/pat | tr -d '\r\n' | { read -r T; \
  curl -s -X POST http://localhost:18443/management/v1/users/_search \
    -H "Authorization: Bearer $T" -H "Content-Type: application/json" -d "{}" ; })"
ADMIN_SUB="$(printf '%s' "$SUB" | python3 -c "import sys,json; d=json.load(sys.stdin); print(next(u['id'] for u in d['result'] if u['userName'].startswith('admin')))")"
docker exec gz-dev-db psql -U gongzzang -d gongzzang -q -c "
  INSERT INTO \"user\" (id, zitadel_sub, email, display_name, user_kind, roles, last_login_at)
  VALUES ('01K4ADM1NDEM0SEED000000000', '$ADMIN_SUB', 'demo-admin@localhost.invalid', 'Admin',
          'individual', ARRAY['Buyer','Admin'], now())
  ON CONFLICT (zitadel_sub) DO NOTHING;" 2>/dev/null || \
docker exec gz-dev-db psql -U gongzzang -d gongzzang -q -c "
  INSERT INTO \"user\" (id, zitadel_sub, email, display_name, user_kind, roles, last_login_at)
  SELECT '01K4ADM1NDEM0SEED000000000', '$ADMIN_SUB', 'demo-admin@localhost.invalid', 'Admin',
         'individual', ARRAY['Buyer','Admin'], now()
  WHERE NOT EXISTS (SELECT 1 FROM \"user\" WHERE zitadel_sub='$ADMIN_SUB');"
echo "    seeded sub=$ADMIN_SUB"

echo "==> [5/6] gongzzang-api (8090)"
( set -a; . "$USER_TREE/.env"; set +a
  export DATABASE_URL="postgres://gongzzang:gz-dev-only@127.0.0.1:15436/gongzzang"
  export API_LISTEN_ADDR=127.0.0.1:8090
  export ZITADEL_ISSUER=http://localhost:18443
  export ZITADEL_AUDIENCE="$CLIENT_ID"
  export REDIS_URL=redis://localhost:6379
  cd "$ROOT" && exec cargo run -p gongzzang-api --bin gongzzang-api ) > /tmp/gz-demo-api.log 2>&1 &
for i in $(seq 1 60); do curl -sf http://127.0.0.1:8090/healthz >/dev/null 2>&1 && break; sleep 3; done
echo "    api healthy"

echo "==> [6/6] web (3010)"
( set -a; . "$USER_TREE/apps/web/.env.local"; set +a
  export NEXT_PUBLIC_API_BASE_URL=http://127.0.0.1:8090
  export ZITADEL_ISSUER=http://localhost:18443
  export ZITADEL_CLIENT_ID="$CLIENT_ID"
  export ZITADEL_AUDIENCE="$CLIENT_ID"
  export ZITADEL_REDIRECT_URI=http://localhost:3010/api/auth/callback
  export REDIS_URL=redis://localhost:6379
  cd "$ROOT/apps/web" && exec pnpm exec next dev -p 3010 ) > /tmp/gz-demo-web.log 2>&1 &

cat <<DONE

================================================================
데모 준비 완료:  http://localhost:3010
로그인: admin@zitadel.localhost / (비밀번호: $Z/.env 의 ZITADEL_ADMIN_PASSWORD)
주의: 3010 리다이렉트가 앱에 등록돼 있어야 한다 (init 후 1회 API 로 추가).
로그: /tmp/gz-demo-api.log, /tmp/gz-demo-web.log
================================================================
DONE
