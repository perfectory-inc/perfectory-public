---
status: current
owner: gongzzang-제품
doc_type: README
last_reviewed: 2026-11-05
---

# 로컬 Docker Compose 인프라

로컬 개발용 PostgreSQL + PostGIS 와 Valkey 컨테이너예요. 이미지 태그·digest는 `docker-compose.yml`에
있고, 정본은 [`tools/technology-versions.contract.json`](../../../../tools/technology-versions.contract.json)이에요.

## 사전 준비

- Docker Desktop이 설치돼 있어야 해요. (`docker --version`으로 확인)
- `.env` 파일을 만들어 주세요. `.env.example`을 복사하면 돼요. `POSTGRES_PASSWORD`는 필수예요.

```bash
cp infrastructure/docker/.env.example infrastructure/docker/.env
```

## 기동

```bash
docker compose -f infrastructure/docker/docker-compose.yml --env-file infrastructure/docker/.env up -d
```

## 중지

```bash
docker compose -f infrastructure/docker/docker-compose.yml down
```

## 데이터 초기화 (볼륨까지 삭제)

```bash
docker compose -f infrastructure/docker/docker-compose.yml down -v
```

## 접속

```bash
docker exec -it gongzzang-postgres psql -U gongzzang
```

## 헬스체크

```bash
docker compose -f infrastructure/docker/docker-compose.yml ps
```

`postgres`, `valkey` 두 서비스 모두 `healthy`로 보이면 정상이에요.
두 서비스의 공개 포트는 호스트의 `127.0.0.1`에만 바인딩돼요 — 기본 `POSTGRES_HOST_PORT=15432`,
`VALKEY_HOST_PORT=6379`.

## 주의 사항

- `.env`는 `.gitignore`에 포함돼 있어요. 절대 커밋하지 마세요.
- `.env.example`의 비밀번호(`changeme_local_only`)는 로컬 전용이에요. **운영 환경에서 절대 사용하지 마세요.**
- 본 컴포즈는 로컬 개발 한정이에요.
