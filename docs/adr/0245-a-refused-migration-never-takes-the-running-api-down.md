# ADR 0245: 거부된 마이그레이션은 돌고 있는 API 를 내리지 않는다

- Status: Accepted
- Date: 2026-10-14

## Context

2026-10-14 08:09 UTC, #465 를 배포하던 자동 배포의 마이그레이션 단계가 실패했다(운영 DB 의 승인 설정이 아직 없어
드롭 마이그레이션이 설계대로 거부). 그런데 그 시점에 `foundation-api` 는 이미 새 컨테이너로 바뀌어 `Created` 상태로
멈춰 있었고, 운영자가 승인 설정을 넣고 복구할 때까지 약 30분 동안 API 가 내려가 있었다.

원인: `foundation-release.sh migrate` 는 `up -d --wait foundation-api` 하나로 체인 전체(postgres → bootstrap →
migrate → grants → finalize → api, 각 고리 `service_completed_successfully`)를 요구했다. compose 는 릴리스 이미지가
새것이면 API 컨테이너를 **먼저** 교체하고 그다음 의존 체인을 돌린다. 체인의 한 고리가 실패하면 새 API 컨테이너는
시작되지 않고, 옛 컨테이너는 이미 지워진 뒤다. 실제 compose(v5.2.0, 운영과 같은 판)로 재현했다(아래 시험의 대조군).

체인을 한 번만 적는다는 결정(가드 `the-deploy-does-not-restate-the-compose-chain`, 2026-09-02 grants 누락 사고)은
옳다. 문제는 순서가 아니라 "API 교체"가 체인 성공보다 앞선 것이다.

## Decision

1. `migrate` 는 두 단계다.
   - 먼저 `runtime_chain_before <wrapper> foundation-api`(`scripts/deploy/runtime-up-in-order.sh`): compose 의 해석된
     설정에서 API 가 `service_completed_successfully` 로 기다리는 one-shot 들을 읽어 `up -d` 하고, `compose wait` 로
     종료 코드를 확인한다. API 는 그 one-shot 들에 의존하는 쪽이라 이 단계는 API 를 건드리지 않는다. 하나라도 0 이
     아니면 실패하고 배포가 멈춘다. 옛 API 는 이전 릴리스로 계속 응답한다.
   - 그다음에야 기존 그대로 `up -d --wait foundation-api`.
2. 체인의 고리 이름은 배포 스크립트 어디에도 적지 않는다(compose 설정에서 읽는다). `--no-deps` 도 쓰지 않는다. 기존
   가드와 그 자가 시험이 그대로 통과한다.

## 증명

- `orchestration/tests/test_runtime_up_in_order.py`: 실제 docker compose 로 같은 모양의 작은 프로젝트(db, migrate,
  finalize, api)를 띄운다. 첫 배포 성공 → 마이그레이션을 실패로 심은 두 번째 배포는 거부되고, **API 컨테이너 ID 가
  같고 실행 중이며 이전 릴리스로 응답한다** → 마이그레이션이 통과하는 세 번째 배포는 API 를 교체한다. 대조군: 같은
  실패를 옛 순서(`up -d --wait api`)로 돌리면 API 가 내려간다 — compose 의 동작이 바뀌어 사고가 재현되지 않게 되면
  이 시험이 그렇게 말한다.

## Consequences

- 성공 경로에서는 체인이 두 번 시작된다(두 번째 `up` 이 끝난 one-shot 을 다시 돈다). 고리마다 멱등이라 결과는 같고,
  몇 초 늘어난다.
- 배포가 마이그레이션에서 멈춰도 서비스는 이전 릴리스로 남는다. 실패 알림과 DAG 처리(ADR-0173)는 그대로다.

## 기각한 대안

- **`up -d --no-deps foundation-api` 로 API 만 교체**: 가드가 막는 형태이고, 체인을 건너뛰는 길을 다시 연다.
- **블루/그린 API 두 벌**: 포트·신원 루프백·관측 대상이 둘이 된다. 이 결함은 순서만 바꾸면 사라진다.
- **마이그레이션을 이미지 밖 별도 단계로**: 마이그레이션이 릴리스 바이너리 안에 있다는 결정(이미지가 오래되면 옛
  마이그레이션 집합이 적용된 사고)을 되돌린다.
