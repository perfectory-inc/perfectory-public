---
status: current
owner: foundation-platform
doc_type: runbook
last_reviewed: 2026-10-09
---

# 필지 by-PNU 묶음 파일 전환 런북

필지 상세 문서를 PNU 하나당 R2 객체에서 법정동별 묶음 파일로 옮기는 절차다. 결정은
[루트 ADR-0147](../../../../docs/adr/0147-by-pnu-documents-are-served-from-section-packs.md) §8(필지 순서),
[ADR-0151](../../../../docs/adr/0151-building-packs-serve-each-document-whole.md)(문서 통째),
[ADR-0160](../../../../docs/adr/0160-one-by-pnu-gateway-source-serves-both-lanes.md)(한 Worker 소스, 레인별 설정)이다.

**절차는 [건물 전환 런북](building-section-pack-cutover.md)과 같다.** 명령·환경변수·스크립트가 레인 이름만 바꿔
그대로 쓰인다. 이 문서는 필지에서 달라지는 것과 순서만 적는다. 각 단계의 판정 기준·되돌리기·비용 설명은 건물 런북의
같은 절을 따른다.

## 0. 레인에서 달라지는 것

| 항목 | 건물 | 필지 |
|---|---|---|
| 계약 블록 | `building_by_pnu_gateway` | `parcel_by_pnu_gateway` |
| 발행기 명령 | `*-building-*` | `*-parcel-*` (`export-parcel-by-pnu-section-packs`, `check-parcel-gateway-version-health`, …) |
| 환경변수 접두어 | `FOUNDATION_PLATFORM_BUILDING_BY_PNU_SERVING_` | `FOUNDATION_PLATFORM_PARCEL_BY_PNU_SERVING_` |
| 묶음 위치 | `serving/buildings/packs/` | `serving/parcels/packs/` |
| Wrangler 설정 | `wrangler.building.jsonc` | `wrangler.parcel.jsonc` |
| 운영 Worker·주소 | 계약 `building_by_pnu_gateway` | 계약 `parcel_by_pnu_gateway` 의 `worker_name`·`public_hostname` |
| 미리보기 Worker | 계약 `…section_packs.preview_worker` | 같은 열쇠, 필지 블록 |
| 단계 배포 | `by-pnu-gateway-canary.sh building …` | `by-pnu-gateway-canary.sh parcel …` |
| 단계 판정 | `by-pnu-gateway-health.sh building …` | `by-pnu-gateway-health.sh parcel …` |
| 매시 감시 | `foundation-by-pnu-serving-monitor@building.timer` | `foundation-by-pnu-serving-monitor@parcel.timer` |
| 감시 환경 파일 | 비밀 계약의 `building-serving-monitor` | 비밀 계약의 `parcel-serving-monitor` |
| 검증 재기반(rebase) | 묶음 서빙 뒤 거부 | 같음 |

필지에는 `?schema=2` 같은 질의가 없다(계약 `request_path.accepted_queries` 가 빈 목록).

## 1. 순서

| 단계 | 하는 일 | 건물 런북 | 사용자가 읽는 것 |
|---|---|---|---|
| 1. 코드 | 이 소스의 필지 Worker 를 묶음 스위치 꺼진 채 단계 배포: `by-pnu-gateway-canary.sh parcel --execute code` | §5 의 1단계 | 객체 |
| 2. 굽기 | 운영 버킷에 세대 굽기: `gold-rebuild` 로 Gold 를 새로 만든 뒤 `bake <세대>` (아래 1-1) | §2 | 객체 |
| 3. 관문 (가) | `verify-parcel-by-pnu-section-pack-equality`: 모든 PNU 가 객체와 바이트 단위로 같은지 | §3 | 객체 |
| 4. 관문 (나) | 미리보기 Worker(`wrangler.parcel.jsonc --env preview`)로 표본 첫 조회 시간 비교 | §4 | 객체 |
| 5. 발행 | `publish-parcel-by-pnu-section-packs`: manifest 에 `section_packs` 블록 | §5 의 2단계 | 객체 |
| 6. 묶음 | `by-pnu-gateway-canary.sh parcel --execute packs <1단계 버전>` | §5 의 3단계 | 묶음 |
| 7. 감시 | 아래 §2 | §6 | 묶음 |
| 8. 예약 굽기 | `foundation-by-pnu-serving-bake.service` 의 인자를 `building` 에서 `all` 로 (PR) | §7 | 묶음 |

1단계는 운영 Worker 배포라 사람이 실행한다. 2단계는 R2 쓰기 비용이 든다. 실행 전에 예상 금액을 적고 승인을 받는다.
쓰기 수는 법정동 묶음 수와 같다.

8단계 전까지 예약 굽기는 필지를 굽지 않는다. 그동안 필지 패널은 마지막으로 구운 객체(또는 묶음)를 서빙한다.

## 1-1. 루트 단계 (루트 ADR-0161, ADR-0166)

Gold 재생성·굽기·관문·발행·감시 표본·단계 판정은 운영자 계정이 허용된 스크립트로 실행한다(첫 설치:
`sudo /opt/perfectory-control/current/platforms/foundation-platform/scripts/deploy/foundation-release.sh operator-access <계정>`).

```bash
op=/opt/perfectory-control/current/platforms/foundation-platform/scripts/ops/by-pnu-pack-operator.sh
sudo -n $op parcel gold-rebuild      # 두 패널 Gold 를 무조건 다시 만든다(이유는 고정), 예약 굽기·묶음 굽기 중에는 거부
sudo -n $op parcel status 1          # 재생성이 끝났는지(foundation-gold-panel-rebuild-unconditional)
sudo -n $op parcel bake 1            # 세대 굽기, 레인 잠금 아래 분리 실행; 실패하면 같은 명령이 이어 굽는다
sudo -n $op parcel equality 1        # 관문 (가), 분리 실행
sudo -n $op parcel latency 1         # 관문 (나), 미리보기 Worker 를 올린 뒤
sudo -n $op parcel status 1          # 두 실행의 상태와 로그 끝
sudo -n $op parcel publish 1         # 두 관문이 통과한 뒤
sudo -n $op parcel monitor-sample 1  # 감시·단계 판정의 표본 파일
# 단계 배포: CANARY_HEALTH_COMMAND='ssh <host> sudo -n '$op' parcel health @NEW@ @OLD@'
```

굽기(`bake`)가 하는 일은 `scripts/ops/by-pnu-pack-bake.sh` 머리말이 정본이다.

- 샤드는 1..9 에서 시작해 수출기가 행 상한(900,000행)으로 거부한 샤드만 한 자리 긴 열 개로 나눈다(예약 굽기와 같은
  `scripts/ops/by-pnu-bake-shards.sh`). 샤드를 손으로 고르지 않는다.
- 첫 샤드의 Gold 스냅숏이 나머지를 고정한다. 굽는 동안 Gold 가 바뀌면 굽기가 멈추고 그 세대는 이어 구울 수 없다.
  그래서 `gold-rebuild` 가 끝난 뒤 시작하고, 둘은 서로 돌고 있으면 거부한다.
- 동시 샤드 수와 시도 수는 `by-pnu-bake-shards.sh` 의 `BY_PNU_PACK_BAKE_WORKERS`·`BY_PNU_PACK_BAKE_ATTEMPTS` 다.
  유닛 메모리 상한은 예약 굽기 유닛의 `MemoryMax` × 동시 샤드 수다. 동시 4샤드의 실제 최대 메모리는 아직 재지 않았다.
  감독 실행처럼 예약 DAG 를 멈추고 돌린다.
- 결과는 `<작업>/logs/bake.log`(샤드마다 한 줄, 마지막에 `complete` 또는 `FAILED`), 시도별 출력은
  `<작업>/shards/shard-<앞자리>.attempt-<n>.log`. 발행하지 않는다.

## 2. 감시

표본 파일은 위의 `monitor-sample` 이 쓴다. 그 뒤 타이머를 켠다.

```bash
sudo systemctl enable --now foundation-by-pnu-serving-monitor@parcel.timer
```

한계와 경보는 계약 `parcel_by_pnu_gateway.section_packs.monitor` 가 정본이다.
