---
status: current
owner: foundation-platform
doc_type: runbook
last_reviewed: 2026-10-08
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
| 2. 굽기 | 운영 버킷에 1세대 굽기, 필지 레인 잠금(`…/by-pnu-bake/parcel/lane.lock`)을 잡고 샤드 계획대로 | §2 | 객체 |
| 3. 관문 (가) | `verify-parcel-by-pnu-section-pack-equality`: 모든 PNU 가 객체와 바이트 단위로 같은지 | §3 | 객체 |
| 4. 관문 (나) | 미리보기 Worker(`wrangler.parcel.jsonc --env preview`)로 표본 첫 조회 시간 비교 | §4 | 객체 |
| 5. 발행 | `publish-parcel-by-pnu-section-packs`: manifest 에 `section_packs` 블록 | §5 의 2단계 | 객체 |
| 6. 묶음 | `by-pnu-gateway-canary.sh parcel --execute packs <1단계 버전>` | §5 의 3단계 | 묶음 |
| 7. 감시 | 아래 §2 | §6 | 묶음 |
| 8. 예약 굽기 | `foundation-by-pnu-serving-bake.service` 의 인자를 `building` 에서 `all` 로 (PR) | §7 | 묶음 |

1단계는 운영 Worker 배포라 사람이 실행한다. 2단계는 R2 쓰기 비용이 든다. 실행 전에 예상 금액을 적고 승인을 받는다.
쓰기 수는 법정동 묶음 수와 같다.

8단계 전까지 예약 굽기는 필지를 굽지 않는다. 그동안 필지 패널은 마지막으로 구운 객체(또는 묶음)를 서빙한다.

## 2. 감시

```bash
echo 'FOUNDATION_PLATFORM_PARCEL_BY_PNU_SERVING_MONITOR_SAMPLE_PATH=<WORK>/equality.json' |
  sudo install -m 0640 -o root -g foundation-platform /dev/stdin /etc/foundation-platform/parcel-serving-monitor.env
sudo systemctl enable --now foundation-by-pnu-serving-monitor@parcel.timer
```

한계와 경보는 계약 `parcel_by_pnu_gateway.section_packs.monitor` 가 정본이다.
