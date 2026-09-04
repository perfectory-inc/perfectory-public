# ADR 0082: 필지 미러의 전국 주장은 계약에서 온다

- Status: Accepted
- Date: 2026-09-05
- Supersedes: [ADR-0030](./0030-parcel-publication-evidence-requires-two-distinct-approvals.md) §1의
  check-report 간접 참조를 **shapefile 레인에 한해** (승인 산출물 자체와 그 필드 계약은 유지)

## Context

전국 필지 발행을 실제로 걸어 보니 설계된 사슬이 두 관절에서 끊겨 있었다 (2026-09-05 실측):

1. **국가 승격 기계는 API 페이지 레인 전용이다.** bronze object manifest 는
   `bronze/source=…/page-NNNNNN.json` 꼴의 키만 받고
   (`national_bronze_object_manifest.rs:631-644`), 샤드 내보내기는 객체 전체를 JSON 으로
   파싱한다. 실제 필지 원천은 ADR-0067 이 확정한 shapefile zip 273개이며, 이 레인은
   구조적으로 진입이 불가능하다.
2. **전국 스코프 run 을 쓰는 명령이 없다.** 국가 미러 재구성기는 run 에
   `{"kind":"bounded","complete":false}` 를 하드코딩하고
   (`postgis_parcel_boundary_mirror_national_rebuild.rs`), 증거 작성기는
   `national+complete+무제한` 을 요구한다. DB CHECK 까지 셋이 상호 배타라 누구도 봉인에
   도달할 수 없었다.

한편 shapefile 레인은 이미 완비돼 있다: ADR-0067 의 원천 계약
(`vworld-parcel-source-objects.json`, 시군구 255 실측 명세), R2 의 실핸드오프
(`silver-handoff/vworldkr__parcel/` 510객체), Silver 39,861,511행, 그리고 ADR-0025/0026 의
봉인·발행 기계.

## Decision

1. **`rebuild-postgis-parcel-boundary-mirror-national-from-contract` 를 추가한다.**
   입력은 ADR-0067 원천 계약 하나다: 시군구 대역 전체(부분집합이면 거부)에서 핸드오프
   키를 파생하고(목록을 두 번 적지 않는다), 운영자가 실측한 전국 행수
   (`…_EXPECTED_ROW_COUNT`, 필수)와 복사 행수가 다르면 실패한다.
2. **전국 주장은 세 사실의 교집합일 때만 쓴다**: 계약의 시군구 대역 전체를 처리했고,
   복사 행수가 운영자 실측 전국 행수와 같고, 지명한 스냅샷이 Iceberg REST 기준
   **현재** 스냅샷일 때(과거 스냅샷의 미러는 존재 자체를 거부). 이때만 run 에
   `{"kind":"national","complete":true}` + 무제한을 쓴다. bounded 레인은 캡 그대로다.
3. **객체별 기대 행수는 선택값이 된다.** API 레인 증거는 객체별 수를 실으므로 그대로
   검증하고, 계약 레인은 전국 총합으로 판정한다(계약에는 객체별 행수가 없고, 지어내지
   않는다). 봉인자가 어차피 전 행을 다시 세고 다이제스트로 대조한다.
4. **shapefile 레인의 증거 작성기는 승인 산출물을 직접 검증한다**
   (`…_NATIONAL_ROLLOUT_APPROVAL_PATH`). ADR-0030 §1 의 check report 는 API 수집 레인의
   사전 증명 6종을 승인에 결속하는 경계였다 — 그 6종은 이 레인의 것이 아니며, 이 레인의
   등가물(계약 완전성·현재 스냅샷 게이트·무결점 품질 보고·다이제스트 동일성·CAS 관문)은
   하류 기계가 이미 강제한다. 산출물의 필드 계약은 계속 승인 모듈이 소유하고, 검증 실패
   조건 각각을 시험이 심어 거부를 증명한다. 생산 컷오버 센티널(ADR-0030 §2)은 그대로다.
5. 핸드오프가 `.gz` 면 COPY 전에 해제한다(계약의 suffix 가 정본이다).

## Consequences

- 전국 필지 발행이 처음으로 완주 가능해진다: 계약 미러 → 증거 → 봉인 → 발행 →
  승격(ADR 직전에 추가된 promote-parcel-boundary-runtime).
- API 페이지 레인의 8단계 계획 기계는 필지 발행 경로에서 제외된다 — 그 레인의
  용도(API 수집 계획·집행)로만 남는다.
- 운영자 실측 행수가 새 입력이 된다. 잘못 적으면 적재가 실패할 뿐 잘못 발행되지는
  않는다(봉인자의 독립 재계수가 이중 방벽).
- 남는 부채: run 의 provenance 쌍(`source_record`/`file_asset`)을 만드는 명령은 여전히
  없다(승격 때와 같은 격차) — 실값 손 삽입으로 진행하고 명령화는 별도 결정.
