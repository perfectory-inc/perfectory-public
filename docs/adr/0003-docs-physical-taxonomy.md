# ADR 0003: Docs physical taxonomy

> Amended by [ADR-0007](./0007-public-code-private-operations-boundary.md): historical archive,
> review, and handoff trees remain in the private transition archive and are not part of the public
> canonical code tree.

- Status: Accepted
- Date: 2026-07-20

## Context

ADR-0002가 문서의 분류(current|archived)와 불변·배너·전역 번호 원칙을 세웠지만,
물리 배치는 각 영역의 탄생기 구조 그대로여서 같은 성격의 문서가 여러 경로로
갈라져 있었고, 새 문서를 어디에 둘지가 매번 재판단 대상이었다. 한편 도구·CI가
경로로 참조하는 디렉토리
(architecture의 pin·boundary JSON, openapi 산출물, adr 번호 인용)는 이동하면 배선이
끊어진다. 배치 규칙 없이 정리를 반복하면 이 배선을 깨는 사고가 재발한다.

## Decision

1. **소유권: 문서는 설명하는 코드 옆에 둔다** (docs-as-code, Google g3doc 원칙).
   루트 `docs/`는 모노레포 횡단 문서만 담는다 — 전역 ADR 시퀀스, 횡단 아키텍처,
   루트 수준 기록. 영역 `docs/`는 그 영역 것만 담는다. 여러 영역에 걸치는 문서는
   소유 영역에 정본을 두고 나머지는 링크한다 (ADR-0002 ⑥).

2. **공개 표준 골격 7종.** 모든 영역 `docs/`는 같은 스켈레톤으로 수렴한다:

   | 항목 | 정의 (1줄) |
   | --- | --- |
   | `README.md` | 그 영역 문서의 지도 — 진입점·색인 |
   | `adr/` | 결정 기록 — 영역 시퀀스는 동결, 신규 번호는 루트 전역 시퀀스 (ADR-0002 ⑤) |
   | `architecture/` | 시스템 설명 + 코드에 배선된 계약(pin·boundary·registry JSON) |
   | `openapi/` | 생성 산출물 (`<name>.v<major>.json`, ADR-0001 §8) — HTTP API를 내는 영역만 |
   | `runbooks/` | 운영 절차 — 배포·장애·정기 작업 |
   | `guides/` | 개발 how-to — 셋업·테스트·기여 절차 |
   | `reference/` | 용어·스키마·레지스트리 등 조회용 사실 |
   위 7종 외 최상위 항목은 만들지 않는다. `openapi/`는 해당 영역에만 존재한다.
   dated 기록은 공개 트리가 아니라 ADR-0007의 비공개 아카이브가 소유한다.

3. **이동 금지 층.** 다음 셋은 물리 재배치 대상에서 제외한다 — 이름·위치가
   코드·CI에 배선된 계약이라 이동이 곧 파손이다:
   - `architecture/` — 코드가 경로로 읽는다: gongzzang `catalog_contract_pin.rs`의
     `include_str!`, repo-guard `capability_layout.rs`의 boundary JSON 로드,
     `generate-traffic-auth-policy`의 registry JSON, gongzzang-ci의 SSOT 검사.
   - `openapi/` — CI가 생성물과 대조한다: foundation-ci `cmp docs/openapi/catalog.v1.json`,
     identity-ci `diff docs/openapi/identity.v1.json`.
   - `adr/` — 번호가 동결된 인용 체계다 (`GZ-ADR-NNNN` 등, ADR-0002 ⑤); 가드도
     `docs/adr/0001*` 경로를 참조한다. 경로 이동은 전 영역의 인용을 깬다.

4. **아카이브 경계.** 구 `superpowers/`·`research/`·`migration/` 및 dated census류는
   공개 코드 트리에서 제외한다. 비공개 전환 아카이브가 원문과 Git 이력을 보존하며,
   공개 maintained docs는 현재 계약만 설명한다 (ADR-0002 ③, ADR-0007).

5. **ADR-0002와의 관계.** 0002의 분류(current|archived)·불변·배너·전역 번호 원칙은
   전부 유지된다. 본 ADR은 물리 배치(어느 폴더에 두는가)만 추가한다. 0002 ③의
   "기록은 현위치 유지" 서술은 ADR-0007의 공개/비공개 경계로 대체한다. 경로 제거에
   따른 링크 파손은 유입 링크 검사와 공개 저장소 가드가 같은 변경에서 차단한다.

## Consequences

- 루트와 각 영역의 `docs/README.md`가 공개 maintained 문서의 지도다.
- `scripts/guard/public-repository-safety.sh`가 공개 트리에 아카이브·시점 기록 경로가
  다시 들어오는 것을 차단한다.
- 문서 경로를 바꾸는 변경은 유입 링크와 린트/가드 설정을 같은 변경에서 갱신한다.
