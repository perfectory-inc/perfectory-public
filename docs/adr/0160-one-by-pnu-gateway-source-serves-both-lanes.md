# ADR 0160: 필지·건물 by-PNU Worker 는 소스 하나를 레인만 바꿔 묶는다

- Status: Accepted
- Date: 2026-10-08
- Builds on: [ADR-0147](./0147-by-pnu-documents-are-served-from-section-packs.md)(묶음 서빙, §8 필지 순서),
  [ADR-0151](./0151-building-packs-serve-each-document-whole.md)(문서 통째, gzip 그대로),
  [ADR-0157](./0157-a-canary-step-counts-its-own-answers-and-cpu-is-bound-by-measurement.md)(버전 헤더)

## Context

필지 상세를 묶음으로 옮길 차례다(ADR-0147 §8). 필지 Worker(`services/foundation-parcel-gateway`)는 건물 Worker
(`services/foundation-building-gateway`)를 처음 만들 때 복사한 코드다. 2026-10-08 비교:

- 묶음 기능(읽기 경로·미리보기·버전 헤더·Server-Timing·gzip 통과)을 빼면, 두 Worker 의 실제 동작 차이는 하나다.
  건물은 `?schema=2` 질의를 받고 필지는 받지 않는다. 나머지는 계약 블록 이름·단위 이름·캐시 원점 이름뿐이다.
- 로컬 검증 스크립트, 배포 설정 생성기, 설정 시험도 같은 방식의 복사본이다.
- 그래서 묶음 기능을 필지에 넣는 일은 그 기능 전체를 한 번 더 복사하는 일이 된다. 같은 사실이 두 곳에 있으면
  한쪽만 고쳐지는 일이 이미 여러 번 있었다(데이터 3벌, 옛 값 시험, 판 방치).
- 사례: 같은 동작을 여러 대상에 배포하는 코드는 대상마다 설정만 다른 빌드 하나로 둔다(Wrangler `define`·환경,
  esbuild 의 컴파일 시점 상수).

## Decision

1. **Worker 소스는 하나다.** `services/foundation-by-pnu-gateway`(옛 건물 Worker 폴더를 옮김)가 두 레인을 서빙한다.
   `services/foundation-parcel-gateway` 는 지운다. Cloudflare 의 Worker 이름·주소는 그대로다
   (`foundation-parcel-gateway`·`catalog.perfectory.io`, `foundation-building-gateway`·`buildings.perfectory.io`).
2. **레인은 묶을 때 정해진다.** `src/lane.ts` 의 `__FOUNDATION_BY_PNU_LANE__` 를 Wrangler 의 `define` 이 채운다.
   코드는 그 레인의 계약 블록(`<레인>_by_pnu_gateway`)만 읽는다. 단위 이름(`<레인>-by-pnu`)과 캐시 원점
   (`https://<worker_name>.invalid`)도 레인에서 나온다.
3. **레인의 차이는 계약에만 있다.** 두 블록은 같은 열쇠를 갖는다. 건물만 받던 `?schema=2` 는
   `request_path.accepted_queries` 가 되고, 필지는 빈 목록이다. 필지 블록은 건물 블록과 같은 Worker 설정을 얻는다
   (도쿄 배치, CPU 한도 50ms, 버전 메타데이터·헤더, 묶음 미리보기·서빙 스위치, 미리보기 Worker
   `foundation-parcel-gateway-preview`·`catalog-preview.perfectory.io`, 대체 기간).
4. **배포 설정은 레인마다 하나다.** `scripts/render-wrangler-config.mjs` 가 `wrangler.building.jsonc` 와
   `wrangler.parcel.jsonc` 를 만들고, `config:check` 가 둘의 어긋남을 막는다. 배포는
   `wrangler deploy -c wrangler.<레인>.jsonc` 다. 단계 배포 스크립트도 그 설정을 쓴다.
5. **시험은 레인마다 돈다.** `vitest.config.ts` 의 두 project 가 같은 소스를 각 레인으로 묶어 돌린다
   (`parcel-*.test.ts` 가 필지). 필지 레인이 필지 묶음 경로·스위치를 읽고 건물 경로를 읽지 않는다는 시험은,
   레인 대응을 일부러 틀리게 하면 실패한다(2026-10-08 확인).

## Consequences

- 필지 Worker 는 이 소스로 처음 배포될 때 묶음 기능, 도쿄 배치, 버전 헤더를 함께 얻는다. 그 배포는 건물과 같은
  단계 배포를 거친다. 묶음 스위치가 꺼진 버전으로 먼저 올리고, 그다음 켠다.
- 앞으로 Worker 동작을 고치는 PR 하나가 두 레인을 함께 고치고, 두 레인의 시험이 함께 돈다.
- 단계 배포·건강 검사·감시 스크립트의 레인 일반화와 필지 단계 배포 설정은 다음 변경이다.
