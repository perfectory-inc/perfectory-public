# ADR 0083: 필지는 토지이용계획 원장에서 용도지역을 배운다

- Status: Accepted
- Date: 2026-09-05

## Context

필지 패널의 용도지역 줄은 값이 있을 때만 그려지는데, 그 값을 만드는 코드가 없다.
`foundation_parcel_lookup.rs` 는 `zoning: None` 을 하드코딩했고, gongzzang 의
`Zoning` enum(주거/상업/공업/녹지/기타) 은 생산자가 없는 어휘로 남아 있다
(전수 조사 2026-09-05: `silver.land_use_*` 문자열은 소스 카탈로그의 `planned`
선언 한 곳에만 존재, 읽는 코드 0).

원천은 이미 수집되어 있다. 2026-09-05 R2 실측:

- `bronze/source=vworldkr__land_use_plan/` — 241객체 70.0GB. 두 갈래가 한 prefix 에
  섞여 있다: `20171128DS00147-*`(34객체 19.8GB)는 D154 **도형** shapefile,
  `20171128DS00148-*`(207객체 50.2GB)는 D155 **필지별 속성** CSV(EUC-KR).
- D155 행 = 고유번호(19자리 PNU) · 저촉여부코드(1 포함/2 저촉/3 접함) ·
  용도지역지구코드/명 · 등록일자 · 데이터기준일자 · 원천시도시군구코드.
  세종(최소 시도) 실측: 1,361,899행, 고유 PNU 207,910, 필지당 평균 6.5행,
  데이터기준일자 2026-06.
- 내부 파일명은 `AL_D155_{시도}_{날짜}.csv` — 같은 시도가 여러 수확 시점으로
  누적되어 있다(append-only 수집). 시도 17개에 객체 207개가 그 증거다.
- `bronze/source=vworldkr__land_use_zone_code/` — 코드표 `LART_LMISZONE.csv`
  1,270행. `PARENT_UCODE` 로 트리를 이룬다: `UQA001 도시지역` 아래
  `UQA100 주거지역 / UQA200 상업지역 / UQA300 공업지역 / UQA400 녹지지역`,
  뿌리(parent `000000`)에 `UQB001 관리지역 / UQC001 농림지역 /
  UQD001 자연환경보전지역`.

즉 용도지역 판정에 필요한 사실은 (필지, 코드) 배정표와 코드 트리 둘뿐이고,
둘 다 원천에 있다. 코드→어휘 매핑을 손으로 옮겨 적으면 거울이 된다
(root ADR 의 반복 교훈) — 트리를 걸어 올라가 앵커에 닿는 방식이어야 한다.

## Decision

1. **`silver.land_use_plan`** 은 D155 속성 CSV 만 싣는다. D154 도형 shapefile 은
   Bronze 에 남는다(도형 서빙은 이 결정의 범위 밖). 적재기는 시도마다 내부
   파일 날짜가 가장 최신인 객체 **하나**를 고르고, 전국 실행은 시도 17개가
   모두 선택되지 않으면 거부한다(ADR-0082 의 부분집합 거부 원칙). 선택된
   객체 키·checksum·내부 파일명은 증거 산출물에 남는다.
2. **`silver.land_use_zone_code`** 는 코드표 1,270행을 그대로 싣는다
   (`UCODE`, `UNAME`, `PARENT_UCODE` 포함).
3. 저촉여부 세 값은 silver 에 원본 그대로 남는다. 다만 **용도 판정 투영은
   접함(3)을 제외한다** — 접해 있다는 사실은 그 필지의 용도가 아니다.
4. **용도지역 판정은 코드 트리 보행이다.** 코드에서 `PARENT_UCODE` 를 따라
   올라가 앵커 집합 {`UQA100`, `UQA200`, `UQA300`, `UQA400`, `UQB001`,
   `UQC001`, `UQD001`} 중 하나에 닿으면 그 앵커가 그 행의 용도지역 가족이고,
   네 도시 앵커를 거치지 않고 `UQA001` 에 닿으면 앵커는 `UQA001`(도시
   미세분)이다. 어떤 앵커에도 닿지 않는 코드는 용도지역이 아니므로 투영에서
   제외하되, 제외 수는 증거에 기록한다(조용한 탈락 금지).
5. **`catalog.parcel_zoning`** 투영(foundation 소유)이 서빙 정본이다:
   (pnu, zone_code, zone_name, anchor_code, inclusion_code) — 앵커에 닿고
   저촉여부 ∈ {포함, 저촉}인 행만. by-pnu API 응답은 이 행들을 nullable
   배열로 나른다. 행이 없으면 없다고 답한다 — 값을 지어내지 않는다
   (ADR-0078). 어휘 번역(앵커→`Zoning` enum)과 대표 선정(포함 우선)은
   gongzzang 쪽 결정이며 foundation 은 앵커 코드를 나를 뿐이다.
6. 실행 순서는 수직 슬라이스: **세종(36) 단독 적재로 사슬을 증명한 뒤
   전국**. 소스 카탈로그의 `silver.status` 는 표에 행과 등록부 증거가 실재할
   때에만 `planned → ready` 로 바뀐다.

## Consequences

- 전국 silver 는 대략 2.6억 행(세종 6.5행/필지 × 3,986만 필지) 규모다.
  ai-server 는 작으므로 시도 단위 처리로 나눈다(호스트 한계는 기록된 실측).
- 같은 레일(시도별 D-계열 CSV → silver)이 `land_individual_price`(공시지가),
  `land_characteristic`(토지특성) 에 그대로 재사용될 수 있다 — 패널의
  공시지가 줄도 지금 `None` 하드코딩이다. 후속 결정으로 남긴다.
- 지목(land_use_type)은 이 결정의 범위 밖이다 — GZ-ADR-0015 가 지적한
  지번 끝 토큰 경로는 별도 결정이 필요하다.
- `vworldkr__sandan_land_use_zone`/`sandan_facility_land_use`(산업단지 내부
  용도)는 어휘가 다른 별개 원장이며 이 레인에 싣지 않는다.
