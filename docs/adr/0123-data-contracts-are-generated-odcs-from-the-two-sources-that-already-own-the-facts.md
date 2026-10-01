# ADR 0123: 데이터 계약(ODCS)은 사실을 이미 가진 두 정본에서 생성한다

- Status: Accepted
- Date: 2026-10-01
- Amends: [ADR-0117](./0117-metadata-has-one-contract-per-dataset-and-one-place-to-look.md) §2
- Builds on: [ADR-0086](./0086-the-pipeline-graph-names-every-dataset-once.md)(데이터 지도),
  [ADR-0097](./0097-dependencies-track-latest-and-version-pins-live-in-one-contract.md)(한 사실 한 파일)

## Context

ADR-0117 §2 는 데이터셋마다 ODCS v3 YAML 하나를 손으로 쓰는 정본으로 두고, 데이터 지도(`pipeline-graph.v1.json`)를
그것에서 생성한다고 정했다. 2026-10-01 실물을 보니 계약에 들어갈 사실은 이미 두 정본에 나뉘어 있다.

| 사실 | 지금의 정본 | 읽는 코드 |
|---|---|---|
| 레이크하우스 표 35개의 칸·타입·필수 여부·허용 값·품질 관문·적재 방식 | `lakehouse-domain` 의 Rust 계약 (`industrial_complex_lakehouse_contracts.json` 은 그 출력이고 시험이 같은지 본다) | Spark 작업, 적재기, 시험 |
| 데이터셋 112개의 이름·설명·담당·선언 상태·연결 | `pipeline-graph.v1.json` | 기준정보 API, 지도 문서, 화해 가드 |

ODCS 를 손으로 쓰는 세 번째 정본으로 두면 칸 목록은 Rust 와 YAML 두 곳에, 설명·상태는 지도와 YAML 두 곳에
생긴다. 같은 사실이 두 곳에 있으면 고칠 수 없다는 것을 이 저장소는 여러 번 겪었다. ODCS 를 정본으로 만들려면
Rust 계약과 Spark 작업이 YAML 을 읽도록 바꿔야 하는데, 그것은 형식을 바꾸는 일이지 사실을 더하는 일이 아니다.

ODCS(Bitol, v3.2.0)가 주는 가치는 **표준 형식**이다. 데이터 카탈로그·계약 도구·외부 소비자가 같은 모양으로 읽는다.

## Decision

1. **ODCS 파일은 생성물이다.** `platforms/foundation-platform/contracts/data/<표 이름>.odcs.yaml` 은
   `scripts/catalog/render-data-contracts.py` 가 두 정본에서 만든다. 손으로 고치지 않는다. 생성 결과와 커밋된
   파일이 다르면 가드(`data-contracts-are-generated`)가 막는다.
2. **사실마다 정본은 하나다.** 칸·타입·필수·허용 값·품질 관문은 Rust 레이크하우스 계약, 이름·설명·담당·상태·상위
   데이터셋은 데이터 지도. 생성기는 둘을 합칠 뿐 값을 만들지 않는다. 한쪽에만 있는 표는 생성을 멈춘다.
3. **ODCS 대응.** `apiVersion: v3.2.0`, `kind: DataContract`, `id` 는 표 이름, `status` 는 지도의 선언 상태를
   ODCS 상태로(implemented→active, contract_only→draft, planned→proposed; 원래 값은 `customProperties`),
   `schema[0].properties` 는 칸, 허용 값은 `enum`, 품질 관문은 `quality` 의 `type: text` 규칙, 상위 데이터셋은
   `customProperties.upstreamDatasets`(ODCS 에 계보 필드가 없다).
4. **범위.** 이번에는 레이크하우스 표 35개다. 원천·서빙 표·서빙 접점(77개)은 같은 생성기로 뒤따른다.
5. **검증.** CI 는 생성 일치(가드)와 우리가 쓰는 ODCS 필드 규칙(시험)을 본다. 공식 JSON 스키마 전체 검증은 계약을
   데이터 카탈로그에 등록하는 단계(다음 변경)가 한다.
6. ADR-0117 §2 의 "지도는 계약에서 생성" 은 이 결정으로 거꾸로 된다: 지도가 정본으로 남고 계약이 생성된다.
   §8 표의 "`pipeline-graph.v1.json` 손 편집 → 계약에서 생성" 줄은 없어진다.

## Consequences

- 계약 파일이 늘어도 고칠 곳은 늘지 않는다. 칸을 바꾸면 Rust 계약을, 설명을 바꾸면 지도를 고치고 생성기를 돌린다.
- 갱신 주기(SLA)·인증 등급은 아직 어느 정본에도 없어 계약에 없다. 넣을 때는 둘 중 한 정본에 칸을 만들고 생성한다.
- 다음: 계약을 데이터 카탈로그에 등록(설명·담당·칸 설명·품질 규칙, 임시 시드 `seed_declared_lineage.py` 대체),
  적재 뒤 품질 규칙 실행과 결과 게시(ADR-0117 §6).
- 출처: [ODCS v3.2.0 JSON 스키마](https://github.com/bitol-io/open-data-contract-standard/tree/main/schema).
