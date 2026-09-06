# ADR 0086: 파이프라인 그래프는 모든 데이터셋을 한 번씩 명명한다

- Status: Accepted
- Date: 2026-09-06

## Context

"우리가 다루는 모든 데이터"의 층별 정본은 이미 셋 있다: 수집은
`public-source-endpoint-catalog.v1.json`(133 endpoint, 8 group), Silver/Gold 는
`industrial_complex_lakehouse_contracts.json`(표 13), 서빙은 `migrations/`(catalog·
serving_postgis 표 70, DROP 없음). 그러나 **층 사이의 연결**을 적는 자리인
`pipeline-graph.v1.json` 은 2026-09-06 실측 40노드 — silver 3/13, 원천 5그룹/8,
용도지역·공시지가·호·행정경계가 없다. 지도가 없는 것이 아니라 지도가 거짓이다.
손으로 늘리면 다시 낡는다는 것은 이 그래프 자신이 증명했다.

## Decision

1. `pipeline-graph.v1.json` 을 schema_version 2 로 다시 쓴다. 노드는 다섯 종:
   `source_group`(endpoint 카탈로그의 group 을 `endpoint_catalog_group` 필드로 가리킴,
   슬러그 목록을 복사하지 않는다), `silver_table`·`gold_table`(계약의 `table_name`),
   `serving_group`(`tables: []` 에 마이그레이션 표 이름들), `serving_surface`(타일·API·
   패널·게이트웨이). 엣지는 `via` 에 실행 명령·스크립트 이름을 적는다. 아직 연결이
   없는 층은 지어내지 않고 노드의 `status: "collected_only"` 등으로 사실대로 둔다.
2. **화해 가드** `scripts/guard/pipeline-graph-covers-every-dataset.sh`: (a) 그래프의
   source_group 집합 == endpoint 카탈로그의 group 집합, (b) silver·gold 노드 집합 ==
   lakehouse 계약 표 집합, (c) serving_group 들의 tables 합집합 == 마이그레이션의
   `CREATE [UNLOGGED] TABLE catalog.*|serving_postgis.*` 집합(중복 금지),
   (d) 모든 엣지 끝점은 존재하는 노드. 비교 집합은 각 정본에서 **가드가 스스로
   수집**한다 — 목록을 가드에 적으면 그것이 거울이다.
3. **렌더러** `scripts/catalog/render-pipeline-map.py` 가 그래프+endpoint 카탈로그에서
   `docs/data-pipeline-map.md`(한국어, 비전공자 가독)를 생성한다. `--check` 를 docs CI 에
   편입한다. 손으로 고치는 문서가 아니다.

## Consequences

- 데이터셋·표·서빙면이 하나라도 늘면 그래프를 같이 고치지 않는 한 CI 가 막는다 —
  이 문서가 낡는 경로가 코드로 닫힌다.
- 그래프가 커진다(약 40→80+ 노드). 사람용 뷰는 렌더된 지도가 담당한다.
- endpoint 카탈로그의 빈 `silver` 필드는 이 그래프의 엣지로 대체된다 — 같은 사실을
  두 곳에 적지 않는다.
