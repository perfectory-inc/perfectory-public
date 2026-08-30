# ADR 0062: 적재 묶음은 자기가 쓴 표 안에 스스로를 기록한다

- Status: Accepted
- Date: 2026-08-28

## Context

2026-08-27, 필지 3,986만 행을 `r2.silver.parcel_boundaries` 에 넣는 동안 같은 묶음이
세 번 적재됐다. 표를 읽어 확인한 수치다.

```
묶음 0 이 담은 행    1,865,891
표에 있던 행         5,597,673   = 1,865,891 × 3
중복 PNU             1,865,891   (전부 count=3)
```

세 번의 실행은 모두 **쓰기를 커밋한 뒤** 다음 단계에서 죽었다 — 두 번은 힙 부족, 한 번은
호출자의 SSH 세션이 끊기면서. 적재기는 "이미 넣었는지"를 자기 옆의 마커 파일
(`~/parcel-load-state/batch-N.load.done`) 로 판단했고, 그 파일은 쓰기가 커밋된 **뒤에**
만들어졌다.

결함은 재시도가 아니라 사실이 두 벌 있었다는 것이다.

```
행         → R2 의 Iceberg 커밋
넣었다는 말 → 서버 파일 시스템의 다른 커밋
```

둘 사이에서 죽으면 둘이 어긋난다. 어긋난 상태에서 적재기는 마커만 보므로 이미 있는 행을
다시 붙인다. [[mirrored-lists-are-the-defect]] 와 같은 형태다 — 같은 사실이 두 곳에 있으면
감시자를 붙일 게 아니라 거울을 없애야 한다.

이 결함은 우리만의 것이 아니고, 해법도 이미 정해져 있다. 조사한 넷이 모두 같은 원리였다.

| 출처 | 방법 |
| --- | --- |
| Apache Iceberg 의 Flink 싱크 | `flink.max-committed-checkpoint-id` 를 스냅숏 요약에 데이터와 같은 커밋으로 넣고, 복구 시 표에서 읽어 이미 커밋된 체크포인트를 건너뛴다 |
| Delta Lake / Databricks | `txnAppId` + `txnVersion` 을 커밋 로그에 기록하고, 같은 값의 재실행을 무시한다 |
| Netflix (WAP, 2017) | 안 보이는 가지에 쓰고 검사한 뒤 통과한 것만 publish 한다 |
| Iceberg Spark | `snapshot-property.<키>` 옵션으로 임의의 값을 그 쓰기의 스냅숏 요약에 함께 커밋한다 |

앞의 둘이 이 결함을 직접 다룬다. 셋째는 검사에 실패한 데이터가 표에 남는 별도의 구멍을
막는 것이라 이 ADR 의 범위가 아니다.

## Decision

1. 레이크하우스에 붙이는 실행은 자기가 담은 Bronze 객체 이름의 **다이제스트**를
   `foundation.ingest-batch-token` 으로, 객체 이름 목록을 `foundation.ingest-batch-objects`
   로 하여 **데이터 파일과 같은 Iceberg 커밋의 스냅숏 요약에** 기록한다.

2. 묶음의 정체는 **담긴 내용**에서 뽑는다. 순번을 쓰지 않는다. 순번은 재개한 적재기가
   같은 번호를 다른 객체 집합에 줄 수 있고, 그때 토큰은 넣지 않은 것을 넣었다고 말한다.
   이름 목록은 정렬한 뒤 해싱하므로 glob 순서가 정체를 바꾸지 못한다.

3. 붙이기 **전에** 표의 `<table>.snapshots` 요약을 읽어 같은 토큰이 있는지 본다. 있으면
   붙이지 않고, 그 토큰이 가리키는 행을 실제로 세어 보고한 뒤 성공으로 끝난다. 판단 근거는
   표 자신이며, 표 밖의 어떤 파일도 이 판단에 참여하지 않는다.

4. 이 경로의 쓰기는 `INSERT INTO` 가 아니라 DataFrame writer 를 쓴다. `snapshot-property.*`
   를 받는 것은 writer 뿐이고, 기록이 데이터와 같은 커밋에 실리는 것이 이 결정의 전부다.

5. `infra/lakehouse/spark/tests` 의 검사 파일은 `unittest.TestCase` 여야 하고, 검사가
   가리키는 잡은 PySpark 없이 import 될 수 있어야 한다. 이 레인의 러너는
   `python3 -m unittest discover` 이고 CI 에는 PySpark 가 없다. 둘 중 하나만 어겨도 검사는
   0개 수집 또는 전부 skip 으로 **초록을 보고한다**.

## Consequences

`vworld_parcel_boundaries_handoff_to_silver.py` 가 이 결정을 구현한다. 실물 R2 에서 같은
2개 객체를 두 번 적재해 두 번째가 건너뛰는 것과 행 수가 늘지 않는 것을 확인한 뒤 채택했다.

**Iceberg 스냅숏이 만료되면 요약도 사라진다.** 보존 기간이 한 적재보다 짧으면 이미 붙인
묶음이 안 붙은 것으로 보인다. Iceberg 의 Flink 문서가 같은 경고를 한다. 적재가 끝날 때까지
그 적재가 만든 스냅숏을 만료시키지 않는다.

**동시에 도는 두 실행은 이것으로 막히지 않는다.** 둘이 같은 토큰으로 동시에 물으면 둘 다
"없다"를 받는다. 이 적재기는 순차 실행이고, Iceberg 의 Flink 싱크도 커미터가 하나라는
전제 위에 같은 구조로 서 있다. 동시 적재가 필요해지면 그때 별도 결정이 필요하다.

**검사 실패가 표를 더럽히는 구멍은 그대로다.** 토큰은 "붙였다"를 정확히 기록할 뿐이고,
붙인 뒤의 검사가 실패하면 그 행은 표에 남는다. 적재기가 그 자리에서 멈추므로 사람이 보게
되지만, 애초에 안 보이게 하려면 Netflix 의 WAP 이 필요하다. 이 저장소에는
`spatial_tile_publication_wap.py` 가 그 기계를 이미 갖고 있고 스스로를 "필지 경계 계약을
위한 WAP 증명"이라 부르는데, 정작 필지 적재기가 그것을 쓰지 않는다. 별도 결정으로 다룬다.

**같은 형태의 결함이 다른 잡에도 있다.** `industrial_complex_bronze_to_silver.py`,
`industrial_complex_silver_to_gold.py`, `industrial_complex_boundaries_handoff_to_silver.py`,
`silver_scalar_handoff_to_lakehouse.py` 는 모두 SQL 로 붙인 뒤 확인하며, 어느 것도 자기가
붙였다는 사실을 표에 남기지 않는다. 실물 대조로 확인했다.

**그러나 넷에 같은 처방을 그대로 쓸 수는 없다.** 결정 1–3 은 "이 묶음이 담은 원천 객체"를
전제하는데, 대상 표마다 그 객체를 가리키는 칸이 있기도 없기도 하다.

```
silver.industrial_complexes           source_record_id 있음
silver.industrial_complex_boundaries  있음
gold.complex_catalog                  없음 — Bronze 객체가 아니라 Silver 에서 파생된다
gold.complex_spatial_locator          없음
silver.building_register_units        없음
```

Gold 표의 한 실행은 Bronze 객체 묶음이 아니라 자기가 읽은 Silver 상태에서 나온다. 그것의
정체를 무엇으로 삼을지는 이 ADR 이 답하지 않은 별도의 질문이며, 답하기 전에 저 넷을
고치면 있지도 않은 칸을 전제한 코드가 된다. `source_snapshot_id` 로 대신하려는 유혹은
특히 위험하다 — 그 칸을 적재 단위로 착각한 것이 애초에 2026-08-27 의 오검출을 만들었다.

**공용 모듈은 먼저 세워 뒀다.** `infra/lakehouse/spark/jobs/lakehouse_ingest.py` 가 토큰·
요약 옵션·판단을 한 곳에 갖고 있고, 필지 잡이 그것을 쓴다. 나머지 잡을 붙일 때 구현을
다시 쓰지 않는다. 요약 키와 옵션 접두사를 다른 잡이 스스로 적으면 검사가 거부한다.
