# ADR 0189: append 만 쌓인 Silver 입력은 추가분이 곧 변경 집합이다

- Status: Accepted
- Date: 2026-10-12
- Amends: [ADR-0180](./0180-panel-gold-recomputes-only-the-pnus-whose-inputs-changed.md) 결정 1(바뀐 행 = 두 판의 차이)
- Builds on: [ADR-0182](./0182-the-gold-rebuild-joins-the-unit-prices-and-the-panel-reads-the-newest-vintage.md),
  [ADR-0185](./0185-a-unit-price-batch-is-its-snapshots-and-the-panel-reads-the-last-one-of-a-vintage.md)
  (패널은 최신 기준연도의 마지막 배치 하나를 읽는다)

## Context

2026-10-12 운영: `gold.building_panel`(약 590만 PNU) 재생성이 한 대의 서버(Spark local[8,8], 16g)에서 약 1시간
걸렸다. 그날 바뀐 것은 `silver.unit_official_price` 에 새 배치(기준연도 202609, 약 1.9억 행)가 **append** 된
것 하나였다. 그런데 ADR-0180 의 증분 경로는 입력마다 고정된 옛 판과 새 판을 **둘 다 전부** 읽어 다중집합 차이
(`changed_rows`)를 계산한다. 스냅숏 메타데이터가 이미 말해 주는 사실("이 행들이 추가됐다")을 알아내려고 1.9억 행
표를 두 번 통째로 읽은 것이다.

가격 입력에는 한 가지 함정이 더 있다. 생산자는 표 전체가 아니라 **최신 배치 하나**를 읽는다(ADR-0182/0185).
새 기준연도 배치가 append 되면 "읽는 배치"가 옛 배치에서 새 배치로 바뀐다. 이때 추가분(새 배치)만 변경 집합으로
보면, 새 배치에 없고 옛 배치에만 있던 호실(가격이 빠진 호)의 PNU 를 놓친다.

## Decision

1. **append 만 쌓인 입력은 차이를 계산하지 않는다.** 고정 판에서 새 판까지 부모 사슬의 모든 스냅숏이 Iceberg
   `append` 이면(`gold_incremental.appended_span`, `<table>.snapshots` 의 `operation`·`parent_id`), 새 판은 옛 판의
   행을 그대로 담고 추가분만 더한 것이다. 그 입력의 변경 집합은 Iceberg 증분 append 읽기
   (`start-snapshot-id`/`end-snapshot-id`, `read_appended`)로 읽은 추가분이다. 사이에 overwrite·delete·replace 가
   하나라도 있거나, 사슬이 옛 판에 닿지 않으면(만료·다른 계보) ADR-0180 의 두 판 차이를 그대로 쓴다.
2. **생산자가 선언한 입력만.** `Lane.append_inputs`(입력 → Iceberg 표)에 이름이 있는 입력만 이 길을 탄다. 건물
   패널은 다섯 입력 전부를 선언한다: 모든 입력을 한 배치로 읽으므로(`assert_single_snapshot`) 읽기가 "그 배치의
   행" 이라는 행 단위 필터이기 때문이다. 필지 패널은 선언하지 않는다(계보 후보 등 읽기가 행 단위가 아니다; 필요해지면
   그 읽기를 따져 선언한다).
3. **읽는 배치가 바뀌면 두 배치를 다 매핑한다.** 건물 패널에서 옛 판과 새 판이 읽는 배치 id 가 같으면 변경 집합은
   그 배치의 추가분이다(정확히 차이와 같다). 다르면(새 기준연도) 변경 집합은 새 판에서 **옛 배치 ∪ 새 배치** 의 행이다.
   append 는 옛 배치를 건드리지 않으므로 이것은 차이의 상위집합이고, PNU 매핑이 읽는 옛·새 행을 모두 담는다. 배치
   id 로 거르기만 하고 행끼리 비교하지 않는다. 상위집합이라 다시 계산되는 PNU 가 늘 수 있지만, 내용이 같은 PNU 는
   `row_digest` 가 같아 합쳐지지 않는다. 늘어난 수가 `max_changed_key_fraction` 을 넘으면 기존대로 전량 재생성이다.
4. **실행 보고와 로그.** 실행 요약의 `rebuild.appended_inputs` 가 이 길을 탄 입력을 적는다. 단계마다 한 줄
   `gold-incremental-phase <affected|restrict|build|parity|merge> seconds=<벽시계 초>` 를 실행 로그에 찍는다.
5. **죽은 실행의 단계 디렉터리.** `Stages` 는 만들 때 같은 로컬 디렉터리의 `gold-incremental-*` 형제를 지운다.
   Gold 실행은 `rebuild.lock`(scripts/ops/gold-panel-rebuild.sh)으로 직렬화되므로 그것은 SIGKILL·OOM·시간 초과로
   `finally` 에 닿지 못한 실행의 것뿐이다.

## Consequences

- 2026-10-12 와 같은 새 기준연도 append 에서 증분 경로는 1.9억 행 표를 두 번 통째로 읽어 지문을 섞는(shuffle)
  대신, 새 판 한 번을 배치 id 로 걸러 등록번호 한 열만 읽는다. 같은 배치에 행이 더해지는 경우는 추가된 데이터 파일만
  읽는다. 단계별 초가 로그에 남으므로 다음 운영 실행에서 실제 절감을 잰다.
- 증명: `tests/test_gold_incremental_iceberg.py::test_an_appended_price_batch_is_its_own_change_set` — 배치 전환
  (옛 배치에만 있던 호실 포함), 같은 배치 append, 사이에 delete 가 낀 경우 각각 병합 결과가 전량 재생성과 같다.
  순수 판정(`appended_span`, 단계 디렉터리 정리)은 `tests/test_gold_incremental.py`.
- 사슬 판정은 `snapshots` 메타데이터 표를 입력마다 한 번 읽는다(행 데이터는 읽지 않는다).
