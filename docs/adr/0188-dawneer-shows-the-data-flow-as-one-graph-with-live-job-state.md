# ADR 0188: 더니어는 데이터 흐름을 한 장의 그래프로 보이고, 작업의 지금 상태를 그 화살표에 싣는다

- Status: Accepted
- Date: 2026-10-12
- Amends: [ADR-0186](./0186-a-scheduled-jobs-typical-duration-is-foundations-and-the-console-shows-what-runs-now.md)
  (지금 도는 작업 목록이 운영 현황의 첫 칸), [ADR-0165](./0165-dawneer-shows-operations-status-from-the-data-catalog-and-the-job-list.md)
  결정 1(운영 현황 메뉴)·결정 2(`GET /data-catalog/v1/scheduled-jobs` 의 칸)

## Context

ADR-0186 의 "지금 도는 작업" 목록을 본 사장의 질문은 "이게 최선이야? 그래프 형태는"이었다. 목록은 작업이 **흐름의
어디에** 있는지, 무엇이 그것을 기다리는지, 오늘 하루가 어땠는지를 보이지 못한다. 또 데이터는 두 메뉴에 갈라져
있었다: 데이터 카탈로그(무엇이 있고 어디서 오나)와 운영 현황(무엇이 돌고 있나). 사장의 범위 지시는 둘을 하나로
합치는 것이다 — 업계에서 "single pane of glass" 라 부르는, 카탈로그와 실행 상태가 같은 그래프인 화면.

확인한 것:

- 흐름의 사실은 이미 한 곳씩 있다. 데이터셋과 그 연결은 `docs/catalog/pipeline-graph.v1.json`, 어느 작업이 어느
  연결을 수행하나는 `orchestration/jobs.v1.json` 의 `pipeline_graph_edges`, 그로부터 각 작업이 읽고 쓰는 데이터셋과
  "누가 누구를 먹이나"(`producers`)는 `orchestration/dags/job_specs.py` 가 계산한다(ADR-0171). Airflow 의 자산
  스케줄링이 바로 이 계산을 쓴다.
- 그 계산을 Rust 나 TS 로 다시 쓰면 같은 규칙이 두 벌이 된다. 두 벌은 어긋나고, 어긋난 화면은 거짓말을 한다.
- 실행 기록은 Foundation 이 이미 작업마다 최근 `RUNS_PER_JOB`(10)개까지 읽는다(ADR-0186). 마지막 하나만 내보냈다.
- 데이터 카탈로그(DataHub)의 데이터셋 이름 규칙은 `infra/datahub/dataset_names.py` 하나다. 그런데 URN 을 만드는
  `urn_of` 만은 `seed_declared_lineage.py` 안에 따로 있었다.

### 벤치마크 — 무엇을 보고 무엇을 가져왔나

| 도구 | 본 것 | 가져온 것 |
|---|---|---|
| Dagster asset graph · global asset lineage | 노드가 **자산(데이터)** 이고, 실행 상태(구체화됨·구체화 중·실패·한 번도 안 됨)가 그 자산 위에 붙는다. 카탈로그와 실행 상태가 한 그래프. 노드를 누르면 옆 패널 | 데이터셋이 노드, 상태는 그것을 만드는 작업에서 온다. 클릭 → 오른쪽 패널 |
| Airflow 3 Graph · Asset 보기 · Gantt | 상태별 색과 테두리, 실행 중 표시. Gantt 는 실행마다 시간축 막대 | 실행 타임라인(최근 24시간, 실행마다 막대, "지금" 선) |
| Prefect run timeline | 진행 중인 실행이 현재까지 자라는 막대 | 실행 중 막대는 지금까지, 보통 걸리는 시간까지 점선 유령 막대 |
| Marquez (OpenLineage UI) | 데이터셋과 작업이 번갈아 서는 이분 그래프, 작업 노드 색이 최근 실행 상태 | 검토 후 **기각**(아래 결정 2) |
| DataHub lineage | 데이터셋 사이의 작업을 가운데 노드로 펼치거나 접는다 | 한 걸음 계보 보기는 옆 패널의 카탈로그 칸으로 그대로 유지 |
| OpenMetadata lineage | 파이프라인이 데이터셋 사이 **간선**에 붙는다 | 작업은 화살표, 그 상태는 화살표의 색·움직임·이름표 |

이들 중 어느 것도 **제품으로 들이지 않는다.** Airflow(실행)와 DataHub(카탈로그)는 이미 운영 중이고, 직원 콘솔은
더니어 하나다(ADR-0114). 패턴만 가져오고 코드는 베끼지 않는다.

## Decision

1. **한 메뉴 "데이터 흐름"(`#/flow`).** 운영 현황과 데이터 카탈로그 메뉴는 없앤다. 옛 주소 `#/operations` 는
   `#/flow`, `#/catalog` 는 `#/flow/catalog` 로 바뀐다(`consoles/dawneer/web/src/lib/routes.ts` 의 `movedTo`,
   기록을 남기지 않는 `history.replaceState`). 옛 두 화면이 보이던 것은 전부 남는다: 그래프 아래 탭
   (`FLOW_TABS`: 실행 타임라인·작업 목록(지금 도는 작업 포함)·품질 검사·수집·적재 기록·데이터 찾기)과 옆 패널
   (데이터셋 설명·만드는 작업과 그 일정·다음 실행·최근 실행·데이터 계약 검사·카탈로그 항목의 칸과 한 걸음 계보).
   `routes.test.ts` 가 옛 주소와 탭 목록을 고정한다.
2. **노드는 데이터셋, 작업은 화살표.** 노드는 파이프라인 그래프의 데이터셋이고, 간선은 그 연결이며, 연결을
   수행하는 예약 작업의 상태를 싣는다(실행 중이면 움직이는 굵은 화살표와 "⟳ 하는 중", 실패면 빨간 화살표).
   데이터셋 노드의 상태는 그것을 쓰는 작업 중 가장 눈이 먼저 가야 할 것(`strongest`: 하는 중 > 재시도 >
   실패 > 기다림 > …)이다.
   이분 그래프(Marquez·DataHub 펼침)를 기각한 이유: 작업 24개를 노드로 세우면 단계마다 열이 둘이 되어 그림이
   두 배로 넓어지고, 사람이 묻는 것("공시지가 표가 지금 어때")은 데이터셋에 대한 질문이다. 작업은 그 데이터셋을
   만드는 손이므로 상태로 붙인다. 한 작업이 여러 연결을 수행하면 그 연결들이 같은 상태를 보인다.
3. **단계 열은 다섯: 자료 받기 → 정리하기 → 합치기 → 굽기 → 서비스.** 데이터셋의 종류(`type`,
   `surface_kind`)가 단계를 정한다(`datasetStage`). 구운 산출물(`tiles`, `r2_baked_documents`)은 굽기, 나머지
   서비스 면은 서비스. 아무 것도 들어오지 않는 데이터셋(데이터가 들어오는 입구)은 자기가 먹이는 가장 이른
   단계보다 늦지 않게 놓는다. 각 열에는 큰 제목과 쉬운 한 문장이 붙는다(`STAGE_TEXT`).
4. **배치는 elkjs(Eclipse Layout Kernel)의 `layered` 알고리즘, 단계마다 partition.** 같은 입력은 같은 그림이다
   (`elk.randomSeed` 고정, 입력을 id 순으로 정렬; `flowLayout.test.ts` 가 순서를 섞어 같은 좌표를 확인).
   순환(직원 결정이 Silver 로 되돌아가는 연결)과 떨어진 데이터셋도 단계 순서를 지킨다.
   - 검토: dagre 는 partition(열 고정)이 없어 단계 열을 보장하려면 좌표를 손으로 고쳐야 하고, 유지보수가 멈춘
     원본과 `@dagrejs` 포크로 갈라져 있다. 손으로 쓰는 층 배치는 순환·교차 최소화를 다시 만드는 일이다.
     React Flow 문서가 층 배치 DAG 에 권하는 것도 elkjs 다.
   - 라이선스 EPL-2.0: 파일 단위 약한 copyleft 로, 수정하지 않고 쓰는 한 우리 코드에 의무가 번지지 않는다. npm
     의존성의 라이선스 거부 목록은 저장소에 없다. 더니어 화면은 공개 배포 패키지가 아니다(`private`).
   - 첫 화면을 기다리게 하지 않도록 elkjs 는 처음 그릴 때 동적으로 불러온다(별도 청크).
   - `docs/technology-stack.md` §1.1 에 "노드·간선 그림과 자동 배치 = `@xyflow/react` + elkjs" 행을 더하고,
     `scripts/guard/utility-library-policy.sh` 가 두 번째 배치(`dagre`, `@dagrejs/dagre`)와 옛 React Flow 패키지
     (`reactflow`)를 거부한다(자기 시험에 거부 사례 추가). 막는 사고: 화면마다 다른 배치 라이브러리가 들어와 같은
     그래프가 화면마다 다르게 그려지는 일.
5. **작업 의존성은 `job_specs.py` 의 계산 하나를, 렌더한 파일로 읽는다(선택지 b).**
   `job_specs.py --write-dependencies` 가 `orchestration/job-dependencies.v1.json` 을 만든다: 작업마다
   `producers`(기다리는 작업), 읽고 쓰는 데이터셋(`dataset`, `catalog_urn`, `node_id`, `title`, `kind`,
   `surface_kind`). Foundation 은 이 파일을 그대로 읽어 `ScheduledJobStatus.upstream_job_ids`·`reads`·`writes` 로
   내보내고, 작업 목록과 순서가 다르면(다른 목록에서 렌더된 파일) 거부한다. 이미지에는 `jobs.v1.json` 옆에
   복사된다(`FOUNDATION_PLATFORM_JOB_DEPENDENCIES_PATH`).
   - 드리프트: `test_job_specs.py` 의 `TheRenderedDependencies` 가 커밋된 파일과 실제 두 원본에서 렌더한 결과를
     바이트 단위로 비교하고, DAG 가 쓰는 `JobSpec.producers`·입출력과도 같음을 확인한다. CI 의 orchestration
     Python 스위트가 이것을 돈다.
   - (a) "Rust 가 같은 규칙으로 계산하고 둘을 비교하는 시험"을 기각한 이유: 규칙이 두 벌이 되고, 비교 시험은
     어긋남을 잡을 뿐 두 벌이 있다는 사실을 없애지 못한다. (b) 는 계산이 하나, 사본은 기계가 만들고 기계가 검사한다.
   - 카탈로그 URN 규칙 `urn_of` 를 `dataset_names.py` 로 옮겨, 시드와 렌더가 같은 함수를 부른다.
6. **API 추가(`GET /data-catalog/v1/scheduled-jobs`):** `recent_runs`(마지막 실행을 담은 카탈로그 작업의 실행만,
   최신부터, 최대 `RUNS_PER_JOB`; DAG 와 태스크가 같은 실행을 각각 보고하므로 한쪽만), `started_by`,
   `pipeline_graph_edges`, `upstream_job_ids`, `reads`, `writes`. OpenAPI 와 더니어 타입은 기존 생성기로 다시 만든다.
7. **아이 눈높이 화면.** 위 한 줄 요약("지금 N개가 일하고 있어요 · 실패 N · 마지막 서비스 반영 N시간 전"), 노드마다
   아이콘·짧은 한국어 이름(파이프라인 그래프의 `title`, 원본 낱말)·상태 한 낱말(✓ 끝남 / ⟳ 하는 중 / ✕ 실패 /
   ⏸ 기다림 / – 꺼짐)·"N분 전", 실행 중이면 "약 N분 남음"과 진행 막대, 그것을 기다리는 데이터셋엔 "앞 단계가 끝나면
   시작해요". 색만으로 말하지 않는다(아이콘+낱말, `STATUS_TEXT`). 노드는 버튼이라 키보드로 닿고 Enter 로 연다.
   그래프는 읽을 수 있는 배율(`READABLE_ZOOM`) 아래로 줄이지 않고 첫 단계 제목에서 열린다(`startViewport`).
8. **모든 파생은 `src/lib/` 의 순수 함수와 단위 시험이다:** `dataFlow.ts`(단계·상태·기다림·요약·이름),
   `flowLayout.ts`(ELK 입력·좌표 읽기·첫 화면), `runTimeline.ts`(막대 기하, 실행 중 유령 막대), `routes.ts`.
   유틸리티는 es-toolkit.

## Consequences

- 작업 목록이나 파이프라인 그래프를 바꾸는 커밋은 `python orchestration/dags/job_specs.py --write-dependencies` 로
  렌더 파일도 함께 바꿔야 한다. 안 하면 CI 의 orchestration 스위트가 빨갛다.
- 그래프는 기본으로 예약 작업이 수행하는 연결만 보인다. "예약 작업이 없는 연결까지 모두 보기"를 켜면 파이프라인
  그래프 전체(사람이나 다른 시스템이 넣는 연결 포함)를 같은 규칙으로 그린다.
- 화면 묶음이 elkjs 청크(약 1.4MB, gzip 약 0.44MB)만큼 커지지만 그래프를 처음 그릴 때만 받는다.
- 남은 것: 작업 하나가 여러 연결을 수행할 때 그 연결들을 한 묶음으로 접어 보이기, 데이터셋의 신선도를 실행 기록
  대신 카탈로그의 스냅숏 시각으로 보이기.
