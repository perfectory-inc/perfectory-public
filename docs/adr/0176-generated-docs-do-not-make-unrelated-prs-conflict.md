# ADR 0176: 서로 무관한 두 PR은 생성 문서에서 충돌하지 않는다

- Status: Accepted
- Date: 2026-10-10
- Builds on: [ADR-0005](./0005-hooks-advisory-ci-authoritative.md)(훅은 조언, CI가 권위),
  [ADR-0086](./0086-the-pipeline-graph-names-every-dataset-once.md)(파이프라인 그래프 정본)

## Context

2026-10-09~10 측정: 거의 모든 PR이 앞서 병합된 모든 PR과 충돌했고, 충돌한 곳은 사람이 손대지
않는 생성·색인 파일뿐이었다. 충돌 한 번마다 rebase와 CI 전체 재실행(약 30분)이 붙었고, 한 PR은
하룻밤에 세 번 충돌했다. 매번 충돌한 파일과 그 원인은 다음과 같다.

| 파일 | 생성기 | 충돌 원인 |
|---|---|---|
| `docs/document-audit.md` | `audit-documentation.py --write` | 문서 총수·언어·메타데이터 합계 줄, 문서마다 한 행(정렬), 유입 링크 수 |
| `docs/document-catalog.md` | `render-document-catalog.py` | 문서 총수, 소유 영역·유형별 수, 정렬된 전체 목록 두 벌 |
| `docs/roadmap/foundation-baseline.md` | `render-foundation-baseline.py` | `ADR: **N개**`·부채 항목 수(ADR마다), 표 수(마이그레이션마다) |
| `docs/adr/README.md` | 손(write-adr 스킬: "같은 변경에서 색인 줄 추가") | ADR마다 파일 끝 같은 자리에 한 줄 |
| `docs/data-pipeline-map.md` | `render-pipeline-map.py` | 그래프 전체 합계 한 줄(원천·endpoint·표 수) |

충돌이 없더라도 문제는 같다. 두 PR이 각자 갱신한 합계는 병합 뒤 둘 다 틀리므로(170→171을 둘이
쓰면 병합본도 171), 깨끗하게 병합돼도 `--check`가 낡았다고 거부해 PR이 다시 돌아온다.

각 파일을 누가 읽는지 저장소 전체(`git grep`)에서 찾았다.

- 감사 보고서·문서 색인·기반 지표를 **읽는 코드·가드·Dawneer·DataHub는 없다.** 읽는 것은
  `docs.yml`·lefthook의 신선도 검사(`--check`)와 사람뿐이고, 사람은 README 15곳의 링크로 왔다.
  생성기 자신이 두 산출물을 입력에서 제외하고 있었다.
- 신선도 검사가 지키던 성질은 "커밋된 사본이 생성기 출력과 같다"뿐이다. 실제로 지켜야 하는
  성질(메타데이터·`doc_type` 어휘·깨진 상대 링크·한글 서술·참조 없는 초안)은 같은 스크립트의
  `--check --strict`가 사본과 무관하게 판정한다. 색인은 `git ls-files`에서만 나오므로 불완전할
  수 없다.
- ADR 색인은 사람이 읽고 19개 파일이 링크한다. 스킬 외에 그 목록을 검사하는 것은 없었고,
  실제로 한때 0016·0017이 빠진 채 방치됐다([기반 목표](../roadmap/foundation-goals.md) G0).
- 파이프라인 지도는 ADR-0086과 제어면 문서가 링크하는 사람용 정본 렌더링이며, 그래프가 바뀔
  때만 바뀐다. 문제는 그래프 전체 합계 한 줄뿐이다.

검토했으나 고르지 않은 것: `.gitattributes`의 `merge=union`은 로컬 git에서만 동작하고 GitHub의
PR 병합·머지 큐는 병합 드라이버를 쓰지 않는다. 합계는 합쳐도 틀린 값이 된다.

## Decision

1. **커밋하지 않는다(생성기 출력은 필요할 때·CI 요약으로).** `docs/document-audit.md`,
   `docs/document-catalog.md`, `docs/roadmap/foundation-baseline.md`를 저장소에서 지운다. 세
   생성기는 인자 없이 실행하면 Markdown을 표준 출력에 내고 `--output PATH`로 파일에 쓴다.
   `docs.yml`이 `main`과 PR마다 셋을 실행해 job summary에 싣는다. 생성기가 실패하면 CI가
   실패하므로 "생성할 수 있다"는 계속 검사된다.
2. **지키던 성질은 그대로 CI가 판정한다.** `audit-documentation.py --check --strict`는 보고서
   비교만 빠지고 메타데이터·어휘·링크·언어·초안 참조 검사는 그대로다. 행 계산을 한 번만 해서
   검사 시간이 줄어든다. 기반 지표 생성기의 자기 시험(`test_foundation_baseline.py`)도 그대로다.
3. **ADR 색인은 디렉터리다.** `docs/adr/README.md`는 번호 규칙과 생성 명령만 담고 결정별 줄을
   두지 않는다. 새 `scripts/catalog/render-adr-index.py`가 각 ADR의 첫 제목에서 목록을 출력하고,
   `--check`는 목록이 기대던 것 — 파일 이름 `NNNN-<kebab-case>.md`, 첫 제목의 번호 = 파일 번호,
   **번호 중복 없음** — 을 검사한다. 두 PR이 같은 다음 번호를 잡으면 텍스트 충돌은 없어졌으므로
   이 검사가 머지 큐에서 잡는다. write-adr 스킬의 "색인 줄 추가" 규칙은 "README는 고치지
   않는다"로 바뀐다.
4. **커밋하는 생성 문서에는 전체 합계를 쓰지 않는다.** 파이프라인 지도의 "현재 범위" 합계 줄을
   지운다. 남는 것은 항목마다 한 행이고, 그 행은 정본 그래프의 같은 항목이 바뀔 때만 바뀐다.
5. **증명은 시험이 한다.** `scripts/catalog/test_generated_docs_merge.py`는 한 기준점에서 두
   브랜치가 각자 인접 번호의 ADR과 서로 다른 안내 문서를 더하고, 트리 자신의 규칙(lefthook의
   `scripts/catalog/` 명령, 실패하면 `--check`/`--strict`를 뺀 같은 명령으로 재생성, README에 결정별
   목록이 있으면 줄 추가)을 따른 뒤 병합해, 텍스트 충돌이 없고 병합본이 재생성 없이 같은 명령을
   통과하는지 본다. `--rev origin/main`으로 이 결정 이전 트리를 돌리면
   `docs/adr/README.md, docs/document-audit.md, docs/document-catalog.md` 충돌로 실패하고, 이 결정의
   트리에서는 통과한다. `docs.yml`이 이 시험을 실행한다.
6. pre-push 훅 `generated-docs-freshness`는 `docs-integrity`로 이름을 바꾸고
   `audit-documentation.py --check --strict`, `render-adr-index.py --check`,
   `render-pipeline-map.py --check`를 돈다.

## Consequences

- 무관한 두 PR은 이 파일들에서 충돌하지 않고, 병합본이 낡았다는 이유로 돌아오지도 않는다.
- GitHub에서 문서 색인·감사 보고서·기반 지표를 링크로 바로 열 수 없다. 대신 명령 한 줄이나
  `main`의 `docs` 워크플로 요약을 본다. README의 링크는 그 안내로 바꿨다. ADR-0009의 깨진 링크는
  허용된 링크 수정으로 고쳤다.
- [기반 목표](../roadmap/foundation-goals.md)의 G0 "모든 지표는 명령으로 재생산된다"는 그대로
  성립한다. 지표 페이지가 저장된 사본에서 명령의 출력으로 바뀌었을 뿐이다.
- 남는 충돌 원천: 정본 자체(같은 JSON 끝에 두 PR이 항목을 붙이는 경우 등)와
  `platforms/foundation-platform/docs/catalog/public-data-collection-catalog.md`의 합계 줄
  (endpoint를 더하는 두 PR). 후자는 이번 측정에서 충돌하지 않아 범위 밖으로 두며, 충돌이
  관측되면 4항을 같은 방식으로 적용한다.
