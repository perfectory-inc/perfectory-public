---
status: current
owner: repository-maintainers
doc_type: guide
last_reviewed: 2026-07-28
---

# 문서 운영 안내

이 저장소의 문서는 코드와 함께 검토하고 배포하는 문서다. 문서도 코드와 같은 단일 진실
원천(SSOT), 소유자, 변경 이력을 가져야 한다.

## 어디에 무엇을 쓰는가

| 질문 | 정본 위치 | 문서 종류 |
|---|---|---|
| 왜 이 결정을 했는가? | `docs/adr/` 또는 소유 영역의 `docs/adr/` | ADR |
| 지금 구조가 어떻게 되어 있는가? | 소유 영역의 `docs/architecture/` | 구조 문서 |
| 개발자가 어떻게 실행하는가? | `docs/guides/` 또는 영역의 `docs/guides/` | 안내서 |
| 장애·배포·복구를 어떻게 하는가? | 영역의 `docs/runbooks/` | 운영 런북 |
| 값·스키마·목록을 어디서 조회하는가? | 영역의 `docs/reference/` 또는 `docs/catalog/` | 조회 문서 |
| 앞으로 무엇을 할 것인가? | 루트 `docs/roadmap/` | 작업 목록 |
| 기계가 읽는 계약은 무엇인가? | `openapi/`, `contracts/`, `schemas/` | 기계 계약 |

루트와 영역의 `README.md`는 지도를 제공한다. 설명을 복사하지 말고 정본으로 연결한다.
같은 규칙을 두 문서에 쓰면 어느 쪽이 최신인지 알 수 없게 되므로 중복을 만들지 않는다.

## 배치 예외

기본 분류와 달리 `catalog/`, `openapi/`, `schemas/`, `events/`, `db/`처럼 코드·CI가
직접 읽는 계약 경로는 소유 영역의 고정 경로를 유지한다. 이런 파일은 이름이나 위치를
바꾸지 않고 README와 자동 색인에서 역할만 명확히 한다.

## 문서 변경 절차

1. 변경하려는 사실의 정본과 소유자를 먼저 찾는다. 없으면 가장 작은 정본 문서를 만든다.
2. 새 문서 또는 전면 개정 문서에는 다음 메타데이터를 넣는다.

   ```yaml
   status: current
   owner: <소유 영역>
   doc_type: adr | architecture | guide | runbook | reference | catalog
   last_reviewed: YYYY-MM-DD
   ```

3. 기존 문서를 옮기기 전에 코드·CI·링크가 경로를 직접 참조하는지 확인한다. 계약 파일,
   생성 산출물, 법률 원문은 임의로 이름을 바꾸거나 삭제하지 않는다.
4. 미완료 작업은 README·ADR에 임시로 적지 않고 [운영 준비 작업 목록](../roadmap/production-readiness.md)에
   기록한다. 결정이 완료되면 ADR, 실행 절차가 완료되면 runbook으로 승격한다.
5. 변경은 조직 저장소의 작업 브랜치에서 사람의 PR로 검토한다. `main`에는 직접 커밋하지
  않는다. PR은 문서 diff, 링크, 자동 색인, 감사 보고서 검사를 통과해야 한다.
6. 병합 후 `python3 scripts/catalog/audit-documentation.py --write`와
   `python3 scripts/catalog/render-document-catalog.py --write`로 생성 문서를 갱신한다.

## 기록과 번역

조사·실행·장애·릴리스의 시간순 기록은 현재 정책 문서와 섞지 않는다. 공개 코드 저장소의
역사·운영 증거 경계는 [ADR 0007](../adr/0007-public-code-private-operations-boundary.md)을
따르며, 필요한 기록은 비공개 운영 증거 저장소에 날짜·범위·결론·근거·후속 작업을 남긴다.
공개 저장소에는 재현 가능한 절차와 검증 도구만 둔다.

사람이 읽는 설명의 정본은 한글이다. 명령어·코드·API 필드·제품명은 원래 표기를 유지한다.
다국어 번역이 필요해지면 `docs/i18n/<locale>/`에 정본의 `source_revision`을 기록하고,
번역본이 별도 정책이나 사실의 정본이 되지 않게 한다. 자세한 규칙은
[ADR 0009](../adr/0009-korean-first-documentation-and-multilingual-readiness.md)를 따른다.

## 확인 명령

```bash
python3 scripts/catalog/audit-documentation.py --check
python3 -m unittest scripts/catalog/test_audit_documentation.py -v
python3 scripts/catalog/render-document-catalog.py --check
git diff --check
```

`audit-documentation.py --check`는 유지 문서에 한글 설명이 전혀 없거나 명백한 영문 서술 문장이
남은 경우 실패시킨다. 계약·fixture
JSON, `AGENTS.md`/`CLAUDE.md` 라우터, 법률 원문은 자동 감사에서 예외로 분류한다. 기술명·코드·명령·
식별자가 섞인 문서는 `mixed`로 보고되지만 사람이 읽는 문장은 한글로 작성해야 한다.

감사 보고서는 [문서 감사 보고서](../document-audit.md), 전체 목록은
[문서 색인](../document-catalog.md)에서 확인한다. 감사 보고서는 정본이 아니라 현재 상태를
점검하기 위한 생성 산출물이다.
