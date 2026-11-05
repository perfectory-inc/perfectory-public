---
status: current
owner: gongzzang
doc_type: README
last_reviewed: 2026-11-05
---

# docs/

도메인별 SSOT 문서 트리. **한 폴더 = 한 도메인**, 각 폴더의 README가 인덱스.

## 전체 문서 트리

```text
docs/
├── adr/           제품 설계 결정
├── architecture/  시스템 구조·플랫폼 경계
├── auth/          인증·인가
├── backend/       서버 구현 규칙
├── compliance/    규제·보존·라이선스 정책
├── conventions/   코드·DB·문서 규칙
├── cost/          비용 기준과 추정
├── database/      DB 스키마·마이그레이션
├── data-sources/  외부 데이터 사용 계약
├── frontend/      웹·지도·접근성
├── runbooks/      운영 절차
└── testing/       테스트 실행 환경
```

전체 모노레포 문서는 [자동 색인 생성 안내](../../../docs/README.md)에서 찾습니다.

## 학습 순서 (새로 합류한 분 기준)

| # | 문서 | 내용 |
|---|------|------|
| 1 | [sss-charter.md](./sss-charter.md) | 7 기둥 SSS 헌법 — *모든 작업의 측정 자* |
| 2 | [glossary.md](./glossary.md) | 한·영 도메인 용어 사전 |
| 3 | [ssot-matrix.md](./ssot-matrix.md) | 정보별 SSOT와 그것을 지키는 검사 |
| 4 | [conventions/](./conventions/README.md) | 코드 스타일 + 네이밍 + 에러 형식 |
| 5 | [data-sources/](./data-sources/README.md) | 외부 데이터 원천 카탈로그 |
| 6 | [adr/](./adr/README.md) | 모든 기술·아키텍처 결정 이력 |

## 도메인 카테고리

각 폴더가 무엇을 다루는지의 세부는 그 폴더의 README가 소유한다. 여기서는 진입점만 둔다.

| 카테고리 | 책임 |
|---------|------|
| [auth/](./auth/README.md) | 인증·인가·세션 |
| [architecture/](./architecture/README.md) | 시스템 구조, 데이터 흐름, 캐싱, 관측성, geo 파이프라인 |
| [database/](./database/README.md) | Postgres + PostGIS 스키마·마이그레이션 |
| [backend/](./backend/README.md) | Rust 서버 구현 규칙 |
| [runbooks/](./runbooks/README.md) | Foundation Platform 연동·공급망 운영 절차 |
| [testing/](./testing/README.md) | 부하 시험·Playwright 실행 환경 (테스트 종류와 레인은 [테스트 컨벤션](./conventions/testing.md)) |
| [frontend/](./frontend/README.md) | 웹 앱·지도·접근성 |
| [compliance/](./compliance/README.md) | 개인정보·보존·라이선스 정책 |
| [cost/](./cost/README.md) | 비용 기준과 추정 |

아직 작성하지 않은 문서는 이 지도에 작업을 쌓지 않는다. 필요한 문서 작업은 루트
[운영 준비 작업 목록](../../../docs/roadmap/production-readiness.md)에서 관리한다.

폴더 없는 도메인의 실질 SSOT: 인프라(IaC) = [../infrastructure/README.md](../infrastructure/README.md),
보안·프라이버시 = [sss-charter.md](./sss-charter.md) §B-3, 캐시/메시징·API 방향 = [adr/](./adr/README.md) (ADR-0006/0007/0046/0047).

## SSOT 원칙

- 한 정보는 *한 폴더*에만 작성
- 다른 곳에서 필요하면 해당 문서에서 실제 상대 경로를 가리키는 Markdown 링크를 사용한다.
- 내부 링크는 루트 `lychee.toml` 링크 검사가, 메타데이터·중복·죽은 경로는 루트 문서 감사
  (`python3 scripts/catalog/audit-documentation.py --check`, `docs` 워크플로)가 본다.
  `.md` 문체는 커밋 전 `markdownlint-cli2`(루트 `lefthook.yml`)가 거른다.

## 역사 기록 경계

dated plan/spec/handoff/research와 운영 증거는 공개 코드 트리에 두지 않는다. 필요한 현행
불변식은 `adr/`, `architecture/`, `runbooks/` 또는 코드로 승격하고, 역사 기록은
[루트 ADR-0007](../../../docs/adr/0007-public-code-private-operations-boundary.md)에 따른
비공개 전환 archive에서만 보존한다.

## 작성 규칙

1. 파일 줄 수 상한은 [영역 AGENTS.md](../AGENTS.md)의 규칙을 따른다(500줄부터 경고, 1500줄 초과 금지). 길어지면 폴더로 분해.
2. 모든 도메인 폴더에 `README.md` 필수.
3. 다른 문서 참조는 명시적 상대 Markdown 링크를 사용하고, 영역 규칙은 [영역 AGENTS.md](../AGENTS.md)를 따른다.
4. 한국어 본문 + 영어 코드 식별자 ([glossary](./glossary.md) 용어를 쓴다 — 자동 검사는 없다).
