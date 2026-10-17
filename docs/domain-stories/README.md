---
status: current
owner: repository-maintainers
doc_type: README
last_reviewed: 2026-10-17
---

# 도메인 스토리 — 서버 작업이 어떻게 돌고, 어디서 사고가 났나

설계([ADR-0270](../adr/0270-server-jobs-come-from-one-spec-and-run-through-one-runner.md))보다 먼저 쓴
이야기입니다. 이야기 하나마다 **지금(as-is)** 과 **바뀐 뒤(to-be)** 를 그림과 문장으로 둡니다.

사장 요약(1쪽, 이전 단계와 완료 기준): [owner-summary.md](./owner-summary.md).
셸 전수 목록(2026-10-17 스냅숏): [shell-inventory.csv](./shell-inventory.csv).

## 이야기 목록

| 번호 | 이야기 | 지금 그림 | 바뀐 뒤 그림 |
|---|---|---|---|
| 1 | [새 판이 나와서 사용자의 필지 패널에 닿기까지](./01-new-edition-to-parcel-panel.md) | [01-as-is.svg](./01-as-is.svg) | [01-to-be.svg](./01-to-be.svg) |
| 2 | [개발자가 고침을 병합해서 운영에서 돌기까지](./02-merged-fix-to-production.md) | [02-as-is.svg](./02-as-is.svg) | [02-to-be.svg](./02-to-be.svg) |
| 3 | [지역 코드가 바뀌어서 관리인이 필요할 때](./03-region-code-change.md) | [03-as-is.svg](./03-as-is.svg) | [03-to-be.svg](./03-to-be.svg) |
| 4 | [밤에 작업이 실패했을 때](./04-job-fails-at-night.md) | [04-as-is.svg](./04-as-is.svg) | [04-to-be.svg](./04-to-be.svg) |

## 표기법 (Hofer·Schwentner 의 도메인 스토리텔링)

| 그림 | 뜻 |
|---|---|
| 사람 그림 | 사람 행위자 (개발자, 운영자, 관리인, 사장, 사용자) |
| 화면 그림 | 시스템 행위자 (Airflow, systemd, 작업 러너, 데이터 창고 …) |
| 문서·표(원통)·메시지(봉투)·파일(폴더) | 일감(work object): 행위자가 주고받는 것 |
| 동그라미 숫자 + 화살표 | 활동(activity)과 그 순서 |
| **빨강** | 현재 이야기에서 사고가 들어온 자리 |

- 문장 형태는 `<번호>. <행위자>가 <일감>을 <활동>한다 (<행위자>에게)` 입니다.
- 범위: 모두 **세밀(fine-grained)**, **디지털(digitalized, 시스템을 행위자로 그림)** 입니다. 순수(pure)
  도메인 판은 따로 두지 않았습니다. 이번 문제가 시스템 사이의 조정에서 났기 때문입니다.
- 그림과 문장은 같은 목록 하나에서 한 번에 그렸습니다. 둘이 다르면 **문장이 정본**입니다.

## 현재 이야기를 무엇과 대조했나

기억이 아니라 `origin/main`(e975badc)의 실물과 대조했습니다.

| 이야기 | 대조한 실물 |
|---|---|
| 1 | `platforms/foundation-platform/orchestration/jobs.v1.json`, `job-dependencies.v1.json`(생산자 사슬), `scripts/ops/start-scheduled-job.sh`, `infra/systemd/*.service` |
| 2 | `scripts/deploy/foundation-autodeploy.sh`, `foundation-deploy.sh`(사전 점검·0~8단계), `deploy-start-once.sh`, 루트 `scripts/deploy/foundation-release-admission.py`(빌드 잠금) |
| 3 | `jobs.v1.json` 의 `legal_dong_code_changes`·`lineage_stewardship`, PR #658, PR #660, ADR-0263 |
| 4 | `scripts/ops/notify-unit-failure.sh`, `start-scheduled-job.sh`, ADR-0208, ADR-0260 |

사고 기록은 비공개 저장소 `perfectory-ops` 의 `postmortems/` 에 있습니다(2026-10-15 두 건).

## 출처

- Stefan Hofer, Henning Schwentner, *Domain Storytelling: A Collaborative, Visual, and Agile Way to Build
  Domain-Driven Software*, Addison-Wesley, 2021.
- <https://domainstorytelling.org/> — 그림 문법(행위자·일감·번호 붙은 활동)과 범위(세밀/거침, 지금/바뀐 뒤,
  순수/디지털).
