# ADR 0419: 릴리스는 생성물을 CI 빌드가 서명한 대로 싣고, 호스트는 아무것도 렌더하지 않는다

- Status: Accepted (2026-11-05, 소유자 지시 "근본 구조전환까지 전부 알아서 빠르게 진행해")
- Date: 2026-11-05
- Builds on: ADR-0418(생성물은 커밋하지 않는다, #1073 — 이 ADR 이 그 §4 를 결정한다),
  [ADR-0276](./0276-ci-builds-and-signs-each-release-and-the-server-only-verifies-and-runs-it.md)(CI 가 한 번 빌드·서명, 호스트는 검증한 바이트만),
  [ADR-0134](./0134-production-installs-only-canonical-main-and-keeps-artifacts-outside-the-release.md)·[ADR-0184](./0184-one-answer-for-running-jobs-and-a-release-the-kernel-keeps-immutable.md)(릴리스 = 정본 커밋의 바이트, 커널이 봉인),
  [ADR-0363](./0363-a-deploy-judges-the-release-not-the-provider-or-the-archive-bytes.md)(같은 커밋은 같은 릴리스)
- 관련 사건: 2026-10-21 단가 레인이 제어 체크아웃의 계약 판과 설치된 릴리스의 판이 달라 한 배포 늦게 반영됐다
  ("관문은 한 배포 늦게", #771). 호스트에서 렌더하면 같은 어긋남이 매 작업 시작의 `verify-current` 에 생긴다.

## Context

ADR-0418 은 정의(`platforms/foundation-platform/definitions/`)를 정본으로 커밋하고, 생성물 8개는 git 에 두지 않는다.
대상은 `jobs.v1.json`, 그래프와 그 예제, `r2-connections`, 호스트 메모리, 레지스트리, 의존 파일, 데이터 지도다.
호스트는 지금 이렇게 동작한다(2026-11-05 main).

- 승인(`scripts/deploy/foundation-release-admission.py`)은 `prepare` 때 독립적으로 받은 정본 커밋의 하위 트리 바이트만 릴리스에 쓴다.
  그 뒤 `seal` 과 매 작업 시작의 `verify-current` 가 같은 바이트와 파일 집합을 다시 요구한다(ADR-0134·0184).
- 배포(`foundation-deploy.sh`)는 사전 점검에서 `jobs.v1.json`·그래프를 `git archive` 로 꺼낸다.
  1단계에서는 커밋 전체를 제어 체크아웃(`/opt/perfectory-control/releases/<sha>`)으로 풀고, 승인 자신이 그 체크아웃의 `jobs.v1.json` 을 읽는다.
- 운영의 산출물은 CI 가 빌드하고 서명한 레지스트리 릴리스다(`tools/release-build.contract.json` `artifacts_from: registry`).
  구성은 Sigstore SLSA 증명, manifest, 파일 이미지(실행 파일과 jar)다(ADR-0276).

호스트에서 생성기를 돌리는 방법은 둘 다 안 된다.

- 제어 체크아웃의 렌더러: 배포 창 동안 제어 판과 설치된 판이 다르면 `verify-current` 가 작업을 거부한다.
- 후보 릴리스의 렌더러: 승인의 신뢰 규칙("후보의 검증기를 운영 권한으로 돌리지 않는다")에 어긋난다.

## Decision

1. **CI 릴리스 빌드가 생성물을 싣고 서명한다.**
   - `scripts/deploy/foundation-release-build.py` 는 체크아웃의 생성물(`scripts/catalog/declared_definitions.py` `GENERATED`, CI 가 체크아웃 직후 생성)을
     저장소 경로 그대로 세 번째 파일 이미지 `generated`(`FROM scratch`)에 넣는다.
   - manifest 의 `generated` 에 경로마다 `sha256`·`size` 를 적고, 증명 대상(`subjects.sha256`)에 그 다이제스트를 더한다.
   - 전환 전 커밋에서도 똑같이 싣는다(그때는 커밋된 파일이다). 경로가 전환 전부터 돌아 검증된다.
2. **승인은 서명된 바이트만 받는다.**
   - `verified_release` 는 manifest 의 생성물 다이제스트가 증명에 서명돼 있기를 요구한다.
   - `registry_generated` 는 이미지 층을 다이제스트로 받고, 파일마다 manifest 의 다이제스트·크기와 맞춘다. manifest 가 이름 대지 않은 파일은 거부한다.
   - 어떤 파일이 생성물인지는 서명된 릴리스가 정한다. CI 빌드는 체크아웃에 없는 생성물이 있으면 실패한다.
     승인은 경로만 막는다: 상대 경로, `..`·숨은 경로 없음, 커밋된 파일 덮어쓰기 없음.
     제어 체크아웃 생성기의 목록과 비교하지 않는다. 그 목록은 생성물을 하나 더한 릴리스보다 한 배포 늦어서, 배포 중에 그 릴리스를 거부하기 때문이다
     (같은 판 어긋남을 이 ADR 이 없애려는 것이다).
3. **릴리스 설치.**
   - 커밋의 하위 트리에 `definitions/` 가 있으면 `prepare` 가 하위 트리의 생성물을 커밋 바이트 옆에 쓴다(모드 0444).
     다이제스트는 표시 파일 `.foundation-release-generated-sha256.json` 에 적는다.
   - 생성물이 커밋에도 있으면 거부한다.
   - `seal`·`verify`·`verify-current` 는 커밋 파일 + 표시 파일이 이름 댄 파일을 기대 집합으로 삼는다(`release_expected`).
     파일 하나라도 다이제스트가 다르면, 표시 파일이 없거나 커밋 파일·트리 밖을 가리키면 거부한다.
   - 정의가 없는 커밋(전환 전 릴리스, 되돌리기 대상)은 레지스트리를 읽지 않고 표시 파일도 없다. 표시 파일이 있으면 거부한다.
   - 표시 파일의 신뢰는 `.foundation-release-id`·아카이브 다이제스트 표시와 같다. 서명을 확인한 `prepare` 만 쓰고, 루트 소유이며 커널이 봉인한다.
   - 원격 읽기는 `prepare` 한 번뿐이고, `verify-current` 는 오프라인이다.
4. **배포의 두 곳은 `generated` 명령으로 받는다.**
   - 승인의 새 하위 명령 `generated <sha> <dir>` 는 서명을 확인한 생성물을 체크아웃에 쓴다. 이미 있는 파일은 같아야 하고 남겨 둔다. 이 ADR 전 릴리스면 아무것도 쓰지 않는다.
   - `foundation-deploy.sh` 의 사전 점검(`release_changes`)과 1단계 제어 체크아웃은 커밋에 `jobs.v1.json` 이 없을 때만 이 명령을 부른다.
     전환 전 커밋은 지금처럼 git 에서 읽는다.
5. **한 배포 늦게.** 배포의 사전 점검과 1단계는 *실행 중인* 제어 체크아웃의 승인으로 돈다. 그래서 #1073(전환)은 이 ADR 의 PR 이 배포된 **다음** 배포에서만 들어갈 수 있다. 같은 창에 병합하지 않는다.
6. **`server-build` 대안 경로.** 이 경로도 정의가 있는 릴리스의 생성물은 서명된 레지스트리 릴리스에서만 받는다. 레지스트리 릴리스가 없으면 그 릴리스는 설치를 거부한다. 렌더하지 않는다.

## Consequences

- 호스트는 생성기·렌더러를 돌리지 않는다. 생성물은 jar·실행 파일과 같은 공급망(같은 서명, 같은 다이제스트 확인)으로 온다.
- 매 작업 시작의 `verify-current` 에 생성물 다이제스트 8개 이하의 계산이 더해진다(수 KB~수 MB, 밀리초).
- 시험:
  - `tests/release_admission/test_release_generated_files.py` 9개: 실제 git, 설치·변조·표시 파일·전환 전 릴리스·집합 불일치·체크아웃 쓰기.
  - `test_release_from_registry.py` 에 생성물 레지스트리 경우 5개 + 기존 16개를 생성물 있는 레지스트리로 다시 돌림.
  - 릴리스 리허설(`foundation-release-test.sh`)에 정의 있는 릴리스 설치·변조 거부 단계.
- 남은 위험: 첫 전환 배포의 사전 점검이 레지스트리 릴리스를 기다린다. autodeploy 는 이미 릴리스 빌드 확인을 기다린다(`release_checks.py`).

## 기각한 대안

- **호스트에서 생성(제어 판 렌더러)**: 배포 창의 판 어긋남이 작업 시작을 거부한다(Context).
- **후보 렌더러를 컨테이너 격리로 호스트에서 실행**: 결정적이지만, 같은 바이트를 CI 가 이미 서명해 두었다. 같은 일을 두 곳에서 하는 것이다.
- **생성물을 파일 이미지(jar·실행 파일)에 섞기**: 파일 이미지는 수백 MB 다. 사전 점검과 1단계가 그것을 받을 이유가 없다.
