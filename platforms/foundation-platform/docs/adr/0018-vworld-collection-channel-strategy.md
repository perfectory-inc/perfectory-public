# ADR 0018 — V-World 수집 채널 전략

- **Status:** Accepted
- **Date:** 2026-06-27
- **Relates:** [ADR 0016](./0016-bronze-commit-protocol.md),
  [ADR 0017](./0017-bronze-collection-protocol.md),
  [ADR 0014](./0014-bronze-source-slug-canonical-naming.md)
- **Per-dataset capability SSOT:** [`docs/catalog/vworld/`](../catalog/vworld/README.md)
- **Evidence boundary:** 측정 scope·record count·provider date·file identity·run 결과는
  [root ADR 0007](../../../../docs/adr/0007-public-code-private-operations-boundary.md).

## 배경

V-World는 같은 원천 데이터셋을 여러 채널로 제공할 수 있다.

| channel | returns | Foundation Platform use |
|---|---|---|
| bulk download | full and, for some datasets, change files in SHP/CSV form | national collection |
| attribute API | attribute JSON | serving, statistics, bounded comparison |
| spatial API | geometry and attributes | serving, bounded comparison |
| WMS | rendered map image | display only |

아키텍처의 질문은 두 번째 전국 API 수집 경로가 더 최신 사실을 주는지, 아니면 훨씬 많은
요청으로 provider snapshot만 중복하는지다.

## 결정

V-World bulk 파일을 **전국 Bronze 수집의 기본 채널**로 사용한다. 속성·공간 API는 제공,
통계, 조사, drift 확인용 경로이며 전국 원본 이력을 중복하지 않는다. WMS는 화면 표시만
담당하고 Bronze 원천으로 사용하지 않는다.

최신성은 다음 방식으로 유지한다.

- 공급자가 검증된 native 변경 산출물을 제공하면 최초 전체 파일을 수집한 뒤 change file을
  수집한다.
- 그렇지 않으면 provider update marker를 polling하고 marker가 전진할 때 새 전체 파일을
  수집한다.
- 신뢰할 수 있는 native delta가 없으면 dataset canonical key와 content hash로 보존 snapshot을
  만들고 downstream에서 변경을 도출한다.

Bronze는 ADR 0016에 따라 immutable·CreateOnly로 유지한다. 데이터셋마다 acquisition watermark
하나를 둔다. 데이터셋별 catalog가 실제 지원 capability와 주기를 기록하며 pipeline code는
UI 라벨만으로 delta 지원을 추론하지 않는다.

## 결정 근거와 재검증

범위를 제한한 동일 조건 비교는 API가 corresponding bulk artifact보다 더 최신 record를 가진다는
사실을 입증하지 못했다. 전국 API pull은 지역과 page에 따라 request를 곱하므로 두 개의 전국
raw history를 유지할 근거가 되지 않았다.

API가 절대 더 최신일 수 없다는 영구적 주장은 아니다. 다음이 바뀌면 결정을 재검증한다.

- provider documentation이나 delivery behavior
- dataset update marker나 native-delta capability
- drift check에서 corresponding bulk slice에 없는 사실을 발견
- bulk availability나 법적·운영 조건

재검증은 같은 dataset·지리 범위·provider 기간·포함 규칙·안정적인 record identity를 비교한다.
공개 저장소에는 방법과 승격 gate만 두며 실제 scope·record count·date·request ID·provider
sample·결과는 private operations evidence에 둔다.

## 영향

- 새 V-World national collection lane은 bulk download를 기본으로 한다.
- dataset별 예외는 capability catalog가 사용 가능한 bulk system of record가 없는 이유와 승인
  대안을 기록할 때만 허용한다.
- API quota는 중복 전국 snapshot이 아니라 serving·제한 확인·명시적 fallback에 사용한다.
- provider가 신뢰할 수 있는 change feed를 주지 않으면 full-file refresh에 downstream
  idempotent snapshot 비교가 필요하다.
- capability catalog는 dataset이 제공하는 기능의 SSOT이고, 이 ADR은 bulk를 collection 기본으로
  삼는 이유의 SSOT다.

## 범위 밖

- Silver or Gold merge implementation; Bronze continues to preserve provider files
- Approval of a national recollection; scope expansion remains owner-gated
- Assuming that a provider UI control proves the existence of a native delta feed
- Removing API adapters used for serving, drift checks, investigation, or approved fallback
