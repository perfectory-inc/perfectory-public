# ADR 0247: 자격 변수 이름은 Cloudflare 토큰 이름에서 기계적으로 만든다

- Status: Accepted
- Date: 2026-10-14
- Area: `platforms/foundation-platform`
- Related: [ADR-0153](./0153-runtime-secrets-have-one-contract.md) (운영 환경 파일 계약)

## Context

Cloudflare 토큰은 대시보드에서 `foundation-platform-<저장소>-<reader|writer>-prod` 로 이름이 붙어 있다. 그 값을 담는
환경 변수 이름은 손으로 지었고, 규칙이 셋으로 갈라져 있었다(2026-10-14 `config/runtime-secrets.contract.json`·
`config/r2-connections.contract.json` 전수).

| 지금 이름 | 어긋난 점 |
|---|---|
| `FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_TOKEN` | 쓰기 토큰인데 이름에 WRITER 가 없다. 지도(ADR-0195)에 읽기 토큰을 주려 하자 둘을 구별할 이름이 없었다 |
| `FOUNDATION_PLATFORM_R2_<저장소>_<READER\|WRITER>_*` (R2 키 쌍 7벌) | 토큰 이름에 없는 `R2_` 가 끼어 있다 |
| `FOUNDATION_PLATFORM_CLOUDFLARE_ANALYTICS_TOKEN` | READER 가 없다 |
| `CLOUDFLARE_API_TOKEN` (Worker 배포) | 우리 이름공간 밖, WRITER 가 없다(wrangler 가 읽는 이름을 호스트 파일에도 그대로 씀) |

이름 목록은 이미 세 곳에 손으로 있었다: 두 계약과 `scripts/guard/r2-env-namespace-consistency.sh` 의 `canonical_keys`.

## Decision

1. **토큰 이름을 한 번만 적는다.** `config/runtime-secrets.contract.json` 의 `cloudflare_tokens.tokens` 가 토큰마다
   대시보드 이름과 주는 값의 종류(`api_token` 또는 `r2_access_key`)를 적는다.
2. **변수 이름은 만든다, 적지 않는다.** `-prod` 를 떼고, 대문자로, `-` 를 `_` 로, 뒤에 `_TOKEN` 또는
   `_ACCESS_KEY_ID`·`_SECRET_ACCESS_KEY`. 규칙은 `scripts/deploy/runtime_secrets.py` 의 `derived_names` 하나다. 그룹은
   `@token:<이름>` 으로 담는다. 규칙에 맞지 않는 토큰 이름은 계약을 읽는 순간 거부된다.
3. **이름 바꾸기는 호스트가 먼저다(무중단).**
   - 1단계(이 ADR 과 함께): 토큰의 `renamed_from` 이 옛 이름을 적는다. 배포의 `verify_runtime_secrets` 가 `host` 검사
     전에 `runtime_secrets.py rename` 을 돌려, 옛 이름만 담은 그룹 파일에 같은 값을 새 이름으로 덧붙인다(같은 소유자·
     모드로 복사본을 쓰고 원자적으로 교체, 출력은 이름뿐). 옛 줄은 남긴다. 코드는 아직 옛 이름을 읽는다.
   - 2단계: 코드가 새 이름만 읽는다. 호스트 파일에 두 이름이 다 있으므로 어느 릴리스가 돌아도 값이 있다. 같은 PR 이
     가드를 붙인다: 자격 모양의 이름(`_TOKEN`, `_ACCESS_KEY_ID`, `_SECRET_ACCESS_KEY`)은 기록된 토큰에서 만들어지거나
     Cloudflare 토큰이 아닌 이유와 함께 예외로 적혀야 한다.
   - 3단계: 2단계 릴리스가 운영에서 돈 뒤, `rename` 이 옛 줄을 지우고 `renamed_from` 을 없앤다.
4. **계약을 거치지 않는 손 편집은 없다.** 호스트 환경 파일은 배포 스크립트(`foundation-release.sh`)가 계약에 따라
   바꾼다. `check` 는 옛 이름을 담으면서 새 이름을 담지 않은 그룹을 거부한다 — 그런 그룹은 `rename` 이 손대지 못한다.
5. **지도는 쓰기 토큰을 갖지 않는다.** `operations-map` 그룹은 `foundation-platform-lakehouse-catalog-reader-prod`
   (카탈로그 읽기 전용)만 담는다.

## 검토한 대안

- **모든 읽는 곳이 두 이름을 다 읽기(새 이름 우선, 옛 이름 경고).** 약 150개 파일 500군데에 넣었다가 다음 PR 에서
  다시 빼야 하고, compose 보간·pgBackRest 설정처럼 "새 이름 우선"을 표현할 수 없는 자리가 있다. 호스트가 먼저
  두 이름을 다 갖게 하면 같은 무중단을 코드 변경 한 번으로 얻는다. 노트북 `.env.local` 과 계약 밖 파일
  (`parcel-publication.env`)은 이 장치 밖이라 2단계 전에 손으로 고친다.
- **새 이름을 계약에 손으로 나열.** 토큰 이름과 변수 이름이 두 목록이 되고, 갈라지는 순간을 아무도 모른다.
- **`sudoedit` 로 서버 파일 고치기.** 어느 파일에 무엇이 있어야 하는지 계약이 모르는 변경이 된다.

## Consequences

- 1단계 배포에서 호스트 파일 7개가 이름 15개를 얻는다(운영 파일 사본으로 미리 돌려 확인: `rename` 뒤 `host` 무결함,
  소유자·모드 유지).
- R2 SQL(Basin SQL Read)은 저장소 어디에서도 쓰지 않는다. 카탈로그 쓰기 토큰의 그 권한은 대시보드에서 뺄 수 있다.
- `r2-env-namespace-consistency.sh` 의 자격 이름 목록은 2단계에서 계약에서 만든 이름으로 대체된다.
