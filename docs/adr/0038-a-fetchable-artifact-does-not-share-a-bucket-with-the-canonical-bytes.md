# ADR 0038: 가져가라고 만든 산출물은 정본 바이트와 버킷을 같이 쓰지 않는다

- Status: Superseded by ADR-0039
- Date: 2026-08-18

## Context

ADR-0036 이 만든 `export-industrial-complex-gold-profiles` 는 처음에 lakehouse 버킷
(`foundation-platform-lakehouse-prod`)에 썼다. 그 버킷에는 Bronze 수집 원본과 Silver/Gold
Iceberg 표가 함께 있다. 프로필 JSON 은 **화면이 가져가라고 만든 산출물**이므로, 언젠가 그
버킷에 커스텀 도메인이 붙으면 수집 원본까지 같은 도메인 아래로 들어간다.

저장소에는 이 구분이 이미 있었다. 타일 쪽 런북이 적어 둔 문장이 그대로 근거다:

> canonical/source geometry는 lakehouse bucket에 남긴다. 별도의 private serving-derivative
> bucket에는 공개 가능한 불변 PMTiles serving release만 둔다.

`tile_derivative_object_storage.rs` 가 그 경계를 코드로 지킨다: 전용 env 네임스페이스, writer 와
reader 자격증명 분리, 그리고 **보호 버킷 거부 목록**(lakehouse, postgres-recovery 등). 다만 그
목록은 그 파일 안에만 있었고 타일 전용이었다.

**사실 정정 하나를 함께 적는다.** serving-derivative 버킷은 지금 문서상 **private** 이다.
Martin 이 인증된 S3 origin 으로 읽고, 런북은 "공개 R2 URL 이나 CORS 정책이 필요 없다" 고 적는다.
즉 이 저장소에는 지금 **공개된 버킷이 없다.** 프로필을 CDN 으로 노출하려면 그 버킷에 도메인을
붙여야 하고, 그러면 PMTiles 릴리스도 같이 열린다 — 그것은 소유자 결정이며 이 ADR 의 범위가 아니다.

## Decision

1. `serving_derivative_object_storage.rs` 가 **"무엇이 serving derivative 인가"의 단일 정의**다.
   - `PROTECTED_BUCKETS`: serving derivative 를 절대 쓸 수 없는 버킷. lakehouse 가 핵심이다.
   - `SERVING_DERIVATIVE_OBJECT_ROOTS`: 그 버킷에 있어도 되는 객체 키 루트. 지금은 두 개이며
     둘 다 키 배치를 소유한 모듈의 상수를 읽는다(`catalog_domain::STATIC_RELEASE_OBJECT_ROOT`,
     `r2_layout::INDUSTRIAL_COMPLEX_GOLD_PROFILE_ROOT`). 경로를 여기 다시 적지 않는다.
   막는 사고: 가져가라고 만든 산출물이 정본 바이트와 같은 버킷에 들어가, 그 버킷에 도메인을
   붙이는 순간 수집 원본 257 GB 가 함께 열리는 것.

2. `TileDerivativeR2Config::validate_bucket` 은 이름 모양과 보호 목록 검사를 위 모듈에 위임한다.
   타일 고유 규칙(버킷 이름에 `tile` 또는 `derivative` 포함)만 그 자리에 남는다. 같은 목록이 두
   파일에 있으면 한쪽만 갱신된다.

3. `export-industrial-complex-gold-profiles` 의 `r2` 드라이버는 **serving-derivative 연결로만**
   쓴다. lakehouse 자격증명으로 프로필을 쓰는 코드 경로는 없다. 각 객체는 쓰기 직전에
   `assert_serving_derivative_key` 를 통과해야 한다.

4. **env 네임스페이스는 `FOUNDATION_PLATFORM_R2_TILE_DERIVATIVES_*` 를 그대로 쓴다.**
   그 이름은 버킷의 역할보다 좁지만, R2 연결 이름 집합은
   `config/r2-connections.contract.json` 과 `scripts/guard/r2-env-namespace-consistency.sh` 가
   함께 고정하고 있어서 네 번째 네임스페이스를 만드는 것은 계약·가드·배포를 함께 옮기는 별도
   변경이다. 버킷은 하나이고 역할은 serving derivative 이며, 그 사실은 이 ADR 이 기록한다.

5. 읽기는 그대로 lakehouse 다. Iceberg manifest 와 데이터 파일은 정본이므로
   `FOUNDATION_PLATFORM_R2_LAKEHOUSE_*` 로 읽는다. 읽는 버킷과 쓰는 버킷이 다르다는 것이
   이 커맨드의 정상 상태다.

## Consequences

- lakehouse 버킷은 공개 대상이 아니며, 프로필을 쓰는 경로에서 그 버킷을 고를 방법이 없다.
- 새 산출물 종류를 serving-derivative 버킷에 두려면 `SERVING_DERIVATIVE_OBJECT_ROOTS` 에 한 줄이
  필요하다. 즉 "무엇이 공개될 수 있는가"의 변경이 리뷰 가능한 한 곳에서 일어난다.
- serving-derivative 버킷에 커스텀 도메인을 붙일지는 아직 열린 소유자 결정이다. 붙이면 PMTiles
  릴리스와 산업단지 프로필이 함께 노출된다. 둘을 다르게 노출해야 한다면 버킷을 하나 더 만들고
  이 ADR 을 대체하는 결정이 필요하다.
- env 이름(`..._TILE_DERIVATIVES_*`)과 역할(serving derivative 전반)이 어긋난 채로 남는다.
  네임스페이스 개명은 R2 연결 계약과 가드를 함께 옮기는 별도 변경이다.
