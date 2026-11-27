# ADR 0624: 레이크하우스 카탈로그는 자체 호스팅 오픈소스 Lakekeeper 다 — Lakekeeper 는 표의 포인터와 커밋, 표마다의 자격, 권한 확인, 표 보호와 권한 감사만 맡고, 나머지는 표준 도구가, 우리 코드는 계약·조정기·생성기만 맡는다

- Status: Accepted (사장 2026-11-27. 근거: dev 리그 시험 W109 #1772·#1775, W109b #1777, 브레인스토밍 `concepts_brief.lakekeeper-to-the-limit.md`)
- Date: 2026-11-27
- 다루는 것: Silver·Gold 표의 카탈로그(무엇을 Lakekeeper 가, Dagster·DuckDB·iceberg-rust·AWS 가, 우리 코드가 하는가), 레인의 규칙과 그것을 지키는 검사,
  전환의 순서와 걸음마다의 시험, 표 정비를 돌리는 것, 비용.
- 다루지 않는 것: Bronze·`media/` 같은 객체 영역(ADR-0623 의 발급 창구 그대로), 타일, gongzzang 의 카탈로그, Lakekeeper Plus 의 구매(비용만 적는다).
- 이 ADR 이 대신하는 곳(그 자리에는 이 ADR 을 가리키는 날짜 붙은 한 줄을 남겼다):
  [ADR-0559](./0559-duckdb-in-the-publisher-runs-every-transform-and-query-spark-keeps-only-table-maintenance-and-a-transform-moves-only-on-an-engine-equality-verdict.md) §9(정비를 돌리는 것의 이름, 장부는 1 에서 끝난다),
  [ADR-0616](./0616-data-jobs-run-as-dagster-assets-on-aws-ecs-and-the-host-job-layer-is-not-ported.md) :76·:78(카탈로그는 AWS, 데이터는 R2)·:93(상류 정비 컨테이너 하나),
  [ADR-0622](./0622-duckdb-does-every-read-transform-staged-file-and-its-own-memory-and-our-code-does-only-the-commit-the-domain-and-what-duckdb-cannot.md) §2(벤딩 자격으로 `ATTACH`)·:109,
  [ADR-0623](./0623-an-aws-job-starts-by-condition-reaches-the-lakehouse-through-short-credentials-vended-per-lane-and-has-no-time-number-of-its-own.md) §2-8(`metadata_location` 의 `iceberg_scan` 대신 카탈로그)·§2-10(커밋 길은 Lakekeeper),
  [ADR-0332](./0332-an-ingest-record-is-copied-into-a-table-property-before-its-snapshot-can-expire.md)(적재 기록은 같은 커밋에),
  [ADR-0619](./0619-the-aws-platform-account-is-one-opentofu-stack-pinned-to-its-account-with-its-budget-first-and-images-from-ghcr.md) §4 의 "상태에는 비밀이 없다"(결정 5).
  [ADR-0259](./0259-ci-migrates-the-production-table-shape.md) 는 조정기의 드리프트 검사(A6)가 대신한다.
  [ADR-0237](./0237-lakehouse-snapshots-are-kept-30-days-12-month-ends-and-every-year-end.md)·[ADR-0338](./0338-a-reader-of-an-old-snapshot-pins-it-with-its-own-tag-and-the-rest-expire-in-30-days.md) 이 지키는 것은 그대로이고, 지키는 수단이 결정 10-1(㉠)의 태그다.

## Context

사장은 2026-11-27 에 카탈로그의 끝 상태를 자체 호스팅 오픈소스 Lakekeeper 로 정했다(옵션 C, dev 시험이 통과하면). 지금의 카탈로그
(Cloudflare R2 Data Catalog, 아래 "Basin")는 API 토큰만 받고 버킷 전체를 읽는 토큰을 요구한다. 그래서 AWS 의 Silver·Gold 커밋 길은
ADR-0623 §2-10 에 열려 있다.

상류에서 확인한 것(Lakekeeper v0.15.1, `docs/designs/lakekeeper-features-2026-11-27.md`, 그리고 이 ADR 을 쓰며 읽은 소스):
- 오픈소스판은 스냅숏 만료·고아 파일 지우기·합치기를 하지 않는다. 셋 모두 Plus 다. 오픈소스판이 하는 것은 옛 metadata 파일 정리(기본 100 판),
  표의 소프트 삭제와 퍼지다. Lakekeeper 문서도 엔진의 정비 절차를 쓰라고 한다.
- 권한은 OpenFGA 다. `write` 는 데이터·스키마·파티션·정렬·속성·가지·태그·이력을 다 바꾼다. OpenFGA 에서 Grants API 의 목록은 501 이다.
- 기본값이 열려 있다(`allowall`, 무인증 허용). 이벤트는 실패하면 버린다(메모리 채널 1,000, 50 ms).
- 표 밖 파일을 가리키는 커밋을 막는 고침(lakekeeper#2217, 2026-11-26 닫힘)은 아직 어느 릴리스에도 없다(2026-11-27 최신 v0.15.1).
- 저장 프로필의 키 접두는 바꿀 수 없다(`S3Profile::update_with`, ImmutableField). 표 위치가 창고 밖이면 `InvalidLocation` 이다.
- iceberg-rust 0.11 은 load-table 의 `config` 로만 표의 파일 접근을 만들고(`storage-credentials` 는 읽어 두고 쓰지 않는다), 그 위에 사용자
  속성을 덮는다. 그래서 정적 키를 옆에 두면 그 키가 벤딩 자격을 이긴다. 참조(태그·가지)를 바꾸는 공개 호출이 없다(`TableCommit` 의
  빌더가 crate 밖에 닫혀 있다). `ExpireSnapshotsAction` 은 메타데이터에서 스냅숏을 빼기만 하고 파일은 지우지 않는다.
- OpenFGA 의 PostgreSQL 풀은 시작 때 열어 둘 수 있다(`OPENFGA_DATASTORE_MIN_OPEN_CONNS`, v1.14.2 설정 스키마).
- Cognito 의 client-credentials 앱 클라이언트는 무료이고, 토큰 요청이 0.00225 USD 다(서울, AWS Price List API 2026-11-27).

dev 리그(`infra/aws-lakekeeper-dev`, 런북 `docs/runbooks/lakekeeper-dev.md`)의 시험 결과:

| 시험 | 판정 | 무엇을 보았나 |
| --- | --- | --- |
| T1·T3 | 통과 | migrate·서비스·창고, 표가 `tables/<네임스페이스>/<표>/` 에 서고 문자열 접두 형제가 따로 선다 |
| T4 | 통과(결정 2 뒤) | 표 전용 키 접두 `tables` 뒤 운영자도 `bronze/`·`media/`·버킷 루트·다른 버킷·`tablesx/` 에서 거부, 레인은 모두 403 |
| T5·T12 | 통과·일부 | 벤딩 자격은 제 표 접두만, 세션 토큰·갱신 끝점, 부모의 비밀 없음. 자격보다 긴 스캔은 돌리지 않았다 |
| T6·T7 | 통과(T7 단서) | 선언 밖 클라이언트·다른 범위·만료 토큰 401, 권한 부여·회수 즉시. OpenFGA 가 내리면 모두 503 |
| T8 | 통과 | 저장 키 없이 OAuth2 클라이언트 자격과 벤딩 자격만으로 iceberg-rust 0.11 과 우리 커미터가 표를 만들고 덧붙이고, 우리 REST 클라이언트가 태그·묵은 가지·적재 가지를 다룬다. 정적 키를 옆에 두면 첫 저장 호출에서 실패하고 아무것도 커밋하지 않는다(정적 키가 벤딩 자격을 이긴다). CI 가 지은 arm64 퍼블리셔로 |
| T9 | 통과 | DuckDB 2.0 이 OAuth2 비밀로 `ATTACH`, 벤딩 자격으로 읽고 쓴다 |
| T11a·T11b | 통과(설정 하나) | 소프트 삭제·복구·퍼지. 상류 Spark·Iceberg 1.12 정비가 참조를 지키고 표의 고아만 지운다. 단 `s3.delete-enabled=true` 가 있어야 한다 |
| T13 | 통과(결정 4 뒤), 간헐 | 풀 16 을 시작 때 열어 두니 동시 50 로드 묶음 일곱 중 다섯이 모두 200(0.7~1.8 초), 자격 발급 1 번. 업그레이드 카나리아 안의 두 묶음은 22/100·9/100 이 503(OpenFGA p95 3.1 초, RDS 포화는 1 분 단위로 보이지 않음): 공유 micro 에서는 약 3 초 권한 기한에 닿을 수 있다 |
| T14·T15·T16 | 일부·일부·통과 | 태스크를 죽여도 실패·중복·누락 0, 롤링 업그레이드 동안 실패 0. RDS 장애 조치·DB 스냅숏 복원은 돌리지 않았다 |
| T17 | A1 에서 | 이전 방법의 시험이다. A1 의 운영 리그가 제 Basin 읽기 길을 가지므로 거기서 실제 꼴로 돈다(코디네이터 2026-11-27) |
| T22·T32·T33·T35 | 통과 | 로그에 비밀 0, 선언 밖 권한을 찾아 지움, 101 커밋 뒤 스냅숏 id·태그로 읽힘(`metadata_location` 고정은 깨짐), 열린 기본값이면 plan 이 멈춤 |
| T31 | 실패, 결정 1 로 닫음 | `write` 로 열 추가·속성 변경이 된다 |
| T36 | 실패, 결정 3 으로 닫음 | 표 밖 파일을 가리키는 커밋을 받는다 |

업그레이드 카나리아(`canary.sh`, 16 걸음, #1777 의 arm64 이미지로 두 번)는 T13 을 빼고 기대대로다(T31·T36 은 기대한 실패). 첫 실행에서 탐침의
다시 돌기 버그 넷(T3·T5·T7·T33)을 찾아 고쳤다. T13 은 공유 micro 위에서 간헐이고, 운영은 §4-4 와 §7 이 다룬다.

잰 값: Lakekeeper 태스크 메모리 최대 약 18 MB, CPU 최대 21 %(0.25 vCPU 의). OpenFGA 메모리 약 98 MB, CPU 최대 45 %(동시 100). OpenFGA 의
BatchCheck 하나가 DB 질의를 최대 346 번 한다. 공유 RDS(db.t4g.micro)는 시험 중 연결 24, 여유 메모리 최저 84~99 MB 였다.

## Decision

### 1. 카탈로그

1. Silver·Gold 의 카탈로그는 자체 호스팅 오픈소스 Lakekeeper 다. Plus 는 사지 않는다. 버전은 `tools/technology-versions.contract.json` 에 digest 로 핀한다.
2. AWS(플랫폼 계정, 서울, Fargate ARM)에서 돈다: Lakekeeper 태스크 둘, OpenFGA 둘, 제 RDS 인스턴스 하나(결정 4), Cognito 기계 신원.
   데이터는 R2 에 그대로 있다.
3. 데이터 호출은 Iceberg REST 명세로만 한다. Lakekeeper 고유의 호출(`/management/v1`)은 OpenTofu 와 조정기에만 있다. 나가는 길은 다른 REST
   카탈로그로의 register 다.
4. 실패하면 닫힌다: 배포는 `allowall` 이나 OIDC 없는 Lakekeeper 를 거부한다(T35, dev 의 plan 전제 조건이 그 꼴이다).

### 2. 누가 무엇을 하나

"(기억)" 은 아직 시험으로 확인하지 않은 것이고, 그 시험이 §6 에 있다.

| 일 | Lakekeeper(오픈소스) | Dagster | DuckDB·iceberg-rust | AWS·표준 도구 | 우리 코드(만) |
| --- | --- | --- | --- | --- | --- |
| 자격 | load-table 에서 표마다 R2 자격(`select` 는 읽기, `write` 는 읽기·쓰기), TTL·갱신 끝점·캐시 | — | load-table 의 자격만 쓴다. `s3.*` 키·`TYPE s3` 비밀·`'none'` 모드 없음 | 객체 영역(`bronze/`, `media/`)은 ADR-0623 의 발급 창구 | TTL 상수 하나(Lakekeeper 와 창구), 창구의 객체 쪽, R2 클라이언트의 세션 토큰, 정적 R2 키가 있으면 시작 거부 |
| 기계 신원 | OIDC JWT 확인(발급자 여럿 가능) | — | OAuth2 client credentials(DuckDB·iceberg-rust 확인) | 레인마다 Cognito 클라이언트, 그 비밀은 그 레인의 역할만 읽는다 | 선언에서 레인마다 클라이언트를 만드는 IaC |
| 누가 어느 표를 쓰나 | OpenFGA: 입력에 `select`, 출력에 `write`. `create`·`modify` 는 조정기만 | — | — | — | 파이프라인 그래프에서 권한을 만드는 생성기 하나, 표마다 드리프트 확인 |
| 창고·네임스페이스·보호·소프트 삭제 | 저장하고 지킨다, 위치 규칙 | — | — | OpenTofu 는 조정기 태스크를 만든다(미리보기 공급자는 쓰지 않는다, 결정 10-4) | 값과 조정기 |
| 표 만들기·스키마·속성 | 창고 안, 겹침 없음, 빈 위치에만 | 조정기를 자산 하나로 | iceberg-rust 의 `create_table`·스키마 변경(기억, T30) | — | 계약과 조정기 하나. 보존·파일 크기 속성도 조정기가 쓴다 |
| 커밋과 참조 | 원자적 커밋, assert-ref | — | iceberg-rust `fast_append`. 참조는 REST 호출 넷(iceberg-rust 0.11 에 공개 호출이 없다) | — | 배치 토큰을 같은 커밋에, 태그 이름, `delete_unnamed` 와 그 실패의 지표 |
| 읽기 | 읽기 벤딩 | — | DuckDB `ATTACH (TYPE iceberg)`, 스냅숏 id 나 태그로 | — | SQL 만 |
| 스냅숏 만료 | metadata 파일 정리만(만료는 Plus) | 쓰는 쪽 뒤에 정비 자산 | — | 상류 `expire_snapshots`(§5) | 보존은 표 속성과 태그. 그 밖에 없음 |
| 고아 파일 | 표를 지울 때 퍼지, 소프트 삭제 | 정비 자산 | — | 상류 `remove_orphan_files`, 벤딩 자격으로(T11b) | 없음 |
| 합치기 | 없음(Plus) | T21 뒤에만 | 쓰는 쪽이 알맞은 크기로(`write.target-file-size-bytes`) | 상류 `rewrite_data_files` | 없음 |
| 다음 일의 시작 | 이벤트는 쓰지 않는다 | 쓰는 쪽의 materialization 이 스냅숏 id 를 싣는다, 자동화 조건(기억) | — | — | 스냅숏 id 를 메타데이터로 |
| 바뀐 행 | — | — | — | — | 도메인 코드(`__changes`, `gold_versions_change_set`, `silver_changes`) |
| 계보 | — | 자산 그래프 | — | — | 파이프라인 그래프의 선언된 의존 |
| 감사 | 권한 감사 로그(stdout, 일부는 Plus) | run 로그 | 스냅숏 요약 | CloudWatch Logs | 사람의 행동만 해시 사슬에 |
| 카탈로그 화면 | Console 은 열지 않는다(사람 발급자가 없다, 결정 10-7) | UI | — | — | Dawneer 도메인 화면 |

이 표는 기계가 읽는 꼴로도 둔다: `lakehouse-engine.contract.json` 의 `engines.jobs` 블록 하나이고, 이 절은 그것을 가리킨다(P0).

### 3. 레인의 규칙과 그것을 지키는 검사

1. 레인은 파이프라인 그래프에 입력과 출력을, 계약을, 쥐는 태그를 적는다. 그것뿐이다.
2. 레인은 다음을 쓰지 않는다: `s3.*` 키·`TYPE s3` 비밀·`ACCESS_DELEGATION_MODE 'none'`·토큰 변수, 표의 만들기·지우기·`ALTER`·권한 호출,
   카탈로그 폴링·카탈로그를 읽는 센서·이벤트 소비, 만료·고아·합치기 호출, `metadata_location` 고정, 손으로 쓴 Iceberg 읽기, `/management/v1` 호출.
3. 검사(규칙마다 하나, 손의 규칙이 아니다):
   - `engines.jobs` 블록을 `test_engine_ledger.py` 가 확인한다: 모든 `ours` 경로와 이름 붙은 검사가 있다.
   - semgrep: 카탈로그 옆 정적 키 없음, `temp-access-credentials` 는 `r2-vend` 밑에만, `create_table` 은 조정기에만,
     **조정기 밖 `ALTER`·스키마·속성 호출 없음(결정 1)**, `expire_snapshots`·`remove_orphan_files`·`rewrite_data_files` 는 정비 실행기에만,
     `/management/v1` 은 OpenTofu 와 조정기에만, 센서에 카탈로그 REST 호출 없음, 기존 `hand-written-iceberg-scan.yml`(목록이 빌 때까지).
   - 시작·배포: 정적 R2 키가 있으면 일이 시작하지 않는다(`check-lakehouse-catalog-writes` 가 이미 그 꼴이다), `allowall`·OIDC 없음이면 배포 거부.
   - 이 검사들이 막는 실제 사고(ADR-0001): 정적 키가 벤딩 자격을 이겨 레인이 버킷 전체 열쇠로 쓴다(iceberg-rust 0.11 이 사용자 속성을 위에 덮는다,
     T8 의 음성 시험), 쓰는 권한으로 표의 모양이 바뀐다(T31), 운영자가 `bronze/` 에 표를 만든다(T4 첫 결과).

### 4. W109 의 열린 일에 대한 코디네이터 결정(2026-11-27)

1. **T31(`write` 가 열·속성을 바꾼다).** 표의 모양은 권한이 아니라 조정기의 드리프트 검사(fail 모드: 선언한 스키마·속성과 실물을 run 마다 비교)와
   semgrep 규칙 "조정기 밖 ALTER·스키마·속성 호출 없음" 이 지킨다. 카나리아의 알려진 실패로 남긴다: 상류가 `write` 를 좁히면 이 결정을 다시 본다.
2. **T4(운영자가 `bronze/`·`media/` 에 표를 둘 수 있다).** 창고에 표 전용 키 접두(Lakekeeper 저장 프로필의 `key-prefix`, dev `tables`)를 주고, 표는
   조정기만 만든다. 두 번째 버킷은 없다. dev 에서 적용하고 T4 가 통과했다. 운영 창고도 같은 키 접두를 계약값으로 갖는다. 키 접두는 바꿀 수 없으므로
   운영 창고를 만들 때 정한다.
3. **T36(표 밖을 가리키는 커밋).** lakekeeper#2217 을 담은 첫 릴리스가 나오는 대로 버전 계약에 핀한다. 그때까지 쓰는 쪽은 iceberg-rust 이고 표 위치 아래에만
   쓴다(우리 커미터는 `<표 위치>/data/` 에만 올린다). T36 은 업그레이드 카나리아의 엄격한 알려진 실패다: 통과하면 카나리아가 실패하고, 그것이 핀할 때를 알린다.
4. **T13(OpenFGA 연결 4 개).** OpenFGA 의 풀 크기는 계약값이다(dev `authz.max_open_connections`·`min_open_connections`). 운영은 공유 micro 가 아니라
   제 DB 인스턴스를 잰 값으로 정한다(§8), 부하가 자라면 §7 의 순서로 간다. dev 는 풀 16 을 시작 때 열어 두고 T13 이 동시 50 에서 통과했다. 연결을 여는 동안(유휴 4)의 첫 묶음은 49/100 이
   503 이었으므로, 풀은 시작 때 다 연다. 업그레이드 카나리아는 공유 micro 위에서 T13 을 동시 10 으로 돈다(동시 50 은 거기서 간헐이고, 그것은 판의
   회귀가 아니라 공유 DB 의 크기다). 동시 50 의 T13 은 운영의 제 DB 위에서 A1 에서 돈다.
5. **Cognito 클라이언트 비밀이 OpenTofu 상태에 든다.** 받아들인다: 상태는 계정 안의 암호화된 버킷이고(SSE-S3, 공개 접근 차단, TLS 만, ADR-0619 §4),
   접근이 좁다. ADR-0619 §4 의 "상태에는 비밀이 없다" 는 이 예외를 적은 문장으로 바뀐다. 비밀의 값은 Secrets Manager 에도 들고, 레인의 역할만 읽는다.
6. **리그는 T8·T17 이 끝날 때까지 남는다.** 그동안 업그레이드 카나리아다(`canary.sh`, 계약의 `upgrade_canary`). 비용은 §8.
7. (덧붙임) **T17 은 A1 에서 돈다.** 이전 방법의 시험이고, A1 의 운영 리그가 제 Basin 읽기 길을 설계로 가지므로 거기서 실제 꼴로 돈다. dev 를 위해
   비밀을 옮기거나 노트북에 설치하거나 사장이 붙여 넣지 않는다.

### 5. 표 정비를 돌리는 것(사장 결정 '가')

1. Iceberg 의 정비 절차(`expire_snapshots`, `remove_orphan_files`, `rewrite_data_files`)를 상류 그대로의 Spark 컨테이너 하나가 돌리고, Dagster 가 그것을
   자산으로, 그 표를 쓰는 자산들 뒤에 예약한다. 도메인 규칙은 그 안에 없다: 보존은 표 속성(`history.expire.*`)과 참조 보존(`max-ref-age-ms`)의 태그다.
2. 자격은 벤딩뿐이다. 실행기에는 `s3.delete-enabled=true` 가 있어야 한다(T11b: 없으면 소프트 삭제 창고가 보낸 false 로 아무것도 지우지 않고 지웠다고 센다).
   Spark 의 표 지우기는 Iceberg 1.12 이상과 `rest-catalog-purge` 로 소프트 삭제를 거친다.
3. 고아 지우기를 처음으로 켠다(지금은 돌지 않는다). `delete_unnamed` 의 실패한 지우기는 지표로 낸다.
4. Plus 는 설정을 바꾸는 것으로 갈아탈 수 있게 둔다. 사지 않는다. iceberg-rust 의 만료는 파일을 지우지 않으므로(0.11) 실행기가 되지 못한다.

### 6. 순서와 시험

걸음마다 조건이 있다. 시간 창도 서명도 없다. P 는 Basin 위(호스트 일에도 대부분 해당), A 는 전환 뒤 Dagster 구성 요소 하나씩, 의존 순서다.

| # | 걸음 | 앞 조건 | 증거 | 되돌리기 | 이 걸음에서 지우는 것 |
| --- | --- | --- | --- | --- | --- |
| P0 | 문서 고침: CB·RES 의 Plus 줄, ADR-0559 §9·ADR-0622 :109 의 실행기 이름, 창구의 표 쪽을 적용하지 않음, `engines.jobs` 블록 | 없음 | CI | revert | — |
| P1 | Trino JDBC 파일 지움, 읽기 관문은 읽기 토큰 | 없음 | 참조 0, 읽기 토큰의 쓰기 거부 | revert | 파일 2, 관문 2 의 쓰기 토큰 |
| P2 | 손으로 쓴 읽기를 DuckDB `ATTACH` 로 | 밀어내기 측정 | T10(개정) | revert | 약 2,415 줄, `LakehouseObjectReader` |
| P3 | 적재 기록을 같은 커밋에 | 없음 | T27 | revert | ADR-0332 의 복사 |
| P4 | 모든 고정을 그 쥔 쪽이 만드는 태그로, 월말은 참조 보존의 태그로 | P3 | T28 | revert | `held_by_pins`, 호스트의 keep JSON, `retention_tags`, `ExpiryRefused` |
| P5 | 정비를 표 속성으로 움직이는 상류 호출로, Dagster 가 쓰는 쪽 뒤에, 고아 지우기 켬 | P3, P4 | T29, Basin 에서 T11b | 옛 작업 다시 | `would_expire`, 합치기 규칙, `fold_deletes`, 04:30 cron, 적재 뒤 방아쇠 |
| P6 | REST 클라이언트의 전송을 `iceberg-catalog-rest` 로, 겉은 그대로 | T25 | T26 | revert | 우리 reqwest 의 load·config·commit |
| P7 | 정적 키 지움, 세션 토큰, DuckDB 벤딩 모드, 시작 거부 | P2, P6 | T18, T8 의 음성 | 환경 되돌림 | 정적 키 읽기 셋 |
| P8 | 조정기 하나가 표를 만들고 바꾼다 | 선언 = 실물 목록 | T30 | revert | 레인의 만들기 14 곳, Spark `ensure_contract_table`, 배포 때 스키마 이전 |
| P9 | 쓰는 쪽이 Dagster 자산, 그 materialization 이 방아쇠 | ADR-0616 의 쓰는 쪽 | T34 | 예약 다시 | `asset_reconcile.py`, `iceberg_metadata.py`, `decide.py` |
| P10 | DataHub·`lineage.py` 지움, Trino 를 거두고 그 검사를 DuckDB 로(결정 10-5) | Dagster 가 간선을 보임 | 같은 검사 결과 | revert | DataHub 44 파일, `lineage.py`, Trino |

**전환 관문.** dev 리그가 개정된 T1~T9, T11a, T12~T16, T22, T31~T33, T35, T36 을 통과한다(T31·T36 은 결정 1·3 의 꼴로).

| # | 걸음 | 앞 조건 | 증거 | 되돌리기 | 지우는 것 |
| --- | --- | --- | --- | --- | --- |
| A1 | 운영 Lakekeeper·OpenFGA·발급자·조정기(dev 와 같은 모듈에 운영의 입력, `infra/aws-lakekeeper`). 그다음 구성 요소마다 복사 또는 다시 짓기, 비교, 전환 | 전환 관문 | T17, 동시 50 의 T13 | 설정을 Basin 으로(얼려 둔다). Lakekeeper 에서 쓴 정본 표는 역 `INSERT … SELECT` | 그 구성 요소의 Basin 표 |
| A2 | 창구의 핀 호출·`read_tables`·`iceberg_catalog.py` | 그 레인이 읽는 표가 Lakekeeper 에 | T5, T9, T33 | revert | 78 줄과 핀 코드 |
| A3 | 카탈로그 토큰·제공자 enum·헤더 스위치·`__r2_data_catalog` 문자열 | 모든 구성 요소가 옮김 | T6, T7, Basin 호출 0 | — | 약 70 파일 |
| A4 | 권한은 만든다, 레인은 `write` 만, 닫힌 실패 | A1 | T31(결정 1), T32, T7 | revert | 레인의 `modify` |
| A5 | 정비 실행기가 Lakekeeper 벤딩으로, 운영에서 고아 지우기 | T11b | T11b, T15 | 자산 멈춤 | (Plus 를 사면 Spark 정비) |
| A6 | 모양 장부·스키마 이전·알려진 드리프트 목록이 조정기의 드리프트 검사(fail) | A4 | fail 모드의 T30 | report 모드 | ADR-0259 의 파일 |
| A7 | 콘솔의 카탈로그 경로를 깊은 링크로 | 사람 발급자(출시 때 없다, 결정 10-7: 그때까지 이 걸음은 서지 않는다) | T37 | revert | `entities.rs`·`facts.rs` 의 카탈로그 부분, `snapshots.rs` |
| A8 | 감사 로그를 CloudWatch 로 | A1 | T22 | — | — |
| A9 | Basin 끔, `__r2_data_catalog/` 지움 | Basin 의 고정·읽기 0 | T19 | 없음 | 제공자 갈래와 문자열 |

새 시험(브레인스토밍 §4, 걸음이 부른다): T25(iceberg-rust 의 참조 호출 — 0.11 소스로 "없음"이 답이고, 그래서 REST 호출 넷이 남는다), T26, T27, T28,
T29, T29b(검사와 `published` 태그로 게시), T30, T34, T37, T38(만료 없이 덧붙이기만 한 표의 metadata.json 크기와 계획 시간).

### 7. DB 부하가 자랄 때(이 순서로, 우리 코드 없이)

코디네이터 2026-11-27. 앞 걸음이 모자랄 때만 다음 걸음으로 간다. 걸음마다 잰 값이 근거이고, 바뀌는 것은 계약값과 IaC 뿐이다.

1. **인스턴스마다의 풀 크기**를 잰 값의 계약값으로 둔다(지금 OpenFGA 16, 시작 때 모두 열기; Lakekeeper 읽기·쓰기 풀 2·2). 시험: T13 의 동시 수를 올려 다시 잰다.
2. **상태 없는 복제를 늘린다**: Lakekeeper·OpenFGA 태스크 수(둘 다 상태가 DB 에만 있다). 연결 수는 복제 × 풀로 자란다.
3. **RDS Proxy 를 앞에 둔다**: 복제 × 풀이 DB 의 `max_connections` 에 다가갈 때. Lakekeeper(sqlx)와 OpenFGA(pgx)의 준비된 문장이 Proxy 에서
   연결을 고정하는지(그러면 나눠 쓰는 이득이 준다)는 기억이고, 그 걸음에서 상류 문서와 시험으로 확인한다.
4. **OpenFGA 자체의 확인 캐시**: dev 는 이미 켰다(`OPENFGA_CHECK_QUERY_CACHE_ENABLED`·`CHECK_ITERATOR_CACHE_ENABLED`·`CACHE_CONTROLLER_ENABLED`,
   Lakekeeper 의 운영 권고; 상류 기본 한도 10,000, TTL 10 초). 이 걸음은 그 한도와 TTL 을 계약값으로 키우는 것이다. TTL 동안은 회수한 권한이
   통과할 수 있다(이미 준 벤딩 자격이 회수로 죽지 않는 것과 같은 꼴, T7). 그 길이는 사장에게 보고하는 값이다.
5. **더 큰 전용 운영 DB** 는 마지막이다(§8 의 db.t4g.small 에서 다음 등급으로).

이 순서와 따로, 운영 DB 는 지금 Single-AZ 이고 출시 때 Multi-AZ 를 켠다(사장 2026-11-27). 그것은 계약값 하나(`database.multi_az`)이고
RDS 가 대기 복제본을 제자리에서 만든다: 데이터 이동이 없다. 그때까지 자동 백업과 시점 복원(PITR)이 지킨다.

운영을 세울 때 경보를 함께 둔다(ADR-0616 의 경보 길, Slack): RDS 의 `DatabaseConnections` 가 `max_connections` 의 정한 몫을 넘을 때,
`FreeableMemory` 가 바닥에 닿을 때, OpenFGA 확인 지연(로그의 `grpc_req_complete` `query_duration_ms` 에서 CloudWatch 지표 필터로 p95)이
Lakekeeper 의 기한에 다가갈 때, Lakekeeper 의 `AuthorizationBackendError`(503, 감사 로그의 `internal_authorization_error`)가 하나라도 날 때.
모두 기존 로그와 AWS 지표의 설정이고 우리 코드가 아니다.

### 8. 비용 (월, 서울, AWS Price List API 2026-11-27, 계산값)

**dev 리그(지금, 결정 6 동안):** Lakekeeper 2 × 0.25 vCPU / 0.5 GB 16.58, OpenFGA 8.29, 공인 IPv4 셋 10.95, Secrets Manager 9 개 3.60,
Cloud Map·호스팅 영역 0.80, 로그 약 1.5, Cognito 토큰 약 1, 일회성·탐침 1 미만, 합 **약 43 USD**(공유 RDS 에는 새 비용 없음). 크기는 잰 값
(Lakekeeper 메모리 3.6 %, CPU 21 %)으로 Fargate 의 가장 작은 ARM 이다. W109b 의 풀 16 은 비용을 바꾸지 않는다.

**운영(추정, A1 에서 만든다):**

| 무엇 | 크기(근거) | USD |
| --- | --- | --- |
| Lakekeeper | 2 × 0.25 vCPU / 0.5 GB(잰 값: 메모리 18 MB, CPU 21 %), 두 AZ | 16.58 |
| OpenFGA | 2 × 0.25 vCPU / 0.5 GB(CPU 45 % at 동시 100), Lakekeeper 와 같은 수 | 16.58 |
| 제 RDS(결정 4) | db.t4g.small, gp3 20 GB, 자동 백업·PITR(micro 공유에서 여유 메모리 최저 84 MB; 이 둘만의 연결은 Lakekeeper 2 × 8 + OpenFGA 2 × 16 = 48). 지금 Single-AZ(0.051/h, gp3 0.131/GB-월), 출시 때 Multi-AZ(0.102/h, 0.262/GB-월). 백업은 DB 크기까지 무료 | 지금 37.23 + 2.62 = 39.85, 출시 뒤 74.46 + 5.24 = 79.70 |
| 공인 IPv4 | 태스크 넷 × 0.005/h | 14.60 |
| Secrets Manager | 0.40 × (암호화 키·로그인 둘·사전 공유 키 = 4, 그리고 레인마다 하나; 레인 60 이면) | 25.60 |
| Cognito 토큰 | 0.00225 × run 마다 하나 이상, 월 3,000~15,000 | 7~34 |
| 정비 실행기 | 상류 Spark 2 vCPU / 8 GB 하루 약 1.5 시간(잰 값 아님) | 약 5 |
| 로그·Cloud Map | 감사 로그 월 약 2 GB × 0.76, 호스팅 영역 | 약 2.3 |
| 합 | | **지금(Single-AZ) 약 128~155, 출시 뒤(Multi-AZ) 약 167~195** |

Single-AZ 와 Multi-AZ 의 차는 월 39.85 USD 다. Single-AZ 동안 RDS 장애 조치(T14 의 남은 부분)는 시험할 수 없다: 출시 때 Multi-AZ 를 켜고 돌린다.
플랫폼 계정의 예산은 200 USD 다(ADR-0619 §5 의 2026-11-27 개정, #1768). Plus 는 사지 않으므로 그 값은 적지 않는다.

### 9. 낱말

- **Basin**: 지금의 Cloudflare R2 Data Catalog. **Spark 이동**: Spark 작업 하나를 DuckDB 로 옮기는 일(ADR-0559).
- **고정(pin)**: 살아남아야 하는 스냅숏. 그것은 Iceberg 태그뿐이다. Lakekeeper 의 관리 태그는 카탈로그 꼬리표이고 Iceberg 참조가 아니다.
- **보호(protection)**: 지우기를 막는다. 만료는 막지 않는다.
- **정비(maintenance)**: 이 ADR 에서는 §5 의 세 절차뿐이다. Lakekeeper 의 metadata 파일 정리는 "metadata 정리"라 부른다.
- **벤딩(vending)**: 카탈로그가 load-table 에서 표 하나의 짧은 저장 자격을 주는 것. **창구**: ADR-0623 의 객체 영역 발급 함수.
- **방아쇠**(다음 일을 시작시키는 것)와 **바뀐 행**(row change set)은 다른 것이다.

### 10. 사장 결정(2026-11-27, 브레인스토밍 §6 의 남은 것)

1. **㉠ 이력.** 파생 표의 월말·연말 이력(ADR-0237)과 만료 바닥(ADR-0338)은 지킨다(사장 결정 2026-10-14 그대로). 지키는 수단은 참조 보존
   (`max-ref-age-ms`)의 태그다(P4). 우리 코드의 보존 목록은 없다.
2. **㉡ 순서.** Lakekeeper 가 AWS 의 첫 Silver·Gold 커밋 전에 선다. Basin 위의 AWS 커밋 길도, 버킷 쓰기 토큰의 예외도 없다.
3. **㉢ 감사.** Lakekeeper 의 로그와 스냅숏 요약이 CloudWatch Logs 로 간다(A8). 커밋은 해시 사슬에 넣지 않는다: 해시 사슬은 사람의 행동만이다.
4. **㉣ 창고·네임스페이스·표·권한을 만드는 것.** 미리보기 OpenTofu 공급자는 쓰지 않는다. OpenTofu 는 부트스트랩·조정기 태스크를 만들고, 그 태스크가
   창고·네임스페이스(`silver`, `gold`)·선언된 표·레인 선언에서 만든 권한을 만들고 드리프트를 fail 모드로 확인한다(§2, P8, A4, A6).
5. **㉤ Trino** 는 거둔다(P10). OPA 다리는 두지 않는다.
6. **㉥ 복구.** 표는 Bronze 에서 다시 짓는다. Bronze 로 다시 지을 수 없는 표만 복제한다. 그 목록은 손으로 적지 않고 파이프라인 그래프에서 나온다:
   `feeds` 간선을 거슬러 원천 묶음(Bronze)에만 닿지 않는 표다. 2026-11-27 의 목록:
   - Bronze 밖에서 오는 것: `silver.map_edit_ledger`(지도 편집 저장소, 사람의 편집). 그것을 읽는 `gold.industrial_complex_boundary_served`·
     `gold.administrative_boundary_served`·`gold.map_edit_stale` 은 그 원장과 Bronze 로 다시 짓는다.
   - 그래프가 입력을 적지 않은 것(입력을 적으면 목록에서 빠진다): `silver.complex_parcel_memberships`, `gold.complex_catalog`,
     `gold.complex_spatial_locator`, `gold.parcel_sub_parcels`, `gold.parcel_energy`, `gold.parcel_demolitions`, `gold.lineage_review_queue`,
     `gold.parcel_building_permits`, `gold.parcel_factories`, `gold.parcel_nearby`, `gold.parcel_register_masters`,
     `gold.real_transaction_region_stats_versions`.
7. **사람의 Console 발급자** 는 출시 때 없다. Dawneer 가 하나의 콘솔이다. A7 은 사람 발급자가 생길 때까지 서지 않는다.

## Consequences

- AWS 의 Silver·Gold 커밋 길(ADR-0623 §2-10)이 닫힌다: 레인은 Cognito 토큰으로 Lakekeeper 에 묻고, 표마다의 벤딩 자격으로 쓴다. 버킷 전체 쓰기 토큰은 AWS 에 오지 않는다.
- Spark 장부는 1 에서 끝난다(정비 실행기). 우리 정비 작업(947 줄)의 도메인 규칙은 P3·P4 에서 커밋·태그·표 속성으로 옮겨야 실행기를 갈 수 있다.
- 우리가 새로 짓는 것: 조정기 하나(약 200~250 줄, 표·권한·드리프트), 파이프라인 그래프에서 권한·창구 범위를 만드는 생성기 하나, `engines.jobs` 블록, 위 semgrep 규칙.
- 우리가 지우는 것: 손으로 쓴 읽기(약 2,415 줄), 우리 REST 전송(1,472 줄 중 전송 부분), 카탈로그 폴링(1,605 줄), 흩어진 표 만들기, 창구의 표 쪽, DataHub, Trino JDBC.
- 비용: dev 약 43 USD/월(결정 6 동안), 운영 약 128~155 USD/월(Single-AZ, 지금), 출시 뒤 약 167~195 USD/월(Multi-AZ). 플랫폼 계정 예산은 200 USD 다.
- 위험: Lakekeeper 는 1.0 전이고 회사 하나가 만든다(dev 가 업그레이드 카나리아, 데이터 호출은 REST 명세만, 나가는 길은 register). #2217 전까지 커밋의 위치 검사는
  우리 커미터에 달렸다. OpenFGA 와 RDS 가 모든 표 호출의 길에 선다(OpenFGA 가 내리면 모두 503, T7).
- 확인하지 못한 것: RDS 장애 조치(T14 의 남은 부분, 출시 때 Multi-AZ 에서)와 DB 스냅숏 복원(T15 의 남은 부분), 자격보다 긴 스캔과 두 자격에 걸친
  multipart(T12), T17·동시 50 의 T13(A1), T25~T30·T34·T37·T38.

## 기각한 대안

| 대안 | 왜 |
| --- | --- |
| 오픈소스 Lakekeeper 가 만료·고아 지우기를 한다 | 둘 다 Plus 다 |
| 정비 없음 | metadata.json 은 커밋마다 자라고, Gold 는 지금 덮어쓰며, 실패한 지우기의 고아가 쌓인다. T38 이 덧붙이기만 하는 표에서 가설로 잰다 |
| Lakekeeper Plus | 유료, 오픈소스 아님, 그 합치기는 쓰기-감사-게시 표를 건너뛰고 만료는 커밋 뒤에만 돈다. 설정 교체로 남긴다 |
| iceberg-rust 의 만료 | 0.11 의 `ExpireSnapshotsAction` 은 파일을 지우지 않는다 |
| 우리 손의 만료(Rust REST 커밋) | 파일 정리를 잃고 ADR-0559 가 금한 손의 만료다 |
| 두 번째(표 전용) 버킷 | 결정 2: 키 접두로 같은 막음을 얻는다 |
| 권한으로 표의 모양을 지킨다(`write` 밑에 더 좁은 권한) | 결정 1: 상류에 그런 권한이 없다. 드리프트 검사(fail)와 semgrep |
| `ContractVerification` 을 넣어 지은 서버, 또는 들임 관문 | 1.0 전 서버의 포크이고, 강제하는 관문은 Plus 다 |
| Grants API 목록으로 드리프트 확인 | OpenFGA 에서 501 이다 |
| Lakekeeper 이벤트(CloudEvents → NATS·Kafka → 센서) | 실패하면 버리고, outbox 도 소비자도 없다 |
| 우리 reqwest 클라이언트에 OAuth2·벤딩을 가르침 | `iceberg-catalog-rest` 가 이미 한다 |
| 손으로 쓴 읽기를 벤딩 자격으로 옮김 | 지우는 것이 더 싸다 |
| `metadata_location` 고정(ADR-0623 :67-69) | metadata 파일은 100 판 뒤에 지워진다(T33) |
| IAM 바깥 연합을 첫 기계 신원으로 | Lakekeeper 에 IAM 로그인이 없고, 엔진마다 받아 오는 접착 코드가 든다 |
| 기계 신원을 위해 Zitadel 을 AWS 로 | 그 일에 필요 없다 |
| LoQE | 브라우저에 자격을 두고 버킷 CORS 가 든다 |
| Basin 을 위한 "vended" 제공자 | 결정 10-2(㉡): Basin 위의 AWS 커밋 길이 없으니 버리는 일이다 |
| dev 에서 T17 을 위해 Basin 읽기 비밀을 옮기거나 노트북에서 포트 포워딩 | 결정 7: A1 의 실제 꼴에서 돈다 |

2026-11-27: 사장 승인(Accepted), §10 의 열린 결정은 결정으로 옮겼다. main 에 있는 것: dev 리그와 그 탐침·업그레이드 카나리아(`infra/aws-lakekeeper-dev`,
#1772·#1775·#1777, #1780 이 되돌린 것을 W109c 가 되살렸다), T8 명령(`check-lakehouse-catalog-writes`), REST 카탈로그 열기 모듈(`lakehouse_rest_catalog`).
운영 카탈로그(A1)는 W109c 가 짓는다.

2026-11-27 (W109c): `infra/aws-lakekeeper-dev` → `infra/aws-lakekeeper`, `docs/runbooks/lakekeeper-dev.md` → `docs/runbooks/lakekeeper.md`: dev 와 prod 가
한 스택이고 입력은 `environment` 하나, 차이는 두 계약(`config/lakekeeper-dev.contract.json`·`config/lakekeeper-prod.contract.json`)에 있다. A1 의 운영
카탈로그를 그 스택의 prod 로 적용했다(제 RDS db.t4g.small Single-AZ·자동 백업 7 일·PITR, Lakekeeper 둘·OpenFGA 둘, Cognito, 경보 다섯, 창고 R2 자격의
빈 칸). 조정기는 `reconcile-lakehouse-catalog` 이고, prod 의 창고는 사장이 새 토큰 `perfectory-lakekeeper-prod` 를 넣은 뒤에 만든다.

2026-11-28 ([ADR-0626](./0626-every-kind-of-data-name-has-one-home-every-other-spelling-is-generated-and-a-name-outside-the-rules-is-refused.md) §5, §9): 조정기의 표 목록(`reconciler.tables`)은 손 목록이 아니라 전환한 구성 요소의 표 계약에서 생성한다. 네임스페이스는 `silver`·`gold` 둘이고, `reference`·`intelligence` 표는 `silver` 에 든다. 표 이름은 그 ADR 의 지도대로 전환 PR 에서 선다.
