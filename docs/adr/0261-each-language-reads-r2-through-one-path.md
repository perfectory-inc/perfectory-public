# ADR 0261: 언어마다 R2 읽기 경로는 하나다

- Status: Accepted
- Date: 2026-10-16
- Related: [ADR-0102](./0102-lakehouse-jobs-survive-r2-connection-resets.md)

## Context

R2 는 놀고 있는 연결을 먼저 끊는다. 그 리셋을 견디는 방법(ADR-0102: 늙은 연결을 먼저 버리고, 깨진 읽기는 새 연결로
지터 백오프 뒤 다시, task 재시도)은 Spark 카탈로그 설정에 9월에 들어갔다. 그 뒤로 같은 수정이 프로그램마다 따로
들어갔다: by-PNU 관문 독자(#559, 2026-10-14), 레이크하우스 유지보수(#594, 2026-10-15). 2026-10-16 전수 조사에서
여전히 재시도가 없던 독자:

- Rust: foundation-outbox 의 seekable 리더(재시도 사이 대기 없음), gongzzang 의 매물 사진 독자(본문 재시도 없음).
- Spark: Hadoop s3a 읽기(재시도 설정 없음), task 재시도 없는 `local[N]` 마스터(계약 11개, 스크립트 10개 등).
- 읽기 루프 넷이 고정 백오프·지터 없음.

새 독자가 생길 때마다 잊을 자리가 하나 더 생겼다.

## Decision

1. **Rust (foundation-platform)**: foundation-outbox `object_storage/r2/read_retry.rs` 가 모든 바이트 읽기의 한 경로다
   (#606). 비동기·blocking 둘 다 같은 시도 함수, 같은 정책(11회, 200ms→30s equal jitter, 바뀌지 않을 응답은 즉시 반환).
2. **Spark/JVM**: 엔진 계약 `r2_read` 한 블록이 숫자를 갖고(#608), `lakehouse_engine` 이 S3FileIO 카탈로그 설정·s3a
   설정·`spark_master()` 를 거기서 만든다. 모든 잡 마스터는 task 재시도를 단다(#610).
3. **영역**: 영역 간 Cargo 경로 의존이 금지(루트 ADR-0001)이므로 R2 를 읽는 영역마다 한 곳. gongzzang 은 지금 두
   파일에 클라이언트가 있고, 같은 정책을 가진 한 모듈로 합친다(후속 PR).
4. **Workers**: R2 바인딩으로 읽고 S3 클라이언트를 만들지 않는다.
5. **가드** `scripts/guard/r2-reads-have-one-path.sh`: `tools/r2-read-paths.contract.json` 이 이름한 곳 밖에서
   aws-sdk-s3 클라이언트 생성(Rust), boto3·botocore·s3fs·pyarrow.fs·fsspec(Python), `fs.s3a.`·S3FileIO 재시도/HTTP
   클라이언트 키(Spark 잡, 시험 제외), S3 클라이언트(TS/JS)를 거부한다. 자가 시험이 넷을 심어 거부를 증명한다.

## Consequences

- 새 독자는 정해진 경로를 쓰는 것 말고 다른 방법이 없다. 경로를 하나 더 만들려면 계약에 적고 리뷰를 받아야 한다.
- Rust 정책 상수와 엔진 계약 `r2_read` 는 같은 숫자다. 두 언어가 같은 값을 쓰는지는 시험이 지킨다(후속).
- 남은 것(별도): aws-cli 읽기의 재시도 환경변수, Workers 의 문서 읽기 재시도, Trino 카탈로그 설정.

## 기각한 대안

- **독자마다 재시도 추가**: 이번까지 세 번 그렇게 했고, 다음 독자가 또 빠진다.
- **영역 공용 크레이트**: 영역 간 경로 의존 금지와 충돌. 영역마다 한 곳 + 같은 숫자 + 가드로 충분하다.
