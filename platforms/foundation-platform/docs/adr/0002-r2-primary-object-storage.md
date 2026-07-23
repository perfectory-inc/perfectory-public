# ADR 0002 - R2 Primary Object Storage

| 항목 | 내용 |
|---|---|
| 작성일 | 2026-05-12 |
| 상태 | Accepted |
| 범위 | `gongzzang`, `dawneer`, `foundation-platform` object storage naming and provider policy |

## 결정

세 서비스의 primary object storage는 **Cloudflare R2**로 둔다.

도메인, DB, API, DTO, 서비스 포트에서는 provider 이름을 쓰지 않고 `objectKey`,
`object_key`, `ObjectStorageService`를 표준으로 쓴다. `s3Key`, `s3_key`,
`S3Service`, `S3_BUCKET_NAME` 같은 이름은 신규 코드와 스키마에서 금지한다.

단, Cloudflare R2가 제공하는 S3-compatible API를 호출하기 위한 구현 세부에서는
외부 SDK 이름을 그대로 둘 수 있다.

- `@aws-sdk/client-s3`, `S3Client`, `PutObjectCommand` 같은 SDK/API 타입
- `s3://` scheme을 요구하는 일부 외부 도구 설정
- "S3-compatible API"라는 기술 설명

이 예외는 provider 선택이 아니라 R2 접근 방식에 대한 구현 설명이다.

## 이유

`gongzzang`과 `dawneer`는 이미지, 도면, 3D asset, 고시/첨부 파일, 벡터 자료처럼
사용자에게 많이 읽히는 객체를 다룬다. 트래픽이 커질수록 egress 비용과 CDN 친화성이
중요하다. R2는 S3-compatible API를 제공하면서 egress 비용 부담이 낮아 현재 제품
방향과 잘 맞는다.

반면 `objectKey`는 저장소 제품과 무관한 데이터 모델 이름이다. 나중에 R2 외 다른
object storage를 붙이더라도 DB/API 계약을 다시 바꾸지 않아도 된다.

## 표준 명명

| 영역 | 표준 | 금지 |
|---|---|---|
| Provider | `Cloudflare R2`, `R2` | `AWS S3`를 primary provider로 표기 |
| Bucket | `R2 bucket` | `S3 bucket` |
| Object key | `objectKey`, `object_key` | `s3Key`, `s3_key` |
| Thumbnail key | `thumbnailObjectKey`, `thumbnail_object_key` | `thumbnailS3Key`, `thumbnail_s3_key` |
| Service interface | `ObjectStorageService` | `S3Service` |
| Env | `R2_BUCKET_NAME`, `R2_ENDPOINT`, `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY` | `S3_BUCKET_NAME`, `S3_ENDPOINT` |
| Metric | `object_storage_operation_*` | `s3_operation_*` |
| API query | `object_key` | `s3_key` |

## Migration policy

1. 서비스 전 환경에서는 legacy compatibility alias를 남기지 않는다.
2. DB/API/DTO는 `objectKey`와 `object_key`로 바로 정리한다.
3. 런타임 provider 설정은 `R2_*`만 공식으로 사용한다.
4. AWS SDK에 넣는 R2 credential은 AWS SES/IAM credential과 공유하지 않는다.
5. "S3" 표현은 SDK/package 이름 또는 "S3-compatible API" 설명에만 허용한다.

## R2 credential shape

```text
R2_ACCOUNT_ID=
R2_BUCKET_NAME=
R2_ENDPOINT=
R2_REGION=auto
R2_ACCESS_KEY_ID=
R2_SECRET_ACCESS_KEY=
R2_PUBLIC_BASE_URL=
FOUNDATION_PLATFORM_R2_SMOKE_OBJECT_KEY=gold/_smoke/foundation-platform-r2-smoke.json
```

`R2_ENDPOINT` 명시를 권장한다. `R2_ACCOUNT_ID`만 있으면
`https://<account_id>.r2.cloudflarestorage.com` 형식으로 endpoint를 만들 수 있다.

## Smoke policy

실제 R2 연결 검증은 provider-neutral `ObjectStorageService` 계약을 유지하되,
R2 adapter에서 dedicated smoke object를 write/read/delete 하는 방식으로 수행한다.

```bash
cargo run -p foundation-outbox-publisher --bin foundation-outbox-publisher -- smoke-r2
```

테스트 러너에서 live R2 round-trip을 실행할 때는 `FOUNDATION_PLATFORM_R2_LIVE_SMOKE=1`을
명시적으로 설정한다. 기본 `cargo test -- --ignored` 로컬 검증은 외부 R2 credential
없이도 완료되어야 하며, 실제 R2 write/read/delete는 opt-in 된 smoke에서만 수행한다.

기본 smoke key는 `gold/_smoke/foundation-platform-r2-smoke.json` 이다.
`gold/manifest.json` 은 runtime pointer 이므로 smoke 대상으로 금지한다.
