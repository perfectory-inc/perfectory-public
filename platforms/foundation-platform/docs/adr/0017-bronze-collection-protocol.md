# ADR 0017 — Bronze 수집 프로토콜(단일 경계 SourceConnector)

- **Status:** Accepted (2026-06-26)
- **Relates:** [ADR 0016](./0016-bronze-commit-protocol.md) — WRITE 경계(`BronzeCommitter`).
이 ADR은 수집을 공급하는 **FETCH/loop 경계**다. 두 ADR은 하나의
  ingestion pipeline: `SourceConnector.collect()` → unit별 처리 → `BronzeCommitter.commit()`.
- **Owner directive:** 이 항목에서는 YAGNI를 명시적으로 완화한다. 땜질이 아니라 근본
  architecture에 투자한다. 다만 범위는 절제한다(Non-goals 참조). speculative plugin
  runtime이 아니라 EXISTING shape를 통합한다.

## 배경

`BronzeCommitter`(ADR 0016)는 Bronze **write** authority를 통합했다. **collection** 쪽은
아직 흩어져 있다. 직접 작성한 `*_ingest.rs` module 약 6개와 provider HTTP client 5개가
있고 collection LOOP가 lane마다 그대로 중복된다(이번 점검에서 확인).
- **Page lanes** (building_register, real_transaction, vworld cadastral/ned/land) all run the
  identical loop: `page_requests_for_batch(request, max_pages)` → `client.fetch_page(...)` →
  `committer.commit_*_page(...)` → `schema_profiles_for_plans(...)`. (e.g. real_transaction.rs:96/313/357
  ≡ vworld_cadastral_ingest.rs:120/393/437.)
- **Bulk lanes** (hub.go.kr bulk, vworld dataset file) run a second duplicated loop:
  skip-check (`find_bronze_object_by_source_partition_key`) → `open_file_stream(...)` →
  stream-commit → next job.

이는 ADR 0016이 고정한 write-side scatter에 대응하는 FETCH-side다. 새 source가 loop를
재구현하면 같은 문제(5 lane schema-profile 복사, env-helper drift, lane별 skip/rate-limit
처리)가 반복된다.

## 결정

현실에 맞는 두 shape를 하나의 **collection seam**으로 통합한다.
- **`PageCollector`** — 공통 page loop를 한 번 소유한다. page request 생성 → skip-check →
  `fetch_page` → 각 `RawFetchResult`를 `BronzeCommitter`에 전달 → schema-profile → 다음 page.
- **`BulkCollector`** — 공통 bulk loop를 한 번 소유한다. job 선택 → skip-check →
  `open_file_stream` → `BronzeCommitter`를 통한 stream-commit → 다음 job.

각 source는 provider 정보를 제공하는 작은 source별 선언(trait)을 매개변수로 받는다.
각 source는 이미 존재하는 provider 정보, client call, request builder, per-source plan을 작은
선언으로 전달한다. lane의 `run()`은 *source 선언* → *공통 collector 호출*이 된다. loop,
skip-check, rate-limit acquire/record, commit handoff, schema-profile은 한 곳에 둔다.

- **Provider HTTP client는 provider별로 유지한다.** 완전히 새로운 provider는 자체
  auth/parsing/error-envelope를 가진 client가 여전히 필요하다. 통합하는 것은 wire parsing이
  아니라 그 주변 LOOP다. connector가 loop/skip/rate/commit 중복을 제거할 뿐 새 API response
  shape를 자동 parse하지 않는다.
- **ADR 0016과 조합:** collector가 모든 unit마다 `BronzeCommitter`를 호출하므로 모든
  collected object가 CreateOnly, 복구 가능한 commit protocol, semantic guard를 자동으로 얻는다.

## 근거(정직한 범위)

근거는 **SSOT**(AGENTS.md #6), 위에서 확인한 중복, **ADR 0016 precedent**(같은 통합의
fetch 쪽)이다. connector/source-declaration model은 철학적으로 **Airbyte / Singer / Gobblin**과
같다(source가 shape를 선언하고 runtime이 loop를 소유). 다만 1:1 채택하지 않는다. 이들은
무거운 JVM framework이고 우리는 작은 in-repo Rust seam을 만든다.

## 영향

- **근본 해결**(collection-authority class): 중복된 page/bulk loop를 공통 collector 2개로
  합친다. 새 page/bulk source는 skip-check·rate-limit·retry·commit/recovery·schema-profile을
  물려받는 얇은 선언이 되고 5 lane schema-profile 복사와 lane별 loop drift가 사라진다.
- **해결하지 않는 것**(정직하고 제한된 extension point): 완전히 새로운 API *shape*(cursor
  pagination, GraphQL, token stream, XML, nested/relational)는 새 collector variant와 client가
  필요하다. 다만 모든 lane을 fork하는 대신 shape를 추가할 명시적 위치가 하나가 된다.
  provider별 wire parsing은 provider별로 유지한다.

## 범위 밖(YAGNI를 완화해도 지키는 경계)

기존 TWO shape(page, bulk)만 통합한다. 실제 source가 필요로 할 때까지 speculative
shape(cursor/GraphQL 등)를 추가하지 않는다. generic plugin/registry runtime, config-driven DSL,
Airbyte/Gobblin 채택을 하지 않는다. connector는 committer처럼 작은 Rust seam과 source별 trait로
구성하며 framework가 아니라 실제 중복을 DRY하게 통합한다.

## 계획(Committer 출시를 단계적으로 반영)

1. **committer write-seam을 먼저 고정한다**(ADR 0016 완료): operation-collapse, semantic
   guard, `no-direct-put` guard를 적용해 collector가 강제된 write seam 위에서 동작하게 한다.
2. `PageCollector` seam을 만들고 page lane 하나를 통과시켜 증명한 뒤 나머지 page lane을 옮긴다.
3. `BulkCollector` seam을 만들고 hub.go.kr bulk와 vworld dataset file을 stream commit으로 옮긴다.
4. async data.go.kr lane은 `bronze_object`도 쓰는 `PageCollector` variant로 편입한다
   (ADR 0016 option-a + recovery).
5. 사용하지 않게 된 lane별 loop body를 삭제한다. 이후 page-size D-A, 5 GiB preflight,
   mini-smoke, 재수집을 진행한다.
