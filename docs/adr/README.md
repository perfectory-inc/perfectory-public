---
status: current
owner: repository-maintainers
doc_type: catalog
last_reviewed: 2026-07-28
---

# 전역 ADR 목록

모노레포 전체에서 사용하는 단일 ADR 번호 체계입니다. 영역에만 적용되는 결정도 다음
전역 번호를 사용합니다. 각 영역의 기존 `docs/adr/` 번호 체계는 마지막 번호에서
동결하며, 영역 결정은 `GZ-ADR-NNNN`, `FP-ADR-NNNN`, `IDP-ADR-NNNN`,
`ITP-ADR-NNNN`처럼 영역 접두사를 붙여 인용합니다.

- [0001 — 모노레포 거버넌스와 규칙](./0001-monorepo-governance-and-conventions.md)
- [0002 — 문서 분류와 보관](./0002-docs-taxonomy-and-archive.md)
- [0003 — 문서 물리 배치](./0003-docs-physical-taxonomy.md)
- [0004 — 검증 단일 진실 원천(`cargo xtask verify`)](./0004-verification-ssot.md)
- [0005 — 훅은 조언, CI는 권위](./0005-hooks-advisory-ci-authoritative.md)
- [0006 — 객체 저장소 우선 제공](./0006-object-storage-first-serving.md)
- [0007 — 공개 코드 단일 원천과 비공개 운영 경계](./0007-public-code-private-operations-boundary.md)
- [0008 — 수동 의존성 업데이트와 조직 브랜치](./0008-manual-dependency-updates-and-organization-branches.md)
- [0009 — 한글 정본 문서와 다국어 확장 준비](./0009-korean-first-documentation-and-multilingual-readiness.md)
- [0010 — 라이브 자원 테스트 레인 (`LiveLane`)](./0010-live-resource-test-lanes.md)
- [0011 — 테스트 실행 집합 완전성](./0011-test-execution-set-completeness.md)
- [0012 — 검증 결과는 그 문면대로여야 한다](./0012-verification-results-must-mean-what-they-say.md)
- [0013 — 릴리스 유일성은 두 소스 종류를 함께 허용한다](./0013-release-uniqueness-admits-both-source-kinds.md)
- [0014 — serving generation은 한 단위의 소스 선택만 추적한다](./0014-serving-generation-tracks-one-unit-source-selection.md)
- [0015 — 키를 가진 Catalog mutation은 원장 하나를 쓴다](./0015-one-idempotency-ledger-for-keyed-catalog-mutations.md)
- [0016 — PostGIS 적재는 신원을 가진 하나의 사실이다](./0016-a-postgis-projection-load-is-a-fact-with-an-identity.md)
- [0017 — 데이터 리비전은 그것이 개정하는 단위에 속한다](./0017-a-data-revision-belongs-to-the-unit-it-revises.md)
- [0018 — 두 언어가 같은 어휘를 적으면 대조한다](./0018-a-vocabulary-written-in-two-languages-is-compared.md)
- [0019 — 소속은 한쪽의 컬럼이 아니라 기간을 가진 사실이다](./0019-membership-is-a-dated-fact-not-a-column.md)
- [0020 — 도형은 사실의 근거가 아니다](./0020-geometry-is-not-evidence-for-a-fact.md)
- [0021 — 아무도 읽지 않는 표면은 옮기지 않고 지운다](./0021-an-unread-surface-is-deleted-not-migrated.md)
- [0022 — "현재"는 오늘이고, 그것을 말하는 뷰는 하나다](./0022-current-means-today-and-one-view-says-so.md)
- [0023 — 편집은 원장의 행이지, 고쳐진 행에만 남는 것이 아니다](./0023-an-edit-is-a-row-in-the-ledger-not-only-in-the-row.md)
- [0024 — 서빙 투영은 타일 계약이 지명한 것만 싣는다](./0024-the-serving-projection-carries-only-what-the-tile-contract-names.md)
- [0025 — 필지 발행은 봉인된 Iceberg 증거 하나를 지명한다](./0025-parcel-publication-names-one-sealed-iceberg-evidence.md)
- [0026 — 필지 증거 봉인자가 유일한 append 경계다](./0026-parcel-evidence-sealer-is-the-only-append-boundary.md)
- [0027 — 모든 가드는 자기 위협 모델을 선언한다](./0027-every-guard-declares-its-threat-model.md)
- [0028 — 공급망 취약점 게이트는 OSV 래칫을 함께 쓴다](./0028-supply-chain-vulnerability-gate-uses-an-osv-ratchet.md)
- [0029 — 필지 발행 실행 증거는 terminal run에서 쓴다](./0029-parcel-publication-evidence-is-written-from-the-terminal-run.md)
- [0030 — 필지 발행 증거는 서로 다른 두 승인을 구별한다](./0030-parcel-publication-evidence-requires-two-distinct-approvals.md)
- [0031 — 필지 mirror run이 발행 scope와 limit을 봉인한다](./0031-parcel-mirror-run-seals-publication-scope.md)
- [0032 — 제공기관 ID는 도메인 라벨에서 파생한다](./0032-provider-identity-is-derived-from-domain-label.md)
- [0033 — 주소 출처가 없는 산업단지는 표현할 수 없다](./0033-an-industrial-complex-without-a-sourced-address-is-not-representable.md)
- [0034 — 행정구역 코드는 자기 정밀도를 싣고 다닌다](./0034-an-administrative-code-carries-its-own-granularity.md)
- [0035 — 쓰지 않는 지역은 필수가 아니다](./0035-a-region-the-pipeline-does-not-use-is-not-required.md)
- [0036 — 가리켜지는 객체는 그것을 쓴 커맨드를 가진다](./0036-a-pointed-at-object-has-the-command-that-wrote-it.md)
- [0037 — 포인터는 객체 키와 함께 주소 틀을 싣는다](./0037-a-pointer-carries-the-address-template-with-its-object-key.md)
- [0038 — 가져가라고 만든 산출물은 정본 바이트와 버킷을 같이 쓰지 않는다](./0038-a-fetchable-artifact-does-not-share-a-bucket-with-the-canonical-bytes.md) (ADR-0039 로 대체됨)
- [0039 — Gold 서빙 아티팩트는 lakehouse 버킷에 살고, 타일만 나간다](./0039-gold-serving-artifacts-live-in-the-lakehouse-bucket-and-tiles-do-not.md)
- [0040 — 아무도 채우지 않는 컬럼은 필수일 수 없다](./0040-a-column-no-producer-fills-cannot-be-required.md)
- [0042 — Silver 경계는 원천 CRS 를 그대로 싣는다](./0042-a-silver-boundary-carries-its-source-crs.md)
- [0043 — 정본 id 는 다시 계산하지 않고 읽는다](./0043-a-canonical-id-is-read-not-recomputed.md)
- [0044 — 사실의 이름을 단 컬럼은 그 사실을 담아야 한다](./0044-a-column-named-for-a-fact-must-hold-that-fact.md)
- [0045 — 서빙 투영의 행은 리비전이 아니라 적재를 이름한다](./0045-a-serving-projection-row-names-its-load-not-its-revision.md)
- [0046 — 수집된 파일에서 온 발행은 그 수집 기록을 이름한다](./0046-a-publication-names-the-object-it-was-collected-from.md)
- [0047 — 고칠 수 있는 경계는 버리지 않고 고쳐서 싣는다](./0047-a-repairable-boundary-is-repaired-not-dropped.md)
- [0048 — 발행된 feature id 에는 그 id 로 여는 조회구가 필요하다](./0048-a-published-feature-id-needs-a-read-keyed-on-it.md)
