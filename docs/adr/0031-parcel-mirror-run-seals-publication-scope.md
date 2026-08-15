---
status: current
owner: repository-maintainers
doc_type: adr
last_reviewed: 2026-08-14
---

# ADR 0031: 필지 mirror run이 발행 scope와 limit을 봉인한다

- Status: Accepted
- Date: 2026-08-14
- Extends: [ADR-0025](./0025-parcel-publication-names-one-sealed-iceberg-evidence.md)
- Supersedes: [ADR-0029](./0029-parcel-publication-evidence-is-written-from-the-terminal-run.md) §2의 scope/limits 고정 결정만

## 맥락

실행 증거의 `scope`와 `limits`는 발행 가능성을 좌우하지만 기존
`serving_postgis.parcel_boundary_mirror_rebuild_run`에는 그 사실이 없다. producer가 이를
`{kind:national,complete:true}`와 null limits로 고정하면, bounded QA run도 provenance UUID만 채웠을 때
전국 완전 실행처럼 보일 수 있다. 현 national rebuild도 실제로 object/row 상한을 강제한다.

## 결정

run에 strict `publication_scope`와 `publication_limits` JSONB를 추가한다. 허용 scope는 exact
`{kind:bounded,complete:false}` 또는 `{kind:national,complete:true}`이고, limits는 object/row/shard 세
키만 가진다. national complete run은 세 limit이 모두 null이어야 하며 bounded run은 적어도 하나의
양의 정수 limit을 가져야 한다.

기존 run은 quality report의 실제 처리량을 상한으로 삼아 bounded로 backfill한다. 이력에는 terminal
run도 있으므로 마이그레이션은 `parcel_boundary_mirror_rebuild_run_state_guard`만 같은 트랜잭션 안에서
잠시 끄고 backfill한 뒤, `NOT NULL`과 CHECK를 추가하기 전에 다시 켠다. 다른 트리거는 끄지 않으며
중간 문장이 실패하면 마이그레이션 트랜잭션 전체가 롤백된다.

두 컬럼에는 DEFAULT를 두지 않는다. 현 bounded rebuild는 자신이 실제 적용한 object/row 상한을 run
생성 시 명시적으로 기록해야 하며, 값을 모르는 새 writer는 `NOT NULL`에서 실패해야 한다. producer는
run의 두 JSON을 기존 typed execution contract로 deserialize하고 publication validator로 거부 또는
수락한다. sealer도 R2 문서와 run의 scope/limits가 exact match인지 다시 확인한다. terminal run
immutability trigger가 provenance, quality와 함께 이 두 값을 변경 불가능하게 만든다.

run의 `source_record_id`와 `source_file_asset_id`는 개별 FK 두 개로 검사하지 않는다. 두 값이 같은
Catalog lineage를 가리키도록 `catalog.file_asset(id, source_record_id)`에 대한 `MATCH FULL` 복합 FK를
둔다. 따라서 둘 중 하나만 채운 provenance와 서로 다른 record/asset을 조합한 provenance는 run 생성
시점에 거부되고, producer가 봉인할 수 없는 terminal run을 만들 수 없다. 기존 NULL/NULL run은 유지한다.

## 기각한 대안

- **producer가 full national 값을 고정한다.** run이 모르는 사실을 생산자가 창작하므로 기각한다.
- **quality report 안에 scope를 넣는다.** 품질 수치와 실행 범위를 한 계약에 섞고 strict quality SSOT를
  흔드므로 별도 run 컬럼을 쓴다.
- **source UUID가 있으면 full run으로 간주한다.** provenance의 완전성과 처리 범위는 독립된 사실이다.
- **bounded 처리량이 충분히 크면 national로 추론한다.** 데이터 규모가 바뀌면 의미가 바뀌는 휴리스틱이다.

## 결과

현 bounded rebuild는 성공할 수 있지만 발행 evidence producer가 거부한다. 별도 production-capable
writer가 실제 national complete 실행을 증명해 null limits와 함께 run에 기록하기 전에는 R2 발행
증거가 생기지 않는다. 이 fail-closed 상태는 기존 bounded QA를 production으로 승격시키지 않는다.
