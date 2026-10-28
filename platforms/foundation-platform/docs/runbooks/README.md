---
status: current
owner: foundation-platform
doc_type: README
last_reviewed: 2026-07-29
---

# Foundation 운영 런북

배포·수집·lakehouse·R2·장애 대응·복구 절차의 색인이다. 실제 명령과 사전조건은 각
런북에만 기록하고 README에는 복제하지 않는다.

## 주요 진입점

- [공공데이터 Bronze 수집](./public-data-bronze-lane-orchestration.md)
- [R2·lakehouse 실시간 검증](./r2-lakehouse-live-verification.md)
- [lakehouse 장애 대응](./lakehouse-incident-response.md)
- [예약 작업 운영 (Airflow) — 설치·작업 옮기기·확인](./production-orchestrator-cutover.md)
- [필지 by-PNU R2 서빙 굽기·발행](./parcel-by-pnu-serving-bake.md)
- [지도 편집 접기 — 설치·운영·확인](./map-edit-fold.md)
- [산업단지 경계 새 판 — 예약 작업](./complex-boundary-release.md)
- [법정동 코드 변경 (code.go.kr) — 수집·적재·스튜어드](./legal-dong-code-changes.md)
- [전국등록공장현황 (data.go.kr) — 수집·주소로 PNU 찾기·적재](./data-go-kr-registered-factory.md)
- [법원경매정보 (courtauction.go.kr) — 정직한 수집·멈춤·적재](./court-auction.md)
- [온비드 공매 (data.go.kr) — 수집·적재](./data-go-kr-onbid.md)
- [VWorld 연속지적도 판 — 확인·수집·제안·Silver 적재](./vworld-parcel-editions.md)
- [Silver 레인 새로 고침 — 건축HUB 레인의 감독 실행](./silver-refresh.md)
- [필지 ID 원장 첫 등록 — 예약 작업](./parcel-registry.md)
- [데이터 카탈로그(DataHub) — 설치·로그인·확인](./data-catalog.md)
- [데이터 흐름 지도 — 읽기 전용 자격 파일·Slack 알림·확인](./operations-map.md)
- [ai-server 메모리 예산 — 컨테이너 상한과 실측](./host-memory-budget.md)
- [호스트 디스크 배치 — OS 는 시스템 디스크, Docker·containerd 는 데이터 디스크](./host-layout.md)
- [Worker 자동 배포 — 토큰 한 번 두기·켜고 끄기·확인](./worker-autodeploy.md)

## 주제별 런북

### 수집

- [hub.go.kr 건축물 벌크 Bronze 수집·매일 훑기](./building-hub-bulk-bronze-ingest.md)
- [공급자 장애·쿼터 대응](./provider-outage-and-quota.md)

### lakehouse·Iceberg

- [Iceberg 스냅샷 되돌리기](./iceberg-snapshot-rollback.md)
- [Lakehouse 백필·스키마 재빌드](./lakehouse-backfill-and-schema-rebuild.md)
- [Lakehouse 카탈로그 읽기 전용 스모크](./lakehouse-catalog-smoke.md)

### R2

- [R2 인벤토리 감사·정리·청구 지표](./r2-inventory-audit.md)
- [R2 네임스페이스 오염 복구](./r2-namespace-contamination-recovery.md)
- [R2 쓰기 스모크·manifest 포인터 발행 확인](./r2-vector-tile-manifest-smoke.md)

### 타일·서빙

- [건물 by-PNU 묶음 파일 전환](./building-section-pack-cutover.md)
- [필지 by-PNU 묶음 파일 전환](./parcel-section-pack-cutover.md)
- [필지 타일 전국 굽기](./parcel-tile-bake.md)
- [객체 저장소 우선 타일(PMTiles·Martin) 증명과 정적 릴리스](./tiles-object-storage-first-slice.md)

### 릴리스·운영·관측

- [Canonical 릴리스 증명 — Silver·Gold·타일 manifest 계보 대조](./canonical-release-proof.md)
- [저비용 운영 강화 — 부하 기준·백업·복구·알림](./foundation-platform-low-cost-production-hardening.md)
- [SLO·알림 정책](./slo-alert-policy.md)
- [모양 원장 버킷·준비된 PR 봇 설정](./shape-ledger-bucket.md)

전체 목록은 [문서 색인 생성 안내](../../../../docs/README.md)에서 확인한다.
