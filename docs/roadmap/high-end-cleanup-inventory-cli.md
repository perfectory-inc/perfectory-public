---
status: current
owner: repository-maintainers
doc_type: roadmap
last_reviewed: 2026-11-04
---

# 하이엔드 정리 인벤토리 — ops CLI 부록

[하이엔드 정리 인벤토리](./high-end-cleanup-inventory.md)의 부록이다. 기준 커밋과 측정 방법은 본문을 따른다.

platforms/foundation-platform/services/foundation-outbox-publisher/src/main.rs에 정의됨(`parse_command`가 문자열을 `Command`로 바꾸고, 디스패치는 `run_command`). 호출자는 그 정확한 문자열을 이름 짓는(단어 경계 일치) 모든 추적 파일을 센 것이며, `main.rs`와 `main_command_tests.rs`는 제외하고, 스크립트 호출자는 systemd unit / `jobs.v1.json` / CI까지 따라간다. 분류: a=예약·CI·배포가 부름, b=런북에서만 운영자가 부름, c=일회성·대체됨, d=아무도 부르지 않음.

| # | 하위 명령 | main.rs 줄 | 처리 모듈(LOC) | 찾은 호출자(수) | 근거 | 분류 |
|---|---|---|---|---|---|---|
| 1 | `run` | 919 | `publisher_runtime::run_publisher` (~25) | none | 장기 실행 publisher를 실행하는 unit/script 없음; outbox는 foundation-outbox-publish.timer의 publish-outbox-once가 비움 | d |
| 2 | `job-run` | 920 | `foundation_jobs::job_run`+`runner` (852) | doc=2 rust=1 | FOUNDATION_JOB_RUN_JOB를 설정하는 곳 없음; jobs.v1.json의 어떤 job도 `command`를 갖지 않음(ADR-0270 s2 진입 스크립트가 도입되지 않음) | d |
| 3 | `audit-r2-inventory` | 921 | `r2_inventory_audit` (1297) | runbook=2 | 런북 r2-inventory-audit, r2-namespace-contamination-recovery | b |
| 4 | `building-register-smoke` | 922 | `building_register_smoke` (387) | rust=1 | 자체 모듈뿐 | d |
| 5 | `delete-r2-candidates` | 923 | `r2_delete_candidates` (418) | runbook=2 | 런북 r2-inventory-audit | b |
| 6 | `ingest-building-register` | 924 | `building_register_ingest` (2444) | rust=4 | data.go.kr API ingest; run-national-data-collection / ledger job_environment에서만 기동(bulk-only 정책) | c |
| 7 | `run-building-register-local-bronze-proof` | 925 | `building_register_local_bronze_proof` (828) | none | 증명용; 호출자 0 | c |
| 8 | `ingest-building-hub-bulk-collection` | 928 | `building_hub_bulk_ingest` (1951) | config=1 doc=1 runbook=1 rust=1 script=1 test=1 | daily-source-sweep.sh ← foundation-source-sweep.service | a |
| 9 | `ingest-building-hub-bulk-file` | 929 | `building_hub_bulk_ingest` (1951) | runbook=1 | 런북 building-hub-bulk-bronze-ingest | b |
| 10 | `plan-building-hub-bulk-collection` | 930 | `building_hub_bulk_collection_plan` (571) | runbook=1 rust=2 script=2 test=2 | daily-source-sweep.sh; staging-smoke.sh(배포) | a |
| 11 | `plan-rt-molit-real-transaction-exports` | 931 | `rt_molit_real_transaction_export_collection_plan` (1267) | none | 호출자 0; 예약된 rt-molit 수집은 plan 모듈을 쓰지 않음 | d |
| 12 | `plan-vworld-dataset-collection` | 934 | `vworld_dataset_collection_plan` (832) | config=2 doc=2 runbook=1 script=6 test=3 | source sweep / parcel-number-change / vworld-parcel-edition | a |
| 13 | `check-national-data-collection-coverage-ledger` | 935 | `national_data_collection_coverage_ledger_check` (1197) | none | 국가 API 패밀리; 호출자 0 | c |
| 14 | `execute-national-data-collection-ledger` | 938 | `national_data_collection_ledger_execute` (2242) | rust=1 | 국가 API 패밀리; resume 모듈에서만 이름이 불림 | c |
| 15 | `execute-rt-molit-real-transaction-export-plan` | 941 | `rt_molit_real_transaction_export_collection_plan` (1267) | none | 호출자 0 | d |
| 16 | `execute-silver-gold-national-promotion` | 944 | `silver_gold_national_promotion_execution` (1136) | none | 국가 승격; 호출자 0 | c |
| 17 | `evaluate-lakehouse-quality-rules` | 947 | `lakehouse_quality_rules_evaluate` (94) | runbook=1 test=1 | 런북 lakehouse-compute-engines(로컬 opt-in) | b |
| 18 | `resume-national-data-collection-ledger` | 948 | `national_data_collection_ledger_resume` (1782) | runbook=1 | 국가 API 패밀리; 런북 provider-outage-and-quota뿐 | c |
| 19 | `run-national-data-collection` | 951 | `national_data_collection_run` (1571) | runbook=1 | 국가 API 패밀리; 런북 lakehouse-registry가 `--execute`를 보여주지만 parse_command가 거부(extra argument) - 낡음 | c |
| 20 | `stop-building-register-floor-cycle` | 952 | `remote_lakehouse_job` (9808) | doc=1 script=1 test=1 | building-register-floor-cycle.sh | a |
| 21 | `stop-gold-panel-rebuild` | 953 | `remote_lakehouse_job` (9808) | doc=1 runbook=1 script=1 test=1 | gold-panel-rebuild.sh | a |
| 22 | `stop-lakehouse-maintenance` | 954 | `remote_lakehouse_job` (9808) | doc=1 script=1 test=1 | lakehouse-maintenance.sh | a |
| 23 | `run-building-register-floor-cycle` | 955 | `remote_lakehouse_job` (9808) | doc=1 runbook=1 rust=2 script=1 test=1 | building-register-floor-cycle.sh | a |
| 24 | `run-silver-refresh` | 956 | `remote_lakehouse_job` (9808) | doc=1 runbook=1 script=1 test=1 | silver-refresh.sh ← foundation-silver-refresh@.service | a |
| 25 | `stop-silver-refresh` | 957 | `remote_lakehouse_job` (9808) | script=1 | silver-refresh.sh | a |
| 26 | `stop-parcel-tile-bake` | 958 | `remote_lakehouse_job` (9808) | script=1 | parcel-tile-bake.sh | a |
| 27 | `run-remote-lakehouse-job` | 959 | `remote_lakehouse_job` (9808) | doc=1 runbook=1 | 런북 remote-lakehouse-job-runner | b |
| 28 | `collect-building-hub-bronze-catalog-recovery-inventory` | 960 | `building_hub_bronze_catalog_recovery` (1023) | test=1 | bronze 카탈로그 복구(일회성); 테스트뿐 | c |
| 29 | `collect-vworld-bronze-catalog-recovery-inventory` | 963 | `vworld_bronze_catalog_recovery` (1097) | test=1 | bronze 카탈로그 복구; 테스트뿐 | c |
| 30 | `compile-national-data-collection-plan` | 966 | `national_data_collection_plan_compile` (1251) | none | 국가 API 패밀리; 호출자 0 | c |
| 31 | `compile-building-hub-bronze-catalog-recovery-manifest` | 969 | `building_hub_bronze_catalog_recovery` (1023) | test=1 | bronze 카탈로그 복구; 테스트뿐 | c |
| 32 | `compile-vworld-bronze-catalog-recovery-manifest` | 972 | `vworld_bronze_catalog_recovery` (1097) | doc=1 test=1 | bronze 카탈로그 복구; 테스트뿐 | c |
| 33 | `recover-bronze-catalog` | 975 | `bronze_catalog_recovery_execute` (543) | doc=1 test=1 | bronze 카탈로그 복구; 테스트뿐 | c |
| 34 | `check-national-data-collection-rollout-approval` | 976 | `national_data_collection_rollout_approval_check` (657) | none | 국가 API 패밀리; 호출자 0 | c |
| 35 | `check-administrative-spatial-scope-registry` | 979 | `administrative_spatial_scope_registry` (1147) | none | 호출자 0(write- 쌍은 CI) | d |
| 36 | `publish-administrative-boundary-postgis` | 982 | `administrative_boundary_postgis_publish` (617) | config=2 doc=4 other=2 runbook=1 rust=1 script=1 test=1 | CI boundary-slice-proof.sh | a |
| 37 | `publish-administrative-boundary-static-release` | 985 | `administrative_boundary_static_release_publish` (21) | runbook=1 | 런북 tiles-object-storage-first-slice뿐; 행정 타일은 지금 bake-lakehouse-tiles에서 나옴 | b |
| 38 | `publish-industrial-complex-boundary-postgis` | 988 | `industrial_complex_boundary_postgis_publish` (981) | config=3 doc=5 other=3 runbook=1 rust=2 script=2 test=1 | CI boundary-slice-proof.sh | a |
| 39 | `publish-industrial-complex-boundary-static-release` | 991 | `industrial_complex_boundary_static_release_publish` (26) | config=2 doc=1 runbook=1 | 런북 first-slice뿐 | b |
| 40 | `prove-industrial-complex-boundary-static-release-mutation-guards` | 994 | `industrial_complex_boundary_static_release_publish` (26) | script=1 | CI boundary-slice-proof.sh | a |
| 41 | `readdress-static-release` | 997 | `static_release_readdress` (413) | runbook=2 | 런북 parcel-tile-bake | b |
| 42 | `export-map-edit-handoff` | 998 | `map_edit_handoff_export` (333) | config=2 doc=2 runbook=1 script=4 | map-edit-fold.sh | a |
| 43 | `bake-lakehouse-tiles` | 999 | `lakehouse_tile_bake` (1316) | config=5 doc=6 job=1 runbook=2 script=4 | map-edit-fold / parcel-tile-bake / zoning-tile-bake | a |
| 44 | `publish-parcel-boundary-postgis` | 1000 | `parcel_boundary_postgis_publish` (719) | config=2 doc=5 rust=2 test=1 | 필지 PostGIS 경로, parcel_tile_bake가 꺼져 있는 동안의 실제 필지 타일 경로; ADR-0194 s5.5가 이후 삭제 | b |
| 45 | `publish-parcel-boundary-static-release` | 1001 | `parcel_boundary_static_release_publish` (23) | doc=1 runbook=1 | 위와 동일; 런북 first-slice | b |
| 46 | `load-unit-official-price-projection` | 1004 | `unit_official_price_projection_load` (291) | config=2 doc=3 | 호출자 0(pipeline-graph 엣지뿐) | d |
| 47 | `load-lineage-review-items` | 1005 | `lineage_review_items_load` (44) | config=2 doc=2 script=4 test=2 | lineage review queue Spark 체인 | a |
| 48 | `measure-bronze-object-members` | 1006 | `bronze_object_members` (1603) | config=2 doc=1 other=1 rust=2 script=2 test=2 | bronze-object-members.sh ← source sweep; staging smoke | a |
| 49 | `clear-staging-namespace` | 1007 | `staging_namespace` (51) | doc=1 rust=1 script=1 test=1 | staging-smoke.sh(배포) | a |
| 50 | `export-lineage-steward-fold` | 1008 | `lineage_steward_fold` (100) | doc=1 rust=1 script=2 test=1 | lineage-stewardship-cycle.sh | a |
| 51 | `export-legal-dong-steward-decisions` | 1009 | `lineage_steward_fold` (100) | config=2 doc=2 runbook=1 rust=1 script=2 test=2 | 법정동 코드 적재 체인 | a |
| 52 | `record-lineage-steward-folds` | 1010 | `lineage_steward_fold` (100) | config=2 doc=2 runbook=1 rust=1 script=4 test=1 | lineage-stewardship-cycle | a |
| 53 | `seal-parcel-publication-evidence` | 1011 | `parcel_publication_evidence_sealer` (334) | doc=3 test=1 | 필지 PostGIS 증명(ADR-0194 s5.5); 테스트+문서뿐 | c |
| 54 | `write-parcel-publication-evidence` | 1012 | `parcel_publication_evidence_writer` (462) | doc=1 test=1 | 위와 동일 | c |
| 55 | `promote-administrative-boundary-runtime` | 1013 | `administrative_boundary_runtime_promote` (79) | config=2 doc=2 runbook=1 rust=1 script=1 test=1 | CI boundary-slice-proof.sh | a |
| 56 | `promote-industrial-complex-boundary-runtime` | 1016 | `industrial_complex_boundary_runtime_promote` (106) | doc=2 other=1 runbook=1 rust=1 script=1 test=1 | CI boundary-slice-proof.sh | a |
| 57 | `promote-parcel-boundary-runtime` | 1019 | `parcel_boundary_runtime_promote` (98) | config=2 doc=3 rust=1 | 필지 PostGIS 경로(ADR-0194 s5.5); script/unit 없음 | b |
| 58 | `check-bounded-live-ingestion-gate` | 1020 | `bounded_live_ingestion_gate_check` (472) | none | 국가 API 게이트; 호출자 0 | c |
| 59 | `check-r2-runtime-target` | 1021 | `bronze_object_storage` (663) | rust=1 test=1 | tools/xtask live.rs | b |
| 60 | `check-postgis-anchor-pbf-regional-proof` | 1022 | `postgis_anchor_pbf_regional_proof_check` (823) | none | 증명용; 호출자 0 | c |
| 61 | `check-regional-data-serving-load` | 1025 | `regional_data_serving_load_check` (918) | none | 호출자 0 | c |
| 62 | `check-silver-gold-national-promotion-execution` | 1026 | `silver_gold_national_promotion_execution` (1136) | none | 호출자 0 | c |
| 63 | `check-silver-gold-national-promotion-plan` | 1029 | `silver_gold_national_promotion_plan` (865) | none | 호출자 0 | c |
| 64 | `write-national-data-collection-scope` | 1032 | `national_data_collection_scope_writer` (464) | none | 호출자 0 | c |
| 65 | `check-national-bronze-object-manifest` | 1035 | `national_bronze_object_manifest` (1160) | rust=4 | 국가 모듈에서만 이름이 불림 | c |
| 66 | `configure-github-actions-secrets` | 1038 | `github_actions_secret_configurator` (481) | none | 컷오버 도구; 호출자 0 | c |
| 67 | `fetch-github-cutover-artifact` | 1039 | `github_cutover_artifact_fetch` (924) | none | 컷오버 도구; 호출자 0 | c |
| 68 | `dispatch-github-cutover-workflow` | 1040 | `github_cutover_dispatch` (648) | none | 컷오버 도구; 호출자 0 | c |
| 69 | `provider-rate-controller` | 1041 | `public_provider_rate_controller` (311) | runbook=1 | 런북 provider-outage-and-quota | b |
| 70 | `collect-r2-billing-export` | 1042 | `r2_billing_export_collect` (262) | runbook=1 | 런북 r2-inventory-audit | b |
| 71 | `run-public-data-bronze-collection-lanes` | 1043 | `public_data_bronze_lane_orchestrator` (683) | runbook=1 | 런북 public-data-bronze-lane-orchestration | b |
| 72 | `write-administrative-spatial-scope-registry` | 1046 | `administrative_spatial_scope_registry` (1147) | doc=1 runbook=1 script=1 | CI boundary-slice-proof.sh | a |
| 73 | `write-official-administrative-boundary-source-snapshot` | 1049 | `official_administrative_boundary_source_snapshot` (713) | config=2 doc=1 runbook=1 script=1 test=1 | CI boundary-slice-proof.sh | a |
| 74 | `write-postgis-mirror-dlq-cutover-evidence` | 1052 | `postgis_mirror_dlq_cutover_evidence` (693) | none | 컷오버 증명; 호출자 0 | c |
| 75 | `write-public-api-dependency-metric` | 1055 | `public_api_metric_writer` (380) | none | 호출자 0 | d |
| 76 | `write-public-api-quota-metric` | 1056 | `public_api_metric_writer` (380) | none | 호출자 0 | d |
| 77 | `write-building-register-page-count-plan` | 1057 | `building_register_page_count_plan_writer` (1004) | none | 국가 API 패밀리; 호출자 0 | c |
| 78 | `write-canonical-silver-gold-cutover-evidence` | 1060 | `canonical_silver_gold_cutover_evidence` (421) | none | 컷오버 증명; 호출자 0 | c |
| 79 | `write-canonical-release-proof` | 1063 | `canonical_release_proof` (278) | runbook=1 test=1 | 런북 canonical-release-proof | b |
| 80 | `write-national-bronze-object-manifest` | 1064 | `national_bronze_object_manifest` (1160) | none | 호출자 0 | c |
| 81 | `write-national-data-collection-rollout-approval` | 1067 | `national_data_collection_rollout_approval_check` (657) | none | 호출자 0 | c |
| 82 | `check-industrial-complex-canonical-source-readiness` | 1070 | `industrial_complex_canonical_source_readiness` (429) | none | 준비 상태 점검; 호출자 0 | c |
| 83 | `write-national-data-collection-shard-manifest` | 1073 | `national_data_collection_shard_manifest_writer` (2090) | none | 호출자 0 | c |
| 84 | `write-national-page-count-plan` | 1076 | `national_page_count_plan_writer` (492) | none | 호출자 0 | c |
| 85 | `write-silver-gold-national-promotion-plan` | 1077 | `silver_gold_national_promotion_plan` (865) | none | 호출자 0 | c |
| 86 | `plan-provider-acquisition-jobs` | 1080 | `provider_acquisition_plan` (625) | doc=1 runbook=1 rust=1 script=1 test=2 | raon-large-files.sh(RAON 레인) | a |
| 87 | `inventory-vworld-dataset-files` | 1081 | `vworld_dataset_file_inventory` (773) | config=2 doc=2 runbook=2 script=4 test=3 | source sweep / parcel-number-change | a |
| 88 | `ingest-real-transaction` | 1082 | `real_transaction_ingest` (1179) | rust=1 | API ingest; national ledger job_environment에서만 이름이 불림 | c |
| 89 | `ingest-rt-molit-real-transaction-export` | 1083 | `rt_molit_real_transaction_export_ingest` (847) | none | 호출자 0(해당 모듈은 예약된 rt-molit 수집이 사용 - 모듈은 유지) | d |
| 90 | `probe-building-register-page-count` | 1086 | `building_register_ingest` (2444) | none | 국가 API 패밀리; 호출자 0 | c |
| 91 | `probe-building-register-page-count-batch` | 1087 | `building_register_page_count_batch` (855) | rust=1 | 국가 API 패밀리 | c |
| 92 | `probe-spatial-tile-wap` | 1090 | `spatial_tile_wap_command` (1284) | doc=1 | 가이드 문서뿐 | d |
| 93 | `probe-vworld-page-count-batch` | 1091 | `vworld_page_count_batch` (1112) | none | 국가 API 패밀리; 호출자 0 | c |
| 94 | `build-parcel-marker-anchor-pbf-artifacts` | 1092 | `parcel_marker_anchor_pbf_artifact_build` (3005) | none | 호출자 0(ADR-0225가 마커를 v2 유닛으로 옮김) | d |
| 95 | `build-parcel-marker-anchor-aggregate-pbf-artifacts` | 1095 | `parcel_marker_anchor_pbf_artifact_build` (3005) | none | 호출자 0 | d |
| 96 | `ingest-vworld-cadastral` | 1098 | `vworld_cadastral_ingest` (730) | config=1 rust=8 | API ingest; 국가 모듈뿐 | c |
| 97 | `ingest-vworld-dataset-files` | 1099 | `vworld_dataset_file_ingest` (3081) | config=4 doc=5 runbook=4 script=4 test=3 | source sweep / parcel-number-change | a |
| 98 | `ingest-vworld-land-register` | 1100 | `vworld_land_register_ingest` (1060) | config=1 rust=4 | API ingest; 국가 모듈뿐 | c |
| 99 | `ingest-vworld-ned-attribute` | 1101 | `vworld_ned_attribute_ingest` (1224) | config=1 | API ingest; lane-registry json뿐 | c |
| 100 | `build-industrial-complex-address-resolution` | 1102 | `industrial_complex_address_resolution_build` (1255) | config=2 doc=1 | 호출자 0(pipeline-graph 엣지뿐) | d |
| 101 | `collect-code-go-kr-legal-dong` | 1105 | `code_go_kr_legal_dong_collect` (848) | config=3 doc=2 runbook=1 script=2 test=1 | legal-dong-code-collect.sh | a |
| 102 | `collect-data-go-kr-onbid` | 1106 | `data_go_kr_onbid_collect` (2972) | config=3 doc=1 runbook=1 script=2 test=1 | onbid-collect.sh | a |
| 103 | `collect-rt-molit-real-transaction` | 1107 | `rt_molit_real_transaction_collect` (1049) | config=3 doc=1 script=2 | rt-molit-collect.sh | a |
| 104 | `collect-courtauction-go-kr` | 1108 | `courtauction_go_kr_collect` (1549) | config=3 doc=1 runbook=1 script=2 test=1 | court-auction-collect.sh(통제된 야간 실행 전까지 job 꺼짐) | a |
| 105 | `collect-data-go-kr-registered-factory` | 1109 | `data_go_kr_registered_factory_collect` (606) | config=3 doc=2 runbook=1 script=2 test=1 | registered-factory-cycle.sh | a |
| 106 | `plan-silver-load-objects` | 1112 | `silver_load_objects` (332) | config=5 doc=3 job=1 runbook=3 script=12 test=5 | onbid/court/rt-molit silver 적재 | a |
| 107 | `collect-industrial-complex-address-source` | 1113 | `industrial_complex_address_source_collect` (851) | none | 호출자 0 | d |
| 108 | `export-industrial-complex-boundary-silver-handoff` | 1116 | `industrial_complex_boundary_silver_export` (855) | config=2 doc=1 job=1 runbook=1 script=1 test=1 | complex-boundary-release.sh | a |
| 109 | `export-industrial-complex-bronze-raw-jsonl` | 1119 | `industrial_complex_bronze_raw_jsonl_export` (2392) | config=2 doc=4 runbook=1 rust=2 script=1 | 산단 경계 release Spark 체인 | a |
| 110 | `export-industrial-complex-gold-profiles` | 1122 | `industrial_complex_gold_profile_export` (769) | config=2 doc=5 runbook=1 rust=2 | 런북 lakehouse-compute-engines(로컬) | b |
| 111 | `export-industrial-complex-silver-handoff` | 1125 | `industrial_complex_silver_export` (144) | rust=1 | canonical_source_readiness에서만 이름이 불림 | c |
| 112 | `export-building-register-floor-silver-handoff` | 1128 | `building_register_floor_silver_export` (7821) | config=2 doc=1 runbook=1 rust=3 | remote_lakehouse_job floor cycle | a |
| 113 | `stage-building-register-floor-inputs` | 1131 | `building_register_floor_silver_export` (7821) | doc=1 rust=2 | remote_lakehouse_job floor cycle | a |
| 114 | `select-building-register-floor-inputs` | 1134 | `building_register_floor_silver_export` (7821) | doc=1 | ADR-0128뿐; 기동하는 곳 없음 | d |
| 115 | `export-building-register-unit-area-silver-handoff` | 1137 | `building_register_unit_area_silver_export` (1243) | config=3 doc=1 rust=2 | silver refresh 레인 계약 `export` | a |
| 116 | `export-building-register-title-silver-handoff` | 1140 | `building_register_title_silver_export` (479) | config=3 doc=1 runbook=1 rust=1 | silver refresh 레인 계약 | a |
| 117 | `export-building-register-unit-silver-handoff` | 1143 | `building_register_unit_silver_export` (2250) | config=3 doc=1 runbook=1 rust=3 | silver refresh 레인 계약 | a |
| 118 | `export-building-by-pnu-serving` | 1146 | `building_by_pnu_serving_export` (13428) | config=2 doc=3 test=1 | by-pnu-serving-bake.sh export-${UNIT}-by-pnu-serving | a |
| 119 | `publish-building-by-pnu-serving-manifest` | 1147 | `by_pnu_serving_manifest_publish` (3017) | doc=2 rust=1 test=1 | by-pnu-serving-bake.sh publish-${UNIT}-by-pnu-serving-manifest | a |
| 120 | `export-parcel-by-pnu-serving` | 1150 | `parcel_by_pnu_serving_export` (1456) | config=2 doc=2 runbook=1 | by-pnu-serving-bake.sh | a |
| 121 | `show-parcel-by-pnu-serving-state` | 1151 | `by_pnu_serving_state` (482) | rust=1 test=1 | by-pnu-serving-bake.sh show-${UNIT}-by-pnu-serving-state | a |
| 122 | `show-building-by-pnu-serving-state` | 1152 | `by_pnu_serving_state` (482) | rust=1 test=1 | by-pnu-serving-bake.sh | a |
| 123 | `verify-parcel-by-pnu-serving-rebase` | 1153 | `by_pnu_serving_rebase` (1559) | doc=1 rust=3 script=1 test=1 | by-pnu-serving-bake.sh | a |
| 124 | `export-parcel-marker-anchor-artifacts` | 1154 | `parcel_marker_anchor_artifact_export` (1359) | runbook=1 | 런북 outbox-webhook-fanout이 언급하지만, 운영자 단계 없음 | d |
| 125 | `export-vworld-cadastral-silver-handoff` | 1157 | `vworld_cadastral_silver_export` (456) | none | API 지적; 호출자 0 | c |
| 126 | `export-vworld-cadastral-shapefile-silver-handoff` | 1160 | `vworld_cadastral_shapefile_silver_export` (833) | config=2 doc=3 script=1 | vworld-parcel-handoff-export.sh(런북 vworld-parcel-editions) | b |
| 127 | `export-land-use-plan-silver-handoff` | 1163 | `land_use_silver_export` (1390) | config=3 doc=1 rust=1 | silver refresh 레인 계약 | a |
| 128 | `export-building-register-exclusive-unit-silver-handoff` | 1164 | `building_register_exclusive_unit_silver_export` (24) | config=3 doc=2 rust=1 | silver refresh 레인 | a |
| 129 | `export-building-permit-basis-outline-silver-handoff` | 1167 | `building_permit_basis_outline_silver_export` (32) | config=3 doc=2 rust=1 | silver refresh 레인 | a |
| 130 | `export-building-register-master-silver-handoff` | 1170 | `building_register_master_silver_export` (32) | config=3 doc=2 rust=1 | silver refresh 레인 | a |
| 131 | `export-building-permit-demolition-loss-silver-handoff` | 1173 | `building_permit_demolition_loss_silver_export` (32) | config=3 doc=2 rust=1 | silver refresh 레인 | a |
| 132 | `export-building-energy-monthly-electricity-silver-handoff` | 1176 | `building_energy_monthly_silver_export` (56) | config=3 doc=1 rust=1 | silver refresh 레인 | a |
| 133 | `export-building-energy-monthly-gas-silver-handoff` | 1179 | `building_energy_monthly_silver_export` (56) | config=3 doc=1 rust=1 | silver refresh 레인 | a |
| 134 | `export-building-register-sub-parcel-silver-handoff` | 1182 | `building_register_sub_parcel_silver_export` (32) | config=3 doc=2 rust=1 | silver refresh 레인 | a |
| 135 | `export-building-register-apartment-price-silver-handoff` | 1185 | `building_register_apartment_price_silver_export` (24) | config=3 doc=2 rust=1 | silver refresh 레인 | a |
| 136 | `export-land-use-zone-code-silver-handoff` | 1188 | `land_use_silver_export` (1390) | config=3 doc=1 rust=1 | silver refresh 레인 | a |
| 137 | `export-land-use-zone-area-silver-handoff` | 1191 | `land_use_zone_area_silver_export` (568) | config=3 doc=2 rust=1 | silver refresh 레인 | a |
| 138 | `export-land-right-registration-silver-handoff` | 1194 | `land_use_silver_export` (1390) | config=3 doc=2 rust=1 | silver refresh 레인 | a |
| 139 | `export-land-ownership-silver-handoff` | 1197 | `land_use_silver_export` (1390) | config=3 doc=2 rust=1 | silver refresh 레인 | a |
| 140 | `export-land-transfer-history-silver-handoff` | 1200 | `land_use_silver_export` (1390) | config=3 doc=2 rust=1 | silver refresh 레인 | a |
| 141 | `export-land-forest-silver-handoff` | 1203 | `land_use_silver_export` (1390) | config=3 doc=2 rust=1 | silver refresh 레인 | a |
| 142 | `export-land-characteristic-silver-handoff` | 1204 | `land_use_silver_export` (1390) | config=3 doc=2 rust=1 | silver refresh 레인 | a |
| 143 | `export-land-individual-price-silver-handoff` | 1207 | `land_use_silver_export` (1390) | config=3 doc=1 rust=1 test=1 | silver refresh 레인 | a |
| 144 | `export-vworld-cadastral-silver-handoff-shard` | 1210 | `vworld_cadastral_silver_shard_export` (776) | config=2 doc=1 rust=1 | 국가 승격뿐 | c |
| 145 | `execute-national-data-collection-async` | 1213 | `national_data_collection_async` (3730) | config=1 rust=1 | 폐기됨, fail-closed(열린 PR #1044가 삭제) | c |
| 146 | `import-industrial-complex-catalog-seed` | 1216 | `industrial_complex_catalog_import` (177) | none | 시드 import; 호출자 0 | c |
| 147 | `import-provider-acquisition-landing` | 1219 | `provider_acquisition_import` (2080) | doc=1 runbook=1 script=2 | raon-large-files.sh | a |
| 148 | `load-industrial-complex-canonical` | 1222 | `industrial_complex_canonical_load` (751) | config=2 doc=2 runbook=1 | 런북 lakehouse-compute-engines | b |
| 149 | `publish-industrial-complex-gold-pointer` | 1223 | `industrial_complex_gold_pointer_publish` (1522) | config=2 doc=3 runbook=1 rust=2 | 런북 lakehouse-compute-engines | b |
| 150 | `publish-parcel-by-pnu-serving-manifest` | 1226 | `by_pnu_serving_manifest_publish` (3017) | doc=1 runbook=1 rust=1 test=2 | by-pnu-serving-bake.sh | a |
| 151 | `publish-industrial-complex-gold-pointers` | 1229 | `industrial_complex_gold_pointer_publish` (1522) | doc=2 rust=1 | ADR 문서; 위 명령의 운영자용 짝 | b |
| 152 | `publish-lakehouse-lineage-event` | 1232 | `publish_lakehouse_lineage_event` (258) | config=2 doc=1 runbook=1 | 런북 lakehouse-compute-engines | b |
| 153 | `publish-outbox-once` | 1233 | `None` (0) | config=2 doc=2 script=1 | publish-outbox.sh ← foundation-outbox-publish.timer | a |
| 154 | `r2-billing-usage-metrics` | 1234 | `r2_billing_usage_metrics` (621) | runbook=1 rust=1 | 런북 r2-inventory-audit | b |
| 155 | `rebuild-parcel-marker-anchors` | 1235 | `None` (0) | doc=2 | ADR-0194 s5.4가 이것과 HTTP 라우트를 삭제하기로 결정; 문서뿐 | c |
| 156 | `rebuild-parcel-marker-anchors-streaming` | 1236 | `parcel_marker_anchor_streaming_rebuild` (816) | doc=2 | 문서뿐; ADR-0194 s5.3가 유지했지만 실제로 실행하는 곳은 없음 | d |
| 157 | `rebuild-postgis-parcel-boundary-mirror` | 1239 | `postgis_parcel_boundary_mirror_rebuild` (930) | doc=1 | 필지 미러(ADR-0194 s5.5); 문서뿐 | c |
| 158 | `rebuild-postgis-parcel-boundary-mirror-national` | 1242 | `postgis_parcel_boundary_mirror_national_rebuild` (1268) | none | 호출자 0 | c |
| 159 | `rebuild-postgis-parcel-boundary-mirror-national-from-contract` | 1245 | `postgis_parcel_boundary_mirror_national_rebuild` (1268) | config=2 doc=2 | pipeline-graph 엣지 + ADR뿐 | c |
| 160 | `abandon-ingestion-run` | 1248 | `ingestion_run_recovery` (179) | rust=1 | 호출자 0 | d |
| 161 | `reconcile-building-register` | 1249 | `building_register_ingest` (2444) | none | API 건축물대장; 호출자 0 | c |
| 162 | `record-lakehouse-bronze-run-evidence` | 1250 | `lakehouse_registry_control` (579) | runbook=1 rust=1 | 국가 API 증명; 런북 lakehouse-registry | c |
| 163 | `requeue-dead-lettered-collection-job` | 1253 | `collection_job_requeue` (42) | none | 호출자 0 | d |
| 164 | `register-serving-source-lineage` | 1256 | `serving_source_lineage_register` (377) | config=2 doc=1 | 호출자 0(pipeline-graph 엣지뿐) | d |
| 165 | `inventory-lakehouse` | 1257 | `lakehouse_inventory` (358) | doc=2 runbook=1 | 런북 lakehouse-registry | b |
| 166 | `inventory-r2` | 1258 | `None` (0) | test=1 | 테스트뿐 | d |
| 167 | `migrate-r2-bronze-keys` | 1259 | `r2_bronze_key_migration` (552) | runbook=1 | 일회성 키 마이그레이션; 런북 r2-namespace-contamination-recovery | c |
| 168 | `write-r2-bronze-key-cleanup-candidates` | 1260 | `r2_bronze_key_cleanup_candidates` (239) | none | 일회성; 호출자 0 | c |
| 169 | `write-r2-bronze-key-migration-plan` | 1263 | `r2_bronze_key_migration_plan` (273) | runbook=1 | 일회성; 런북 | c |
| 170 | `seed-lakehouse-registry` | 1264 | `lakehouse_registry_control` (579) | config=2 doc=1 runbook=1 | 런북 lakehouse-registry | b |
| 171 | `smoke-r2` | 1265 | `None` (0) | doc=1 runbook=3 | 런북들 | b |
| 172 | `smoke-vworld-cadastral` | 1266 | `vworld_cadastral_smoke` (149) | runbook=1 rust=1 | API 지적 스모크; 로컬 런북뿐 | c |
| 173 | `verify-static-release-toolchain` | 1267 | `static_release_toolchain` (452) | ci=1 runbook=1 test=1 | CI foundation-ci.yml | a |
| 174 | `verify-lakehouse-registry` | 1268 | `lakehouse_registry_control` (579) | config=2 doc=2 runbook=1 rust=1 | 런북 lakehouse-registry | b |
| 175 | `verify-r2-cleanup` | 1269 | `r2_cleanup_verify` (279) | runbook=1 | 런북 r2-inventory-audit | b |
| 176 | `validate-tile-derivative-r2` | 1270 | `tile_derivative_object_storage` (236) | runbook=1 test=1 | 런북 first-slice | b |
| 177 | `wait-trino-ready` | 1271 | `trino_ready_wait` (112) | doc=1 | ADR-0343 문서뿐 | d |
| - | `{plan,export,verify,probe,publish,inspect,monitor,show}-<lane>-by-pnu-*`, `check-<lane>-gateway-version-health`(PackCommand::parse, 동적) | 1259 | `building_by_pnu_serving_export::section_packs` (13428) | by-pnu-serving-bake.sh, by-pnu-pack-bake.sh, by-pnu-pack-operator.sh, by-pnu-serving-monitor.sh, by-pnu-gateway-health.sh | 예약된 스크립트에서 `${UNIT}`/`${LANE}`로부터 만들어짐 | a |

분류 합계: a(예약/CI/배포) = 68, b(운영자/런북) = 29, c(일회성/대체됨) = 57, d(호출 안 됨) = 23; 총 177(+동적 패밀리 1, class a).
