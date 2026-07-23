-- Name: allowed_industry_complex_idx; Type: INDEX; Schema: catalog; Owner: -
--

CREATE INDEX allowed_industry_complex_idx ON catalog.allowed_industry USING btree (complex_id);


--
-- Name: blueprint_complex_idx; Type: INDEX; Schema: catalog; Owner: -
--

CREATE INDEX blueprint_complex_idx ON catalog.blueprint USING btree (complex_id);


--
-- Name: bronze_object_run_idx; Type: INDEX; Schema: catalog; Owner: -
--

CREATE INDEX bronze_object_run_idx ON catalog.bronze_object USING btree (ingestion_run_id, collected_at, id);


--
-- Name: bronze_object_snapshot_period_idx; Type: INDEX; Schema: catalog; Owner: -
--

CREATE INDEX bronze_object_snapshot_period_idx ON catalog.bronze_object USING btree (source_catalog_id, snapshot_period, collected_at DESC, id) WHERE (snapshot_period IS NOT NULL);


--
-- Name: bronze_object_source_catalog_object_key_key; Type: INDEX; Schema: catalog; Owner: -
--

CREATE UNIQUE INDEX bronze_object_source_catalog_object_key_key ON catalog.bronze_object USING btree (source_catalog_id, object_key);


--
-- Name: bronze_object_source_identity_idx; Type: INDEX; Schema: catalog; Owner: -
--

CREATE INDEX bronze_object_source_identity_idx ON catalog.bronze_object USING btree (source_catalog_id, source_identity_key, collected_at DESC, id);


--
-- Name: bronze_object_source_partition_idx; Type: INDEX; Schema: catalog; Owner: -
--

CREATE INDEX bronze_object_source_partition_idx ON catalog.bronze_object USING btree (source_catalog_id, source_partition_key) WHERE (source_partition_key IS NOT NULL);


--
-- Name: building_parcel_id_idx; Type: INDEX; Schema: catalog; Owner: -
--

CREATE INDEX building_parcel_id_idx ON catalog.building USING btree (parcel_id);


--
-- Name: building_unit_parcel_id_idx; Type: INDEX; Schema: catalog; Owner: -
--

CREATE INDEX building_unit_parcel_id_idx ON catalog.building_unit USING btree (parcel_id);


--
-- Name: complex_notice_complex_published_idx; Type: INDEX; Schema: catalog; Owner: -
--

CREATE INDEX complex_notice_complex_published_idx ON catalog.complex_notice USING btree (complex_id, published_at DESC);


--
-- Name: complex_notice_source_record_idx; Type: INDEX; Schema: catalog; Owner: -
--

CREATE INDEX complex_notice_source_record_idx ON catalog.complex_notice USING btree (source_record_id);


--
-- Name: digital_twin_asset_complex_idx; Type: INDEX; Schema: catalog; Owner: -
--

CREATE INDEX digital_twin_asset_complex_idx ON catalog.digital_twin_asset USING btree (complex_id);


--
-- Name: digital_twin_asset_parcel_idx; Type: INDEX; Schema: catalog; Owner: -
--

CREATE INDEX digital_twin_asset_parcel_idx ON catalog.digital_twin_asset USING btree (parcel_id) WHERE (parcel_id IS NOT NULL);


--
-- Name: file_asset_source_record_idx; Type: INDEX; Schema: catalog; Owner: -
--

CREATE INDEX file_asset_source_record_idx ON catalog.file_asset USING btree (source_record_id);


--
-- Name: industrial_complex_active_idx; Type: INDEX; Schema: catalog; Owner: -
--

CREATE INDEX industrial_complex_active_idx ON catalog.industrial_complex USING btree (official_complex_code, id) WHERE (archived_at IS NULL);


--
-- Name: industrial_complex_active_official_code_idx; Type: INDEX; Schema: catalog; Owner: -
--

CREATE UNIQUE INDEX industrial_complex_active_official_code_idx ON catalog.industrial_complex USING btree (official_complex_code) WHERE (archived_at IS NULL);


--
-- Name: industrial_complex_gold_pointer_profile_file_asset_idx; Type: INDEX; Schema: catalog; Owner: -
--

CREATE INDEX industrial_complex_gold_pointer_profile_file_asset_idx ON catalog.industrial_complex_gold_pointer USING btree (profile_file_asset_id);


--
-- Name: industrial_complex_gold_pointer_published_idx; Type: INDEX; Schema: catalog; Owner: -
--

CREATE INDEX industrial_complex_gold_pointer_published_idx ON catalog.industrial_complex_gold_pointer USING btree (published_at DESC);


--
-- Name: industrial_complex_gold_pointer_source_record_idx; Type: INDEX; Schema: catalog; Owner: -
--

CREATE INDEX industrial_complex_gold_pointer_source_record_idx ON catalog.industrial_complex_gold_pointer USING btree (source_record_id);


--
-- Name: industrial_complex_primary_bjdong_code_idx; Type: INDEX; Schema: catalog; Owner: -
--

CREATE INDEX industrial_complex_primary_bjdong_code_idx ON catalog.industrial_complex USING btree (primary_bjdong_code);


--
-- Name: industry_group_complex_idx; Type: INDEX; Schema: catalog; Owner: -
--

CREATE INDEX industry_group_complex_idx ON catalog.industry_group USING btree (complex_id);


--
-- Name: ingestion_run_source_started_idx; Type: INDEX; Schema: catalog; Owner: -
--

CREATE INDEX ingestion_run_source_started_idx ON catalog.ingestion_run USING btree (source_catalog_id, started_at DESC);


--
-- Name: ingestion_run_status_idx; Type: INDEX; Schema: catalog; Owner: -
--

CREATE INDEX ingestion_run_status_idx ON catalog.ingestion_run USING btree (status, started_at DESC);


--
-- Name: lakehouse_batch_run_contract_created_idx; Type: INDEX; Schema: catalog; Owner: -
--

CREATE INDEX lakehouse_batch_run_contract_created_idx ON catalog.lakehouse_batch_run USING btree (contract, created_at DESC);


--
-- Name: lakehouse_batch_run_promotion_candidate_idx; Type: INDEX; Schema: catalog; Owner: -
--

CREATE INDEX lakehouse_batch_run_promotion_candidate_idx ON catalog.lakehouse_batch_run USING btree (contract, created_at DESC, recorded_at DESC) WHERE ((source_snapshot_truncated = false) AND (persisted_row_count = row_count) AND (write_disposition <> 'validate_only'::text));


--
-- Name: lakehouse_batch_run_recorded_idx; Type: INDEX; Schema: catalog; Owner: -
--

CREATE INDEX lakehouse_batch_run_recorded_idx ON catalog.lakehouse_batch_run USING btree (recorded_at DESC);


--
-- Name: lakehouse_dataset_version_asset_created_idx; Type: INDEX; Schema: catalog; Owner: -
--

CREATE INDEX lakehouse_dataset_version_asset_created_idx ON catalog.lakehouse_dataset_version USING btree (data_asset_id, created_at DESC);


--
-- Name: lakehouse_dataset_version_one_active_idx; Type: INDEX; Schema: catalog; Owner: -
--

CREATE UNIQUE INDEX lakehouse_dataset_version_one_active_idx ON catalog.lakehouse_dataset_version USING btree (data_asset_id) WHERE (state = 'active'::text);


--
-- Name: lakehouse_object_artifact_version_idx; Type: INDEX; Schema: catalog; Owner: -
--

CREATE INDEX lakehouse_object_artifact_version_idx ON catalog.lakehouse_object_artifact USING btree (dataset_version_id, created_at DESC);


--
-- Name: lakehouse_storage_namespace_physical_idx; Type: INDEX; Schema: catalog; Owner: -
--

CREATE UNIQUE INDEX lakehouse_storage_namespace_physical_idx ON catalog.lakehouse_storage_namespace USING btree (provider, bucket_name, COALESCE(root_prefix, ''::text));


--
-- Name: manufacturer_parcel_idx; Type: INDEX; Schema: catalog; Owner: -
--

CREATE INDEX manufacturer_parcel_idx ON catalog.manufacturer USING btree (primary_parcel_id);


--
-- Name: normalization_application_principal_created_idx; Type: INDEX; Schema: catalog; Owner: -
--

CREATE INDEX normalization_application_principal_created_idx ON catalog.normalization_application USING btree (applied_by_principal_id, applied_at DESC);


--
-- Name: normalization_application_proposal_apply_once_idx; Type: INDEX; Schema: catalog; Owner: -
--

CREATE UNIQUE INDEX normalization_application_proposal_apply_once_idx ON catalog.normalization_application USING btree (proposal_id) WHERE (rollback_of IS NULL);


--
-- Name: normalization_application_proposal_idx; Type: INDEX; Schema: catalog; Owner: -
--

CREATE INDEX normalization_application_proposal_idx ON catalog.normalization_application USING btree (proposal_id);


--
-- Name: normalization_application_rollback_once_idx; Type: INDEX; Schema: catalog; Owner: -
--

CREATE UNIQUE INDEX normalization_application_rollback_once_idx ON catalog.normalization_application USING btree (rollback_of) WHERE (rollback_of IS NOT NULL);


--
-- Name: normalization_application_target_idx; Type: INDEX; Schema: catalog; Owner: -
--

CREATE INDEX normalization_application_target_idx ON catalog.normalization_application USING btree (target_kind, target_id) WHERE (target_id IS NOT NULL);


--
-- Name: normalization_proposal_bronze_object_idx; Type: INDEX; Schema: catalog; Owner: -
--

CREATE INDEX normalization_proposal_bronze_object_idx ON catalog.normalization_proposal USING btree (bronze_object_id) WHERE (bronze_object_id IS NOT NULL);


--
-- Name: normalization_proposal_raw_record_idx; Type: INDEX; Schema: catalog; Owner: -
--

CREATE INDEX normalization_proposal_raw_record_idx ON catalog.normalization_proposal USING btree (source_system, raw_record_id);


--
-- Name: normalization_proposal_review_principal_created_idx; Type: INDEX; Schema: catalog; Owner: -
--

CREATE INDEX normalization_proposal_review_principal_created_idx ON catalog.normalization_proposal_review USING btree (reviewer_principal_id, created_at DESC);


--
-- Name: normalization_proposal_review_proposal_created_idx; Type: INDEX; Schema: catalog; Owner: -
--

CREATE INDEX normalization_proposal_review_proposal_created_idx ON catalog.normalization_proposal_review USING btree (proposal_id, created_at DESC);


--
-- Name: normalization_proposal_status_created_idx; Type: INDEX; Schema: catalog; Owner: -
--

CREATE INDEX normalization_proposal_status_created_idx ON catalog.normalization_proposal USING btree (status, created_at DESC);


--
-- Name: normalization_proposal_submission_audit_principal_idx; Type: INDEX; Schema: catalog; Owner: -
--

CREATE INDEX normalization_proposal_submission_audit_principal_idx ON catalog.normalization_proposal_submission_audit USING btree (submitted_by_principal_id, submitted_at DESC);


--
-- Name: outbox_quarantine_event_idx; Type: INDEX; Schema: catalog; Owner: -
--

CREATE INDEX outbox_quarantine_event_idx ON catalog.outbox_quarantine USING btree (source_outbox_table, event_id);


--
-- Name: outbox_quarantine_failure_stage_idx; Type: INDEX; Schema: catalog; Owner: -
--

CREATE INDEX outbox_quarantine_failure_stage_idx ON catalog.outbox_quarantine USING btree (failure_stage, last_failed_at DESC);


--
-- Name: outbox_quarantine_unresolved_idx; Type: INDEX; Schema: catalog; Owner: -
--

CREATE INDEX outbox_quarantine_unresolved_idx ON catalog.outbox_quarantine USING btree (last_failed_at DESC, consumer_key) WHERE (resolved_at IS NULL);


--
-- Name: outbox_unpublished_idx; Type: INDEX; Schema: catalog; Owner: -
--

CREATE INDEX outbox_unpublished_idx ON catalog.outbox_event USING btree (occurred_at) WHERE (published_at IS NULL);


--
-- Name: parcel_complex_id_idx; Type: INDEX; Schema: catalog; Owner: -
--

CREATE INDEX parcel_complex_id_idx ON catalog.parcel USING btree (complex_id);


--
-- Name: parcel_industry_assignment_parcel_idx; Type: INDEX; Schema: catalog; Owner: -
--

CREATE INDEX parcel_industry_assignment_parcel_idx ON catalog.parcel_industry_assignment USING btree (parcel_id);


--
-- Name: parcel_marker_anchor_active_point_gix; Type: INDEX; Schema: catalog; Owner: -
--

CREATE INDEX parcel_marker_anchor_active_point_gix ON catalog.parcel_marker_anchor USING gist (anchor_point) WHERE is_active;


--
-- Name: parcel_marker_anchor_generation_run_idx; Type: INDEX; Schema: catalog; Owner: -
--

CREATE INDEX parcel_marker_anchor_generation_run_idx ON catalog.parcel_marker_anchor USING btree (generation_run_id, pnu);


--
-- Name: parcel_marker_anchor_generation_run_snapshot_idx; Type: INDEX; Schema: catalog; Owner: -
--

CREATE INDEX parcel_marker_anchor_generation_run_snapshot_idx ON catalog.parcel_marker_anchor_generation_run USING btree (source_snapshot_id, started_at DESC);


--
-- Name: parcel_marker_anchor_generation_run_status_idx; Type: INDEX; Schema: catalog; Owner: -
--

CREATE INDEX parcel_marker_anchor_generation_run_status_idx ON catalog.parcel_marker_anchor_generation_run USING btree (status, started_at DESC);


--
-- Name: parcel_marker_anchor_one_active_per_pnu_idx; Type: INDEX; Schema: catalog; Owner: -
--

CREATE UNIQUE INDEX parcel_marker_anchor_one_active_per_pnu_idx ON catalog.parcel_marker_anchor USING btree (pnu) WHERE is_active;


--
-- Name: parcel_marker_anchor_parcel_idx; Type: INDEX; Schema: catalog; Owner: -
--

CREATE INDEX parcel_marker_anchor_parcel_idx ON catalog.parcel_marker_anchor USING btree (parcel_id) WHERE (parcel_id IS NOT NULL);


--
-- Name: parcel_marker_anchor_source_geometry_idx; Type: INDEX; Schema: catalog; Owner: -
--

CREATE INDEX parcel_marker_anchor_source_geometry_idx ON catalog.parcel_marker_anchor USING btree (source_geometry_version, pnu);


--
-- Name: schema_profile_source_idx; Type: INDEX; Schema: catalog; Owner: -
--

CREATE INDEX schema_profile_source_idx ON catalog.schema_profile USING btree (source_catalog_id, profiled_at DESC);


--
-- Name: source_catalog_provider_dataset_idx; Type: INDEX; Schema: catalog; Owner: -
--

CREATE INDEX source_catalog_provider_dataset_idx ON catalog.source_catalog USING btree (provider, dataset_name);


--
-- Name: source_record_source_external_idx; Type: INDEX; Schema: catalog; Owner: -
--

CREATE INDEX source_record_source_external_idx ON catalog.source_record USING btree (source, external_id) WHERE (external_id IS NOT NULL);


--
-- Name: spatial_layer_complex_idx; Type: INDEX; Schema: catalog; Owner: -
--

CREATE INDEX spatial_layer_complex_idx ON catalog.spatial_layer USING btree (complex_id);


--
-- Name: spatial_layer_parcel_idx; Type: INDEX; Schema: catalog; Owner: -
--

CREATE INDEX spatial_layer_parcel_idx ON catalog.spatial_layer USING btree (parcel_id) WHERE (parcel_id IS NOT NULL);


--
-- Name: vector_tile_artifact_manifest_idx; Type: INDEX; Schema: catalog; Owner: -
--

CREATE INDEX vector_tile_artifact_manifest_idx ON catalog.vector_tile_artifact USING btree (manifest_id);


--
-- Name: vector_tile_manifest_one_active_idx; Type: INDEX; Schema: catalog; Owner: -
--

CREATE UNIQUE INDEX vector_tile_manifest_one_active_idx ON catalog.vector_tile_manifest USING btree (is_active) WHERE is_active;


--
-- Name: vector_tile_manifest_published_idx; Type: INDEX; Schema: catalog; Owner: -
--

CREATE INDEX vector_tile_manifest_published_idx ON catalog.vector_tile_manifest USING btree (published_at DESC);


--
-- Name: parcel_boundary_mirror_complex_idx; Type: INDEX; Schema: serving_postgis; Owner: -
--

CREATE INDEX parcel_boundary_mirror_complex_idx ON serving_postgis.parcel_boundary_mirror USING btree (complex_id) WHERE (complex_id IS NOT NULL);


--
-- Name: parcel_boundary_mirror_geom_gix; Type: INDEX; Schema: serving_postgis; Owner: -
--

CREATE INDEX parcel_boundary_mirror_geom_gix ON serving_postgis.parcel_boundary_mirror USING gist (geom);


--
-- Name: parcel_boundary_mirror_parcel_idx; Type: INDEX; Schema: serving_postgis; Owner: -
--

CREATE INDEX parcel_boundary_mirror_parcel_idx ON serving_postgis.parcel_boundary_mirror USING btree (parcel_id) WHERE (parcel_id IS NOT NULL);


--
-- Name: parcel_boundary_mirror_rebuild_idx; Type: INDEX; Schema: serving_postgis; Owner: -
--

CREATE INDEX parcel_boundary_mirror_rebuild_idx ON serving_postgis.parcel_boundary_mirror USING btree (rebuild_run_id, pnu);


--
-- Name: parcel_boundary_mirror_rebuild_run_snapshot_idx; Type: INDEX; Schema: serving_postgis; Owner: -
--

CREATE INDEX parcel_boundary_mirror_rebuild_run_snapshot_idx ON serving_postgis.parcel_boundary_mirror_rebuild_run USING btree (source_snapshot_id, started_at DESC);


--
-- Name: parcel_boundary_mirror_rebuild_run_status_idx; Type: INDEX; Schema: serving_postgis; Owner: -
--

CREATE INDEX parcel_boundary_mirror_rebuild_run_status_idx ON serving_postgis.parcel_boundary_mirror_rebuild_run USING btree (status, started_at DESC);


--
-- Name: parcel_boundary_mirror_snapshot_idx; Type: INDEX; Schema: serving_postgis; Owner: -
--

CREATE INDEX parcel_boundary_mirror_snapshot_idx ON serving_postgis.parcel_boundary_mirror USING btree (source_snapshot_id, pnu);


--
