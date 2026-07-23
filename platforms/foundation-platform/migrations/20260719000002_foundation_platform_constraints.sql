--
-- PostgreSQL database dump
--


-- Dumped from database version 17.10
-- Dumped by pg_dump version 17.10

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET transaction_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
-- Removed pg_dump's `set_config('search_path','',false)`: it leaks a session-level
-- empty search_path into sqlx's same-transaction `_sqlx_migrations` INSERT (42P01).
-- DDL below is fully schema-qualified, so this is behaviour-preserving.
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

SET default_tablespace = '';

--
-- Name: allowed_industry allowed_industry_complex_id_industry_group_id_rule_kind_key; Type: CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.allowed_industry
    ADD CONSTRAINT allowed_industry_complex_id_industry_group_id_rule_kind_key UNIQUE (complex_id, industry_group_id, rule_kind);


--
-- Name: allowed_industry allowed_industry_pkey; Type: CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.allowed_industry
    ADD CONSTRAINT allowed_industry_pkey PRIMARY KEY (id);


--
-- Name: blueprint blueprint_pkey; Type: CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.blueprint
    ADD CONSTRAINT blueprint_pkey PRIMARY KEY (id);


--
-- Name: bronze_object bronze_object_pkey; Type: CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.bronze_object
    ADD CONSTRAINT bronze_object_pkey PRIMARY KEY (id);


--
-- Name: bronze_object bronze_object_source_catalog_id_dedupe_key_key; Type: CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.bronze_object
    ADD CONSTRAINT bronze_object_source_catalog_id_dedupe_key_key UNIQUE (source_catalog_id, dedupe_key);


--
-- Name: building building_pkey; Type: CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.building
    ADD CONSTRAINT building_pkey PRIMARY KEY (id);


--
-- Name: building_unit building_unit_pkey; Type: CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.building_unit
    ADD CONSTRAINT building_unit_pkey PRIMARY KEY (id);


--
-- Name: complex_attachment complex_attachment_pkey; Type: CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.complex_attachment
    ADD CONSTRAINT complex_attachment_pkey PRIMARY KEY (complex_id, file_asset_id);


--
-- Name: complex_notice complex_notice_pkey; Type: CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.complex_notice
    ADD CONSTRAINT complex_notice_pkey PRIMARY KEY (id);


--
-- Name: digital_twin_asset digital_twin_asset_pkey; Type: CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.digital_twin_asset
    ADD CONSTRAINT digital_twin_asset_pkey PRIMARY KEY (id);


--
-- Name: file_asset file_asset_object_key_key; Type: CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.file_asset
    ADD CONSTRAINT file_asset_object_key_key UNIQUE (object_key);


--
-- Name: file_asset file_asset_pkey; Type: CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.file_asset
    ADD CONSTRAINT file_asset_pkey PRIMARY KEY (id);


--
-- Name: industrial_complex industrial_complex_archive_actor_required; Type: CHECK CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE catalog.industrial_complex
    ADD CONSTRAINT industrial_complex_archive_actor_required CHECK ((((archived_at IS NULL) AND (archived_by_staff_id IS NULL) AND (archive_reason IS NULL)) OR ((archived_at IS NOT NULL) AND (archived_by_staff_id IS NOT NULL)))) NOT VALID;


--
-- Name: industrial_complex industrial_complex_archive_reason_non_blank; Type: CHECK CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE catalog.industrial_complex
    ADD CONSTRAINT industrial_complex_archive_reason_non_blank CHECK (((archive_reason IS NULL) OR (btrim(archive_reason) <> ''::text))) NOT VALID;


--
-- Name: industrial_complex_gold_pointer industrial_complex_gold_pointer_pkey; Type: CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.industrial_complex_gold_pointer
    ADD CONSTRAINT industrial_complex_gold_pointer_pkey PRIMARY KEY (complex_id);


--
-- Name: industrial_complex industrial_complex_pkey; Type: CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.industrial_complex
    ADD CONSTRAINT industrial_complex_pkey PRIMARY KEY (id);


--
-- Name: industry_group industry_group_complex_id_name_key; Type: CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.industry_group
    ADD CONSTRAINT industry_group_complex_id_name_key UNIQUE (complex_id, name);


--
-- Name: industry_group_member industry_group_member_pkey; Type: CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.industry_group_member
    ADD CONSTRAINT industry_group_member_pkey PRIMARY KEY (industry_group_id, industry_code, industry_code_system);


--
-- Name: industry_group industry_group_pkey; Type: CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.industry_group
    ADD CONSTRAINT industry_group_pkey PRIMARY KEY (id);


--
-- Name: ingestion_run ingestion_run_pkey; Type: CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.ingestion_run
    ADD CONSTRAINT ingestion_run_pkey PRIMARY KEY (id);


--
-- Name: lakehouse_access_policy lakehouse_access_policy_pkey; Type: CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.lakehouse_access_policy
    ADD CONSTRAINT lakehouse_access_policy_pkey PRIMARY KEY (data_asset_id, principal_service, action);


--
-- Name: lakehouse_batch_run lakehouse_batch_run_job_name_contract_created_at_write_disp_key; Type: CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.lakehouse_batch_run
    ADD CONSTRAINT lakehouse_batch_run_job_name_contract_created_at_write_disp_key UNIQUE NULLS NOT DISTINCT (job_name, contract, created_at, write_disposition, input_path, target_path, target_qualified_table);


--
-- Name: lakehouse_batch_run lakehouse_batch_run_pkey; Type: CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.lakehouse_batch_run
    ADD CONSTRAINT lakehouse_batch_run_pkey PRIMARY KEY (id);


--
-- Name: lakehouse_data_asset lakehouse_data_asset_pkey; Type: CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.lakehouse_data_asset
    ADD CONSTRAINT lakehouse_data_asset_pkey PRIMARY KEY (id);


--
-- Name: lakehouse_data_asset lakehouse_data_asset_qualified_name_key; Type: CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.lakehouse_data_asset
    ADD CONSTRAINT lakehouse_data_asset_qualified_name_key UNIQUE (qualified_name);


--
-- Name: lakehouse_dataset_version lakehouse_dataset_version_data_asset_id_version_key; Type: CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.lakehouse_dataset_version
    ADD CONSTRAINT lakehouse_dataset_version_data_asset_id_version_key UNIQUE (data_asset_id, version);


--
-- Name: lakehouse_dataset_version lakehouse_dataset_version_pkey; Type: CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.lakehouse_dataset_version
    ADD CONSTRAINT lakehouse_dataset_version_pkey PRIMARY KEY (id);


--
-- Name: lakehouse_lineage_edge lakehouse_lineage_edge_pkey; Type: CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.lakehouse_lineage_edge
    ADD CONSTRAINT lakehouse_lineage_edge_pkey PRIMARY KEY (from_version_id, to_version_id, transform_name);


--
-- Name: lakehouse_object_artifact lakehouse_object_artifact_namespace_id_object_key_key; Type: CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.lakehouse_object_artifact
    ADD CONSTRAINT lakehouse_object_artifact_namespace_id_object_key_key UNIQUE (namespace_id, object_key);


--
-- Name: lakehouse_object_artifact lakehouse_object_artifact_pkey; Type: CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.lakehouse_object_artifact
    ADD CONSTRAINT lakehouse_object_artifact_pkey PRIMARY KEY (id);


--
-- Name: lakehouse_quality_check lakehouse_quality_check_pkey; Type: CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.lakehouse_quality_check
    ADD CONSTRAINT lakehouse_quality_check_pkey PRIMARY KEY (dataset_version_id, check_name);


--
-- Name: lakehouse_storage_namespace lakehouse_storage_namespace_pkey; Type: CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.lakehouse_storage_namespace
    ADD CONSTRAINT lakehouse_storage_namespace_pkey PRIMARY KEY (id);


--
-- Name: lakehouse_storage_namespace lakehouse_storage_namespace_provider_environment_owner_serv_key; Type: CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.lakehouse_storage_namespace
    ADD CONSTRAINT lakehouse_storage_namespace_provider_environment_owner_serv_key UNIQUE (provider, environment, owner_service);


--
-- Name: manufacturer manufacturer_pkey; Type: CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.manufacturer
    ADD CONSTRAINT manufacturer_pkey PRIMARY KEY (id);


--
-- Name: normalization_application normalization_application_pkey; Type: CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.normalization_application
    ADD CONSTRAINT normalization_application_pkey PRIMARY KEY (id);


--
-- Name: normalization_proposal normalization_proposal_pkey; Type: CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.normalization_proposal
    ADD CONSTRAINT normalization_proposal_pkey PRIMARY KEY (id);


--
-- Name: normalization_proposal normalization_proposal_proposal_key_key; Type: CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.normalization_proposal
    ADD CONSTRAINT normalization_proposal_proposal_key_key UNIQUE (proposal_key);


--
-- Name: normalization_proposal_review normalization_proposal_review_pkey; Type: CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.normalization_proposal_review
    ADD CONSTRAINT normalization_proposal_review_pkey PRIMARY KEY (id);


--
-- Name: normalization_proposal_submission_audit normalization_proposal_submission_audit_pkey; Type: CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.normalization_proposal_submission_audit
    ADD CONSTRAINT normalization_proposal_submission_audit_pkey PRIMARY KEY (proposal_id);


--
-- Name: notice_attachment notice_attachment_pkey; Type: CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.notice_attachment
    ADD CONSTRAINT notice_attachment_pkey PRIMARY KEY (notice_id, file_asset_id);


--
-- Name: outbox_event outbox_event_pkey; Type: CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.outbox_event
    ADD CONSTRAINT outbox_event_pkey PRIMARY KEY (event_id);


--
-- Name: outbox_quarantine outbox_quarantine_pkey; Type: CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.outbox_quarantine
    ADD CONSTRAINT outbox_quarantine_pkey PRIMARY KEY (id);


--
-- Name: outbox_quarantine outbox_quarantine_source_outbox_table_check; Type: CHECK CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE catalog.outbox_quarantine
    ADD CONSTRAINT outbox_quarantine_source_outbox_table_check CHECK ((source_outbox_table = 'catalog.outbox_event'::text)) NOT VALID;


--
-- Name: outbox_quarantine outbox_quarantine_source_outbox_table_event_id_consumer_key_key; Type: CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.outbox_quarantine
    ADD CONSTRAINT outbox_quarantine_source_outbox_table_event_id_consumer_key_key UNIQUE (source_outbox_table, event_id, consumer_key);


--
-- Name: parcel_industry_assignment parcel_industry_assignment_parcel_id_industry_group_id_assi_key; Type: CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.parcel_industry_assignment
    ADD CONSTRAINT parcel_industry_assignment_parcel_id_industry_group_id_assi_key UNIQUE (parcel_id, industry_group_id, assignment_kind);


--
-- Name: parcel_industry_assignment parcel_industry_assignment_pkey; Type: CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.parcel_industry_assignment
    ADD CONSTRAINT parcel_industry_assignment_pkey PRIMARY KEY (id);


--
-- Name: parcel_marker_anchor_generation_run parcel_marker_anchor_generation_run_id_source_snapshot_id_key; Type: CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.parcel_marker_anchor_generation_run
    ADD CONSTRAINT parcel_marker_anchor_generation_run_id_source_snapshot_id_key UNIQUE (id, source_snapshot_id);


--
-- Name: parcel_marker_anchor_generation_run parcel_marker_anchor_generation_run_pkey; Type: CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.parcel_marker_anchor_generation_run
    ADD CONSTRAINT parcel_marker_anchor_generation_run_pkey PRIMARY KEY (id);


--
-- Name: parcel_marker_anchor parcel_marker_anchor_pkey; Type: CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.parcel_marker_anchor
    ADD CONSTRAINT parcel_marker_anchor_pkey PRIMARY KEY (id);


--
-- Name: parcel_marker_anchor parcel_marker_anchor_pnu_source_geometry_version_algorithm__key; Type: CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.parcel_marker_anchor
    ADD CONSTRAINT parcel_marker_anchor_pnu_source_geometry_version_algorithm__key UNIQUE (pnu, source_geometry_version, algorithm, algorithm_version);


--
-- Name: parcel parcel_pkey; Type: CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.parcel
    ADD CONSTRAINT parcel_pkey PRIMARY KEY (id);


--
-- Name: parcel parcel_pnu_key; Type: CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.parcel
    ADD CONSTRAINT parcel_pnu_key UNIQUE (pnu);


--
-- Name: schema_profile schema_profile_ingestion_run_id_field_path_key; Type: CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.schema_profile
    ADD CONSTRAINT schema_profile_ingestion_run_id_field_path_key UNIQUE (ingestion_run_id, field_path);


--
-- Name: schema_profile schema_profile_pkey; Type: CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.schema_profile
    ADD CONSTRAINT schema_profile_pkey PRIMARY KEY (id);


--
-- Name: source_catalog source_catalog_pkey; Type: CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.source_catalog
    ADD CONSTRAINT source_catalog_pkey PRIMARY KEY (id);


--
-- Name: source_catalog source_catalog_slug_key; Type: CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.source_catalog
    ADD CONSTRAINT source_catalog_slug_key UNIQUE (slug);


--
-- Name: source_record source_record_pkey; Type: CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.source_record
    ADD CONSTRAINT source_record_pkey PRIMARY KEY (id);


--
-- Name: spatial_layer spatial_layer_pkey; Type: CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.spatial_layer
    ADD CONSTRAINT spatial_layer_pkey PRIMARY KEY (id);


--
-- Name: vector_tile_artifact vector_tile_artifact_manifest_id_layer_key; Type: CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.vector_tile_artifact
    ADD CONSTRAINT vector_tile_artifact_manifest_id_layer_key UNIQUE (manifest_id, layer);


--
-- Name: vector_tile_artifact vector_tile_artifact_pkey; Type: CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.vector_tile_artifact
    ADD CONSTRAINT vector_tile_artifact_pkey PRIMARY KEY (id);


--
-- Name: vector_tile_artifact_source_file_asset vector_tile_artifact_source_file_asset_pkey; Type: CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.vector_tile_artifact_source_file_asset
    ADD CONSTRAINT vector_tile_artifact_source_file_asset_pkey PRIMARY KEY (artifact_id, file_asset_id);


--
-- Name: vector_tile_manifest vector_tile_manifest_current_version_key; Type: CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.vector_tile_manifest
    ADD CONSTRAINT vector_tile_manifest_current_version_key UNIQUE (current_version);


--
-- Name: vector_tile_manifest vector_tile_manifest_pkey; Type: CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.vector_tile_manifest
    ADD CONSTRAINT vector_tile_manifest_pkey PRIMARY KEY (id);


--
-- Name: parcel_boundary_mirror parcel_boundary_mirror_pkey; Type: CONSTRAINT; Schema: serving_postgis; Owner: -
--

ALTER TABLE ONLY serving_postgis.parcel_boundary_mirror
    ADD CONSTRAINT parcel_boundary_mirror_pkey PRIMARY KEY (pnu);


--
-- Name: parcel_boundary_mirror_rebuild_run parcel_boundary_mirror_rebuild_run_id_source_snapshot_id_key; Type: CONSTRAINT; Schema: serving_postgis; Owner: -
--

ALTER TABLE ONLY serving_postgis.parcel_boundary_mirror_rebuild_run
    ADD CONSTRAINT parcel_boundary_mirror_rebuild_run_id_source_snapshot_id_key UNIQUE (id, source_snapshot_id);


--
-- Name: parcel_boundary_mirror_rebuild_run parcel_boundary_mirror_rebuild_run_pkey; Type: CONSTRAINT; Schema: serving_postgis; Owner: -
--

ALTER TABLE ONLY serving_postgis.parcel_boundary_mirror_rebuild_run
    ADD CONSTRAINT parcel_boundary_mirror_rebuild_run_pkey PRIMARY KEY (id);


--
