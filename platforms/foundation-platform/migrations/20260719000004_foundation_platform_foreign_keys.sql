-- Name: allowed_industry allowed_industry_complex_id_fkey; Type: FK CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.allowed_industry
    ADD CONSTRAINT allowed_industry_complex_id_fkey FOREIGN KEY (complex_id) REFERENCES catalog.industrial_complex(id);


--
-- Name: allowed_industry allowed_industry_industry_group_id_fkey; Type: FK CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.allowed_industry
    ADD CONSTRAINT allowed_industry_industry_group_id_fkey FOREIGN KEY (industry_group_id) REFERENCES catalog.industry_group(id);


--
-- Name: allowed_industry allowed_industry_source_record_id_fkey; Type: FK CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.allowed_industry
    ADD CONSTRAINT allowed_industry_source_record_id_fkey FOREIGN KEY (source_record_id) REFERENCES catalog.source_record(id);


--
-- Name: blueprint blueprint_complex_id_fkey; Type: FK CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.blueprint
    ADD CONSTRAINT blueprint_complex_id_fkey FOREIGN KEY (complex_id) REFERENCES catalog.industrial_complex(id);


--
-- Name: blueprint blueprint_file_asset_id_fkey; Type: FK CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.blueprint
    ADD CONSTRAINT blueprint_file_asset_id_fkey FOREIGN KEY (file_asset_id) REFERENCES catalog.file_asset(id);


--
-- Name: blueprint blueprint_source_record_id_fkey; Type: FK CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.blueprint
    ADD CONSTRAINT blueprint_source_record_id_fkey FOREIGN KEY (source_record_id) REFERENCES catalog.source_record(id);


--
-- Name: bronze_object bronze_object_ingestion_run_id_fkey; Type: FK CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.bronze_object
    ADD CONSTRAINT bronze_object_ingestion_run_id_fkey FOREIGN KEY (ingestion_run_id) REFERENCES catalog.ingestion_run(id);


--
-- Name: bronze_object bronze_object_source_catalog_id_fkey; Type: FK CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.bronze_object
    ADD CONSTRAINT bronze_object_source_catalog_id_fkey FOREIGN KEY (source_catalog_id) REFERENCES catalog.source_catalog(id);


--
-- Name: bronze_object bronze_object_source_record_id_fkey; Type: FK CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.bronze_object
    ADD CONSTRAINT bronze_object_source_record_id_fkey FOREIGN KEY (source_record_id) REFERENCES catalog.source_record(id);


--
-- Name: building building_parcel_id_fkey; Type: FK CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.building
    ADD CONSTRAINT building_parcel_id_fkey FOREIGN KEY (parcel_id) REFERENCES catalog.parcel(id);


--
-- Name: building_unit building_unit_parcel_id_fkey; Type: FK CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.building_unit
    ADD CONSTRAINT building_unit_parcel_id_fkey FOREIGN KEY (parcel_id) REFERENCES catalog.parcel(id);


--
-- Name: complex_attachment complex_attachment_complex_id_fkey; Type: FK CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.complex_attachment
    ADD CONSTRAINT complex_attachment_complex_id_fkey FOREIGN KEY (complex_id) REFERENCES catalog.industrial_complex(id) ON DELETE CASCADE;


--
-- Name: complex_attachment complex_attachment_file_asset_id_fkey; Type: FK CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.complex_attachment
    ADD CONSTRAINT complex_attachment_file_asset_id_fkey FOREIGN KEY (file_asset_id) REFERENCES catalog.file_asset(id);


--
-- Name: complex_notice complex_notice_complex_id_fkey; Type: FK CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.complex_notice
    ADD CONSTRAINT complex_notice_complex_id_fkey FOREIGN KEY (complex_id) REFERENCES catalog.industrial_complex(id);


--
-- Name: complex_notice complex_notice_source_record_id_fkey; Type: FK CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.complex_notice
    ADD CONSTRAINT complex_notice_source_record_id_fkey FOREIGN KEY (source_record_id) REFERENCES catalog.source_record(id);


--
-- Name: digital_twin_asset digital_twin_asset_building_id_fkey; Type: FK CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.digital_twin_asset
    ADD CONSTRAINT digital_twin_asset_building_id_fkey FOREIGN KEY (building_id) REFERENCES catalog.building(id);


--
-- Name: digital_twin_asset digital_twin_asset_complex_id_fkey; Type: FK CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.digital_twin_asset
    ADD CONSTRAINT digital_twin_asset_complex_id_fkey FOREIGN KEY (complex_id) REFERENCES catalog.industrial_complex(id);


--
-- Name: digital_twin_asset digital_twin_asset_file_asset_id_fkey; Type: FK CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.digital_twin_asset
    ADD CONSTRAINT digital_twin_asset_file_asset_id_fkey FOREIGN KEY (file_asset_id) REFERENCES catalog.file_asset(id);


--
-- Name: digital_twin_asset digital_twin_asset_parcel_id_fkey; Type: FK CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.digital_twin_asset
    ADD CONSTRAINT digital_twin_asset_parcel_id_fkey FOREIGN KEY (parcel_id) REFERENCES catalog.parcel(id);


--
-- Name: digital_twin_asset digital_twin_asset_source_record_id_fkey; Type: FK CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.digital_twin_asset
    ADD CONSTRAINT digital_twin_asset_source_record_id_fkey FOREIGN KEY (source_record_id) REFERENCES catalog.source_record(id);


--
-- Name: file_asset file_asset_source_record_id_fkey; Type: FK CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.file_asset
    ADD CONSTRAINT file_asset_source_record_id_fkey FOREIGN KEY (source_record_id) REFERENCES catalog.source_record(id);


--
-- Name: industrial_complex_gold_pointer industrial_complex_gold_point_spatial_locator_file_asset_i_fkey; Type: FK CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.industrial_complex_gold_pointer
    ADD CONSTRAINT industrial_complex_gold_point_spatial_locator_file_asset_i_fkey FOREIGN KEY (spatial_locator_file_asset_id) REFERENCES catalog.file_asset(id);


--
-- Name: industrial_complex_gold_pointer industrial_complex_gold_pointer_complex_id_fkey; Type: FK CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.industrial_complex_gold_pointer
    ADD CONSTRAINT industrial_complex_gold_pointer_complex_id_fkey FOREIGN KEY (complex_id) REFERENCES catalog.industrial_complex(id) ON DELETE CASCADE;


--
-- Name: industrial_complex_gold_pointer industrial_complex_gold_pointer_profile_file_asset_id_fkey; Type: FK CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.industrial_complex_gold_pointer
    ADD CONSTRAINT industrial_complex_gold_pointer_profile_file_asset_id_fkey FOREIGN KEY (profile_file_asset_id) REFERENCES catalog.file_asset(id);


--
-- Name: industrial_complex_gold_pointer industrial_complex_gold_pointer_source_record_id_fkey; Type: FK CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.industrial_complex_gold_pointer
    ADD CONSTRAINT industrial_complex_gold_pointer_source_record_id_fkey FOREIGN KEY (source_record_id) REFERENCES catalog.source_record(id);


--
-- Name: industry_group industry_group_complex_id_fkey; Type: FK CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.industry_group
    ADD CONSTRAINT industry_group_complex_id_fkey FOREIGN KEY (complex_id) REFERENCES catalog.industrial_complex(id);


--
-- Name: industry_group_member industry_group_member_industry_group_id_fkey; Type: FK CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.industry_group_member
    ADD CONSTRAINT industry_group_member_industry_group_id_fkey FOREIGN KEY (industry_group_id) REFERENCES catalog.industry_group(id) ON DELETE CASCADE;


--
-- Name: ingestion_run ingestion_run_source_catalog_id_fkey; Type: FK CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.ingestion_run
    ADD CONSTRAINT ingestion_run_source_catalog_id_fkey FOREIGN KEY (source_catalog_id) REFERENCES catalog.source_catalog(id);


--
-- Name: lakehouse_access_policy lakehouse_access_policy_data_asset_id_fkey; Type: FK CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.lakehouse_access_policy
    ADD CONSTRAINT lakehouse_access_policy_data_asset_id_fkey FOREIGN KEY (data_asset_id) REFERENCES catalog.lakehouse_data_asset(id) ON DELETE CASCADE;


--
-- Name: lakehouse_dataset_version lakehouse_dataset_version_created_by_ingestion_run_id_fkey; Type: FK CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.lakehouse_dataset_version
    ADD CONSTRAINT lakehouse_dataset_version_created_by_ingestion_run_id_fkey FOREIGN KEY (created_by_ingestion_run_id) REFERENCES catalog.ingestion_run(id);


--
-- Name: lakehouse_dataset_version lakehouse_dataset_version_data_asset_id_fkey; Type: FK CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.lakehouse_dataset_version
    ADD CONSTRAINT lakehouse_dataset_version_data_asset_id_fkey FOREIGN KEY (data_asset_id) REFERENCES catalog.lakehouse_data_asset(id) ON DELETE CASCADE;


--
-- Name: lakehouse_lineage_edge lakehouse_lineage_edge_from_version_id_fkey; Type: FK CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.lakehouse_lineage_edge
    ADD CONSTRAINT lakehouse_lineage_edge_from_version_id_fkey FOREIGN KEY (from_version_id) REFERENCES catalog.lakehouse_dataset_version(id) ON DELETE CASCADE;


--
-- Name: lakehouse_lineage_edge lakehouse_lineage_edge_to_version_id_fkey; Type: FK CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.lakehouse_lineage_edge
    ADD CONSTRAINT lakehouse_lineage_edge_to_version_id_fkey FOREIGN KEY (to_version_id) REFERENCES catalog.lakehouse_dataset_version(id) ON DELETE CASCADE;


--
-- Name: lakehouse_object_artifact lakehouse_object_artifact_dataset_version_id_fkey; Type: FK CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.lakehouse_object_artifact
    ADD CONSTRAINT lakehouse_object_artifact_dataset_version_id_fkey FOREIGN KEY (dataset_version_id) REFERENCES catalog.lakehouse_dataset_version(id) ON DELETE CASCADE;


--
-- Name: lakehouse_object_artifact lakehouse_object_artifact_namespace_id_fkey; Type: FK CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.lakehouse_object_artifact
    ADD CONSTRAINT lakehouse_object_artifact_namespace_id_fkey FOREIGN KEY (namespace_id) REFERENCES catalog.lakehouse_storage_namespace(id);


--
-- Name: lakehouse_quality_check lakehouse_quality_check_dataset_version_id_fkey; Type: FK CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.lakehouse_quality_check
    ADD CONSTRAINT lakehouse_quality_check_dataset_version_id_fkey FOREIGN KEY (dataset_version_id) REFERENCES catalog.lakehouse_dataset_version(id) ON DELETE CASCADE;


--
-- Name: manufacturer manufacturer_primary_parcel_id_fkey; Type: FK CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.manufacturer
    ADD CONSTRAINT manufacturer_primary_parcel_id_fkey FOREIGN KEY (primary_parcel_id) REFERENCES catalog.parcel(id);


--
-- Name: normalization_application normalization_application_outbox_event_id_fkey; Type: FK CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.normalization_application
    ADD CONSTRAINT normalization_application_outbox_event_id_fkey FOREIGN KEY (outbox_event_id) REFERENCES catalog.outbox_event(event_id);


--
-- Name: normalization_application normalization_application_proposal_id_fkey; Type: FK CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.normalization_application
    ADD CONSTRAINT normalization_application_proposal_id_fkey FOREIGN KEY (proposal_id) REFERENCES catalog.normalization_proposal(id);


--
-- Name: normalization_application normalization_application_rollback_of_fkey; Type: FK CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.normalization_application
    ADD CONSTRAINT normalization_application_rollback_of_fkey FOREIGN KEY (rollback_of) REFERENCES catalog.normalization_application(id);


--
-- Name: normalization_proposal normalization_proposal_bronze_object_id_fkey; Type: FK CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.normalization_proposal
    ADD CONSTRAINT normalization_proposal_bronze_object_id_fkey FOREIGN KEY (bronze_object_id) REFERENCES catalog.bronze_object(id);


--
-- Name: normalization_proposal_review normalization_proposal_review_proposal_id_fkey; Type: FK CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.normalization_proposal_review
    ADD CONSTRAINT normalization_proposal_review_proposal_id_fkey FOREIGN KEY (proposal_id) REFERENCES catalog.normalization_proposal(id);


--
-- Name: normalization_proposal_submission_audit normalization_proposal_submission_audit_proposal_id_fkey; Type: FK CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.normalization_proposal_submission_audit
    ADD CONSTRAINT normalization_proposal_submission_audit_proposal_id_fkey FOREIGN KEY (proposal_id) REFERENCES catalog.normalization_proposal(id) ON DELETE RESTRICT;


--
-- Name: notice_attachment notice_attachment_file_asset_id_fkey; Type: FK CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.notice_attachment
    ADD CONSTRAINT notice_attachment_file_asset_id_fkey FOREIGN KEY (file_asset_id) REFERENCES catalog.file_asset(id);


--
-- Name: notice_attachment notice_attachment_notice_id_fkey; Type: FK CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.notice_attachment
    ADD CONSTRAINT notice_attachment_notice_id_fkey FOREIGN KEY (notice_id) REFERENCES catalog.complex_notice(id) ON DELETE CASCADE;


--
-- Name: parcel parcel_complex_id_fkey; Type: FK CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.parcel
    ADD CONSTRAINT parcel_complex_id_fkey FOREIGN KEY (complex_id) REFERENCES catalog.industrial_complex(id);


--
-- Name: parcel_industry_assignment parcel_industry_assignment_industry_group_id_fkey; Type: FK CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.parcel_industry_assignment
    ADD CONSTRAINT parcel_industry_assignment_industry_group_id_fkey FOREIGN KEY (industry_group_id) REFERENCES catalog.industry_group(id);


--
-- Name: parcel_industry_assignment parcel_industry_assignment_parcel_id_fkey; Type: FK CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.parcel_industry_assignment
    ADD CONSTRAINT parcel_industry_assignment_parcel_id_fkey FOREIGN KEY (parcel_id) REFERENCES catalog.parcel(id);


--
-- Name: parcel_industry_assignment parcel_industry_assignment_source_record_id_fkey; Type: FK CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.parcel_industry_assignment
    ADD CONSTRAINT parcel_industry_assignment_source_record_id_fkey FOREIGN KEY (source_record_id) REFERENCES catalog.source_record(id);


--
-- Name: parcel_marker_anchor parcel_marker_anchor_generation_run_id_source_geometry_ver_fkey; Type: FK CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.parcel_marker_anchor
    ADD CONSTRAINT parcel_marker_anchor_generation_run_id_source_geometry_ver_fkey FOREIGN KEY (generation_run_id, source_geometry_version) REFERENCES catalog.parcel_marker_anchor_generation_run(id, source_snapshot_id) ON DELETE RESTRICT;


--
-- Name: parcel_marker_anchor_generation_run parcel_marker_anchor_generation_run_source_file_asset_id_fkey; Type: FK CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.parcel_marker_anchor_generation_run
    ADD CONSTRAINT parcel_marker_anchor_generation_run_source_file_asset_id_fkey FOREIGN KEY (source_file_asset_id) REFERENCES catalog.file_asset(id);


--
-- Name: parcel_marker_anchor_generation_run parcel_marker_anchor_generation_run_source_record_id_fkey; Type: FK CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.parcel_marker_anchor_generation_run
    ADD CONSTRAINT parcel_marker_anchor_generation_run_source_record_id_fkey FOREIGN KEY (source_record_id) REFERENCES catalog.source_record(id);


--
-- Name: parcel_marker_anchor parcel_marker_anchor_parcel_id_fkey; Type: FK CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.parcel_marker_anchor
    ADD CONSTRAINT parcel_marker_anchor_parcel_id_fkey FOREIGN KEY (parcel_id) REFERENCES catalog.parcel(id) ON DELETE SET NULL;


--
-- Name: parcel_marker_anchor parcel_marker_anchor_source_file_asset_id_fkey; Type: FK CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.parcel_marker_anchor
    ADD CONSTRAINT parcel_marker_anchor_source_file_asset_id_fkey FOREIGN KEY (source_file_asset_id) REFERENCES catalog.file_asset(id);


--
-- Name: parcel_marker_anchor parcel_marker_anchor_source_record_id_fkey; Type: FK CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.parcel_marker_anchor
    ADD CONSTRAINT parcel_marker_anchor_source_record_id_fkey FOREIGN KEY (source_record_id) REFERENCES catalog.source_record(id);


--
-- Name: schema_profile schema_profile_ingestion_run_id_fkey; Type: FK CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.schema_profile
    ADD CONSTRAINT schema_profile_ingestion_run_id_fkey FOREIGN KEY (ingestion_run_id) REFERENCES catalog.ingestion_run(id);


--
-- Name: schema_profile schema_profile_source_catalog_id_fkey; Type: FK CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.schema_profile
    ADD CONSTRAINT schema_profile_source_catalog_id_fkey FOREIGN KEY (source_catalog_id) REFERENCES catalog.source_catalog(id);


--
-- Name: spatial_layer spatial_layer_blueprint_id_fkey; Type: FK CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.spatial_layer
    ADD CONSTRAINT spatial_layer_blueprint_id_fkey FOREIGN KEY (blueprint_id) REFERENCES catalog.blueprint(id);


--
-- Name: spatial_layer spatial_layer_complex_id_fkey; Type: FK CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.spatial_layer
    ADD CONSTRAINT spatial_layer_complex_id_fkey FOREIGN KEY (complex_id) REFERENCES catalog.industrial_complex(id);


--
-- Name: spatial_layer spatial_layer_parcel_id_fkey; Type: FK CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.spatial_layer
    ADD CONSTRAINT spatial_layer_parcel_id_fkey FOREIGN KEY (parcel_id) REFERENCES catalog.parcel(id);


--
-- Name: spatial_layer spatial_layer_source_record_id_fkey; Type: FK CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.spatial_layer
    ADD CONSTRAINT spatial_layer_source_record_id_fkey FOREIGN KEY (source_record_id) REFERENCES catalog.source_record(id);


--
-- Name: vector_tile_artifact vector_tile_artifact_manifest_id_fkey; Type: FK CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.vector_tile_artifact
    ADD CONSTRAINT vector_tile_artifact_manifest_id_fkey FOREIGN KEY (manifest_id) REFERENCES catalog.vector_tile_manifest(id) ON DELETE CASCADE;


--
-- Name: vector_tile_artifact_source_file_asset vector_tile_artifact_source_file_asset_artifact_id_fkey; Type: FK CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.vector_tile_artifact_source_file_asset
    ADD CONSTRAINT vector_tile_artifact_source_file_asset_artifact_id_fkey FOREIGN KEY (artifact_id) REFERENCES catalog.vector_tile_artifact(id) ON DELETE CASCADE;


--
-- Name: vector_tile_artifact_source_file_asset vector_tile_artifact_source_file_asset_file_asset_id_fkey; Type: FK CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.vector_tile_artifact_source_file_asset
    ADD CONSTRAINT vector_tile_artifact_source_file_asset_file_asset_id_fkey FOREIGN KEY (file_asset_id) REFERENCES catalog.file_asset(id);


--
-- Name: vector_tile_artifact vector_tile_artifact_source_record_id_fkey; Type: FK CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.vector_tile_artifact
    ADD CONSTRAINT vector_tile_artifact_source_record_id_fkey FOREIGN KEY (source_record_id) REFERENCES catalog.source_record(id);


--
-- Name: vector_tile_artifact vector_tile_artifact_tilejson_file_asset_id_fkey; Type: FK CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.vector_tile_artifact
    ADD CONSTRAINT vector_tile_artifact_tilejson_file_asset_id_fkey FOREIGN KEY (tilejson_file_asset_id) REFERENCES catalog.file_asset(id);


--
-- Name: vector_tile_manifest vector_tile_manifest_manifest_file_asset_id_fkey; Type: FK CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.vector_tile_manifest
    ADD CONSTRAINT vector_tile_manifest_manifest_file_asset_id_fkey FOREIGN KEY (manifest_file_asset_id) REFERENCES catalog.file_asset(id);


--
-- Name: vector_tile_manifest vector_tile_manifest_source_record_id_fkey; Type: FK CONSTRAINT; Schema: catalog; Owner: -
--

ALTER TABLE ONLY catalog.vector_tile_manifest
    ADD CONSTRAINT vector_tile_manifest_source_record_id_fkey FOREIGN KEY (source_record_id) REFERENCES catalog.source_record(id);


--
-- Name: parcel_boundary_mirror parcel_boundary_mirror_complex_id_fkey; Type: FK CONSTRAINT; Schema: serving_postgis; Owner: -
--

ALTER TABLE ONLY serving_postgis.parcel_boundary_mirror
    ADD CONSTRAINT parcel_boundary_mirror_complex_id_fkey FOREIGN KEY (complex_id) REFERENCES catalog.industrial_complex(id) ON DELETE SET NULL;


--
-- Name: parcel_boundary_mirror parcel_boundary_mirror_parcel_id_fkey; Type: FK CONSTRAINT; Schema: serving_postgis; Owner: -
--

ALTER TABLE ONLY serving_postgis.parcel_boundary_mirror
    ADD CONSTRAINT parcel_boundary_mirror_parcel_id_fkey FOREIGN KEY (parcel_id) REFERENCES catalog.parcel(id) ON DELETE SET NULL;


--
-- Name: parcel_boundary_mirror parcel_boundary_mirror_rebuild_run_id_source_snapshot_id_fkey; Type: FK CONSTRAINT; Schema: serving_postgis; Owner: -
--

ALTER TABLE ONLY serving_postgis.parcel_boundary_mirror
    ADD CONSTRAINT parcel_boundary_mirror_rebuild_run_id_source_snapshot_id_fkey FOREIGN KEY (rebuild_run_id, source_snapshot_id) REFERENCES serving_postgis.parcel_boundary_mirror_rebuild_run(id, source_snapshot_id) ON DELETE RESTRICT;


--
-- Name: parcel_boundary_mirror_rebuild_run parcel_boundary_mirror_rebuild_run_source_file_asset_id_fkey; Type: FK CONSTRAINT; Schema: serving_postgis; Owner: -
--

ALTER TABLE ONLY serving_postgis.parcel_boundary_mirror_rebuild_run
    ADD CONSTRAINT parcel_boundary_mirror_rebuild_run_source_file_asset_id_fkey FOREIGN KEY (source_file_asset_id) REFERENCES catalog.file_asset(id);


--
-- Name: parcel_boundary_mirror_rebuild_run parcel_boundary_mirror_rebuild_run_source_record_id_fkey; Type: FK CONSTRAINT; Schema: serving_postgis; Owner: -
--

ALTER TABLE ONLY serving_postgis.parcel_boundary_mirror_rebuild_run
    ADD CONSTRAINT parcel_boundary_mirror_rebuild_run_source_record_id_fkey FOREIGN KEY (source_record_id) REFERENCES catalog.source_record(id);


--
-- Name: parcel_boundary_mirror parcel_boundary_mirror_source_file_asset_id_fkey; Type: FK CONSTRAINT; Schema: serving_postgis; Owner: -
--

ALTER TABLE ONLY serving_postgis.parcel_boundary_mirror
    ADD CONSTRAINT parcel_boundary_mirror_source_file_asset_id_fkey FOREIGN KEY (source_file_asset_id) REFERENCES catalog.file_asset(id);


--
-- Name: parcel_boundary_mirror parcel_boundary_mirror_source_record_id_fkey; Type: FK CONSTRAINT; Schema: serving_postgis; Owner: -
--

ALTER TABLE ONLY serving_postgis.parcel_boundary_mirror
    ADD CONSTRAINT parcel_boundary_mirror_source_record_id_fkey FOREIGN KEY (source_record_id) REFERENCES catalog.source_record(id);


--
-- PostgreSQL database dump complete
--
