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
-- NOTE: pg_dump emits `set_config('search_path','',false)` here so its DDL is
-- fully schema-qualified. Removed: sqlx runs each migration and its own
-- `_sqlx_migrations` bookkeeping INSERT in ONE transaction, and a session-level
-- empty search_path leaks into that unqualified INSERT -> 42P01. All DDL below
-- is already schema-qualified, so dropping this line is behaviour-preserving.
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: catalog; Type: SCHEMA; Schema: -; Owner: -
--

-- IF NOT EXISTS: the Compose bootstrap (infra/db/init + bootstrap-foundation.sql)
-- pre-creates catalog/serving_postgis to set ownership before migrate runs. On a
-- bare DB (postgres-integration CI) these are created fresh. Idempotent either way.
CREATE SCHEMA IF NOT EXISTS catalog;


--
-- Name: SCHEMA catalog; Type: COMMENT; Schema: -; Owner: -
--

COMMENT ON SCHEMA catalog IS 'Foundation Catalog context for canonical and collected data';


--
-- Name: serving_postgis; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA IF NOT EXISTS serving_postgis;


--
-- Name: postgis; Type: EXTENSION; Schema: -; Owner: -
--

-- Extension lifecycle belongs to the privileged bootstrap (superuser), NOT this
-- migrator-run migration. `CREATE EXTENSION IF NOT EXISTS` is a safe idempotent
-- no-op when postgis already exists (Compose pre-seeds it) and creates it on the
-- superuser postgres-integration path; but COMMENT-ON / ALTER of the extension
-- object require ownership the least-privilege foundation_migrator role does not
-- (and cannot) hold — PostGIS install is superuser-only. The extension's COMMENT
-- is therefore set once by infra/compose/bootstrap-foundation.sql. Emitting it
-- here raised `42501 must be owner of extension postgis` on the Compose path.
CREATE EXTENSION IF NOT EXISTS postgis WITH SCHEMA public;


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: allowed_industry; Type: TABLE; Schema: catalog; Owner: -
--

CREATE TABLE catalog.allowed_industry (
    id uuid NOT NULL,
    complex_id uuid NOT NULL,
    industry_group_id uuid NOT NULL,
    rule_kind text NOT NULL,
    source_record_id uuid,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    version bigint DEFAULT 1 NOT NULL,
    CONSTRAINT allowed_industry_rule_kind_check CHECK ((rule_kind = ANY (ARRAY['allowed'::text, 'recommended'::text, 'restricted'::text])))
);


--
-- Name: blueprint; Type: TABLE; Schema: catalog; Owner: -
--

CREATE TABLE catalog.blueprint (
    id uuid NOT NULL,
    complex_id uuid NOT NULL,
    file_asset_id uuid NOT NULL,
    blueprint_kind text NOT NULL,
    coordinate_system text NOT NULL,
    scale text,
    source_record_id uuid,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    version bigint DEFAULT 1 NOT NULL,
    CONSTRAINT blueprint_blueprint_kind_check CHECK ((blueprint_kind = ANY (ARRAY['master_plan'::text, 'parcel_map'::text, 'utility_plan'::text, 'floor_plan'::text, 'other'::text])))
);


--
-- Name: bronze_object; Type: TABLE; Schema: catalog; Owner: -
--

CREATE TABLE catalog.bronze_object (
    id uuid NOT NULL,
    source_catalog_id uuid NOT NULL,
    ingestion_run_id uuid NOT NULL,
    source_record_id uuid,
    source_partition_key text,
    dedupe_key text NOT NULL,
    request_params jsonb DEFAULT '{}'::jsonb NOT NULL,
    object_key text NOT NULL,
    checksum_sha256 text NOT NULL,
    content_type text NOT NULL,
    size_bytes bigint NOT NULL,
    collected_at timestamp with time zone DEFAULT now() NOT NULL,
    effective_date date,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    logical_record_count bigint,
    source_identity_key text NOT NULL,
    snapshot_period text,
    snapshot_date date NOT NULL,
    snapshot_granularity text NOT NULL,
    snapshot_basis text NOT NULL,
    provider_file_id text,
    provider_file_name text,
    provider_updated_at date,
    CONSTRAINT bronze_object_checksum_sha256_check CHECK ((checksum_sha256 ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT bronze_object_content_type_check CHECK ((content_type <> ''::text)),
    CONSTRAINT bronze_object_dedupe_key_check CHECK ((dedupe_key <> ''::text)),
    CONSTRAINT bronze_object_logical_record_count_check CHECK (((logical_record_count IS NULL) OR (logical_record_count >= 0))),
    CONSTRAINT bronze_object_object_key_check CHECK ((object_key <> ''::text)),
    CONSTRAINT bronze_object_object_key_no_backslash_check CHECK ((POSITION(('\'::text) IN (object_key)) = 0)),
    CONSTRAINT bronze_object_object_key_no_current_dir_check CHECK (((object_key !~~ '%/./%'::text) AND (object_key !~~ './%'::text) AND (object_key !~~ '%/.'::text))),
    CONSTRAINT bronze_object_object_key_no_empty_segment_check CHECK ((object_key !~~ '%//%'::text)),
    CONSTRAINT bronze_object_object_key_no_leading_slash_check CHECK ((object_key !~~ '/%'::text)),
    CONSTRAINT bronze_object_object_key_no_parent_dir_check CHECK (((object_key !~~ '%/../%'::text) AND (object_key !~~ '../%'::text) AND (object_key !~~ '%/..'::text))),
    CONSTRAINT bronze_object_provider_file_id_check CHECK (((provider_file_id IS NULL) OR (provider_file_id <> ''::text))),
    CONSTRAINT bronze_object_provider_file_name_check CHECK (((provider_file_name IS NULL) OR (provider_file_name <> ''::text))),
    CONSTRAINT bronze_object_request_params_check CHECK ((jsonb_typeof(request_params) = 'object'::text)),
    CONSTRAINT bronze_object_size_bytes_check CHECK ((size_bytes >= 0)),
    CONSTRAINT bronze_object_snapshot_basis_check CHECK ((snapshot_basis = ANY (ARRAY['provider_snapshot_date'::text, 'provider_file_period'::text, 'request_month'::text, 'provider_updated_at'::text, 'collected_at_fallback'::text]))),
    CONSTRAINT bronze_object_snapshot_granularity_check CHECK ((snapshot_granularity = ANY (ARRAY['day'::text, 'month'::text]))),
    CONSTRAINT bronze_object_snapshot_period_check CHECK (((snapshot_period IS NULL) OR (snapshot_period ~ '^[0-9]{4}-[0-9]{2}$'::text))),
    CONSTRAINT bronze_object_source_identity_key_check CHECK ((source_identity_key <> ''::text)),
    CONSTRAINT bronze_object_source_partition_key_check CHECK (((source_partition_key IS NULL) OR (source_partition_key <> ''::text)))
);


--
-- Name: building; Type: TABLE; Schema: catalog; Owner: -
--

CREATE TABLE catalog.building (
    id uuid NOT NULL,
    parcel_id uuid NOT NULL,
    purpose_code text NOT NULL,
    structure_code text NOT NULL,
    floor_area_m2 double precision NOT NULL,
    stories smallint NOT NULL,
    built_year integer NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    below_ground_floors smallint DEFAULT 0 NOT NULL,
    has_rooftop boolean DEFAULT false NOT NULL,
    rooftop_area_m2 double precision,
    rooftop_usage text DEFAULT ''::text NOT NULL
);


--
-- Name: building_unit; Type: TABLE; Schema: catalog; Owner: -
--

CREATE TABLE catalog.building_unit (
    id uuid NOT NULL,
    parcel_id uuid NOT NULL,
    building_name text DEFAULT ''::text NOT NULL,
    dong_name text DEFAULT ''::text NOT NULL,
    ho_name text DEFAULT ''::text NOT NULL,
    floor_label text DEFAULT ''::text NOT NULL,
    exclusive_area_m2 double precision,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    usage_name text DEFAULT ''::text NOT NULL,
    structure_name text DEFAULT ''::text NOT NULL
);


--
-- Name: complex_attachment; Type: TABLE; Schema: catalog; Owner: -
--

CREATE TABLE catalog.complex_attachment (
    complex_id uuid NOT NULL,
    file_asset_id uuid NOT NULL,
    asset_kind text NOT NULL,
    display_order integer DEFAULT 0 NOT NULL,
    CONSTRAINT complex_attachment_asset_kind_check CHECK ((asset_kind = ANY (ARRAY['official_image'::text, 'official_document'::text, 'blueprint'::text, 'notice_attachment'::text, 'digital_twin'::text, 'raw_snapshot'::text, 'other'::text]))),
    CONSTRAINT complex_attachment_display_order_check CHECK ((display_order >= 0))
);


--
-- Name: complex_notice; Type: TABLE; Schema: catalog; Owner: -
--

CREATE TABLE catalog.complex_notice (
    id uuid NOT NULL,
    complex_id uuid NOT NULL,
    notice_type text NOT NULL,
    title text NOT NULL,
    summary text,
    published_at timestamp with time zone,
    source_record_id uuid,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    version bigint DEFAULT 1 NOT NULL,
    CONSTRAINT complex_notice_notice_type_check CHECK ((notice_type = ANY (ARRAY['notice'::text, 'announcement'::text, 'sale'::text, 'regulation'::text, 'maintenance'::text, 'other'::text])))
);


--
-- Name: digital_twin_asset; Type: TABLE; Schema: catalog; Owner: -
--

CREATE TABLE catalog.digital_twin_asset (
    id uuid NOT NULL,
    complex_id uuid NOT NULL,
    parcel_id uuid,
    building_id uuid,
    file_asset_id uuid NOT NULL,
    asset_kind text NOT NULL,
    coordinate_transform jsonb,
    source_record_id uuid,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    version bigint DEFAULT 1 NOT NULL,
    CONSTRAINT digital_twin_asset_asset_kind_check CHECK ((asset_kind = ANY (ARRAY['model_3d'::text, 'tileset_3d'::text, 'point_cloud'::text, 'panorama'::text, 'other'::text])))
);


--
-- Name: file_asset; Type: TABLE; Schema: catalog; Owner: -
--

CREATE TABLE catalog.file_asset (
    id uuid NOT NULL,
    object_key text NOT NULL,
    mime_type text NOT NULL,
    size_bytes bigint NOT NULL,
    checksum_sha256 character(64),
    title text,
    source_record_id uuid,
    visibility text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    version bigint DEFAULT 1 NOT NULL,
    CONSTRAINT file_asset_object_key_check CHECK ((object_key <> ''::text)),
    CONSTRAINT file_asset_object_key_check1 CHECK ((object_key !~~ '/%'::text)),
    CONSTRAINT file_asset_object_key_check2 CHECK ((POSITION(('\'::text) IN (object_key)) = 0)),
    CONSTRAINT file_asset_object_key_check3 CHECK (((object_key !~~ '%/./%'::text) AND (object_key !~~ './%'::text) AND (object_key !~~ '%/.'::text))),
    CONSTRAINT file_asset_object_key_check4 CHECK (((object_key !~~ '%/../%'::text) AND (object_key !~~ '../%'::text) AND (object_key !~~ '%/..'::text))),
    CONSTRAINT file_asset_size_bytes_check CHECK ((size_bytes >= 0)),
    CONSTRAINT file_asset_visibility_check CHECK ((visibility = ANY (ARRAY['public'::text, 'internal'::text, 'private'::text])))
);


--
-- Name: industrial_complex; Type: TABLE; Schema: catalog; Owner: -
--

CREATE TABLE catalog.industrial_complex (
    id uuid NOT NULL,
    name text NOT NULL,
    kind text NOT NULL,
    primary_bjdong_code character(10) NOT NULL,
    area_m2 bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    version bigint DEFAULT 1 NOT NULL,
    official_complex_code text NOT NULL,
    archived_at timestamp with time zone,
    archived_by_staff_id uuid,
    archive_reason text,
    CONSTRAINT industrial_complex_area_m2_check CHECK ((area_m2 >= 0)),
    CONSTRAINT industrial_complex_kind_check CHECK ((kind = ANY (ARRAY['national'::text, 'general'::text, 'agricultural'::text, 'urban_high_tech'::text]))),
    CONSTRAINT industrial_complex_official_code_non_empty CHECK ((length(btrim(official_complex_code)) > 0)),
    CONSTRAINT industrial_complex_primary_bjdong_code_shape CHECK ((primary_bjdong_code ~ '^[0-9]{10}$'::text))
);


--
-- Name: industrial_complex_gold_pointer; Type: TABLE; Schema: catalog; Owner: -
--

CREATE TABLE catalog.industrial_complex_gold_pointer (
    complex_id uuid NOT NULL,
    current_version text NOT NULL,
    previous_version text,
    profile_file_asset_id uuid NOT NULL,
    spatial_locator_file_asset_id uuid,
    source_record_id uuid NOT NULL,
    source_snapshot_id text NOT NULL,
    iceberg_snapshot_id text NOT NULL,
    profile_row_count bigint NOT NULL,
    profile_checksum_sha256 character(64) NOT NULL,
    published_at timestamp with time zone NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    version bigint DEFAULT 1 NOT NULL,
    CONSTRAINT industrial_complex_gold_pointer_check CHECK (((previous_version IS NULL) OR (previous_version <> current_version))),
    CONSTRAINT industrial_complex_gold_pointer_current_version_check CHECK ((current_version <> ''::text)),
    CONSTRAINT industrial_complex_gold_pointer_iceberg_snapshot_id_check CHECK ((iceberg_snapshot_id <> ''::text)),
    CONSTRAINT industrial_complex_gold_pointer_previous_version_check CHECK (((previous_version IS NULL) OR (previous_version <> ''::text))),
    CONSTRAINT industrial_complex_gold_pointer_profile_checksum_sha256_check CHECK ((profile_checksum_sha256 ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT industrial_complex_gold_pointer_profile_row_count_check CHECK ((profile_row_count > 0)),
    CONSTRAINT industrial_complex_gold_pointer_source_snapshot_id_check CHECK ((source_snapshot_id <> ''::text))
);


--
-- Name: industry_group; Type: TABLE; Schema: catalog; Owner: -
--

CREATE TABLE catalog.industry_group (
    id uuid NOT NULL,
    complex_id uuid NOT NULL,
    name text NOT NULL,
    description text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    version bigint DEFAULT 1 NOT NULL
);


--
-- Name: industry_group_member; Type: TABLE; Schema: catalog; Owner: -
--

CREATE TABLE catalog.industry_group_member (
    industry_group_id uuid NOT NULL,
    industry_code text NOT NULL,
    industry_code_system text NOT NULL,
    CONSTRAINT industry_group_member_industry_code_system_check CHECK ((industry_code_system = 'ksic'::text))
);


--
-- Name: ingestion_run; Type: TABLE; Schema: catalog; Owner: -
--

CREATE TABLE catalog.ingestion_run (
    id uuid NOT NULL,
    source_catalog_id uuid NOT NULL,
    trigger text NOT NULL,
    status text NOT NULL,
    request_params jsonb DEFAULT '{}'::jsonb NOT NULL,
    started_at timestamp with time zone DEFAULT now() NOT NULL,
    finished_at timestamp with time zone,
    logical_records_seen bigint DEFAULT 0 NOT NULL,
    objects_written bigint DEFAULT 0 NOT NULL,
    error_message text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    version bigint DEFAULT 1 NOT NULL,
    CONSTRAINT ingestion_run_check CHECK (((finished_at IS NULL) OR (finished_at >= started_at))),
    CONSTRAINT ingestion_run_error_message_check CHECK (((error_message IS NULL) OR (error_message <> ''::text))),
    CONSTRAINT ingestion_run_records_seen_check CHECK ((logical_records_seen >= 0)),
    CONSTRAINT ingestion_run_records_written_check CHECK ((objects_written >= 0)),
    CONSTRAINT ingestion_run_request_params_check CHECK ((jsonb_typeof(request_params) = 'object'::text)),
    CONSTRAINT ingestion_run_status_check CHECK ((status = ANY (ARRAY['planned'::text, 'running'::text, 'succeeded'::text, 'failed'::text, 'cancelled'::text]))),
    CONSTRAINT ingestion_run_trigger_check CHECK ((trigger = ANY (ARRAY['manual'::text, 'scheduled'::text, 'backfill'::text, 'replay'::text, 'test'::text]))),
    CONSTRAINT ingestion_run_version_check CHECK ((version >= 1))
);


--
-- Name: lakehouse_access_policy; Type: TABLE; Schema: catalog; Owner: -
--

CREATE TABLE catalog.lakehouse_access_policy (
    data_asset_id uuid NOT NULL,
    principal_service text NOT NULL,
    action text NOT NULL,
    decision text NOT NULL,
    reason text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT lakehouse_access_policy_action_check CHECK ((action = ANY (ARRAY['read'::text, 'write'::text, 'promote'::text, 'rollback'::text]))),
    CONSTRAINT lakehouse_access_policy_decision_check CHECK ((decision = ANY (ARRAY['allow'::text, 'deny'::text]))),
    CONSTRAINT lakehouse_access_policy_principal_service_check CHECK ((principal_service = ANY (ARRAY['foundation-platform'::text, 'gongzzang'::text, 'dawneer'::text]))),
    CONSTRAINT lakehouse_access_policy_reason_check CHECK (((reason IS NULL) OR (reason <> ''::text)))
);


--
-- Name: lakehouse_batch_run; Type: TABLE; Schema: catalog; Owner: -
--

CREATE TABLE catalog.lakehouse_batch_run (
    id uuid NOT NULL,
    schema_version text NOT NULL,
    job_name text NOT NULL,
    contract text NOT NULL,
    created_at timestamp with time zone NOT NULL,
    input_kind text NOT NULL,
    input_path text NOT NULL,
    target_kind text NOT NULL,
    target_path text,
    target_catalog text,
    target_namespace text,
    target_table text,
    target_qualified_table text,
    write_mode text NOT NULL,
    write_disposition text NOT NULL,
    row_count bigint NOT NULL,
    persisted_row_count bigint,
    source_snapshot_count bigint NOT NULL,
    source_snapshot_ids text[] NOT NULL,
    source_snapshot_truncated boolean NOT NULL,
    summary_json jsonb NOT NULL,
    recorded_by_staff_id uuid NOT NULL,
    request_id text,
    recorded_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT lakehouse_batch_run_check CHECK (((cardinality(source_snapshot_ids))::bigint = source_snapshot_count)),
    CONSTRAINT lakehouse_batch_run_check1 CHECK ((((write_disposition = 'validate_only'::text) AND (persisted_row_count IS NULL)) OR ((write_disposition <> 'validate_only'::text) AND (persisted_row_count = row_count)))),
    CONSTRAINT lakehouse_batch_run_check2 CHECK ((((target_kind = 'parquet'::text) AND (write_mode = 'parquet'::text) AND (target_path IS NOT NULL) AND (target_path <> ''::text) AND (target_catalog IS NULL) AND (target_namespace IS NULL) AND (target_table IS NULL) AND (target_qualified_table IS NULL)) OR ((target_kind = 'iceberg'::text) AND (write_mode = 'iceberg'::text) AND (target_path IS NULL) AND (target_catalog IS NOT NULL) AND (target_catalog <> ''::text) AND (target_namespace IS NOT NULL) AND (target_namespace <> ''::text) AND (target_table IS NOT NULL) AND (target_table <> ''::text) AND (target_qualified_table IS NOT NULL) AND (target_qualified_table <> ''::text)))),
    CONSTRAINT lakehouse_batch_run_contract_check CHECK ((contract <> ''::text)),
    CONSTRAINT lakehouse_batch_run_input_kind_check CHECK ((input_kind <> ''::text)),
    CONSTRAINT lakehouse_batch_run_input_path_check CHECK ((input_path <> ''::text)),
    CONSTRAINT lakehouse_batch_run_job_name_check CHECK ((job_name <> ''::text)),
    CONSTRAINT lakehouse_batch_run_persisted_row_count_check CHECK (((persisted_row_count IS NULL) OR (persisted_row_count >= 0))),
    CONSTRAINT lakehouse_batch_run_request_id_check CHECK (((request_id IS NULL) OR (btrim(request_id) <> ''::text))),
    CONSTRAINT lakehouse_batch_run_row_count_check CHECK ((row_count >= 0)),
    CONSTRAINT lakehouse_batch_run_schema_version_check CHECK ((schema_version = 'foundation-platform.spark_run_summary.v1'::text)),
    CONSTRAINT lakehouse_batch_run_source_snapshot_count_check CHECK ((source_snapshot_count >= 0)),
    CONSTRAINT lakehouse_batch_run_summary_json_check CHECK ((jsonb_typeof(summary_json) = 'object'::text)),
    CONSTRAINT lakehouse_batch_run_target_kind_check CHECK ((target_kind = ANY (ARRAY['parquet'::text, 'iceberg'::text]))),
    CONSTRAINT lakehouse_batch_run_write_disposition_check CHECK ((write_disposition = ANY (ARRAY['validate_only'::text, 'parquet_overwrite'::text, 'iceberg_append'::text, 'iceberg_overwrite'::text]))),
    CONSTRAINT lakehouse_batch_run_write_mode_check CHECK ((write_mode = ANY (ARRAY['parquet'::text, 'iceberg'::text])))
);


--
-- Name: lakehouse_data_asset; Type: TABLE; Schema: catalog; Owner: -
--

CREATE TABLE catalog.lakehouse_data_asset (
    id uuid NOT NULL,
    qualified_name text NOT NULL,
    owner_service text NOT NULL,
    layer text NOT NULL,
    asset_kind text NOT NULL,
    schema_contract_ref text,
    status text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    version bigint DEFAULT 1 NOT NULL,
    CONSTRAINT lakehouse_data_asset_asset_kind_check CHECK ((asset_kind = ANY (ARRAY['raw_object_set'::text, 'iceberg_table'::text, 'pbf_tile_set'::text, 'manifest'::text, 'media_set'::text]))),
    CONSTRAINT lakehouse_data_asset_check CHECK ((((owner_service = 'foundation-platform'::text) AND (qualified_name ~~ 'foundation_platform.%'::text)) OR ((owner_service = 'gongzzang'::text) AND (qualified_name ~~ 'gongzzang.%'::text)) OR ((owner_service = 'dawneer'::text) AND (qualified_name ~~ 'dawneer.%'::text)))),
    CONSTRAINT lakehouse_data_asset_check1 CHECK ((POSITION(((('.'::text || layer) || '.'::text)) IN (qualified_name)) > 0)),
    CONSTRAINT lakehouse_data_asset_layer_check CHECK ((layer = ANY (ARRAY['bronze'::text, 'silver'::text, 'gold'::text]))),
    CONSTRAINT lakehouse_data_asset_owner_service_check CHECK ((owner_service = ANY (ARRAY['foundation-platform'::text, 'gongzzang'::text, 'dawneer'::text]))),
    CONSTRAINT lakehouse_data_asset_qualified_name_check CHECK ((qualified_name ~ '^[a-z0-9_]+\.(bronze|silver|gold)\.[a-z0-9_]+$'::text)),
    CONSTRAINT lakehouse_data_asset_schema_contract_ref_check CHECK (((schema_contract_ref IS NULL) OR (schema_contract_ref <> ''::text))),
    CONSTRAINT lakehouse_data_asset_status_check CHECK ((status = ANY (ARRAY['active'::text, 'deprecated'::text, 'quarantined'::text]))),
    CONSTRAINT lakehouse_data_asset_version_check CHECK ((version >= 1))
);


--
-- Name: lakehouse_dataset_version; Type: TABLE; Schema: catalog; Owner: -
--

CREATE TABLE catalog.lakehouse_dataset_version (
    id uuid NOT NULL,
    data_asset_id uuid NOT NULL,
    version text NOT NULL,
    state text NOT NULL,
    schema_version text NOT NULL,
    artifact_format text NOT NULL,
    created_by_ingestion_run_id uuid,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT lakehouse_dataset_version_artifact_format_check CHECK ((artifact_format = ANY (ARRAY['json'::text, 'jsonl'::text, 'parquet'::text, 'geoparquet'::text, 'iceberg'::text, 'pbf'::text, 'zip'::text, 'object_set'::text]))),
    CONSTRAINT lakehouse_dataset_version_schema_version_check CHECK ((schema_version <> ''::text)),
    CONSTRAINT lakehouse_dataset_version_state_check CHECK ((state = ANY (ARRAY['candidate'::text, 'active'::text, 'previous'::text, 'retired'::text, 'quarantined'::text]))),
    CONSTRAINT lakehouse_dataset_version_version_check CHECK ((version <> ''::text))
);


--
-- Name: lakehouse_lineage_edge; Type: TABLE; Schema: catalog; Owner: -
--

CREATE TABLE catalog.lakehouse_lineage_edge (
    from_version_id uuid NOT NULL,
    to_version_id uuid NOT NULL,
    transform_name text NOT NULL,
    evidence_ref text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT lakehouse_lineage_edge_check CHECK ((from_version_id <> to_version_id)),
    CONSTRAINT lakehouse_lineage_edge_evidence_ref_check CHECK (((evidence_ref IS NULL) OR (evidence_ref <> ''::text))),
    CONSTRAINT lakehouse_lineage_edge_transform_name_check CHECK ((transform_name <> ''::text))
);


--
-- Name: lakehouse_object_artifact; Type: TABLE; Schema: catalog; Owner: -
--

CREATE TABLE catalog.lakehouse_object_artifact (
    id uuid NOT NULL,
    dataset_version_id uuid NOT NULL,
    namespace_id uuid NOT NULL,
    object_key text NOT NULL,
    content_type text NOT NULL,
    checksum_sha256 text NOT NULL,
    size_bytes bigint NOT NULL,
    logical_record_count bigint,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT lakehouse_object_artifact_checksum_sha256_check CHECK ((checksum_sha256 ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT lakehouse_object_artifact_content_type_check CHECK ((content_type <> ''::text)),
    CONSTRAINT lakehouse_object_artifact_logical_record_count_check CHECK (((logical_record_count IS NULL) OR (logical_record_count >= 0))),
    CONSTRAINT lakehouse_object_artifact_object_key_check CHECK ((object_key <> ''::text)),
    CONSTRAINT lakehouse_object_artifact_object_key_check1 CHECK ((object_key !~~ '/%'::text)),
    CONSTRAINT lakehouse_object_artifact_object_key_check2 CHECK ((POSITION(('\'::text) IN (object_key)) = 0)),
    CONSTRAINT lakehouse_object_artifact_object_key_check3 CHECK ((object_key !~~ '%//%'::text)),
    CONSTRAINT lakehouse_object_artifact_object_key_check4 CHECK (((object_key !~~ '%/./%'::text) AND (object_key !~~ './%'::text) AND (object_key !~~ '%/.'::text))),
    CONSTRAINT lakehouse_object_artifact_object_key_check5 CHECK (((object_key !~~ '%/../%'::text) AND (object_key !~~ '../%'::text) AND (object_key !~~ '%/..'::text))),
    CONSTRAINT lakehouse_object_artifact_size_bytes_check CHECK ((size_bytes >= 0))
);


--
-- Name: lakehouse_quality_check; Type: TABLE; Schema: catalog; Owner: -
--

CREATE TABLE catalog.lakehouse_quality_check (
    dataset_version_id uuid NOT NULL,
    check_name text NOT NULL,
    status text NOT NULL,
    measured_value jsonb,
    evidence_ref text,
    checked_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT lakehouse_quality_check_check_name_check CHECK ((check_name <> ''::text)),
    CONSTRAINT lakehouse_quality_check_evidence_ref_check CHECK (((evidence_ref IS NULL) OR (evidence_ref <> ''::text))),
    CONSTRAINT lakehouse_quality_check_measured_value_check CHECK (((measured_value IS NULL) OR (jsonb_typeof(measured_value) = ANY (ARRAY['object'::text, 'array'::text, 'string'::text, 'number'::text, 'boolean'::text])))),
    CONSTRAINT lakehouse_quality_check_status_check CHECK ((status = ANY (ARRAY['passed'::text, 'failed'::text, 'warning'::text])))
);


--
-- Name: lakehouse_storage_namespace; Type: TABLE; Schema: catalog; Owner: -
--

CREATE TABLE catalog.lakehouse_storage_namespace (
    id uuid NOT NULL,
    provider text NOT NULL,
    environment text NOT NULL,
    owner_service text NOT NULL,
    bucket_name text NOT NULL,
    root_prefix text,
    catalog_provider text NOT NULL,
    status text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    version bigint DEFAULT 1 NOT NULL,
    CONSTRAINT lakehouse_storage_namespace_bucket_name_check CHECK ((bucket_name ~ '^[a-z0-9][a-z0-9-]{1,61}[a-z0-9]$'::text)),
    CONSTRAINT lakehouse_storage_namespace_bucket_name_check1 CHECK ((bucket_name !~~ '%--%'::text)),
    CONSTRAINT lakehouse_storage_namespace_catalog_provider_check CHECK ((catalog_provider = ANY (ARRAY['none'::text, 'r2_data_catalog'::text, 'iceberg_rest'::text]))),
    CONSTRAINT lakehouse_storage_namespace_check CHECK (((environment <> 'production'::text) OR (provider <> 'r2'::text) OR (((owner_service = 'foundation-platform'::text) AND (bucket_name = 'foundation-platform-lakehouse-prod'::text)) OR ((owner_service = 'gongzzang'::text) AND (bucket_name = 'gongzzang-lakehouse-prod'::text)) OR ((owner_service = 'dawneer'::text) AND (bucket_name = 'dawneer-lakehouse-prod'::text))))),
    CONSTRAINT lakehouse_storage_namespace_environment_check CHECK ((environment = ANY (ARRAY['local'::text, 'staging'::text, 'production'::text]))),
    CONSTRAINT lakehouse_storage_namespace_owner_service_check CHECK ((owner_service = ANY (ARRAY['foundation-platform'::text, 'gongzzang'::text, 'dawneer'::text]))),
    CONSTRAINT lakehouse_storage_namespace_provider_check CHECK ((provider = 'r2'::text)),
    CONSTRAINT lakehouse_storage_namespace_root_prefix_check CHECK (((root_prefix IS NULL) OR (root_prefix <> ''::text))),
    CONSTRAINT lakehouse_storage_namespace_root_prefix_check1 CHECK (((root_prefix IS NULL) OR (root_prefix !~~ '/%'::text))),
    CONSTRAINT lakehouse_storage_namespace_root_prefix_check2 CHECK (((root_prefix IS NULL) OR (POSITION(('\'::text) IN (root_prefix)) = 0))),
    CONSTRAINT lakehouse_storage_namespace_root_prefix_check3 CHECK (((root_prefix IS NULL) OR (root_prefix !~~ '%//%'::text))),
    CONSTRAINT lakehouse_storage_namespace_root_prefix_check4 CHECK (((root_prefix IS NULL) OR (root_prefix !~~ '%/./%'::text))),
    CONSTRAINT lakehouse_storage_namespace_root_prefix_check5 CHECK (((root_prefix IS NULL) OR (root_prefix !~~ './%'::text))),
    CONSTRAINT lakehouse_storage_namespace_root_prefix_check6 CHECK (((root_prefix IS NULL) OR (root_prefix !~~ '%/.'::text))),
    CONSTRAINT lakehouse_storage_namespace_root_prefix_check7 CHECK (((root_prefix IS NULL) OR (root_prefix !~~ '%/../%'::text))),
    CONSTRAINT lakehouse_storage_namespace_root_prefix_check8 CHECK (((root_prefix IS NULL) OR (root_prefix !~~ '../%'::text))),
    CONSTRAINT lakehouse_storage_namespace_root_prefix_check9 CHECK (((root_prefix IS NULL) OR (root_prefix !~~ '%/..'::text))),
    CONSTRAINT lakehouse_storage_namespace_status_check CHECK ((status = ANY (ARRAY['active'::text, 'deprecated'::text, 'quarantined'::text]))),
    CONSTRAINT lakehouse_storage_namespace_version_check CHECK ((version >= 1))
);


--
-- Name: manufacturer; Type: TABLE; Schema: catalog; Owner: -
--

CREATE TABLE catalog.manufacturer (
    id uuid NOT NULL,
    primary_parcel_id uuid NOT NULL,
    name text NOT NULL,
    ksic_code text NOT NULL,
    business_registration_number character(11) NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: normalization_application; Type: TABLE; Schema: catalog; Owner: -
--

CREATE TABLE catalog.normalization_application (
    id uuid NOT NULL,
    proposal_id uuid NOT NULL,
    command_type text NOT NULL,
    target_kind text NOT NULL,
    target_id uuid,
    expected_version bigint,
    before_snapshot jsonb,
    after_snapshot jsonb,
    applied_by_principal_id uuid NOT NULL,
    applied_at timestamp with time zone DEFAULT now() NOT NULL,
    rollback_of uuid,
    outbox_event_id uuid,
    CONSTRAINT normalization_application_after_object_check CHECK (((after_snapshot IS NULL) OR (jsonb_typeof(after_snapshot) = 'object'::text))),
    CONSTRAINT normalization_application_before_object_check CHECK (((before_snapshot IS NULL) OR (jsonb_typeof(before_snapshot) = 'object'::text))),
    CONSTRAINT normalization_application_command_type_check CHECK ((btrim(command_type) <> ''::text)),
    CONSTRAINT normalization_application_no_self_rollback_check CHECK (((rollback_of IS NULL) OR (rollback_of <> id))),
    CONSTRAINT normalization_application_target_kind_check CHECK ((target_kind = ANY (ARRAY['industrial_complex'::text, 'building_register_floor'::text, 'building_register_unit'::text]))),
    CONSTRAINT normalization_application_target_requires_id_check CHECK (((target_kind <> 'industrial_complex'::text) OR (target_id IS NOT NULL)))
);


--
-- Name: normalization_proposal; Type: TABLE; Schema: catalog; Owner: -
--

CREATE TABLE catalog.normalization_proposal (
    id uuid NOT NULL,
    proposal_key text NOT NULL,
    submitted_by_service text NOT NULL,
    source_system text NOT NULL,
    raw_record_id text NOT NULL,
    raw_object_key text,
    raw_checksum_sha256 character(64),
    bronze_object_id uuid,
    target_kind text NOT NULL,
    target_identity jsonb NOT NULL,
    target_schema_version text NOT NULL,
    proposal_schema_version text NOT NULL,
    proposed_record jsonb NOT NULL,
    proposed_record_sha256 character(64) NOT NULL,
    proposed_patch jsonb,
    confidence double precision NOT NULL,
    evidence jsonb NOT NULL,
    validation jsonb NOT NULL,
    model_profile_id text,
    model_id text,
    prompt_id text,
    prompt_version text,
    policy_id text NOT NULL,
    policy_version text NOT NULL,
    trace_id text NOT NULL,
    status text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT normalization_proposal_confidence_check CHECK (((confidence >= (0)::double precision) AND (confidence <= (1)::double precision))),
    CONSTRAINT normalization_proposal_evidence_object_check CHECK ((jsonb_typeof(evidence) = 'object'::text)),
    CONSTRAINT normalization_proposal_policy_id_check CHECK ((btrim(policy_id) <> ''::text)),
    CONSTRAINT normalization_proposal_policy_version_check CHECK ((btrim(policy_version) <> ''::text)),
    CONSTRAINT normalization_proposal_proposal_schema_version_check CHECK ((btrim(proposal_schema_version) <> ''::text)),
    CONSTRAINT normalization_proposal_proposed_patch_object_check CHECK (((proposed_patch IS NULL) OR (jsonb_typeof(proposed_patch) = 'object'::text))),
    CONSTRAINT normalization_proposal_proposed_record_object_check CHECK ((jsonb_typeof(proposed_record) = 'object'::text)),
    CONSTRAINT normalization_proposal_proposed_record_sha256_check CHECK ((proposed_record_sha256 ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT normalization_proposal_raw_checksum_check CHECK (((raw_checksum_sha256 IS NULL) OR (raw_checksum_sha256 ~ '^[0-9a-f]{64}$'::text))),
    CONSTRAINT normalization_proposal_raw_object_key_check CHECK (((raw_object_key IS NULL) OR (btrim(raw_object_key) <> ''::text))),
    CONSTRAINT normalization_proposal_raw_record_id_check CHECK ((btrim(raw_record_id) <> ''::text)),
    CONSTRAINT normalization_proposal_source_system_check CHECK ((btrim(source_system) <> ''::text)),
    CONSTRAINT normalization_proposal_status_check CHECK ((status = ANY (ARRAY['pending_review'::text, 'approved'::text, 'rejected'::text, 'superseded'::text, 'applied'::text, 'apply_failed'::text, 'rolled_back'::text]))),
    CONSTRAINT normalization_proposal_submitted_by_service_check CHECK ((btrim(submitted_by_service) <> ''::text)),
    CONSTRAINT normalization_proposal_target_identity_object_check CHECK ((jsonb_typeof(target_identity) = 'object'::text)),
    CONSTRAINT normalization_proposal_target_kind_check CHECK ((target_kind = ANY (ARRAY['industrial_complex'::text, 'building_register_floor'::text, 'building_register_unit'::text]))),
    CONSTRAINT normalization_proposal_target_schema_version_check CHECK ((btrim(target_schema_version) <> ''::text)),
    CONSTRAINT normalization_proposal_trace_id_check CHECK ((btrim(trace_id) <> ''::text)),
    CONSTRAINT normalization_proposal_validation_object_check CHECK ((jsonb_typeof(validation) = 'object'::text))
);


--
-- Name: normalization_proposal_review; Type: TABLE; Schema: catalog; Owner: -
--

CREATE TABLE catalog.normalization_proposal_review (
    id uuid NOT NULL,
    proposal_id uuid NOT NULL,
    reviewer_principal_id uuid NOT NULL,
    decision text NOT NULL,
    reason text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT normalization_proposal_review_decision_check CHECK ((decision = ANY (ARRAY['approved'::text, 'rejected'::text, 'needs_changes'::text]))),
    CONSTRAINT normalization_proposal_review_reason_check CHECK ((btrim(reason) <> ''::text))
);


--
-- Name: normalization_proposal_submission_audit; Type: TABLE; Schema: catalog; Owner: -
--

CREATE TABLE catalog.normalization_proposal_submission_audit (
    proposal_id uuid NOT NULL,
    submitted_by_principal_id uuid NOT NULL,
    submitted_by_service text NOT NULL,
    trace_id text NOT NULL,
    submitted_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT normalization_proposal_submission_au_submitted_by_service_check CHECK ((btrim(submitted_by_service) <> ''::text)),
    CONSTRAINT normalization_proposal_submission_audit_trace_id_check CHECK ((btrim(trace_id) <> ''::text))
);


--
-- Name: notice_attachment; Type: TABLE; Schema: catalog; Owner: -
--

CREATE TABLE catalog.notice_attachment (
    notice_id uuid NOT NULL,
    file_asset_id uuid NOT NULL,
    display_order integer DEFAULT 0 NOT NULL,
    CONSTRAINT notice_attachment_display_order_check CHECK ((display_order >= 0))
);


--
-- Name: outbox_event; Type: TABLE; Schema: catalog; Owner: -
--

CREATE TABLE catalog.outbox_event (
    event_id uuid NOT NULL,
    type text NOT NULL,
    payload jsonb NOT NULL,
    occurred_at timestamp with time zone DEFAULT now() NOT NULL,
    published_at timestamp with time zone,
    retry_count integer DEFAULT 0 NOT NULL,
    lease_owner uuid,
    lease_until timestamp with time zone
);


--
-- Name: outbox_quarantine; Type: TABLE; Schema: catalog; Owner: -
--

CREATE TABLE catalog.outbox_quarantine (
    id uuid NOT NULL,
    source_outbox_table text NOT NULL,
    event_id uuid NOT NULL,
    consumer_key text NOT NULL,
    event_type text NOT NULL,
    payload jsonb NOT NULL,
    failure_stage text NOT NULL,
    failure_code text NOT NULL,
    failure_message text NOT NULL,
    attempt_count integer NOT NULL,
    first_failed_at timestamp with time zone NOT NULL,
    last_failed_at timestamp with time zone NOT NULL,
    next_retry_at timestamp with time zone,
    resolved_at timestamp with time zone,
    resolution_kind text,
    resolution_note text,
    consumer_endpoint_sha256 character(64),
    lineage jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    version bigint DEFAULT 1 NOT NULL,
    CONSTRAINT outbox_quarantine_attempt_count_check CHECK ((attempt_count >= 1)),
    CONSTRAINT outbox_quarantine_check CHECK ((last_failed_at >= first_failed_at)),
    CONSTRAINT outbox_quarantine_check1 CHECK (((next_retry_at IS NULL) OR (next_retry_at >= last_failed_at))),
    CONSTRAINT outbox_quarantine_check2 CHECK ((((resolved_at IS NULL) AND (resolution_kind IS NULL)) OR ((resolved_at IS NOT NULL) AND (resolution_kind IS NOT NULL) AND (resolved_at >= last_failed_at)))),
    CONSTRAINT outbox_quarantine_consumer_endpoint_sha256_check CHECK (((consumer_endpoint_sha256 IS NULL) OR (consumer_endpoint_sha256 ~ '^[0-9a-f]{64}$'::text))),
    CONSTRAINT outbox_quarantine_consumer_key_check CHECK ((consumer_key ~ '^[a-z0-9][a-z0-9._:-]{1,127}$'::text)),
    CONSTRAINT outbox_quarantine_event_type_check CHECK ((event_type <> ''::text)),
    CONSTRAINT outbox_quarantine_failure_code_check CHECK ((failure_code ~ '^[a-z0-9][a-z0-9._:-]{1,127}$'::text)),
    CONSTRAINT outbox_quarantine_failure_message_check CHECK ((btrim(failure_message) <> ''::text)),
    CONSTRAINT outbox_quarantine_failure_stage_check CHECK ((failure_stage = ANY (ARRAY['serialize'::text, 'deliver'::text, 'consumer_ack'::text, 'retry_exhausted'::text, 'unknown'::text]))),
    CONSTRAINT outbox_quarantine_lineage_check CHECK ((jsonb_typeof(lineage) = 'object'::text)),
    CONSTRAINT outbox_quarantine_payload_check CHECK ((jsonb_typeof(payload) = 'object'::text)),
    CONSTRAINT outbox_quarantine_resolution_kind_check CHECK ((resolution_kind = ANY (ARRAY['replayed'::text, 'discarded'::text, 'superseded'::text]))),
    CONSTRAINT outbox_quarantine_resolution_note_check CHECK (((resolution_note IS NULL) OR (btrim(resolution_note) <> ''::text))),
    CONSTRAINT outbox_quarantine_version_check CHECK ((version >= 1))
);


--
-- Name: parcel; Type: TABLE; Schema: catalog; Owner: -
--

CREATE TABLE catalog.parcel (
    id uuid NOT NULL,
    complex_id uuid NOT NULL,
    pnu character(19) NOT NULL,
    kind text NOT NULL,
    area_m2 bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    version bigint DEFAULT 1 NOT NULL,
    CONSTRAINT parcel_area_m2_check CHECK ((area_m2 >= 0)),
    CONSTRAINT parcel_kind_check CHECK ((kind = ANY (ARRAY['factory'::text, 'support'::text, 'public'::text, 'river'::text, 'other'::text])))
);


--
-- Name: parcel_industry_assignment; Type: TABLE; Schema: catalog; Owner: -
--

CREATE TABLE catalog.parcel_industry_assignment (
    id uuid NOT NULL,
    parcel_id uuid NOT NULL,
    industry_group_id uuid NOT NULL,
    assignment_kind text NOT NULL,
    source_record_id uuid,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    version bigint DEFAULT 1 NOT NULL,
    CONSTRAINT parcel_industry_assignment_assignment_kind_check CHECK ((assignment_kind = ANY (ARRAY['allowed'::text, 'recommended'::text, 'restricted'::text])))
);


--
-- Name: parcel_marker_anchor; Type: TABLE; Schema: catalog; Owner: -
--

CREATE UNLOGGED TABLE catalog.parcel_marker_anchor (
    id uuid NOT NULL,
    pnu character(19) NOT NULL,
    parcel_id uuid,
    generation_run_id uuid NOT NULL,
    source_geometry_version text NOT NULL,
    source_table text NOT NULL,
    source_record_id uuid,
    source_file_asset_id uuid,
    source_object_key text NOT NULL,
    source_row_id text,
    anchor_point public.geometry(Point,4326) NOT NULL,
    anchor_lng double precision GENERATED ALWAYS AS (public.st_x(anchor_point)) STORED,
    anchor_lat double precision GENERATED ALWAYS AS (public.st_y(anchor_point)) STORED,
    algorithm text NOT NULL,
    algorithm_version text NOT NULL,
    source_geometry_checksum_sha256 character(64) NOT NULL,
    computed_at_utc timestamp with time zone NOT NULL,
    activated_at_utc timestamp with time zone,
    superseded_at_utc timestamp with time zone,
    is_active boolean DEFAULT false NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    version bigint DEFAULT 1 NOT NULL,
    CONSTRAINT parcel_marker_anchor_algorithm_check CHECK ((algorithm = ANY (ARRAY['official_label_point'::text, 'polylabel'::text]))),
    CONSTRAINT parcel_marker_anchor_algorithm_version_check CHECK ((algorithm_version ~ '^[a-z0-9][a-z0-9._:-]{1,127}$'::text)),
    CONSTRAINT parcel_marker_anchor_anchor_lat_check CHECK (((anchor_lat >= ('-90'::integer)::double precision) AND (anchor_lat <= (90)::double precision))),
    CONSTRAINT parcel_marker_anchor_anchor_lng_check CHECK (((anchor_lng >= ('-180'::integer)::double precision) AND (anchor_lng <= (180)::double precision))),
    CONSTRAINT parcel_marker_anchor_anchor_point_check CHECK ((public.st_srid(anchor_point) = 4326)),
    CONSTRAINT parcel_marker_anchor_anchor_point_check1 CHECK (public.st_isvalid(anchor_point)),
    CONSTRAINT parcel_marker_anchor_check CHECK (((is_active = false) OR ((activated_at_utc IS NOT NULL) AND (superseded_at_utc IS NULL)))),
    CONSTRAINT parcel_marker_anchor_check1 CHECK (((superseded_at_utc IS NULL) OR (activated_at_utc IS NULL) OR (superseded_at_utc >= activated_at_utc))),
    CONSTRAINT parcel_marker_anchor_pnu_check CHECK ((pnu ~ '^[0-9]{19}$'::text)),
    CONSTRAINT parcel_marker_anchor_source_geometry_checksum_sha256_check CHECK ((source_geometry_checksum_sha256 ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT parcel_marker_anchor_source_geometry_version_check CHECK ((source_geometry_version ~ '^iceberg:[A-Za-z0-9][A-Za-z0-9._:-]{2,127}$'::text)),
    CONSTRAINT parcel_marker_anchor_source_object_key_check CHECK ((source_object_key <> ''::text)),
    CONSTRAINT parcel_marker_anchor_source_object_key_check1 CHECK ((source_object_key !~~ '/%'::text)),
    CONSTRAINT parcel_marker_anchor_source_object_key_check2 CHECK ((POSITION(('\'::text) IN (source_object_key)) = 0)),
    CONSTRAINT parcel_marker_anchor_source_object_key_check3 CHECK ((source_object_key !~~ '%//%'::text)),
    CONSTRAINT parcel_marker_anchor_source_object_key_check4 CHECK (((source_object_key !~~ '%/./%'::text) AND (source_object_key !~~ './%'::text) AND (source_object_key !~~ '%/.'::text))),
    CONSTRAINT parcel_marker_anchor_source_object_key_check5 CHECK (((source_object_key !~~ '%/../%'::text) AND (source_object_key !~~ '../%'::text) AND (source_object_key !~~ '%/..'::text))),
    CONSTRAINT parcel_marker_anchor_source_row_id_check CHECK (((source_row_id IS NULL) OR (btrim(source_row_id) <> ''::text))),
    CONSTRAINT parcel_marker_anchor_source_table_check CHECK ((source_table = 'silver.parcel_boundaries'::text)),
    CONSTRAINT parcel_marker_anchor_version_check CHECK ((version >= 1))
);


--
-- Name: parcel_marker_anchor_generation_run; Type: TABLE; Schema: catalog; Owner: -
--

CREATE TABLE catalog.parcel_marker_anchor_generation_run (
    id uuid NOT NULL,
    source_snapshot_id text NOT NULL,
    source_table text NOT NULL,
    source_record_id uuid,
    source_file_asset_id uuid,
    algorithm text NOT NULL,
    algorithm_version text NOT NULL,
    srid integer DEFAULT 4326 NOT NULL,
    status text NOT NULL,
    loaded_row_count bigint DEFAULT 0 NOT NULL,
    rejected_row_count bigint DEFAULT 0 NOT NULL,
    quality_report jsonb DEFAULT '{}'::jsonb NOT NULL,
    started_at timestamp with time zone NOT NULL,
    finished_at timestamp with time zone,
    error_message text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    version bigint DEFAULT 1 NOT NULL,
    CONSTRAINT parcel_marker_anchor_generation_run_algorithm_check CHECK ((algorithm = ANY (ARRAY['official_label_point'::text, 'polylabel'::text]))),
    CONSTRAINT parcel_marker_anchor_generation_run_algorithm_version_check CHECK ((algorithm_version ~ '^[a-z0-9][a-z0-9._:-]{1,127}$'::text)),
    CONSTRAINT parcel_marker_anchor_generation_run_check CHECK (((finished_at IS NULL) OR (finished_at >= started_at))),
    CONSTRAINT parcel_marker_anchor_generation_run_check1 CHECK (((status <> 'succeeded'::text) OR ((finished_at IS NOT NULL) AND (loaded_row_count > 0)))),
    CONSTRAINT parcel_marker_anchor_generation_run_check2 CHECK (((status <> 'failed'::text) OR (error_message IS NOT NULL))),
    CONSTRAINT parcel_marker_anchor_generation_run_error_message_check CHECK (((error_message IS NULL) OR (btrim(error_message) <> ''::text))),
    CONSTRAINT parcel_marker_anchor_generation_run_loaded_row_count_check CHECK ((loaded_row_count >= 0)),
    CONSTRAINT parcel_marker_anchor_generation_run_quality_report_check CHECK ((jsonb_typeof(quality_report) = 'object'::text)),
    CONSTRAINT parcel_marker_anchor_generation_run_rejected_row_count_check CHECK ((rejected_row_count >= 0)),
    CONSTRAINT parcel_marker_anchor_generation_run_source_snapshot_id_check CHECK ((source_snapshot_id ~ '^iceberg:[A-Za-z0-9][A-Za-z0-9._:-]{2,127}$'::text)),
    CONSTRAINT parcel_marker_anchor_generation_run_source_table_check CHECK ((source_table = 'silver.parcel_boundaries'::text)),
    CONSTRAINT parcel_marker_anchor_generation_run_srid_check CHECK ((srid = 4326)),
    CONSTRAINT parcel_marker_anchor_generation_run_status_check CHECK ((status = ANY (ARRAY['planned'::text, 'running'::text, 'succeeded'::text, 'failed'::text, 'cancelled'::text]))),
    CONSTRAINT parcel_marker_anchor_generation_run_version_check CHECK ((version >= 1))
);


--
-- Name: schema_profile; Type: TABLE; Schema: catalog; Owner: -
--

CREATE TABLE catalog.schema_profile (
    id uuid NOT NULL,
    source_catalog_id uuid NOT NULL,
    ingestion_run_id uuid NOT NULL,
    field_path text NOT NULL,
    observed_type text NOT NULL,
    nonnull_count bigint NOT NULL,
    null_count bigint NOT NULL,
    sample_values jsonb DEFAULT '[]'::jsonb NOT NULL,
    candidate_key_score double precision DEFAULT 0 NOT NULL,
    profiled_at timestamp with time zone DEFAULT now() NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    version bigint DEFAULT 1 NOT NULL,
    CONSTRAINT schema_profile_candidate_key_score_check CHECK (((candidate_key_score >= (0)::double precision) AND (candidate_key_score <= (1)::double precision))),
    CONSTRAINT schema_profile_field_path_check CHECK ((field_path <> ''::text)),
    CONSTRAINT schema_profile_nonnull_count_check CHECK ((nonnull_count >= 0)),
    CONSTRAINT schema_profile_null_count_check CHECK ((null_count >= 0)),
    CONSTRAINT schema_profile_observed_type_check CHECK ((observed_type = ANY (ARRAY['null'::text, 'boolean'::text, 'number'::text, 'string'::text, 'object'::text, 'array'::text, 'mixed'::text]))),
    CONSTRAINT schema_profile_sample_values_check CHECK ((jsonb_typeof(sample_values) = 'array'::text)),
    CONSTRAINT schema_profile_version_check CHECK ((version >= 1))
);


--
-- Name: source_catalog; Type: TABLE; Schema: catalog; Owner: -
--

CREATE TABLE catalog.source_catalog (
    id uuid NOT NULL,
    slug text NOT NULL,
    name text NOT NULL,
    provider text NOT NULL,
    dataset_name text NOT NULL,
    base_url text,
    auth_kind text NOT NULL,
    payload_format text NOT NULL,
    license_name text,
    license_url text,
    terms_url text,
    collection_frequency text,
    is_active boolean DEFAULT true NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    version bigint DEFAULT 1 NOT NULL,
    CONSTRAINT source_catalog_auth_kind_check CHECK ((auth_kind = ANY (ARRAY['none'::text, 'service_key'::text, 'oauth2'::text, 'manual'::text]))),
    CONSTRAINT source_catalog_base_url_check CHECK (((base_url IS NULL) OR (base_url <> ''::text))),
    CONSTRAINT source_catalog_collection_frequency_check CHECK (((collection_frequency IS NULL) OR (collection_frequency <> ''::text))),
    CONSTRAINT source_catalog_dataset_name_check CHECK ((dataset_name <> ''::text)),
    CONSTRAINT source_catalog_license_name_check CHECK (((license_name IS NULL) OR (license_name <> ''::text))),
    CONSTRAINT source_catalog_license_url_check CHECK (((license_url IS NULL) OR (license_url <> ''::text))),
    CONSTRAINT source_catalog_name_check CHECK ((name <> ''::text)),
    CONSTRAINT source_catalog_payload_format_check CHECK ((payload_format = ANY (ARRAY['json'::text, 'xml'::text, 'csv'::text, 'zip'::text, 'html'::text, 'binary'::text, 'unknown'::text]))),
    CONSTRAINT source_catalog_provider_check CHECK ((provider <> ''::text)),
    CONSTRAINT source_catalog_slug_check CHECK ((slug <> ''::text)),
    CONSTRAINT source_catalog_slug_check1 CHECK ((slug = lower(slug))),
    CONSTRAINT source_catalog_slug_check2 CHECK ((slug ~ '^[a-z0-9][a-z0-9_-]*$'::text)),
    CONSTRAINT source_catalog_terms_url_check CHECK (((terms_url IS NULL) OR (terms_url <> ''::text))),
    CONSTRAINT source_catalog_version_check CHECK ((version >= 1))
);


--
-- Name: source_record; Type: TABLE; Schema: catalog; Owner: -
--

CREATE TABLE catalog.source_record (
    id uuid NOT NULL,
    source text NOT NULL,
    source_url text,
    external_id text,
    captured_at timestamp with time zone DEFAULT now() NOT NULL,
    checksum_sha256 character(64),
    raw_object_key text,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: spatial_layer; Type: TABLE; Schema: catalog; Owner: -
--

CREATE TABLE catalog.spatial_layer (
    id uuid NOT NULL,
    complex_id uuid NOT NULL,
    parcel_id uuid,
    blueprint_id uuid,
    layer_kind text NOT NULL,
    geometry_object_key text,
    source_record_id uuid,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    version bigint DEFAULT 1 NOT NULL,
    CONSTRAINT spatial_layer_layer_kind_check CHECK ((layer_kind = ANY (ARRAY['complex_boundary'::text, 'parcel_boundary'::text, 'zone'::text, 'road'::text, 'utility'::text, 'blueprint_overlay'::text, 'other'::text])))
);


--
-- Name: vector_tile_artifact; Type: TABLE; Schema: catalog; Owner: -
--

CREATE TABLE catalog.vector_tile_artifact (
    id uuid NOT NULL,
    manifest_id uuid NOT NULL,
    layer text NOT NULL,
    source_layer text NOT NULL,
    tile_min_zoom smallint NOT NULL,
    tile_max_zoom smallint NOT NULL,
    render_min_zoom smallint NOT NULL,
    render_max_zoom smallint NOT NULL,
    tilejson_file_asset_id uuid NOT NULL,
    object_key_prefix text NOT NULL,
    flat_tile_count bigint NOT NULL,
    flat_tile_total_bytes bigint NOT NULL,
    source_record_id uuid NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    version bigint DEFAULT 1 NOT NULL,
    CONSTRAINT vector_tile_artifact_check CHECK ((tile_min_zoom <= tile_max_zoom)),
    CONSTRAINT vector_tile_artifact_check1 CHECK ((render_min_zoom <= render_max_zoom)),
    CONSTRAINT vector_tile_artifact_flat_tile_count_check CHECK ((flat_tile_count >= 0)),
    CONSTRAINT vector_tile_artifact_flat_tile_total_bytes_check CHECK ((flat_tile_total_bytes >= 0)),
    CONSTRAINT vector_tile_artifact_layer_check CHECK ((layer <> ''::text)),
    CONSTRAINT vector_tile_artifact_object_key_prefix_check CHECK ((object_key_prefix <> ''::text)),
    CONSTRAINT vector_tile_artifact_object_key_prefix_check1 CHECK ((object_key_prefix !~~ '/%'::text)),
    CONSTRAINT vector_tile_artifact_object_key_prefix_check2 CHECK ((POSITION(('\'::text) IN (object_key_prefix)) = 0)),
    CONSTRAINT vector_tile_artifact_object_key_prefix_check3 CHECK ((object_key_prefix !~~ '%//%'::text)),
    CONSTRAINT vector_tile_artifact_object_key_prefix_check4 CHECK (((object_key_prefix !~~ '%/./%'::text) AND (object_key_prefix !~~ './%'::text) AND (object_key_prefix !~~ '%/.'::text))),
    CONSTRAINT vector_tile_artifact_object_key_prefix_check5 CHECK (((object_key_prefix !~~ '%/../%'::text) AND (object_key_prefix !~~ '../%'::text) AND (object_key_prefix !~~ '%/..'::text))),
    CONSTRAINT vector_tile_artifact_render_max_zoom_check CHECK (((render_max_zoom >= 0) AND (render_max_zoom <= 24))),
    CONSTRAINT vector_tile_artifact_render_min_zoom_check CHECK (((render_min_zoom >= 0) AND (render_min_zoom <= 24))),
    CONSTRAINT vector_tile_artifact_source_layer_check CHECK ((source_layer <> ''::text)),
    CONSTRAINT vector_tile_artifact_tile_max_zoom_check CHECK (((tile_max_zoom >= 0) AND (tile_max_zoom <= 24))),
    CONSTRAINT vector_tile_artifact_tile_min_zoom_check CHECK (((tile_min_zoom >= 0) AND (tile_min_zoom <= 24)))
);


--
-- Name: vector_tile_artifact_source_file_asset; Type: TABLE; Schema: catalog; Owner: -
--

CREATE TABLE catalog.vector_tile_artifact_source_file_asset (
    artifact_id uuid NOT NULL,
    file_asset_id uuid NOT NULL
);


--
-- Name: vector_tile_manifest; Type: TABLE; Schema: catalog; Owner: -
--

CREATE TABLE catalog.vector_tile_manifest (
    id uuid NOT NULL,
    current_version text NOT NULL,
    previous_version text NOT NULL,
    tiles_url_template text NOT NULL,
    manifest_file_asset_id uuid NOT NULL,
    source_record_id uuid NOT NULL,
    is_active boolean DEFAULT false NOT NULL,
    published_at timestamp with time zone DEFAULT now() NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    version bigint DEFAULT 1 NOT NULL,
    CONSTRAINT vector_tile_manifest_check CHECK ((current_version <> previous_version)),
    CONSTRAINT vector_tile_manifest_current_version_check CHECK ((current_version <> ''::text)),
    CONSTRAINT vector_tile_manifest_previous_version_check CHECK ((previous_version <> ''::text)),
    CONSTRAINT vector_tile_manifest_tiles_url_template_check CHECK ((tiles_url_template <> ''::text)),
    CONSTRAINT vector_tile_manifest_tiles_url_template_check3 CHECK ((POSITION(('{z}'::text) IN (tiles_url_template)) > 0)),
    CONSTRAINT vector_tile_manifest_tiles_url_template_check4 CHECK ((POSITION(('{x}'::text) IN (tiles_url_template)) > 0)),
    CONSTRAINT vector_tile_manifest_tiles_url_template_check5 CHECK ((POSITION(('{y}'::text) IN (tiles_url_template)) > 0)),
    CONSTRAINT vector_tile_manifest_tiles_url_template_object_key_prefix_check CHECK ((POSITION(('{object_key_prefix}'::text) IN (tiles_url_template)) > 0))
);


--
-- Name: parcel_boundary_mirror; Type: TABLE; Schema: serving_postgis; Owner: -
--

CREATE UNLOGGED TABLE serving_postgis.parcel_boundary_mirror (
    pnu character(19) NOT NULL,
    rebuild_run_id uuid NOT NULL,
    source_snapshot_id text NOT NULL,
    source_table text NOT NULL,
    source_record_id uuid,
    source_file_asset_id uuid,
    source_object_key text NOT NULL,
    source_row_id text,
    complex_id uuid,
    parcel_id uuid,
    geometry_checksum_sha256 character(64) NOT NULL,
    properties jsonb DEFAULT '{}'::jsonb NOT NULL,
    geom public.geometry(MultiPolygon,5179) NOT NULL,
    loaded_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    version bigint DEFAULT 1 NOT NULL,
    CONSTRAINT parcel_boundary_mirror_geom_check CHECK ((public.st_srid(geom) = 5179)),
    CONSTRAINT parcel_boundary_mirror_geom_check1 CHECK (public.st_isvalid(geom)),
    CONSTRAINT parcel_boundary_mirror_geometry_checksum_sha256_check CHECK ((geometry_checksum_sha256 ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT parcel_boundary_mirror_pnu_check CHECK ((pnu ~ '^[0-9]{19}$'::text)),
    CONSTRAINT parcel_boundary_mirror_properties_check CHECK ((jsonb_typeof(properties) = 'object'::text)),
    CONSTRAINT parcel_boundary_mirror_source_object_key_check CHECK ((source_object_key <> ''::text)),
    CONSTRAINT parcel_boundary_mirror_source_object_key_check1 CHECK ((source_object_key !~~ '/%'::text)),
    CONSTRAINT parcel_boundary_mirror_source_object_key_check2 CHECK ((POSITION(('\'::text) IN (source_object_key)) = 0)),
    CONSTRAINT parcel_boundary_mirror_source_object_key_check3 CHECK ((source_object_key !~~ '%//%'::text)),
    CONSTRAINT parcel_boundary_mirror_source_object_key_check4 CHECK (((source_object_key !~~ '%/./%'::text) AND (source_object_key !~~ './%'::text) AND (source_object_key !~~ '%/.'::text))),
    CONSTRAINT parcel_boundary_mirror_source_object_key_check5 CHECK (((source_object_key !~~ '%/../%'::text) AND (source_object_key !~~ '../%'::text) AND (source_object_key !~~ '%/..'::text))),
    CONSTRAINT parcel_boundary_mirror_source_row_id_check CHECK (((source_row_id IS NULL) OR (btrim(source_row_id) <> ''::text))),
    CONSTRAINT parcel_boundary_mirror_source_snapshot_id_check CHECK ((source_snapshot_id ~ '^iceberg:[A-Za-z0-9][A-Za-z0-9._:-]{2,127}$'::text)),
    CONSTRAINT parcel_boundary_mirror_source_table_check CHECK ((source_table = 'silver.parcel_boundaries'::text)),
    CONSTRAINT parcel_boundary_mirror_version_check CHECK ((version >= 1))
);


--
-- Name: parcel_boundary_mirror_rebuild_run; Type: TABLE; Schema: serving_postgis; Owner: -
--

CREATE TABLE serving_postgis.parcel_boundary_mirror_rebuild_run (
    id uuid NOT NULL,
    source_snapshot_id text NOT NULL,
    source_table text NOT NULL,
    source_record_id uuid,
    source_file_asset_id uuid,
    srid integer DEFAULT 5179 NOT NULL,
    status text NOT NULL,
    loaded_row_count bigint DEFAULT 0 NOT NULL,
    rejected_row_count bigint DEFAULT 0 NOT NULL,
    quality_report jsonb DEFAULT '{}'::jsonb NOT NULL,
    started_at timestamp with time zone NOT NULL,
    finished_at timestamp with time zone,
    error_message text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    version bigint DEFAULT 1 NOT NULL,
    CONSTRAINT parcel_boundary_mirror_rebuild_run_check CHECK (((finished_at IS NULL) OR (finished_at >= started_at))),
    CONSTRAINT parcel_boundary_mirror_rebuild_run_check1 CHECK (((status <> 'succeeded'::text) OR ((finished_at IS NOT NULL) AND (loaded_row_count > 0)))),
    CONSTRAINT parcel_boundary_mirror_rebuild_run_check2 CHECK (((status <> 'failed'::text) OR (error_message IS NOT NULL))),
    CONSTRAINT parcel_boundary_mirror_rebuild_run_error_message_check CHECK (((error_message IS NULL) OR (btrim(error_message) <> ''::text))),
    CONSTRAINT parcel_boundary_mirror_rebuild_run_loaded_row_count_check CHECK ((loaded_row_count >= 0)),
    CONSTRAINT parcel_boundary_mirror_rebuild_run_quality_report_check CHECK ((jsonb_typeof(quality_report) = 'object'::text)),
    CONSTRAINT parcel_boundary_mirror_rebuild_run_rejected_row_count_check CHECK ((rejected_row_count >= 0)),
    CONSTRAINT parcel_boundary_mirror_rebuild_run_source_snapshot_id_check CHECK ((source_snapshot_id ~ '^iceberg:[A-Za-z0-9][A-Za-z0-9._:-]{2,127}$'::text)),
    CONSTRAINT parcel_boundary_mirror_rebuild_run_source_table_check CHECK ((source_table = 'silver.parcel_boundaries'::text)),
    CONSTRAINT parcel_boundary_mirror_rebuild_run_srid_check CHECK ((srid = 5179)),
    CONSTRAINT parcel_boundary_mirror_rebuild_run_status_check CHECK ((status = ANY (ARRAY['planned'::text, 'running'::text, 'succeeded'::text, 'failed'::text, 'cancelled'::text]))),
    CONSTRAINT parcel_boundary_mirror_rebuild_run_version_check CHECK ((version >= 1))
);


--
-- PostgreSQL database dump complete
--


-- Final production storage namespaces.
INSERT INTO catalog.lakehouse_storage_namespace (
    id, provider, environment, owner_service, bucket_name, root_prefix, catalog_provider, status, version
)
VALUES
    ('018f0000-0000-7000-8000-000000000901', 'r2', 'production', 'foundation-platform', 'foundation-platform-lakehouse-prod', NULL, 'r2_data_catalog', 'active', 2),
    ('018f0000-0000-7000-8000-000000000902', 'r2', 'production', 'gongzzang', 'gongzzang-lakehouse-prod', NULL, 'r2_data_catalog', 'active', 1),
    ('018f0000-0000-7000-8000-000000000903', 'r2', 'production', 'dawneer', 'dawneer-lakehouse-prod', NULL, 'r2_data_catalog', 'active', 1);
