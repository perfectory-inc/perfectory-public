from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[4]
DBT_ROOT = ROOT / "infra" / "lakehouse" / "dbt"


class FoundationDbtModelContractTest(unittest.TestCase):
    def read(self, relative: str) -> str:
        return (DBT_ROOT / relative).read_text(encoding="utf-8")

    def test_silver_entity_link_model_stays_candidate(self) -> None:
        sql = self.read("models/silver/entity_link/silver_entity_link_assertion_candidate.sql")
        self.assertIn("'candidate' as publish_state", sql)
        self.assertNotIn("'published' as publish_state", sql)

    def test_entity_link_model_uses_ref_not_physical_paths(self) -> None:
        sql = self.read("models/silver/entity_link/silver_entity_link_assertion_candidate.sql")
        self.assertIn("{{ ref('int_entity_resolution__court_auction_building_unit_candidates') }}", sql)
        self.assertNotIn("r2://", sql.lower())
        self.assertNotIn("s3://", sql.lower())

    def test_staging_models_use_sources(self) -> None:
        court = self.read("models/staging/gongzzang/stg_gongzzang__court_auction_observation.sql")
        building = self.read("models/staging/foundation/stg_foundation__building_register_unit.sql")
        self.assertIn("{{ source('gongzzang', 'court_auction_property') }}", court)
        self.assertIn("{{ source('foundation', 'building_register_unit') }}", building)

    def test_court_auction_source_matches_real_gongzzang_silver_table(self) -> None:
        sources = self.read("models/sources/foundation_sources.yml")
        court = self.read("models/staging/gongzzang/stg_gongzzang__court_auction_observation.sql")
        fixtures = self.read("smoke/source-fixtures.sql")

        self.assertIn("name: court_auction_property", sources)
        self.assertIn("{{ source('gongzzang', 'court_auction_property') }}", court)
        self.assertIn("court_office_code", court)
        self.assertIn("case_no", court)
        self.assertIn("gds_seq", court)
        self.assertIn("objct_seq", court)
        self.assertNotIn("court_auction_observation", sources)
        self.assertNotIn("court_auction_observation", court)
        self.assertNotIn("court_auction_observation", fixtures)

    def test_court_auction_staging_requires_explicit_lineage_envs(self) -> None:
        court = self.read("models/staging/gongzzang/stg_gongzzang__court_auction_observation.sql")
        macro_path = DBT_ROOT / "macros/required_env.sql"
        readme = self.read("README.md")

        self.assertTrue(macro_path.exists(), "required env macro must exist")

        macro = self.read("macros/required_env.sql")

        self.assertIn("required_env_var_sql_literal", macro)
        self.assertIn(
            "required_env_var_sql_literal('FOUNDATION_DBT_COURT_AUCTION_SOURCE_SNAPSHOT_ID')",
            court,
        )
        self.assertIn(
            "required_env_var_sql_literal('FOUNDATION_DBT_COURT_AUCTION_LINEAGE_RUN_ID')",
            court,
        )
        self.assertNotIn("court-auction-property-snapshot-unknown", court)
        self.assertNotIn("court-auction-property-lineage-unknown", court)
        self.assertIn("FOUNDATION_DBT_COURT_AUCTION_SOURCE_SNAPSHOT_ID", readme)
        self.assertIn("FOUNDATION_DBT_COURT_AUCTION_LINEAGE_RUN_ID", readme)

    def test_building_register_sources_map_to_remote_silver_tables(self) -> None:
        sources = self.read("models/sources/foundation_sources.yml")

        self.assertIn("name: building_register_unit", sources)
        self.assertIn("identifier: building_register_units", sources)
        self.assertIn("name: building_register_unit_area", sources)
        self.assertIn("identifier: building_register_unit_areas", sources)

    def test_gongzzang_and_foundation_sources_have_independent_schema_envs(self) -> None:
        sources = self.read("models/sources/foundation_sources.yml")

        self.assertIn("FOUNDATION_DBT_GONGZZANG_SOURCE_SCHEMA", sources)
        self.assertIn("FOUNDATION_DBT_FOUNDATION_SOURCE_SCHEMA", sources)
        self.assertIn("env_var('FOUNDATION_DBT_GONGZZANG_SOURCE_SCHEMA'", sources)
        self.assertIn("env_var('FOUNDATION_DBT_FOUNDATION_SOURCE_SCHEMA'", sources)
        self.assertIn("env_var('FOUNDATION_DBT_SOURCE_SCHEMA', 'silver')", sources)

    def test_building_register_staging_uses_remote_silver_columns(self) -> None:
        building = self.read("models/staging/foundation/stg_foundation__building_register_unit.sql")
        fixtures = self.read("smoke/source-fixtures.sql")

        self.assertIn("unit_row_id as building_unit_id", building)
        self.assertIn("source_snapshot_id as target_snapshot_id", building)
        self.assertIn("register_parcel_key", building)
        self.assertIn("building_mgm_bldrgst_pk", building)
        self.assertIn("dong_join_name", building)
        self.assertIn("unit_designation", building)
        self.assertIn("unit_name_raw", building)
        self.assertIn("unit_label_ko", building)
        self.assertIn("unit_number", building)
        self.assertIn("floor_number", building)
        self.assertIn("floor_index", building)
        self.assertIn("mgm_bldrgst_pk", building)
        self.assertIn("register_parcel_key VARCHAR", fixtures)
        self.assertIn("building_mgm_bldrgst_pk VARCHAR", fixtures)
        self.assertNotIn("exclusive_areas as", building)
        self.assertNotIn("select\n    building_unit_id,", building)

    def test_court_auction_staging_exposes_numeric_unit_and_floor_evidence(self) -> None:
        court = self.read("models/staging/gongzzang/stg_gongzzang__court_auction_observation.sql")
        macro = self.read("macros/unit_designation_parsing.sql")
        fixtures = self.read("smoke/source-fixtures.sql")

        self.assertIn("unit_number", court)
        self.assertIn("floor_from_designation", court)
        self.assertIn("foundation_unit_number_from_designation('unit_designation')", court)
        self.assertIn("foundation_floor_designation_hint_from_designation('unit_designation')", court)
        self.assertNotIn("regexp_extract(unit_designation", court)
        self.assertIn("([0-9]+)[^0-9]*$", macro)
        self.assertIn("as unit_number", court)
        self.assertIn("unit_designation VARCHAR", fixtures)

    def test_court_auction_staging_exposes_pnu_recovery_evidence(self) -> None:
        court = self.read("models/staging/gongzzang/stg_gongzzang__court_auction_observation.sql")
        fixtures = self.read("smoke/source-fixtures.sql")

        self.assertIn("ltno_addr as lot_address_raw", court)
        self.assertIn("road_addr as road_address_raw", court)
        self.assertIn("print_addr_raw", court)
        self.assertIn("x_crd as x_coordinate", court)
        self.assertIn("y_crd as y_coordinate", court)
        self.assertIn("ltno_addr VARCHAR", fixtures)
        self.assertIn("road_addr VARCHAR", fixtures)
        self.assertIn("print_addr_raw VARCHAR", fixtures)
        self.assertIn("x_crd DOUBLE", fixtures)
        self.assertIn("y_crd DOUBLE", fixtures)

    def test_custom_schema_names_are_layer_names_not_target_schema_suffixes(self) -> None:
        macro = self.read("macros/generate_schema_name.sql")

        self.assertIn("custom_schema_name | trim", macro)
        self.assertNotIn("target.schema ~ '_'", macro)

    def test_smoke_target_uses_isolated_schemas(self) -> None:
        profile = self.read("profiles.example.yml")
        macro = self.read("macros/generate_schema_name.sql")
        sources = self.read("models/sources/foundation_sources.yml")
        fixtures = self.read("smoke/source-fixtures.sql")

        self.assertIn("smoke:", profile)
        self.assertIn("target.name == 'smoke'", macro)
        self.assertIn("smoke_{{ custom_schema_name | trim }}", macro)
        self.assertIn("FOUNDATION_DBT_SOURCE_SCHEMA", sources)
        self.assertIn("CREATE SCHEMA IF NOT EXISTS foundation_platform.smoke_source", fixtures)
        self.assertNotIn("DROP TABLE IF EXISTS foundation_platform.silver", fixtures)

    def test_large_foundation_staging_tests_are_full_quality_only(self) -> None:
        schema = self.read("models/schema.yml")

        start = schema.index("  - name: stg_foundation__building_register_unit")
        end = schema.index(
            "  - name: int_entity_resolution__building_register_unit_number_collision_candidates"
        )
        block = schema[start:end]

        self.assertEqual(block.count('tags: ["full_quality"]'), 4)
        self.assertIn("dbt test --target smoke --exclude tag:full_quality", self.read("README.md"))

    def test_court_auction_match_funnel_is_modelled_as_diagnostic_output(self) -> None:
        relative = (
            "models/intermediate/entity_resolution/"
            "int_entity_resolution__court_auction_building_unit_match_funnel.sql"
        )
        self.assertTrue((DBT_ROOT / relative).exists())

        sql = self.read(relative)
        schema = self.read("models/schema.yml")

        self.assertIn("{{ config(materialized='table', tags=['full_quality']) }}", sql)
        self.assertIn("source_total", sql)
        self.assertIn("source_with_pnu", sql)
        self.assertIn("court_pnu", sql)
        self.assertIn("building_relevant", sql)
        self.assertIn("pnu_match", sql)
        self.assertIn("pnu_dong_match", sql)
        self.assertIn("pnu_dong_unit_label_match", sql)
        self.assertIn("pnu_dong_unit_number_match", sql)
        self.assertIn("pnu_dong_unit_number_unique", sql)
        self.assertIn("PNU_DONG_UNIT_NUMBER_AREA", self.read(
            "models/intermediate/entity_resolution/int_entity_resolution__court_auction_building_unit_candidates.sql"
        ))
        self.assertIn("PNU_DONG_UNIT_NUMBER_UNIQUE", self.read(
            "models/intermediate/entity_resolution/int_entity_resolution__court_auction_building_unit_candidates.sql"
        ))
        self.assertIn("pnu_dong_unit_label_area_match", sql)
        self.assertIn("pnu_dong_unit_number_area_match", sql)
        self.assertIn("candidate_output", sql)
        self.assertIn("then court_auction.source_observation_id", sql)
        self.assertIn("count(*) as candidate_pair_count", sql)
        self.assertIn("building_unit_pnu_rollup", sql)
        self.assertIn("building_unit_pnu_dong_rollup", sql)
        self.assertIn("building_unit_pnu_dong_label_rollup", sql)
        self.assertIn("building_unit_pnu_dong_number_rollup", sql)
        self.assertIn("int_entity_resolution__court_auction_building_unit_match_funnel", schema)
        self.assertIn("match_stage", schema)
        self.assertIn("source_observation_count", schema)
        readme = self.read("README.md")
        self.assertIn("dbt run --target smoke --exclude tag:full_quality", readme)
        self.assertIn("dbt run --target smoke --select tag:full_quality", readme)

    def test_candidate_model_admits_unit_number_only_when_source_has_single_target(self) -> None:
        sql = self.read(
            "models/intermediate/entity_resolution/int_entity_resolution__court_auction_building_unit_candidates.sql"
        )
        silver = self.read("models/silver/entity_link/silver_entity_link_assertion_candidate.sql")

        self.assertIn("{{ config(materialized='table') }}", sql)
        self.assertIn("court_pnu as", sql)
        self.assertIn("building_relevant as", sql)
        self.assertIn("candidate_area_keys as", sql)
        self.assertIn("{{ source('foundation', 'building_register_unit_area') }}", sql)
        self.assertIn("area_kind = 'exclusive'", sql)
        self.assertIn("unit_number_scope_rollup as", sql)
        self.assertIn("safe_unit_number_scope as", sql)
        self.assertIn("distinct_target_count", sql)
        self.assertIn("distinct_designation_count", sql)
        self.assertIn("unit_number_scope_state", sql)
        self.assertIn("unit_number_scope_rollup.distinct_target_count = 1", sql)
        self.assertIn("unit_number_scope_rollup.distinct_designation_count = 1", sql)
        self.assertIn("'safe' as unit_number_scope_state", sql)
        self.assertIn("'collision' as unit_number_scope_state", sql)
        self.assertIn("unit_number_candidate_source_counts", sql)
        self.assertIn("distinct_target_count = 1", sql)
        self.assertIn("'PNU_DONG_UNIT_NUMBER_UNIQUE' as match_path", sql)
        self.assertIn("'medium' as confidence_band", sql)
        self.assertIn("else 'needs_review'", silver)

    def test_court_auction_pnu_recovery_funnel_is_diagnostic_only(self) -> None:
        status_relative = (
            "models/intermediate/entity_resolution/"
            "int_entity_resolution__court_auction_pnu_recovery_status.sql"
        )
        relative = (
            "models/intermediate/entity_resolution/"
            "int_entity_resolution__court_auction_pnu_recovery_funnel.sql"
        )
        self.assertTrue((DBT_ROOT / status_relative).exists())
        self.assertTrue((DBT_ROOT / relative).exists())

        status_sql = self.read(status_relative)
        sql = self.read(relative)
        schema = self.read("models/schema.yml")

        self.assertIn("{{ config(materialized='table', tags=['full_quality']) }}", status_sql)
        self.assertIn("{{ ref('stg_gongzzang__court_auction_observation') }}", status_sql)
        self.assertIn("{{ ref('stg_foundation__building_register_unit') }}", status_sql)
        self.assertIn("court_flipped_pnu", status_sql)
        self.assertIn("flipped_dong_unit_number_match_count", status_sql)
        self.assertIn("has_address_evidence", status_sql)
        self.assertIn("has_coordinate_evidence", status_sql)
        self.assertIn("{{ ref('int_entity_resolution__court_auction_pnu_recovery_status') }}", sql)
        self.assertNotIn("{{ ref('stg_foundation__building_register_unit') }}", sql)
        self.assertIn("source_without_pnu", sql)
        self.assertIn("source_pnu_exact_match", sql)
        self.assertIn("source_pnu_exact_missing", sql)
        self.assertIn("source_pnu_11th_flip_possible", sql)
        self.assertIn("source_pnu_11th_flip_with_dong", sql)
        self.assertIn("source_pnu_11th_flip_with_unit_number", sql)
        self.assertIn("source_pnu_11th_flip_with_dong_unit_number", sql)
        self.assertIn("source_without_pnu_with_dong_unit_number_area", sql)
        self.assertIn("source_without_pnu_with_any_address", sql)
        self.assertIn("source_without_pnu_with_coordinates", sql)
        self.assertIn("source_without_pnu_with_address_or_coordinate", sql)
        self.assertIn("source_without_pnu_requires_address_or_coordinate_evidence", sql)
        self.assertNotIn("silver_entity_link_assertion_candidate", sql)
        self.assertNotIn("PNU_11TH_FLIP", self.read(
            "models/intermediate/entity_resolution/int_entity_resolution__court_auction_building_unit_candidates.sql"
        ))
        self.assertIn("int_entity_resolution__court_auction_pnu_recovery_status", schema)
        self.assertIn("int_entity_resolution__court_auction_pnu_recovery_funnel", schema)
        self.assertIn("recovery_stage", schema)

    def test_building_register_unit_number_collision_diagnostics_are_full_quality(self) -> None:
        candidates_relative = (
            "models/intermediate/entity_resolution/"
            "int_entity_resolution__building_register_unit_number_collision_candidates.sql"
        )
        funnel_relative = (
            "models/intermediate/entity_resolution/"
            "int_entity_resolution__building_register_unit_number_collision_funnel.sql"
        )
        self.assertTrue((DBT_ROOT / candidates_relative).exists())
        self.assertTrue((DBT_ROOT / funnel_relative).exists())

        candidates = self.read(candidates_relative)
        funnel = self.read(funnel_relative)
        schema = self.read("models/schema.yml")

        self.assertIn("{{ config(materialized='table', tags=['full_quality']) }}", candidates)
        self.assertIn("{{ ref('stg_foundation__building_register_unit') }}", candidates)
        self.assertIn("register_parcel_key", candidates)
        self.assertIn("building_mgm_bldrgst_pk", candidates)
        self.assertIn("dong_name_key", candidates)
        self.assertIn("floor_index", candidates)
        self.assertIn("unit_number", candidates)
        self.assertIn("unit_designation", candidates)
        self.assertIn("count(distinct coalesce(unit_designation", candidates)
        self.assertIn("regexp_like(coalesce(unit_designation", candidates)
        self.assertIn("\\s*-\\s*", candidates)
        self.assertIn("parenthesized_suffix_count", candidates)
        self.assertIn(
            "{{ ref('int_entity_resolution__building_register_unit_number_collision_candidates') }}",
            funnel,
        )
        self.assertNotIn("court_auction", candidates)
        self.assertIn("source_total", funnel)
        self.assertIn("unit_number_present", funnel)
        self.assertIn("unit_number_collision_groups", funnel)
        self.assertIn("designation_collision_groups", funnel)
        self.assertIn("hyphenated_designation_collision_groups", funnel)
        self.assertIn("parenthesized_suffix_collision_groups", funnel)
        self.assertIn("int_entity_resolution__building_register_unit_number_collision_candidates", schema)
        self.assertIn("int_entity_resolution__building_register_unit_number_collision_funnel", schema)
        self.assertIn("diagnostic_stage", schema)


if __name__ == "__main__":
    unittest.main()
