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

    def test_land_right_source_and_staging_contract(self) -> None:
        sources = self.read("models/sources/foundation_sources.yml")
        staging = self.read("models/staging/foundation/stg_foundation__land_right_registration.sql")

        self.assertIn("name: land_right_registration", sources)
        self.assertIn("identifier: land_right_registration", sources)
        self.assertIn("{{ source('foundation', 'land_right_registration') }}", staging)
        self.assertNotIn("r2://", staging.lower())
        self.assertNotIn("s3://", staging.lower())

    def test_land_right_corroboration_uses_shared_normalizer_and_stays_unique(self) -> None:
        candidates = self.read(
            "models/intermediate/entity_resolution/"
            "int_entity_resolution__building_register_unit_land_right_candidates.sql"
        )
        funnel = self.read(
            "models/intermediate/entity_resolution/"
            "int_entity_resolution__building_register_unit_land_right_funnel.sql"
        )
        macro = self.read("macros/unit_name_normalization.sql")

        # 정규형 SSOT는 매크로 한 곳 (ADR-0106 결정 3) — 모델이 정규식을 직접
        # 들고 있으면 대장·등기 정규형이 갈라진 채 초록일 수 있다.
        self.assertIn("foundation_normalized_unit_name", macro)
        self.assertIn("{{ foundation_normalized_unit_name(", candidates)
        self.assertIn("{{ foundation_normalized_unit_name(", funnel)
        # 양방향 유일성 — 모호 일치가 후보로 새면 잘못된 링크가 확정된다.
        self.assertIn("unit_count = 1", candidates)
        self.assertIn("right_count = 1", candidates)
        # 대상 조건도 SSOT 한 곳 — 옛 엔진 딱지(proposal_required)만 보면
        # 헐겁게 '확정'된 지저분한 표기(실측 10만+행)를 놓친다.
        self.assertIn("foundation_unit_name_is_string_clean", macro)
        self.assertIn("{{ foundation_unit_name_is_string_clean(", candidates)
        self.assertIn("{{ foundation_unit_name_is_string_clean(", funnel)
        # 층 회수 티어 — 이름만으로 모호한 호를 (PNU, 층, 정규형) 3중 유일로
        # 회수한다(운영 검산 +5,516행). 1단계 중복 방출은 제외돼야 한다.
        self.assertIn("PNU_FLOOR_NORMALIZED_UNIT_NAME_UNIQUE", candidates)
        self.assertIn("unit_triple_counts.unit_count = 1", candidates)
        self.assertIn("right_triple_counts.right_count = 1", candidates)
        self.assertIn("not in (", candidates)
        self.assertIn("floor_recovered", funnel)
        # 붙임표 보존 — 정규형이 하이픈을 지우면 6-2호와 62호가 한 호로 붕괴한다.
        self.assertNotIn("'-'", macro)

    def test_name_bearing_tables_are_registered_for_entity_resolution(self) -> None:
        """동·호·건물 이름 칸을 가진 표는 전부 ER 역할이 배정돼야 한다.

        같은 실물(호·동)을 여러 기관이 다른 글자로 적는다(ADR-0106). 이름 칸을
        가진 표가 새로 계약에 들어왔는데 아무도 엔티티 취급을 결정하지 않으면,
        그 표는 조용히 정합 밖에 남는다 — 이 시험이 그 누락을 시끄럽게 만든다.
        새 표가 걸리면 아래 등록부에 역할을 적고 필요한 교차확증을 설계하라.
        """
        import json
        import re

        registered_roles = {
            "silver.building_register_exclusive_unit": "대장 전유부 원본 — 정규화 대상(본류)",
            "silver.land_right_registration": "등기 대지권 — 교차확증 증인 (본 후보 모델)",
            "silver.unit_official_price": (
                "세대 공시가격 — 열쇠 기반 증인 (price_candidates; 원천이 대장 "
                "관리번호 보유, 운영 검산 열쇠 조인 24,736·이름 검증 일치 99.45%)"
            ),
            "silver.industrial_complexes": "단지 표시명 — 식별은 코드(두 id 체계), 이름은 표시용",
            "silver.building_register_floors": "층구분명 원문 — 어휘 칸, 층 규칙이 정규화(식별자 아님)",
            "silver.building_register_titles": "표제부 동명 — canonical_dong_join_key 로 호↔건물 연결에 사용",
            "silver.building_register_units": "정규화된 전유부 — 본 교차확증의 대상(unit_designation 이 충돌-프리 키)",
            "silver.building_register_unit_areas": "전유공용면적 — 동·호명은 전유부와 같은 계보, 면적 증거 공급원",
        }
        contracts = json.loads(
            (DBT_ROOT.parent / "contracts" / "industrial_complex_lakehouse_contracts.json")
            .read_text(encoding="utf-8")
        )["contracts"]
        name_column = re.compile(
            r"^(ho|dong|building|room|floor|complex|apt|unit)_.*name|^(unit_name_raw|dong_join_name)$"
        )
        for qualified_name, contract in contracts.items():
            columns = [
                column["name"]
                for column in contract.get("columns", [])
                if name_column.match(column.get("name", ""))
            ]
            if columns:
                self.assertIn(
                    qualified_name,
                    registered_roles,
                    f"{qualified_name} carries identity-name columns {columns} "
                    "but has no entity-resolution role assigned",
                )

    def test_number_disputes_worklist_grades_with_the_shared_normalizer(self) -> None:
        disputes = self.read(
            "models/intermediate/entity_resolution/"
            "int_entity_resolution__building_register_unit_number_disputes.sql"
        )
        schema = self.read("models/schema.yml")

        # 채점도 같은 정규형 SSOT 로 — 다른 정규화로 채점하면 가짜 분쟁이 생긴다.
        self.assertIn("{{ foundation_normalized_unit_name(", disputes)
        self.assertIn("{{ foundation_unit_name_is_string_clean(", disputes)
        # 분쟁 정의: 유일 일치 + 숫자 증인 + 번호 불일치.
        self.assertIn("unit_name_counts.unit_count = 1", disputes)
        self.assertIn("right_name_counts.right_count = 1", disputes)
        self.assertIn("<> unit_scope.unit_number", disputes)
        # 이 모델은 후보를 내지 않는다 — 링크 합류 모델이 참조하면 안 된다.
        link = self.read("models/silver/entity_link/silver_entity_link_assertion_candidate.sql")
        self.assertNotIn("number_disputes", link)
        self.assertIn("int_entity_resolution__building_register_unit_number_disputes", schema)

    def test_entity_link_model_unions_land_right_candidates(self) -> None:
        sql = self.read("models/silver/entity_link/silver_entity_link_assertion_candidate.sql")

        self.assertIn(
            "{{ ref('int_entity_resolution__building_register_unit_land_right_candidates') }}",
            sql,
        )
        self.assertIn("'building-unit-land-right-corroboration.v1' as rule_version", sql)

    def test_price_witness_is_key_based_with_a_name_agreement_band(self) -> None:
        sources = self.read("models/sources/foundation_sources.yml")
        staging = self.read("models/staging/foundation/stg_foundation__unit_official_price.sql")
        candidates = self.read(
            "models/intermediate/entity_resolution/"
            "int_entity_resolution__building_register_unit_price_candidates.sql"
        )
        link = self.read("models/silver/entity_link/silver_entity_link_assertion_candidate.sql")

        self.assertIn("name: unit_official_price", sources)
        self.assertIn("{{ source('foundation', 'unit_official_price') }}", staging)
        # 열쇠 기반: 관리번호 + pnu 동시 일치, 열쇠당 단일 표기만 후보다.
        self.assertIn("price_latest.mgm_bldrgst_pk = unit_scope.mgm_bldrgst_pk", candidates)
        self.assertIn("price_latest.pnu = unit_scope.pnu", candidates)
        self.assertIn("key_rollup.distinct_names = 1", candidates)
        # 이름 합치는 밴드를 가른다 — 표기 불일치는 medium/needs_review 로 남는다.
        self.assertIn("MGM_KEY_AND_NORMALIZED_NAME", candidates)
        self.assertIn("MGM_KEY_ONLY", candidates)
        self.assertIn("{{ foundation_normalized_unit_name(", candidates)
        self.assertIn("{{ foundation_unit_name_is_string_clean(", candidates)
        self.assertIn(
            "{{ ref('int_entity_resolution__building_register_unit_price_candidates') }}",
            link,
        )
        self.assertIn("'building-unit-price-corroboration.v1' as rule_version", link)


if __name__ == "__main__":
    unittest.main()
