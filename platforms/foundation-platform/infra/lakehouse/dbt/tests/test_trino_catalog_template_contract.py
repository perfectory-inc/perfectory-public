import re
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[4]
TEMPLATE = ROOT / "infra/lakehouse/trino/templates/foundation-platform-jdbc-iceberg.properties.template"
INIT_SQL = ROOT / "infra/lakehouse/trino/init/foundation-platform-jdbc-iceberg-catalog.sql"
DBT_README = ROOT / "infra/lakehouse/dbt/README.md"


class TrinoCatalogTemplateContractTest(unittest.TestCase):
    def test_foundation_jdbc_catalog_uses_jdbc_catalog_without_unsupported_auto_init(self) -> None:
        text = TEMPLATE.read_text(encoding="utf-8")

        self.assertIn("iceberg.catalog.type=jdbc", text)
        self.assertIn("iceberg.jdbc-catalog.catalog-name=foundation_platform", text)
        self.assertNotIn("iceberg.jdbc-catalog.initialize-catalog-tables", text)

    def test_foundation_jdbc_catalog_init_sql_creates_required_iceberg_tables(self) -> None:
        text = INIT_SQL.read_text(encoding="utf-8")

        self.assertIn("CREATE SCHEMA IF NOT EXISTS lakehouse_catalog", text)
        self.assertIn("CREATE TABLE IF NOT EXISTS lakehouse_catalog.iceberg_tables", text)
        self.assertIn("CREATE TABLE IF NOT EXISTS lakehouse_catalog.iceberg_namespace_properties", text)
        self.assertIn("iceberg_type VARCHAR(5)", text)

    def test_foundation_jdbc_catalog_uses_foundation_bucket_name(self) -> None:
        text = TEMPLATE.read_text(encoding="utf-8")

        self.assertIn(
            "iceberg.jdbc-catalog.default-warehouse-dir=s3://foundation-platform-lakehouse-prod/warehouse",
            text,
        )
        self.assertNotIn("foundation-platform-lakehouse-prod", text)

    def test_template_contains_placeholders_not_live_secrets(self) -> None:
        text = TEMPLATE.read_text(encoding="utf-8")

        self.assertNotRegex(text, re.compile(r"cfat_[A-Za-z0-9_\\-]+"))
        self.assertNotRegex(text, re.compile(r"(?i)(password|token|secret-key|access-key)=.+[A-Za-z0-9]{12,}"))
        self.assertIn("${ENV:FOUNDATION_PLATFORM_LAKEHOUSE_JDBC_PASSWORD}", text)
        self.assertIn("${ENV:R2_SECRET_ACCESS_KEY}", text)

    def test_dbt_readme_names_the_runtime_catalog_file(self) -> None:
        text = DBT_README.read_text(encoding="utf-8")

        self.assertIn("foundation_platform.properties", text)
        self.assertIn("catalog file name is the Trino catalog name", text)


if __name__ == "__main__":
    unittest.main()
