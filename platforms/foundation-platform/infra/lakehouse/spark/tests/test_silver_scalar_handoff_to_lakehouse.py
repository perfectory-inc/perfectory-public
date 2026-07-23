import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory


JOBS_DIR = Path(__file__).resolve().parents[1] / "jobs"
sys.path.insert(0, str(JOBS_DIR))

from silver_scalar_handoff_to_lakehouse import (  # noqa: E402
    build_run_summary,
    cluster_frame_for_iceberg_write,
    collect_input_batches,
    default_iceberg_target,
    handoff_storage_level,
    iceberg_write_mode_for_input_batch,
    merge_quality_metrics,
    read_handoff_jsonl,
    read_handoff_parquet,
    run_summary_disposition,
    should_skip_spark_stop_after_success,
    simple_parquet_partition_columns,
    source_snapshot_summary_from_ids,
    validate_args,
    write_silver_iceberg,
)
from platform_contracts import load_lakehouse_contract  # noqa: E402


class SilverScalarHandoffToLakehouseTest(unittest.TestCase):
    def test_default_iceberg_target_uses_contract_namespace_and_table(self) -> None:
        self.assertEqual(
            default_iceberg_target("silver.building_register_floors"),
            ("silver", "building_register_floors"),
        )

    def test_default_iceberg_target_rejects_non_qualified_contract(self) -> None:
        with self.assertRaisesRegex(ValueError, "must be namespace.table"):
            default_iceberg_target("building_register_floors")

    def test_parquet_partitions_only_keep_plain_columns(self) -> None:
        contract = {
            "partition_spec": [
                "sido_code",
                "bucket(32, complex_id)",
                "sigungu_code",
            ]
        }

        self.assertEqual(
            simple_parquet_partition_columns(contract),
            ("sido_code", "sigungu_code"),
        )

    def test_read_handoff_jsonl_uses_contract_schema_before_reading(self) -> None:
        contract = {
            "columns": [
                {"name": "floor_row_id", "logical_type": "string", "required": True},
                {"name": "source_line_number", "logical_type": "long", "required": False},
            ]
        }
        spark = FakeSpark()

        frame = read_handoff_jsonl(spark, "/tmp/floors", contract, FakeSparkTypes)

        self.assertEqual(frame.selected_columns, ("floor_row_id", "source_line_number"))
        self.assertEqual(spark.read.schema_value, (
            ("floor_row_id", "string", False),
            ("source_line_number", "long", True),
        ))
        self.assertEqual(spark.read.json_path, "/tmp/floors")

    def test_read_handoff_jsonl_accepts_building_register_unit_contract(self) -> None:
        contract = load_lakehouse_contract("silver.building_register_units")
        spark = FakeSpark()

        frame = read_handoff_jsonl(spark, "/tmp/units", contract, FakeSparkTypes)

        self.assertEqual(frame.selected_columns, tuple(column["name"] for column in contract["columns"]))
        self.assertEqual(spark.read.schema_value[0], ("unit_row_id", "string", False))
        # pnu는 표준 사투리, 블록 필지에서 null (ADR 0023); 내부 키는 필수.
        self.assertEqual(spark.read.schema_value[2], ("pnu", "string", True))
        self.assertEqual(
            spark.read.schema_value[3], ("register_parcel_key", "string", False)
        )
        self.assertEqual(spark.read.schema_value[7], ("unit_number", "int", True))
        self.assertIn(("unit_label_ko", "string", True), spark.read.schema_value)
        self.assertIn(("normalization_application_id", "string", True), spark.read.schema_value)
        self.assertIn(("ingested_at_utc", "timestamp", False), spark.read.schema_value)
        self.assertEqual(spark.read.json_path, "/tmp/units")

    def test_read_handoff_parquet_uses_contract_columns(self) -> None:
        contract = {
            "columns": [
                {"name": "floor_row_id", "logical_type": "string", "required": True},
                {"name": "source_line_number", "logical_type": "long", "required": False},
            ]
        }
        spark = FakeSpark()

        frame = read_handoff_parquet(spark, "/tmp/floors", contract)

        self.assertEqual(frame.selected_columns, ("floor_row_id", "source_line_number"))
        self.assertEqual(spark.read.parquet_path, "/tmp/floors")

    def test_read_handoff_parquet_expands_file_batches_for_spark_varargs(self) -> None:
        contract = {
            "columns": [
                {"name": "floor_row_id", "logical_type": "string", "required": True},
                {"name": "source_line_number", "logical_type": "long", "required": False},
            ]
        }
        spark = FakeSpark()

        frame = read_handoff_parquet(
            spark,
            ["/tmp/floors/part-000001.parquet", "/tmp/floors/part-000002.parquet"],
            contract,
        )

        self.assertEqual(frame.selected_columns, ("floor_row_id", "source_line_number"))
        self.assertEqual(
            spark.read.parquet_paths,
            ("/tmp/floors/part-000001.parquet", "/tmp/floors/part-000002.parquet"),
        )

    def test_large_handoff_persistence_is_disk_first(self) -> None:
        self.assertEqual(handoff_storage_level(FakeStorageLevel), "disk-only")

    def test_cluster_frame_splits_bucketed_iceberg_writes(self) -> None:
        contract = {
            "columns": [
                {"name": "floor_row_id", "logical_type": "string", "required": True},
                {"name": "mgm_bldrgst_pk", "logical_type": "string", "required": True},
            ],
            "partition_spec": ["bucket(16, mgm_bldrgst_pk)"],
            "sort_order": ["mgm_bldrgst_pk", "floor_row_id"],
        }
        frame = FakeClusterFrame()

        clustered = cluster_frame_for_iceberg_write(frame, contract, FakeFunctions)

        self.assertIs(clustered, frame)
        self.assertEqual(
            frame.calls,
            [
                ("withColumn", "__iceberg_write_bucket", "pmod(xxhash64(col(mgm_bldrgst_pk)),lit(16))"),
                ("withColumn", "__iceberg_write_split", "pmod(xxhash64(col(floor_row_id)),lit(16))"),
                ("repartition", 256, ("__iceberg_write_bucket", "__iceberg_write_split")),
                ("sortWithinPartitions", ("__iceberg_write_bucket", "mgm_bldrgst_pk", "floor_row_id")),
                ("drop", ("__iceberg_write_bucket", "__iceberg_write_split")),
            ],
        )

    def test_collect_input_batches_sorts_jsonl_files_and_batches_them(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "part-000003.jsonl").write_text("{}\n", encoding="utf-8")
            (root / "part-000001.jsonl").write_text("{}\n", encoding="utf-8")
            (root / "part-000002.jsonl").write_text("{}\n", encoding="utf-8")
            (root / "ignore.txt").write_text("not-jsonl\n", encoding="utf-8")

            batches = collect_input_batches(str(root), 2, "jsonl")

        self.assertEqual(
            [[Path(path).name for path in batch] for batch in batches],
            [["part-000001.jsonl", "part-000002.jsonl"], ["part-000003.jsonl"]],
        )

    def test_collect_input_batches_uses_only_selected_physical_format(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "part-000001.parquet").write_bytes(b"PAR1")
            (root / "part-000002.parquet").write_bytes(b"PAR1")
            (root / "part-000099.jsonl").write_text("{}\n", encoding="utf-8")

            batches = collect_input_batches(str(root), 1, "parquet")

        self.assertEqual(
            [[Path(path).name for path in batch] for batch in batches],
            [["part-000001.parquet"], ["part-000002.parquet"]],
        )

    def test_iceberg_batch_write_mode_only_uses_overwrite_for_first_batch(self) -> None:
        args = FakeArgs(iceberg_write_mode="overwrite")

        self.assertEqual(iceberg_write_mode_for_input_batch(args, 0), "overwrite")
        self.assertEqual(iceberg_write_mode_for_input_batch(args, 1), "append")
        self.assertEqual(iceberg_write_mode_for_input_batch(args, 9), "append")

    def test_input_file_batching_preserves_iceberg_write_disposition(self) -> None:
        args = FakeArgs(
            validate_only=False,
            write_mode="iceberg",
            iceberg_write_mode="overwrite",
            input_file_batch_size=1,
        )

        self.assertEqual(run_summary_disposition(args), "iceberg_overwrite")

    def test_input_file_batch_size_is_iceberg_only(self) -> None:
        args = FakeArgs(
            summary_output=None,
            write_mode="parquet",
            output="/tmp/out",
            input_file_batch_size=2,
        )

        with self.assertRaisesRegex(ValueError, "input file batching is only supported"):
            validate_args(args)

    def test_deferred_iceberg_readback_validation_is_iceberg_only(self) -> None:
        args = FakeArgs(
            summary_output=None,
            write_mode="parquet",
            output="/tmp/out",
            input_file_batch_size=0,
            defer_iceberg_readback_validation=True,
        )

        with self.assertRaisesRegex(ValueError, "deferred Iceberg readback validation"):
            validate_args(args)

    def test_parquet_run_summary_omits_iceberg_readback_validation(self) -> None:
        args = FakeArgs(
            contract="silver.building_register_floors",
            input="/tmp/in.jsonl",
            output="/tmp/out",
            write_mode="parquet",
            validate_only=False,
        )
        contract = {
            "columns": [
                {"name": "floor_row_id", "required": True},
                {"name": "floor_label", "required": False},
            ]
        }

        summary = build_run_summary(
            args,
            contract,
            row_count=1,
            persisted_row_count=1,
            quality_metrics={"row_count": 1},
            source_snapshot_summary=source_snapshot_summary_from_ids(["snapshot-1"]),
        )

        self.assertNotIn("iceberg_readback_validation", summary)

    def test_merge_quality_metrics_sums_seen_metric_keys(self) -> None:
        merged = merge_quality_metrics(
            [
                {"row_count": 10, "floor_label__null_count": 1},
                {"row_count": 20, "invalid_checksum_count": 2},
            ]
        )

        self.assertEqual(
            merged,
            {
                "row_count": 30,
                "floor_label__null_count": 1,
                "invalid_checksum_count": 2,
            },
        )

    def test_source_snapshot_summary_from_ids_sorts_unique_ids(self) -> None:
        summary = source_snapshot_summary_from_ids(["b", "a", "b"])

        self.assertEqual(
            summary,
            {
                "source_snapshot_count": 2,
                "source_snapshot_ids": ["a", "b"],
                "source_snapshot_truncated": False,
            },
        )

    def test_skip_spark_stop_after_success_is_explicit_env_opt_in(self) -> None:
        self.assertFalse(should_skip_spark_stop_after_success(lambda _: None))
        self.assertFalse(should_skip_spark_stop_after_success(lambda _: "true"))
        self.assertTrue(should_skip_spark_stop_after_success(lambda _: "1"))

    def test_iceberg_write_adds_missing_nullable_contract_columns_before_insert(self) -> None:
        contract = {
            "columns": [
                {"name": "unit_row_id", "logical_type": "string", "required": True},
                {"name": "normalization_application_id", "logical_type": "string", "required": False},
                {"name": "row_checksum_sha256", "logical_type": "string", "required": True},
            ],
            "partition_spec": ["bucket(16, unit_row_id)"],
        }
        args = FakeArgs(
            contract="silver.building_register_units",
            iceberg_catalog_name="r2",
            iceberg_namespace="silver",
            iceberg_table="building_register_units",
            iceberg_write_mode="overwrite",
        )
        spark = FakeIcebergSpark(
            existing_columns=("unit_row_id", "row_checksum_sha256"),
        )
        frame = FakeIcebergFrame()

        write_silver_iceberg(spark, frame, args, contract, FakeFunctions)

        self.assertIn(
            "ALTER TABLE `r2`.`silver`.`building_register_units` ADD COLUMNS (normalization_application_id STRING)",
            compact_sql_statements(spark.sql_statements),
        )
        self.assertLess(
            compact_sql_statements(spark.sql_statements).index("ALTER TABLE"),
            compact_sql_statements(spark.sql_statements).index("INSERT OVERWRITE"),
        )
        self.assertIn(
            "INSERT OVERWRITE `r2`.`silver`.`building_register_units` (unit_row_id, normalization_application_id, row_checksum_sha256)",
            compact_sql_statements(spark.sql_statements),
        )


class FakeSparkTypes:
    @staticmethod
    def StringType() -> str:
        return "string"

    @staticmethod
    def IntegerType() -> str:
        return "int"

    @staticmethod
    def LongType() -> str:
        return "long"

    @staticmethod
    def DoubleType() -> str:
        return "double"

    @staticmethod
    def DateType() -> str:
        return "date"

    @staticmethod
    def TimestampType() -> str:
        return "timestamp"

    @staticmethod
    def DecimalType(precision: int, scale: int) -> str:
        return f"decimal({precision},{scale})"

    @staticmethod
    def StructField(name: str, logical_type: str, nullable: bool) -> tuple[str, str, bool]:
        return (name, logical_type, nullable)

    @staticmethod
    def StructType(fields: list[tuple[str, str, bool]]) -> tuple[tuple[str, str, bool], ...]:
        return tuple(fields)


class FakeFrame:
    def __init__(self, columns: tuple[str, ...] = ("floor_row_id", "source_line_number")) -> None:
        self.columns = columns
        self.selected_columns: tuple[str, ...] | None = None

    def select(self, *columns: str) -> "FakeFrame":
        self.selected_columns = tuple(columns)
        return self


class FakeReader:
    def __init__(self) -> None:
        self.schema_value: tuple[tuple[str, str, bool], ...] | None = None
        self.json_path: str | None = None
        self.parquet_path: str | None = None
        self.parquet_paths: tuple[str, ...] | None = None

    def schema(self, schema: tuple[tuple[str, str, bool], ...]) -> "FakeReader":
        self.schema_value = schema
        return self

    def json(self, input_path: str) -> FakeFrame:
        self.json_path = input_path
        columns = (
            tuple(field[0] for field in self.schema_value)
            if self.schema_value is not None
            else ("floor_row_id", "source_line_number")
        )
        return FakeFrame(columns)

    def parquet(self, *input_paths: str) -> FakeFrame:
        self.parquet_paths = tuple(input_paths)
        self.parquet_path = input_paths[0] if len(input_paths) == 1 else None
        return FakeFrame()


class FakeSpark:
    def __init__(self) -> None:
        self.read = FakeReader()


class FakeDescribeRow:
    def __init__(self, col_name: str) -> None:
        self.col_name = col_name


class FakeDescribeResult:
    def __init__(self, columns: tuple[str, ...]) -> None:
        self.columns = columns

    def collect(self) -> list[FakeDescribeRow]:
        return [FakeDescribeRow(column) for column in self.columns]


class FakeIcebergSpark:
    def __init__(self, existing_columns: tuple[str, ...]) -> None:
        self.existing_columns = existing_columns
        self.sql_statements: list[str] = []

    def sql(self, statement: str) -> FakeDescribeResult | None:
        self.sql_statements.append(statement)
        if statement.strip().upper().startswith("DESCRIBE "):
            return FakeDescribeResult(self.existing_columns)
        return None


class FakeIcebergFrame:
    def __init__(self) -> None:
        self.selected_columns: tuple[str, ...] | None = None
        self.view_name: str | None = None

    def select(self, *columns: str) -> "FakeIcebergFrame":
        self.selected_columns = tuple(columns)
        return self

    def withColumn(self, _name: str, _value: str) -> "FakeIcebergFrame":
        return self

    def repartition(self, _partitions: int, *_columns: str) -> "FakeIcebergFrame":
        return self

    def sortWithinPartitions(self, *_columns: str) -> "FakeIcebergFrame":
        return self

    def drop(self, *_columns: str) -> "FakeIcebergFrame":
        return self

    def createOrReplaceTempView(self, view_name: str) -> None:
        self.view_name = view_name


def compact_sql_statements(statements: list[str]) -> str:
    return "\n".join(" ".join(statement.split()) for statement in statements)


class FakeArgs:
    def __init__(self, **values: str) -> None:
        self.input_format = "jsonl"
        self.__dict__.update(values)


class FakeStorageLevel:
    DISK_ONLY = "disk-only"
    MEMORY_AND_DISK = "memory-and-disk"


class FakeFunctions:
    @staticmethod
    def col(name: str) -> str:
        return f"col({name})"

    @staticmethod
    def lit(value: int) -> str:
        return f"lit({value})"

    @staticmethod
    def xxhash64(value: str) -> str:
        return f"xxhash64({value})"

    @staticmethod
    def pmod(left: str, right: str) -> str:
        return f"pmod({left},{right})"


class FakeClusterFrame:
    def __init__(self) -> None:
        self.calls: list[tuple] = []

    def withColumn(self, name: str, value: str) -> "FakeClusterFrame":
        self.calls.append(("withColumn", name, value))
        return self

    def repartition(self, partitions: int, *columns: str) -> "FakeClusterFrame":
        self.calls.append(("repartition", partitions, tuple(columns)))
        return self

    def sortWithinPartitions(self, *columns: str) -> "FakeClusterFrame":
        self.calls.append(("sortWithinPartitions", tuple(columns)))
        return self

    def drop(self, *columns: str) -> "FakeClusterFrame":
        self.calls.append(("drop", tuple(columns)))
        return self


if __name__ == "__main__":
    unittest.main()
