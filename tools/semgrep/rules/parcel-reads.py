# Fixture for tools/semgrep/rules/parcel-reads.yml (`opengrep scan --test`). Not imported.
PARCEL_SOURCE = "silver.parcel_boundaries"


def the_auction_universe(spark, catalog):
    # ruleid: parcel-current-rows-go-through-the-served-reader
    parcels = current_rows(spark.table(f"`{catalog}`.`silver`.`parcel_boundaries`"), contract)
    return parcels


def through_a_module_name(frames):
    # ruleid: parcel-current-rows-go-through-the-served-reader
    return pc.current_rows_of_versioned(frames[PARCEL_SOURCE], PARCEL_SOURCE)


def the_served_reader(spark):
    # ok: parcel-current-rows-go-through-the-served-reader
    parcels = served_rows(spark.table("silver.parcel_boundaries")).select("pnu")
    # ok: parcel-current-rows-go-through-the-served-reader
    titles = current_rows(spark.table("silver.building_register_titles"), contract)
    # current_rows(spark.table("silver.parcel_boundaries")) was the 공매 bug
    return parcels, titles


def selectors(frame, spark, contract):
    # ok: parcel-current-row-selector-comes-from-the-contract
    current = frame.where(F.expr(current_row_predicate(contract)))
    # ok: parcel-current-row-selector-comes-from-the-contract
    schema_only = frame.select(F.col("valid_to_utc"))
    # ok: parcel-current-row-selector-comes-from-the-contract
    unrelated = frame.where(F.col("geometry_wkb").isNull())
    # ok: parcel-current-row-selector-comes-from-the-contract
    unrelated_sql = "geometry_wkb IS NULL"
    # current = frame.where(F.col("valid_to_utc").isNull())
    # ruleid: parcel-current-row-selector-comes-from-the-contract
    current = spark.sql("SELECT * FROM parcels WHERE valid_to_utc IS NULL")
    current = frame.where(
        # ruleid: parcel-current-row-selector-comes-from-the-contract
        F.col("valid_to_utc").isNull()
    )
    # ruleid: parcel-current-row-selector-comes-from-the-contract
    current = frame["valid_to_utc"].isNotNull()
    # ruleid: parcel-current-row-selector-comes-from-the-contract
    current = frame.valid_to_utc.isNull()
    # ruleid: parcel-current-row-selector-comes-from-the-contract
    current = frame.where(F.isnull(F.col("valid_to_utc")))
    # ruleid: parcel-current-row-selector-comes-from-the-contract
    current = spark.sql("SELECT * FROM parcels WHERE `valid_to_utc` IS NOT NULL")
    # ruleid: parcel-current-row-selector-comes-from-the-contract
    current = contract["current_row_predicate"]
    # ruleid: parcel-current-row-selector-comes-from-the-contract
    current = contract.get("current_row_predicate")
    return current, schema_only, unrelated, unrelated_sql


def through_a_local_name(spark, catalog):
    table = f"{catalog}.silver.parcel_boundaries"
    # ruleid: parcel-current-rows-go-through-the-served-reader
    return current_rows(spark.table(table), contract)


def current_rows_of_the_served_edition(spark):
    # ok: parcel-current-rows-go-through-the-served-reader
    return current_rows(served_rows(spark.table("silver.parcel_boundaries")), contract)
