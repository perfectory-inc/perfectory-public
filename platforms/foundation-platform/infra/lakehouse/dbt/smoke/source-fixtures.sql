-- public-repository-safety: synthetic-fixture
CREATE SCHEMA IF NOT EXISTS foundation_platform.smoke_source;

DROP TABLE IF EXISTS foundation_platform.smoke_source.court_auction_property;
DROP TABLE IF EXISTS foundation_platform.smoke_source.building_register_unit_areas;
DROP TABLE IF EXISTS foundation_platform.smoke_source.building_register_units;

CREATE TABLE foundation_platform.smoke_source.court_auction_property (
    court_office_code VARCHAR,
    case_no VARCHAR,
    gds_seq INTEGER,
    objct_seq INTEGER,
    pnu VARCHAR,
    dong_name VARCHAR,
    unit_designation VARCHAR,
    exclusive_area DOUBLE,
    ltno_addr VARCHAR,
    road_addr VARCHAR,
    print_addr_raw VARCHAR,
    x_crd DOUBLE,
    y_crd DOUBLE
) WITH (
    format = 'PARQUET'
);

CREATE TABLE foundation_platform.smoke_source.building_register_units (
    unit_row_id VARCHAR,
    mgm_bldrgst_pk VARCHAR,
    pnu VARCHAR,
    register_parcel_key VARCHAR,
    building_mgm_bldrgst_pk VARCHAR,
    dong_join_name VARCHAR,
    unit_name_raw VARCHAR,
    unit_label_ko VARCHAR,
    unit_designation VARCHAR,
    unit_number INTEGER,
    floor_kind VARCHAR,
    floor_number INTEGER,
    floor_index INTEGER,
    source_snapshot_id VARCHAR
) WITH (
    format = 'PARQUET'
);

CREATE TABLE foundation_platform.smoke_source.building_register_unit_areas (
    area_row_id VARCHAR,
    mgm_bldrgst_pk VARCHAR,
    pnu VARCHAR,
    area_kind VARCHAR,
    area_m2 DOUBLE,
    source_snapshot_id VARCHAR
) WITH (
    format = 'PARQUET'
);

INSERT INTO foundation_platform.smoke_source.court_auction_property
VALUES
    ('SYNTHETIC-COURT', 'SYNTHETIC-CASE-0001', 1, 1, '9999900601100010000', '101', '201', 84.95, 'SYNTHETIC-LOT-ADDRESS-1', 'SYNTHETIC-ROAD-ADDRESS-1', 'SYNTHETIC-PRINT-ADDRESS-1', 127.123464, 36.123448),
    ('SYNTHETIC-COURT', 'SYNTHETIC-CASE-0002', 1, 1, '9999900601100010000', '101', '999', 84.95, 'SYNTHETIC-LOT-ADDRESS-1', 'SYNTHETIC-ROAD-ADDRESS-1', 'SYNTHETIC-PRINT-ADDRESS-2', 127.123464, 36.123448),
    ('SYNTHETIC-COURT', 'SYNTHETIC-CASE-0003', 1, 1, '9999900601100010000', '101', '2F-01', 90.00, 'SYNTHETIC-LOT-ADDRESS-1', 'SYNTHETIC-ROAD-ADDRESS-1', 'SYNTHETIC-PRINT-ADDRESS-3', 127.123464, 36.123448),
    ('SYNTHETIC-COURT', 'SYNTHETIC-CASE-0004', 1, 1, '9999900601100010000', '102', '1', 30.00, 'SYNTHETIC-LOT-ADDRESS-1', 'SYNTHETIC-ROAD-ADDRESS-1', 'SYNTHETIC-PRINT-ADDRESS-4', 127.123464, 36.123448);

INSERT INTO foundation_platform.smoke_source.building_register_units
VALUES
    ('synthetic-building-unit-1', 'synthetic-building-pk-1', '9999900601100010000', '9999900601100010000', 'synthetic-building-title-1', '101', '201', '', '201', 201, 'above_ground', 2, 2, 'synthetic-building-snapshot-1'),
    ('synthetic-building-unit-collision-a', 'synthetic-building-pk-collision-a', '9999900601100010000', '9999900601100010000', 'synthetic-building-title-2', '102', '6-1', '', '6-1', 1, 'above_ground', 1, 1, 'synthetic-building-snapshot-1'),
    ('synthetic-building-unit-collision-b', 'synthetic-building-pk-collision-b', '9999900601100010000', '9999900601100010000', 'synthetic-building-title-2', '102', '5-1', '', '5-1', 1, 'above_ground', 1, 1, 'synthetic-building-snapshot-1');

INSERT INTO foundation_platform.smoke_source.building_register_unit_areas
VALUES
    ('synthetic-building-unit-area-1', 'synthetic-building-pk-1', '9999900601100010000', 'exclusive', 84.95, 'synthetic-building-area-snapshot-1'),
    ('synthetic-building-unit-area-2', 'synthetic-building-pk-1', '9999900601100010000', 'common', 12.50, 'synthetic-building-area-snapshot-1'),
    ('synthetic-building-unit-area-collision-a', 'synthetic-building-pk-collision-a', '9999900601100010000', 'exclusive', 30.00, 'synthetic-building-area-snapshot-1'),
    ('synthetic-building-unit-area-collision-b', 'synthetic-building-pk-collision-b', '9999900601100010000', 'exclusive', 40.00, 'synthetic-building-area-snapshot-1');
