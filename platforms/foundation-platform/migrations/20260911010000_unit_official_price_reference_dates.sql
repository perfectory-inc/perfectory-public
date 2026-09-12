-- ADR-0101: preserve year-only rows without inventing a reference date.
ALTER TABLE catalog.unit_official_price RENAME TO unit_official_price_legacy_year;
ALTER TABLE catalog.unit_official_price_legacy_year
    RENAME CONSTRAINT unit_official_price_pkey TO unit_official_price_legacy_year_pkey;

CREATE TABLE catalog.unit_official_price (
    pnu character(19) NOT NULL,
    dong_name text NOT NULL,
    ho_name text NOT NULL,
    base_date text NOT NULL CHECK (base_date ~ '^[0-9]{8}$'),
    price_won bigint NOT NULL CHECK (price_won >= 0),
    source_snapshot_id text NOT NULL CHECK (btrim(source_snapshot_id) <> ''),
    PRIMARY KEY (source_snapshot_id, pnu, dong_name, ho_name, base_date),
    CHECK (pnu ~ '^[0-9]{19}$')
);

CREATE TABLE catalog.unit_official_price_publication (
    publication_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_snapshot_id text NOT NULL CHECK (btrim(source_snapshot_id) <> ''),
    row_count bigint NOT NULL CHECK (row_count > 0)
);

-- The generic temporal trigger permits a publisher override; these facts never do.
CREATE FUNCTION catalog.reject_unit_official_price_mutation() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION '% is append-only', TG_TABLE_NAME USING ERRCODE = '55000';
END;
$$;

CREATE TRIGGER unit_official_price_append_only
BEFORE UPDATE OR DELETE OR TRUNCATE ON catalog.unit_official_price
FOR EACH STATEMENT EXECUTE FUNCTION catalog.reject_unit_official_price_mutation();
CREATE TRIGGER unit_official_price_publication_append_only
BEFORE UPDATE OR DELETE OR TRUNCATE ON catalog.unit_official_price_publication
FOR EACH STATEMENT EXECUTE FUNCTION catalog.reject_unit_official_price_mutation();
CREATE TRIGGER unit_official_price_legacy_year_append_only
BEFORE UPDATE OR DELETE OR TRUNCATE ON catalog.unit_official_price_legacy_year
FOR EACH STATEMENT EXECUTE FUNCTION catalog.reject_unit_official_price_mutation();
