-- Rebuildable serving projection; observations remain append-only in Silver (root ADR-0095).
CREATE TABLE catalog.unit_official_price (
    pnu character(19) NOT NULL,
    dong_name text NOT NULL,
    ho_name text NOT NULL,
    base_year smallint NOT NULL,
    price_won bigint NOT NULL,
    source_snapshot_id text NOT NULL,
    CONSTRAINT unit_official_price_pkey PRIMARY KEY (pnu, dong_name, ho_name, base_year),
    CONSTRAINT unit_official_price_pnu_check CHECK (pnu ~ '^[0-9]{19}$'::text),
    CONSTRAINT unit_official_price_year_check CHECK (base_year BETWEEN 1000 AND 9999),
    CONSTRAINT unit_official_price_value_check CHECK (price_won >= 0)
);
COMMENT ON TABLE catalog.unit_official_price IS
    'Annual unit assessments reindexed through the exclusive register; replaceable serving projection (ADR-0095).';
