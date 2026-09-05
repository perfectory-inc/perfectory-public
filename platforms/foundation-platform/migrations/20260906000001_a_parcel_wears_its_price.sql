-- A parcel wears its price (root ADR-0085 §2).
--
-- One row is one parcel's newest official land price assessment within the selected D151
-- vintage: the (base_year, base_month) pair the assessment ledger stamped, the price in
-- won per square meter as the source's integer, and the announcement date carried verbatim.
-- The loader (`load-parcel-price-catalog-projection`) is the only writer and picks the
-- newest (base_year, base_month) per parcel; history stays in silver.land_individual_price.
--
-- No FK to catalog.parcel for the same reason parcel_zoning has none: the assessment ledger
-- legitimately names parcels the boundary source has not delivered yet, and a price fact
-- should not be dropped because the boundary row arrives later.
CREATE TABLE catalog.parcel_price (
    pnu character(19) NOT NULL,
    price_per_m2 bigint NOT NULL,
    base_year smallint NOT NULL,
    base_month smallint NOT NULL,
    -- Carried verbatim from the source (공시일자); interpreting or formatting it is the
    -- consumer's job, and an unparsed value cannot be silently reshaped here.
    announced_date text,
    source_snapshot_id text NOT NULL,
    loaded_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT parcel_price_pkey PRIMARY KEY (pnu),
    CONSTRAINT parcel_price_pnu_check CHECK (pnu ~ '^[0-9]{19}$'::text),
    CONSTRAINT parcel_price_value_check CHECK (price_per_m2 >= 0),
    CONSTRAINT parcel_price_month_check CHECK (base_month BETWEEN 1 AND 12),
    CONSTRAINT parcel_price_year_check CHECK (base_year > 0)
);

COMMENT ON TABLE catalog.parcel_price IS
    'Per-parcel newest official land price assessment projected from silver.land_individual_price (root ADR-0085).';
