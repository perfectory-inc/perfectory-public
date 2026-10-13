-- The parcel values copied onto a listing name the Foundation contract and data snapshot
-- they came from (root ADR-0223).
--
-- `admin_code`, `parcel_land_use_type`, and `parcel_zoning` stay on the row because card
-- search filters on them in SQL (`card_summaries.rs`); a read-time lookup against the
-- Foundation edge cannot sit inside that WHERE clause. A copy that cannot say which
-- Foundation snapshot it is can never be found stale, so the copy now carries:
--   parcel_source_contract    — the edge document's `schema_version`
--   parcel_source_snapshot_id — the Gold Iceberg snapshot it was baked from (its as-of)
--
-- `parcel_lookup_at` held now() at write time: when Gongzzang asked, not what Foundation
-- data it got. Nothing reads it, and the snapshot id replaces it.

alter table listing
    add column parcel_source_contract text,
    add column parcel_source_snapshot_id text,
    drop column parcel_lookup_at;

-- Copies written before this migration.
-- They name no source, so they are cleared and the refresh refills them; until then card
-- search does not match them by land use. Leaving them behind a NOT VALID check is not an
-- option: Postgres checks it on every UPDATE, and listing save is an upsert, so those
-- listings could never be edited again.
update listing
set parcel_land_use_type = null, parcel_zoning = null
where (parcel_land_use_type is not null or parcel_zoning is not null)
  and (parcel_source_contract is null or parcel_source_snapshot_id is null);

alter table listing
    add constraint listing_parcel_copy_names_its_source_chk
    check (
        (parcel_land_use_type is null and parcel_zoning is null)
        or (parcel_source_contract is not null and parcel_source_snapshot_id is not null)
    );
