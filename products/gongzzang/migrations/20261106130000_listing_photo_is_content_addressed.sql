-- A listing photo is stored once by the hash of its bytes, like every other photo (root ADR-0427).
--
-- Until now a listing photo lived at an id key (`media/listing-photo/listings/<lst>/photos/<lph>.<ext>`)
-- with its own MIME list and a `thumbnail_r2_key` nothing ever filled. From here:
--
-- - a new upload names its sha256 and is put at `media/listing-photo/sha256/<hex>.<ext>`; the row
--   keeps the hash in `content_sha256` and `r2_key` is that content-addressed key;
-- - an old id-key photo keeps working through its `r2_key`; the backfill job copies its object into
--   the content-addressed store (never deleting the old one) and appends the old key -> hash pair to
--   `listing_photo_content_address`, which a read joins on `r2_key`;
-- - the display ladder sits beside the hash (`media/listing-photo/renditions/<version>/<hex>/...`),
--   so `thumbnail_r2_key` is no longer read or written. It is left in place (no rows lose a value).

alter table listing_photo
    add column content_sha256 char(64)
        check (content_sha256 ~ '^[0-9a-f]{64}$');

comment on column listing_photo.thumbnail_r2_key is
    'Deprecated (root ADR-0427): never written; the display ladder beside content_sha256 replaces it.';

create table listing_photo_content_address (
    legacy_r2_key text primary key,
    content_sha256 char(64) not null check (content_sha256 ~ '^[0-9a-f]{64}$'),
    content_key text not null,
    size_bytes bigint not null check (size_bytes > 0),
    copied_at timestamptz not null default now()
);

create or replace function reject_listing_photo_content_address_mutation()
returns trigger
language plpgsql
as $$
begin
    raise exception 'listing_photo_content_address is append-only: % not allowed', tg_op
        using errcode = '45000';
end $$;

create trigger trg_listing_photo_content_address_append_only
    before update or delete on listing_photo_content_address
    for each row
    execute function reject_listing_photo_content_address_mutation();

grant select, insert on listing_photo_content_address to gongzzang_app_writer;
grant select on listing_photo_content_address to gongzzang_app_reader;
