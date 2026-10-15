-- Listing status and price history is an append-only table, not a reading of audit JSON.
--
-- Until now the only record of past prices/statuses was audit_log.before_state/after_state
-- (a jsonb snapshot of the whole row). The trigger below writes one typed row per change
-- of status or price, whatever path wrote the listing. Decision: root ADR-0221.

create table listing_status_price_history (
    history_id bigint generated always as identity primary key,
    listing_id char(30) not null references listing(id) on delete cascade,
    listing_version bigint not null,
    status varchar(20) not null,
    price_krw bigint not null,
    deposit_krw bigint,
    monthly_rent_krw bigint,
    -- The listing's own updated_at for that version (domain time), not the insert time.
    changed_at timestamptz not null
);

create index listing_status_price_history_listing_idx
    on listing_status_price_history(listing_id, history_id);

-- security definer: the app role may only read this table; the trigger writes it.
create or replace function record_listing_status_price_history() returns trigger
language plpgsql
security definer
set search_path = public
as $$
begin
    insert into listing_status_price_history (
        listing_id, listing_version, status,
        price_krw, deposit_krw, monthly_rent_krw, changed_at
    )
    values (
        new.id, new.version, new.status,
        new.price_krw, new.deposit_krw, new.monthly_rent_krw, new.updated_at
    );
    return null;
end $$;

create trigger trg_listing_status_price_history_insert
    after insert on listing
    for each row
    execute function record_listing_status_price_history();

create trigger trg_listing_status_price_history_update
    after update of status, price_krw, deposit_krw, monthly_rent_krw on listing
    for each row
    when (
        old.status is distinct from new.status
        or old.price_krw is distinct from new.price_krw
        or old.deposit_krw is distinct from new.deposit_krw
        or old.monthly_rent_krw is distinct from new.monthly_rent_krw
    )
    execute function record_listing_status_price_history();

create or replace function reject_listing_status_price_history_update() returns trigger
language plpgsql
as $$
begin
    raise exception 'listing_status_price_history is append-only: UPDATE not allowed'
        using errcode = '45000';
end $$;

create trigger trg_listing_status_price_history_no_update
    before update on listing_status_price_history
    for each row
    execute function reject_listing_status_price_history_update();

-- Existing listings start their history at their current state.
insert into listing_status_price_history (
    listing_id, listing_version, status,
    price_krw, deposit_krw, monthly_rent_krw, changed_at
)
select id, version, status, price_krw, deposit_krw, monthly_rent_krw, updated_at
from listing;

-- Rows leave only with their listing (on delete cascade); the app role never edits them.
grant select on listing_status_price_history to gongzzang_app_writer;
grant select on listing_status_price_history to gongzzang_app_reader;
