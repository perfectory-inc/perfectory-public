-- Listing views are an append-only event log, not a column on the listing row.
--
-- listing.view_count was rewritten by every aggregate save (stale value read before the
-- save) and every view moved listing.updated_at. Views now land here and readers count
-- them. Decision: root ADR-0220.

create table listing_view_event (
    view_event_id bigint generated always as identity primary key,
    listing_id char(30) not null references listing(id) on delete cascade,
    viewed_at timestamptz not null default now()
);

create index listing_view_event_listing_idx on listing_view_event(listing_id);

-- Carry the counts recorded so far: one event per counted view, stamped with the
-- listing's last update (the only time the old column kept).
insert into listing_view_event (listing_id, viewed_at)
select l.id, l.updated_at
from listing l
cross join lateral generate_series(1, l.view_count)
where l.view_count > 0;

alter table listing drop column view_count;

-- bookmark_count was deprecated in favour of counting bookmark_listing (SP6-iii) but kept
-- on the row, where every save could still write it. Readers already count the relation.
alter table listing drop column bookmark_count;

create or replace function reject_listing_view_event_update() returns trigger
language plpgsql
as $$
begin
    raise exception 'listing_view_event is append-only: UPDATE not allowed'
        using errcode = '45000';
end $$;

create trigger trg_listing_view_event_no_update
    before update on listing_view_event
    for each row
    execute function reject_listing_view_event_update();

-- Rows leave only with their listing (on delete cascade); the app role never edits them.
grant select, insert on listing_view_event to gongzzang_app_writer;
grant select on listing_view_event to gongzzang_app_reader;
