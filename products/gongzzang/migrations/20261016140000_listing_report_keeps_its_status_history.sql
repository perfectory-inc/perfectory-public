-- A listing report keeps every status it passed through and who handled it (root ADR-0264).
--
-- `listing_report` is upserted in place: investigating -> confirmed overwrites the status, the
-- handler and the note, so the time a report was taken up and by whom are lost. The same shape
-- as business_verification_status_history (root ADR-0258): a trigger appends one row per
-- insert and per change of status, handler or note, and the app role may only read it.

create table listing_report_status_history (
    history_id bigint generated always as identity primary key,
    report_id char(30) not null references listing_report(id),
    status varchar(20) not null,
    handler_id char(30),
    handler_note text,
    -- The terminal decision time the row carries (resolved_at); otherwise the transaction time.
    changed_at timestamptz not null
);

create index listing_report_status_history_report_idx
    on listing_report_status_history(report_id, history_id);

-- security definer: the app role may only read the history; the trigger writes it.
create or replace function record_listing_report_status_history() returns trigger
language plpgsql
security definer
set search_path = public
as $$
begin
    insert into listing_report_status_history (
        report_id, status, handler_id, handler_note, changed_at
    )
    values (
        new.id, new.status, new.handler_id, new.handler_note, coalesce(new.resolved_at, now())
    );
    return null;
end $$;

create trigger trg_listing_report_status_history_insert
    after insert on listing_report
    for each row
    execute function record_listing_report_status_history();

create trigger trg_listing_report_status_history_update
    after update of status, handler_id, handler_note on listing_report
    for each row
    when (old.status is distinct from new.status
          or old.handler_id is distinct from new.handler_id
          or old.handler_note is distinct from new.handler_note)
    execute function record_listing_report_status_history();

create or replace function reject_listing_report_status_history_mutation()
returns trigger
language plpgsql
as $$
begin
    raise exception 'listing_report_status_history is append-only: % not allowed', tg_op
        using errcode = '45000';
end $$;

create trigger trg_listing_report_status_history_append_only
    before update or delete on listing_report_status_history
    for each row
    execute function reject_listing_report_status_history_mutation();

grant select on listing_report_status_history to gongzzang_app_writer;
grant select on listing_report_status_history to gongzzang_app_reader;

-- Existing reports start their history at their current status.
insert into listing_report_status_history (
    report_id, status, handler_id, handler_note, changed_at
)
select id, status, handler_id, handler_note, coalesce(resolved_at, created_at)
from listing_report;
