-- Business verification lives in one place: business_verification_queue holds each
-- verification's state, and business_verification_status_history keeps every status it
-- passed through (append-only). "user".business_number / business_verified_at were a second,
-- unsynchronised copy and go. Decision: root ADR-0258.

alter table business_verification_queue
    drop constraint business_verification_queue_status_check;
alter table business_verification_queue
    add constraint business_verification_queue_status_check check (status in
        ('pending', 'approved', 'rejected', 'needs_more_info', 'revoked'));

create table business_verification_status_history (
    history_id bigint generated always as identity primary key,
    verification_id char(30) not null
        references business_verification_queue(id) on delete cascade,
    user_id char(30) not null,
    business_number varchar(12) not null,
    status varchar(20) not null,
    reviewer_id char(30),
    reviewer_note text,
    -- The decision time the queue row carries (reviewed_at); a submission has none, so its
    -- transaction time.
    changed_at timestamptz not null
);

create index business_verification_status_history_user_idx
    on business_verification_status_history(user_id, history_id);
create index business_verification_status_history_verification_idx
    on business_verification_status_history(verification_id, history_id);

-- security definer: the app role may only read the history; the trigger writes it.
create or replace function record_business_verification_status_history() returns trigger
language plpgsql
security definer
set search_path = public
as $$
begin
    insert into business_verification_status_history (
        verification_id, user_id, business_number, status,
        reviewer_id, reviewer_note, changed_at
    )
    values (
        new.id, new.user_id, new.business_number, new.status,
        new.reviewer_id, new.reviewer_note, coalesce(new.reviewed_at, now())
    );
    return null;
end $$;

create trigger trg_business_verification_status_history_insert
    after insert on business_verification_queue
    for each row
    execute function record_business_verification_status_history();

create trigger trg_business_verification_status_history_update
    after update of status on business_verification_queue
    for each row
    when (old.status is distinct from new.status)
    execute function record_business_verification_status_history();

create or replace function reject_business_verification_status_history_update()
returns trigger
language plpgsql
as $$
begin
    raise exception 'business_verification_status_history is append-only: UPDATE not allowed'
        using errcode = '45000';
end $$;

create trigger trg_business_verification_status_history_no_update
    before update on business_verification_status_history
    for each row
    execute function reject_business_verification_status_history_update();

grant select on business_verification_status_history to gongzzang_app_writer;
grant select on business_verification_status_history to gongzzang_app_reader;

-- Existing queue rows start their history at their current status.
insert into business_verification_status_history (
    verification_id, user_id, business_number, status,
    reviewer_id, reviewer_note, changed_at
)
select id, user_id, business_number, status, reviewer_id, reviewer_note,
       coalesce(reviewed_at, submitted_at)
from business_verification_queue;

-- backfill:begin
-- Move the user-row copy into the queue before the columns go, so no fact is lost. The id
-- reuses the user's own ULID ('usr_<ulid>' -> 'bvq_<ulid>'): valid, and one row per user.
-- A verified business with no approved queue row for that number becomes an approved row.
insert into business_verification_queue (
    id, user_id, business_number, submitted_documents, status,
    reviewer_note, submitted_at, reviewed_at
)
select 'bvq_' || substr(u.id, 5), u.id, u.business_number, '[]'::jsonb, 'approved',
       'carried over from user.business_verified_at (ADR-0258)',
       u.business_verified_at, u.business_verified_at
from "user" u
where u.business_verified_at is not null
  and u.business_number is not null
  and not exists (
      select 1 from business_verification_queue q
      where q.user_id = u.id
        and q.business_number = u.business_number
        and q.status = 'approved'
  );

-- A number kept without a verification in force (User::revoke_business_verification kept
-- the number and cleared the time) becomes a revoked row, so the number is not lost.
insert into business_verification_queue (
    id, user_id, business_number, submitted_documents, status,
    reviewer_note, submitted_at, reviewed_at
)
select 'bvq_' || substr(u.id, 5), u.id, u.business_number, '[]'::jsonb, 'revoked',
       'carried over from user.business_number without a verification in force (ADR-0258)',
       u.updated_at, u.updated_at
from "user" u
where u.business_verified_at is null
  and u.business_number is not null
  and not exists (
      select 1 from business_verification_queue q
      where q.user_id = u.id and q.business_number = u.business_number
  );

drop index if exists user_business_number_idx;
alter table "user" drop column business_verified_at;
alter table "user" drop column business_number;
-- backfill:end
