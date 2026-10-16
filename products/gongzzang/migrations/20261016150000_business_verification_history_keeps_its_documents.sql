-- A business verification's history keeps the documents of every submission (root ADR-0264).
--
-- business_verification_status_history (root ADR-0258) records each status, but not
-- submitted_documents. A resubmission (needs_more_info -> pending) overwrites the queue row's
-- documents, so the documents first submitted were lost; a document change without a status
-- change was not recorded at all. The history now carries the documents, the trigger also fires
-- when the documents or the number change, and the history refuses DELETE as well as UPDATE.

-- Rows recorded before this migration did not keep their documents: NULL means not recorded.
alter table business_verification_status_history add column submitted_documents jsonb;

create or replace function record_business_verification_status_history() returns trigger
language plpgsql
security definer
set search_path = public
as $$
begin
    insert into business_verification_status_history (
        verification_id, user_id, business_number, status,
        reviewer_id, reviewer_note, changed_at, submitted_documents
    )
    values (
        new.id, new.user_id, new.business_number, new.status,
        new.reviewer_id, new.reviewer_note, coalesce(new.reviewed_at, now()),
        new.submitted_documents
    );
    return null;
end $$;

drop trigger trg_business_verification_status_history_update on business_verification_queue;

create trigger trg_business_verification_status_history_update
    after update of status, submitted_documents, business_number on business_verification_queue
    for each row
    when (old.status is distinct from new.status
          or old.submitted_documents is distinct from new.submitted_documents
          or old.business_number is distinct from new.business_number)
    execute function record_business_verification_status_history();

create or replace function reject_business_verification_status_history_update()
returns trigger
language plpgsql
as $$
begin
    raise exception 'business_verification_status_history is append-only: % not allowed', tg_op
        using errcode = '45000';
end $$;

drop trigger trg_business_verification_status_history_no_update
    on business_verification_status_history;

create trigger trg_business_verification_status_history_append_only
    before update or delete on business_verification_status_history
    for each row
    execute function reject_business_verification_status_history_update();
