-- No role may delete audit_log rows while nothing archives them (root ADR-0264).
--
-- 20260719000106 granted gongzzang_audit_archiver DELETE on audit_log, and 20260719000107 exempted
-- that role from both immutability triggers, for a "1 year in RDS, then 6 years in R2" retention.
-- The archive that retention assumes was never built: no job copies audit_log to R2, so any
-- delete through that role would lose the rows for good. Until an archive exists and proves what
-- it copied, the role keeps SELECT only and the triggers refuse every UPDATE and DELETE.

revoke delete on audit_log from gongzzang_audit_archiver;

create or replace function reject_audit_mutation() returns trigger
language plpgsql
as $$
begin
    raise exception 'audit_log is immutable: % not allowed (current_user=%)', tg_op, current_user
        using errcode = '45000';
end $$;

drop trigger trg_audit_no_update on audit_log;
create trigger trg_audit_no_update
    before update on audit_log
    for each row
    execute function reject_audit_mutation();

drop trigger trg_audit_no_delete on audit_log;
create trigger trg_audit_no_delete
    before delete on audit_log
    for each row
    execute function reject_audit_mutation();
