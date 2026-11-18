-- A source's account is tied to a staff member by a validity interval (root ADR-0569).
--
-- An audit event names its actor by the source's immutable id: `github:<numeric user id>`,
-- `unix:<uid>@<host>`. This table says which staff member that id belonged to, and when. It is
-- append-only: linking appends a row with an open `valid_to`; unlinking appends a row for the same
-- link (`staff_id, provider, external_id, valid_from`) with `valid_to` filled. The latest record of
-- a link is its state. An event is read with the link valid at its `occurred_at`; no row is ever
-- rewritten, so an event read last year reads the same today.

CREATE TABLE identity.staff_external_account (
    record_seq   BIGINT GENERATED ALWAYS AS IDENTITY
        CONSTRAINT identity_staff_external_account_pkey PRIMARY KEY,
    staff_id     UUID NOT NULL,
    provider     TEXT NOT NULL,
    external_id  TEXT NOT NULL,
    valid_from   TIMESTAMPTZ NOT NULL,
    valid_to     TIMESTAMPTZ,
    linked_by    UUID NOT NULL,
    recorded_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT identity_staff_external_account_staff_fkey
        FOREIGN KEY (staff_id) REFERENCES identity.staff(id),
    CONSTRAINT identity_staff_external_account_linked_by_fkey
        FOREIGN KEY (linked_by) REFERENCES identity.staff(id),
    -- The source's immutable id only: a login name is renamed and reused.
    CONSTRAINT identity_staff_external_account_id_check CHECK (
        (provider = 'github' AND external_id ~ '^[1-9][0-9]{0,19}$')
        OR (provider = 'unix' AND external_id ~ '^[0-9]{1,10}@[a-z0-9][a-z0-9-]{0,62}$')
    ),
    CONSTRAINT identity_staff_external_account_interval_check
        CHECK (valid_to IS NULL OR valid_to >= valid_from)
);

CREATE INDEX identity_staff_external_account_latest_idx
    ON identity.staff_external_account (provider, external_id, record_seq DESC);
CREATE INDEX identity_staff_external_account_staff_idx
    ON identity.staff_external_account (staff_id, record_seq DESC);

CREATE FUNCTION identity.reject_staff_external_account_mutation()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, identity, pg_temp
AS $function$
BEGIN
    RAISE EXCEPTION 'staff external account records are append-only; append the unlink instead'
        USING ERRCODE = '42501';
END;
$function$;

-- Statement-level, so a rewrite is refused even when it matches no row.
CREATE TRIGGER staff_external_account_append_only
BEFORE UPDATE OR DELETE OR TRUNCATE ON identity.staff_external_account
FOR EACH STATEMENT EXECUTE FUNCTION identity.reject_staff_external_account_mutation();

REVOKE ALL ON identity.staff_external_account FROM PUBLIC;
