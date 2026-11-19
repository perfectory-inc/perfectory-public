-- The hourly digest is made by the database and witnessed after it is kept (root ADR-0569 §4, ADR-0584 §2).
--
-- A table owner can disable the triggers and rewrite a row; the database alone cannot show that. So
-- every hour the rows since the last digest are summed up in one digest whose SHA-256 the host signs
-- into the public Rekor log, which nobody can rewrite: a rewrite of any row a digest covers then
-- disagrees with a hash the world already holds (CloudTrail's log file validation does the same).
--
-- Everything that decides is here, once. `audit.make_digest` keeps the next digest (its trigger,
-- `audit.digest_follows`, checks it against the rows); the job only signs `audit.digest_text` of each
-- digest not yet witnessed, and `audit.witness_digest` keeps the Rekor entry after checking the entry
-- is for that digest's hash. A failed upload leaves the digest unwitnessed and the next run signs it
-- again: one range never gets two digests.

-- The bytes a digest's hash covers and anyone re-hashes to check it: one jsonb object's text of these
-- seven fields (snapshots in their text form, hashes in lower-case hex, the first's missing values as
-- null). `audit.digest_sha256` is now the SHA-256 of exactly this, so the two cannot drift.
CREATE FUNCTION audit.digest_text(
    p_digest_no bigint, p_from pg_snapshot, p_to pg_snapshot, p_row_count bigint, p_rows_sha256 bytea,
    p_previous bytea
) RETURNS text
LANGUAGE sql STABLE PARALLEL SAFE AS $$
    SELECT jsonb_build_object(
        'schema', 'audit.digest.v1',
        'digest_no', p_digest_no,
        'from_snapshot', p_from::text,
        'to_snapshot', p_to::text,
        'row_count', p_row_count,
        'rows_sha256', encode(p_rows_sha256, 'hex'),
        'previous_digest_sha256', encode(p_previous, 'hex')
    )::text
$$;

CREATE OR REPLACE FUNCTION audit.digest_sha256(
    p_digest_no bigint, p_from pg_snapshot, p_to pg_snapshot, p_row_count bigint, p_rows_sha256 bytea,
    p_previous bytea
) RETURNS bytea
LANGUAGE sql STABLE PARALLEL SAFE AS $$
    SELECT sha256(convert_to(audit.digest_text(p_digest_no, p_from, p_to, p_row_count, p_rows_sha256,
                                               p_previous), 'UTF8'))
$$;

-- Keeps the next digest: the rows the current snapshot sees and the last digest's did not. None when
-- there is no such row (an hour with no change costs no Rekor entry). Serialized on the digest table,
-- so two runs at once make one digest and the other finds nothing new.
CREATE FUNCTION audit.make_digest() RETURNS SETOF audit.digest
LANGUAGE plpgsql SECURITY DEFINER SET search_path = audit, pg_temp AS $$
DECLARE
    prev audit.digest;
    snap pg_snapshot;
    n_rows bigint;
    hash bytea;
    made audit.digest;
BEGIN
    LOCK TABLE audit.digest IN SHARE ROW EXCLUSIVE MODE;
    SELECT * INTO prev FROM audit.digest ORDER BY digest_no DESC LIMIT 1;
    snap := pg_current_snapshot();
    SELECT count(*) INTO n_rows FROM audit.rows_between(prev.to_snapshot, snap);
    IF n_rows = 0 THEN
        RETURN;
    END IF;
    hash := audit.range_sha256(prev.to_snapshot, snap);
    INSERT INTO audit.digest (digest_no, from_snapshot, to_snapshot, row_count, rows_sha256,
                              previous_digest_sha256, digest_sha256)
    VALUES (coalesce(prev.digest_no, 0) + 1, prev.to_snapshot, snap, n_rows, hash, prev.digest_sha256,
            audit.digest_sha256(coalesce(prev.digest_no, 0) + 1, prev.to_snapshot, snap, n_rows, hash,
                                prev.digest_sha256))
    RETURNING * INTO made;
    RETURN NEXT made;
END;
$$;

-- Keeps a digest's Rekor entry: the cosign bundle (sigstore bundle v0.3, what cosign 3 `sign-blob
-- --bundle` writes) must be for this digest's hash, and its log index is read from it, so a caller
-- cannot attach another entry. One witness per digest; a second is refused by the key.
CREATE FUNCTION audit.witness_digest(p_digest_no bigint, p_bundle jsonb) RETURNS bigint
LANGUAGE plpgsql SECURITY DEFINER SET search_path = audit, pg_temp AS $$
DECLARE
    d audit.digest;
    signed bytea;
    log_index bigint;
BEGIN
    SELECT * INTO d FROM audit.digest WHERE digest_no = p_digest_no;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'there is no digest % (root ADR-0584 §2)', p_digest_no
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    BEGIN
        signed := decode(p_bundle #>> '{messageSignature,messageDigest,digest}', 'base64');
        log_index := (p_bundle #>> '{verificationMaterial,tlogEntries,0,logIndex}')::bigint;
    EXCEPTION WHEN OTHERS THEN
        signed := NULL;
    END;
    IF signed IS DISTINCT FROM d.digest_sha256
       OR p_bundle #>> '{messageSignature,messageDigest,algorithm}' IS DISTINCT FROM 'SHA2_256'
       OR log_index IS NULL OR log_index < 0 THEN
        RAISE EXCEPTION 'the bundle is not a Rekor entry for digest % (root ADR-0584 §2)', p_digest_no
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    INSERT INTO audit.digest_witness (digest_no, rekor_log_index, rekor_inclusion_proof)
    VALUES (p_digest_no, log_index, p_bundle);
    RETURN log_index;
END;
$$;

-- The weekly check from the first digest: each follows the one before, holds the hash its own fields
-- make, and still matches its rows. One problem a line; none when all holds. The ranges cover every
-- row by construction (each snapshot sees all the one before saw), so a row planted later with any
-- `txid` changes some range's hash. Rekor's side is the job's; a digest still waiting for its witness
-- is the importer's two-hour alarm, not this check's.
CREATE FUNCTION audit.verify_all() RETURNS SETOF text
LANGUAGE plpgsql STABLE SET search_path = audit, pg_temp AS $$
DECLARE
    d audit.digest;
    prev audit.digest;
BEGIN
    FOR d IN SELECT * FROM audit.digest ORDER BY digest_no LOOP
        IF d.digest_no <> coalesce(prev.digest_no, 0) + 1
           OR d.from_snapshot::text IS DISTINCT FROM prev.to_snapshot::text
           OR d.previous_digest_sha256 IS DISTINCT FROM prev.digest_sha256 THEN
            RETURN NEXT format('digest %s does not follow the digest before it', d.digest_no);
        END IF;
        IF d.digest_sha256 <> audit.digest_sha256(d.digest_no, d.from_snapshot, d.to_snapshot, d.row_count,
                                                  d.rows_sha256, d.previous_digest_sha256) THEN
            RETURN NEXT format('digest %s holds a hash its own fields do not make', d.digest_no);
        END IF;
        IF d.row_count <> (SELECT count(*) FROM audit.rows_between(d.from_snapshot, d.to_snapshot))
           OR d.rows_sha256 <> audit.range_sha256(d.from_snapshot, d.to_snapshot) THEN
            RETURN NEXT format('digest %s no longer matches its rows', d.digest_no);
        END IF;
        prev := d;
    END LOOP;
END;
$$;

-- Only the digest job's role makes and witnesses digests; the grant is the check, as for `audit.record`.
REVOKE ALL ON FUNCTION audit.make_digest() FROM PUBLIC;
REVOKE ALL ON FUNCTION audit.witness_digest(bigint, jsonb) FROM PUBLIC;
