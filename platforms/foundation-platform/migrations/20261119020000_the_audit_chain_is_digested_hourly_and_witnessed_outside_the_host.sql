-- The audit chain is digested every hour and the digest is witnessed outside the host (root ADR-0569 §4).
--
-- A table owner can disable the chain's triggers, rewrite a row and recompute every hash after it;
-- the chain alone cannot show that. So every hour the rows since the last digest are checked and
-- summed up in one digest, whose SHA-256 the host signs and puts in the public Rekor transparency
-- log, which nobody can rewrite. A rewrite of any row a digest covers then disagrees with a digest
-- the world already holds. This is CloudTrail's log file validation: hourly digest files, each
-- naming the one before.
--
-- Everything that decides is here, in SQL, once: which range comes next, that its rows chain from
-- the last digest's head, the digest's bytes and hash. The job only carries the hash to Rekor and
-- back, and `audit.record_digest` computes the same digest again before it keeps the witness.

-- How many rows of [p_from, p_to] break the chain: a hash that is not the row's own, a `seq` that is
-- not the one before plus one, or a `prev_hash` that is not the `row_hash` of the row before (the
-- row before `p_from` included, so a range starts where the last one ended).
CREATE FUNCTION audit.chain_breaks(p_from bigint, p_to bigint) RETURNS bigint
LANGUAGE sql STABLE SET search_path = audit, pg_temp AS $$
    SELECT count(*) FROM (
        SELECT e.seq, e.prev_hash, e.row_hash, audit.row_hash(e) AS recomputed,
               lag(e.seq) OVER (ORDER BY e.seq) AS before_seq,
               lag(e.row_hash) OVER (ORDER BY e.seq) AS before_hash
        FROM audit.event AS e
        WHERE e.seq BETWEEN greatest(p_from - 1, 1) AND p_to
    ) AS c
    WHERE c.seq >= p_from
      AND (c.row_hash <> c.recomputed
           OR (c.before_seq IS NULL AND (c.seq <> 1 OR c.prev_hash <> decode(repeat('00', 32), 'hex')))
           OR (c.before_seq IS NOT NULL AND (c.seq <> c.before_seq + 1 OR c.prev_hash <> c.before_hash)))
$$;

-- The bytes a digest's hash covers, one jsonb object's text like a row's (`audit.canon`).
CREATE FUNCTION audit.digest_text(p_from bigint, p_to bigint, p_count bigint, p_head bytea, p_previous bytea)
RETURNS text LANGUAGE sql IMMUTABLE AS $$
    SELECT jsonb_build_object(
        'from_seq', p_from,
        'to_seq', p_to,
        'count', p_count,
        'head_hash', encode(p_head, 'hex'),
        'previous_digest_sha256', encode(p_previous, 'hex')
    )::text
$$;

-- The next digest: the rows after the last digest up to the newest, checked. None when no row is
-- new; an exception naming the range when a row there breaks the chain, so nothing is witnessed.
CREATE FUNCTION audit.next_digest()
RETURNS TABLE (from_seq bigint, to_seq bigint, count bigint, head_hash bytea,
               previous_digest_sha256 bytea, digest_text text, digest_sha256 bytea)
LANGUAGE plpgsql STABLE SET search_path = audit, pg_temp AS $$
DECLARE
    last_digest audit.digest;
    first_seq bigint;
    last_seq bigint;
    head bytea;
    broken bigint;
    text_value text;
BEGIN
    SELECT * INTO last_digest FROM audit.digest AS d ORDER BY d.to_seq DESC LIMIT 1;
    first_seq := coalesce(last_digest.to_seq, 0) + 1;
    SELECT max(e.seq) INTO last_seq FROM audit.event AS e;
    IF last_seq IS NULL OR last_seq < first_seq THEN
        RETURN;
    END IF;
    broken := audit.chain_breaks(first_seq, last_seq);
    IF broken > 0 THEN
        RAISE EXCEPTION 'the audit chain is broken in seq %..%: % rows do not chain (root ADR-0569 §4)',
            first_seq, last_seq, broken USING ERRCODE = 'data_corrupted';
    END IF;
    IF last_digest.to_seq IS NOT NULL
       AND (SELECT e.prev_hash FROM audit.event AS e WHERE e.seq = first_seq) <> last_digest.head_hash THEN
        RAISE EXCEPTION 'seq % does not chain from the last digest''s head (root ADR-0569 §4)', first_seq
            USING ERRCODE = 'data_corrupted';
    END IF;
    SELECT e.row_hash INTO head FROM audit.event AS e WHERE e.seq = last_seq;
    text_value := audit.digest_text(first_seq, last_seq, last_seq - first_seq + 1, head, last_digest.digest_sha256);
    RETURN QUERY SELECT first_seq, last_seq, last_seq - first_seq + 1, head, last_digest.digest_sha256,
                        text_value, sha256(convert_to(text_value, 'UTF8'));
END;
$$;

-- Keeps a witnessed digest. The digest is computed again here, so a caller can only keep the one
-- the chain makes, and only with the Rekor entry the witness answered.
CREATE FUNCTION audit.record_digest(p_to_seq bigint, p_digest_sha256 bytea, p_rekor_log_index bigint,
                                    p_rekor_bundle jsonb)
RETURNS void
LANGUAGE plpgsql SECURITY DEFINER SET search_path = audit, pg_temp AS $$
DECLARE
    next record;
BEGIN
    IF NOT EXISTS (SELECT 1 FROM audit.source WHERE source = 'audit' AND writer_role = session_user) THEN
        RAISE EXCEPTION 'role % may not record an audit digest (root ADR-0569 §4)', session_user
            USING ERRCODE = 'insufficient_privilege';
    END IF;
    IF p_rekor_log_index IS NULL OR p_rekor_bundle IS NULL OR jsonb_typeof(p_rekor_bundle) <> 'object' THEN
        RAISE EXCEPTION 'a digest is kept with its Rekor entry (root ADR-0569 §4)'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    SELECT * INTO next FROM audit.next_digest();
    IF NOT FOUND OR next.to_seq <> p_to_seq OR next.digest_sha256 <> p_digest_sha256 THEN
        RAISE EXCEPTION 'digest of seq ..% is not the chain''s next digest (root ADR-0569 §4)', p_to_seq
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    INSERT INTO audit.digest (to_seq, from_seq, count, head_hash, previous_digest_sha256, digest_sha256,
                              rekor_log_index, rekor_inclusion_proof)
    VALUES (next.to_seq, next.from_seq, next.count, next.head_hash, next.previous_digest_sha256,
            next.digest_sha256, p_rekor_log_index, p_rekor_bundle);
END;
$$;

-- The weekly check from the first row: every row chains, the digests cover the chain from seq 1
-- without a hole or an overlap, each names the one before, and each holds the head and hash the
-- rows make now. One problem a line; none when all holds. Rekor's side is checked by the job.
CREATE FUNCTION audit.verify_all() RETURNS SETOF text
LANGUAGE plpgsql STABLE SET search_path = audit, pg_temp AS $$
DECLARE
    d record;
    expected_from bigint := 1;
    previous bytea := NULL;
    head bytea;
    broken bigint;
    last_seq bigint;
BEGIN
    SELECT max(seq) INTO last_seq FROM audit.event;
    broken := audit.chain_breaks(1, coalesce(last_seq, 0));
    IF broken > 0 THEN
        RETURN NEXT format('%s rows do not chain in seq 1..%s', broken, last_seq);
    END IF;
    FOR d IN SELECT * FROM audit.digest ORDER BY to_seq LOOP
        IF d.from_seq <> expected_from THEN
            RETURN NEXT format('digest ..%s starts at %s, not %s', d.to_seq, d.from_seq, expected_from);
        END IF;
        IF d.previous_digest_sha256 IS DISTINCT FROM previous THEN
            RETURN NEXT format('digest ..%s does not name the digest before it', d.to_seq);
        END IF;
        SELECT row_hash INTO head FROM audit.event WHERE seq = d.to_seq;
        IF head IS DISTINCT FROM d.head_hash THEN
            RETURN NEXT format('digest ..%s holds a head the rows no longer make', d.to_seq);
        END IF;
        IF sha256(convert_to(audit.digest_text(d.from_seq, d.to_seq, d.count, d.head_hash, d.previous_digest_sha256), 'UTF8'))
           <> d.digest_sha256 THEN
            RETURN NEXT format('digest ..%s holds a hash its own fields do not make', d.to_seq);
        END IF;
        expected_from := d.to_seq + 1;
        previous := d.digest_sha256;
    END LOOP;
END;
$$;

REVOKE ALL ON FUNCTION audit.record_digest(bigint, bytea, bigint, jsonb) FROM PUBLIC;
