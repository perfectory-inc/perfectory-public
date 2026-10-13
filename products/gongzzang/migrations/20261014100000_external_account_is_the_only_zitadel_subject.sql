-- Root ADR-0232: the Zitadel subject lives only in external_account (provider = 'zitadel').
-- "user".zitadel_sub was a second copy whose write could fail on its own; it is dropped.

-- One Zitadel subject per user (lookups by user id must return a single subject).
CREATE UNIQUE INDEX external_account_one_zitadel_per_user_idx
    ON external_account (user_id) WHERE provider = 'zitadel';

-- Backfill any user whose best-effort link never landed. Id = 'ext_' + 26 Crockford chars.
INSERT INTO external_account (id, user_id, provider, external_id)
SELECT 'ext_0' || upper(substr(md5(u.id), 1, 25)), u.id, 'zitadel', u.zitadel_sub
FROM "user" u
WHERE NOT EXISTS (
    SELECT 1 FROM external_account ea
    WHERE ea.user_id = u.id AND ea.provider = 'zitadel'
);

ALTER TABLE "user" DROP COLUMN zitadel_sub;
