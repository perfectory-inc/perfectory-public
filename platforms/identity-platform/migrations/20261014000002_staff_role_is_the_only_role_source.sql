-- A staff role lives only in identity.staff_role (root ADR-0213).
--
-- `identity.staff.primary_role_code` was a second copy: the effective-role read
-- unioned it with `identity.staff_role`, the bootstrap wrote both, and a role in
-- the column had no grantor, no grant time and no event. Every primary role that
-- has no matching grant row becomes one (granted by the staff member themself at
-- account creation — the only grantor the column ever implied), then the column
-- is dropped. Effective roles are unchanged by construction.

INSERT INTO identity.staff_role (staff_id, role_code, granted_at, granted_by)
SELECT staff.id, staff.primary_role_code, staff.created_at, staff.id
FROM identity.staff AS staff
WHERE NOT EXISTS (
    SELECT 1
    FROM identity.staff_role AS role
    WHERE role.staff_id = staff.id
      AND role.role_code = staff.primary_role_code
);

ALTER TABLE identity.staff DROP COLUMN primary_role_code;
