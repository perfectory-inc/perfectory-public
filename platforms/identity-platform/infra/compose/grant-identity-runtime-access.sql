GRANT USAGE ON SCHEMA identity TO :"identity_api_role";
REVOKE UPDATE ON identity.staff FROM :"identity_api_role";
GRANT SELECT, INSERT ON identity.staff TO :"identity_api_role";
GRANT SELECT, INSERT ON identity.staff_role TO :"identity_api_role";
GRANT SELECT, INSERT, UPDATE ON identity.staff_session TO :"identity_api_role";
GRANT SELECT, INSERT, UPDATE ON identity.revoked_jti TO :"identity_api_role";
GRANT SELECT ON identity.service_principal TO :"identity_api_role";
GRANT SELECT ON identity.service_capability_grant TO :"identity_api_role";
GRANT INSERT ON identity.outbox_event TO :"identity_api_role";

GRANT USAGE ON SCHEMA identity TO :"identity_policy_worker_role";
GRANT SELECT, UPDATE ON identity.outbox_event TO :"identity_policy_worker_role";

GRANT USAGE ON SCHEMA identity TO :"identity_provisioner_role";
GRANT SELECT, INSERT, UPDATE ON identity.service_principal
    TO :"identity_provisioner_role";
-- Every subject and display name a principal held is an append-only record (root ADR-0264).
GRANT SELECT, INSERT ON identity.service_principal_record
    TO :"identity_provisioner_role";
-- Grants are an append-only ledger (root ADR-0212): the provisioner appends records and their
-- outbox events and reads the current-grant view. It cannot update or delete a record.
GRANT SELECT ON identity.service_capability_grant TO :"identity_provisioner_role";
GRANT SELECT, INSERT ON identity.service_capability_grant_record
    TO :"identity_provisioner_role";
GRANT INSERT ON identity.outbox_event TO :"identity_provisioner_role";
