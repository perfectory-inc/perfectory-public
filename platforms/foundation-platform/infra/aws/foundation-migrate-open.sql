-- Before every migration (root ADR-0616 A1b, ADR-0177): the master lets the migrator create in the foundation
-- database, as the data host's bootstrap does on every deploy (infra/compose/bootstrap-foundation.sql). The
-- migrations create schemas and trusted extensions; finalize-foundation.sql, the migrate task's last step,
-- takes CREATE back. The first step of the migrate task (foundation-database.tf), as the master, connected to
-- `foundation`. Idempotent.
\set ON_ERROR_STOP on

GRANT CREATE ON DATABASE :"DBNAME" TO foundation_migrator;
GRANT USAGE, CREATE ON SCHEMA public TO foundation_migrator;
