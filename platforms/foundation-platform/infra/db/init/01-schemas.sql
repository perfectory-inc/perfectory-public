-- Foundation container initialization creates only its local namespace.
-- Table definitions and runtime schemas are owned by the Foundation SQLx baseline.

CREATE SCHEMA IF NOT EXISTS catalog;

COMMENT ON SCHEMA catalog IS
    'Foundation Catalog context for canonical and collected data';
