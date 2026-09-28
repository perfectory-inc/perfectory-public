-- ADR-0112: edits that are not yet folded into the R2 base tiles.
-- Rows are append-only. A row may be deleted only after a fold has recorded that the served
-- tiles already contain it; nothing else may remove or rewrite an edit.
-- Statements are separated by a blank line so tests can apply this file statement by statement.
CREATE TABLE map_edit (
  change_seq INTEGER PRIMARY KEY AUTOINCREMENT,
  unit TEXT NOT NULL,
  feature_id TEXT NOT NULL,
  op TEXT NOT NULL CHECK (op IN ('upsert', 'delete')),
  geometry TEXT CHECK (geometry IS NULL OR json_valid(geometry)),
  properties TEXT NOT NULL CHECK (json_valid(properties) AND json_type(properties) = 'object'),
  editor TEXT NOT NULL CHECK (length(editor) BETWEEN 1 AND 128),
  edited_at TEXT NOT NULL,
  idempotency_key TEXT NOT NULL UNIQUE CHECK (length(idempotency_key) BETWEEN 1 AND 128),
  request_sha256 TEXT NOT NULL CHECK (length(request_sha256) = 64),
  CHECK ((op = 'upsert') = (geometry IS NOT NULL))
);

CREATE INDEX map_edit_unit_seq ON map_edit (unit, change_seq);

CREATE TABLE map_edit_fold (
  unit TEXT PRIMARY KEY,
  folded_through_change_seq INTEGER NOT NULL CHECK (folded_through_change_seq >= 0),
  release_id TEXT NOT NULL,
  folded_at TEXT NOT NULL
);

CREATE TRIGGER map_edit_is_append_only
BEFORE UPDATE ON map_edit
BEGIN
  SELECT RAISE(ABORT, 'map_edit rows are append-only');
END;

CREATE TRIGGER map_edit_retires_only_folded_rows
BEFORE DELETE ON map_edit
WHEN OLD.change_seq > COALESCE(
  (SELECT folded_through_change_seq FROM map_edit_fold WHERE unit = OLD.unit), 0)
BEGIN
  SELECT RAISE(ABORT, 'map_edit row is not folded into the served tiles');
END;

CREATE TRIGGER map_edit_fold_only_moves_forward
BEFORE UPDATE ON map_edit_fold
WHEN NEW.folded_through_change_seq < OLD.folded_through_change_seq
BEGIN
  SELECT RAISE(ABORT, 'a fold cannot move backwards');
END;
