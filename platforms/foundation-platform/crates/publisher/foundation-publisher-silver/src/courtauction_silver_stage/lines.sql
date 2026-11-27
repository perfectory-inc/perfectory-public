-- An object's lines as the job read them (root ADR-0616 §3-1: the typed read): each line's number, its
-- envelope as JSON (`j`, without the body), whether it is a read (`read_outcome`), and its body as
-- `json.loads(line.get("body") or "")` read it (None when it is not JSON), parsed once.
--
-- DuckDB reads the file itself (`read_json`): the envelope's fields are declared as JSON, so the
-- lane's value rules read the same JSON values as before (a missing field and a null one read alike,
-- as `line.get` read them); `WITH ORDINALITY` numbers the non-blank lines in file order, as the
-- publisher numbered them. A line that is not JSON, or not an object, refuses the object.
-- The roles whose lines are too long for this (a search's 165 KiB answers, a notice's lists) read
-- the file themselves (searches.sql, sale_notices.sql).
-- Parameters: {object_path}, {maximum_line_bytes} (silver_load.maximum_line_bytes).
CREATE OR REPLACE TEMP TABLE ca_parsed AS
SELECT ordinality AS line_number,
       json_object('request', request, 'http_status', http_status, 'read_outcome', read_outcome,
                   'requested_at_utc', requested_at_utc, 'list_fingerprint', list_fingerprint,
                   'photo_count', photo_count, 'operation', operation, 'facts', facts, 'sha256', sha256,
                   'media_format', media_format, 'size_bytes', size_bytes, 'outcome', outcome) AS j,
       coalesce(json_extract_string(read_outcome, '$') = 'read', false) AS read,
       TRY_CAST(CASE WHEN py_truthy(body) THEN json_extract_string(body, '$') END AS JSON) AS body
FROM read_json('{object_path}', format = 'newline_delimited', records = true, maximum_object_size = {maximum_line_bytes},
               columns = {request: 'JSON', http_status: 'JSON', read_outcome: 'JSON', requested_at_utc: 'JSON',
                          list_fingerprint: 'JSON', photo_count: 'JSON', operation: 'JSON', facts: 'JSON',
                          sha256: 'JSON', media_format: 'JSON', size_bytes: 'JSON', outcome: 'JSON',
                          body: 'JSON'})
     WITH ORDINALITY;
