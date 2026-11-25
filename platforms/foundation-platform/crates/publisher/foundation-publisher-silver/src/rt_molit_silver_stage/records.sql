-- One export's records as DuckDB's CSV reader reads them (root ADR-0349 §2, ADR-0364; root ADR-0616 §3-1:
-- DuckDB reads the provider's file, the publisher only transcodes it to UTF-8).
--
-- {csv}: the export from its header line on, UTF-8. {columns}: `c000` … as VARCHAR, more than a header
-- holds, so a record with more cells than its header still reads them (`null_padding` fills the rest
-- with NULL, and an empty cell stays '' because no cell reads as NULL). {first_line}: the line of the
-- Bronze object the header is on.
--
-- `rt_cells_{i}` (record, idx, value): every cell, the header being record 1. `rt_records_{i}`: per record
-- after the header, its cell count, its first cell stripped (the provider's `NO`), whether every cell is
-- blank (such a record is skipped, as `csv.reader` reading an empty line), the line of the Bronze object
-- it starts on (the header's, plus one per record before it and per line end inside their quoted cells),
-- and its cells quoted again as the line the rejected table keeps.
CREATE OR REPLACE TEMP TABLE rt_cells_{i} AS
WITH raw AS (
  SELECT * FROM read_csv('{csv}', header = false, columns = {columns}, delim = ',', quote = '"', escape = '"',
                         strict_mode = false, null_padding = true, nullstr = chr(0), allow_quoted_nulls = false,
                         auto_detect = false, parallel = false)
  WITH ORDINALITY
)
SELECT ordinality AS record, CAST(substr(cell, 2) AS INTEGER) AS idx, value
FROM raw UNPIVOT (value FOR cell IN (COLUMNS('^c[0-9]+$')));

CREATE OR REPLACE TEMP TABLE rt_header_{i} AS
SELECT idx, value AS name FROM rt_cells_{i} WHERE record = 1;

CREATE OR REPLACE TEMP TABLE rt_records_{i} AS
WITH per_record AS (
  SELECT record,
         count(*) AS cells,
         max(CASE WHEN idx = 0 THEN rt_strip(value) END) AS first_cell,
         bool_and(rt_strip(value) = '') AS blank,
         sum(length(value) - length(replace(value, chr(10), ''))) AS line_ends,
         '"' || string_agg(value, '","' ORDER BY idx) || '"' AS raw_line
  FROM rt_cells_{i}
  WHERE record > 1
  GROUP BY record
)
SELECT record, cells, first_cell, blank, raw_line,
       CAST({first_line} + record - 1
            + coalesce(sum(line_ends) OVER (ORDER BY record ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0)
            AS INTEGER) AS source_line_number
FROM per_record;
