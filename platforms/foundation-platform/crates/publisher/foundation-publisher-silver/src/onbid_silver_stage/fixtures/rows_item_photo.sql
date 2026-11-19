CREATE OR REPLACE TEMP TABLE onbid_values AS
SELECT line_number, ordinal, json_extract(item, ['$."cltrMngNo"', '$."pbctCdtnNo"', '$."potoUrlList"', '$."apslEvlClgList"', '$."atchFileList"', '$."lmapUrlAdrListMedia"', '$."lrmUrlAdrListMedia"', '$."poto360DgrUrlListMedia"', '$."vdoUrlAdrListMedia"', '$."potoUrlListOriginal"']) AS v
FROM onbid_items;

CREATE OR REPLACE TEMP TABLE onbid_items_keyed AS
SELECT line_number, ordinal, v, coalesce(py_text(v[1]), '') AS cltr,
       onbid_whole(v[2]) AS pbct
FROM onbid_values;

CREATE OR REPLACE TEMP TABLE onbid_items_checked AS SELECT CASE WHEN cltr = '' OR pbct IS NULL THEN error('{object_key} line ' || line_number || ': an item without cltrMngNo and pbctCdtnNo') END AS refused FROM onbid_items_keyed;

-- One row per stored media file of each item; the last line, field and position that names an
-- (item, hash) wins.
CREATE OR REPLACE TEMP TABLE onbid_media AS
SELECT k.line_number, k.ordinal, k.cltr, k.pbct, s.source_index, s.media_kind, e.position, e.entry
FROM onbid_items_keyed k,
LATERAL (
  SELECT 0 AS source_index, 'photo' AS media_kind, v[3] AS entries
  UNION ALL
  SELECT 1 AS source_index, 'appraisal' AS media_kind, v[4] AS entries
  UNION ALL
  SELECT 2 AS source_index, 'announcement_attachment' AS media_kind, v[5] AS entries
  UNION ALL
  SELECT 3 AS source_index, 'location_map' AS media_kind, v[6] AS entries
  UNION ALL
  SELECT 4 AS source_index, 'cadastral_map' AS media_kind, v[7] AS entries
  UNION ALL
  SELECT 5 AS source_index, 'photo_360' AS media_kind, v[8] AS entries
  UNION ALL
  SELECT 6 AS source_index, 'video' AS media_kind, v[9] AS entries
  UNION ALL
  SELECT 7 AS source_index, 'photo_original' AS media_kind, v[10] AS entries
) s,
LATERAL (SELECT unnest(range(CAST(json_array_length(s.entries) AS BIGINT))) AS position, unnest(json_extract(s.entries, '$[*]')) AS entry) e
WHERE json_type(s.entries) = 'ARRAY' AND json_type(e.entry) = 'OBJECT'
  AND py_text(json_extract(e.entry, '$.sha256')) IS NOT NULL;

CREATE OR REPLACE TEMP TABLE onbid_rows AS
SELECT
  CAST(cltr AS VARCHAR) AS cltr_mng_no,
  CAST(pbct AS BIGINT) AS pbct_cdtn_no,
  CAST(py_text(json_extract(entry, '$.sha256')) AS VARCHAR) AS sha256,
  CAST(media_kind AS VARCHAR) AS media_kind,
  CAST(CASE WHEN list_contains(json_keys(entry), 'sequence') THEN onbid_whole(json_extract(entry, '$.sequence')) ELSE position END AS INTEGER) AS sequence,
  CAST(py_text(json_extract(entry, '$.apslEvlOrgNm')) AS VARCHAR) AS title,
  CAST(py_text(json_extract(entry, '$.media_format')) AS VARCHAR) AS media_format,
  CAST(onbid_whole(json_extract(entry, '$.size_bytes')) AS BIGINT) AS size_bytes,
  CAST('{object_key}' AS VARCHAR) AS source_record_id,
  CAST(line_number AS INTEGER) AS source_line_number,
  CAST('{snapshot_id}' AS VARCHAR) AS source_snapshot_id,
  CAST(to_timestamp({ingested_at_epoch_seconds}) AS TIMESTAMPTZ) AS ingested_at_utc
FROM (
  SELECT *, row_number() OVER (PARTITION BY cltr, pbct, py_text(json_extract(entry, '$.sha256'))
                               ORDER BY line_number DESC, ordinal DESC, source_index DESC, position DESC) AS onbid_last
  FROM onbid_media
) WHERE onbid_last = 1;
