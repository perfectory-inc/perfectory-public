CREATE OR REPLACE TEMP TABLE onbid_values AS
SELECT line_number, ordinal, json_extract(item, ['$."onbidPbancNo"', '$."pbctNo"', '$."pbancMngNo"', '$."pbctNsq"', '$."pbctsn"', '$."pbancKindCd"', '$."pbancKindNm"', '$."prptDivCd"', '$."prptDivNm"', '$."dspsMthodCd"', '$."dspsMthodNm"', '$."onbidPbancNm"', '$."orgNm"', '$."rspbSbrNm"', '$."pbancYmd"', '$."cltrOpbdDt"', '$."exctStatCd"', '$."exctStatNm"']) AS v
FROM onbid_items;

CREATE OR REPLACE TEMP TABLE onbid_built AS
SELECT line_number, ordinal,
  CAST(onbid_whole(v[1]) AS BIGINT) AS onbid_pbanc_no,
  CAST(onbid_whole(v[2]) AS BIGINT) AS pbct_no,
  CAST(py_text(v[3]) AS VARCHAR) AS pbanc_mng_no,
  CAST(py_text(v[4]) AS VARCHAR) AS pbct_nsq,
  CAST(py_text(v[5]) AS VARCHAR) AS pbctsn,
  CAST(py_text(v[6]) AS VARCHAR) AS pbanc_kind_cd,
  CAST(py_text(v[7]) AS VARCHAR) AS pbanc_kind_nm,
  CAST(py_text(v[8]) AS VARCHAR) AS prpt_div_cd,
  CAST(py_text(v[9]) AS VARCHAR) AS prpt_div_nm,
  CAST(py_text(v[10]) AS VARCHAR) AS dsps_mthod_cd,
  CAST(py_text(v[11]) AS VARCHAR) AS dsps_mthod_nm,
  CAST(py_text(v[12]) AS VARCHAR) AS onbid_pbanc_nm,
  CAST(py_text(v[13]) AS VARCHAR) AS org_nm,
  CAST(py_text(v[14]) AS VARCHAR) AS rspb_sbr_nm,
  CAST(onbid_ymd(v[15]) AS DATE) AS pbanc_ymd,
  CAST(onbid_kst(v[16]) AS TIMESTAMPTZ) AS cltr_opbd_dt,
  CAST(py_text(v[17]) AS VARCHAR) AS exct_stat_cd,
  CAST(py_text(v[18]) AS VARCHAR) AS exct_stat_nm,
  CAST('{object_key}' AS VARCHAR) AS source_record_id,
  CAST(line_number AS INTEGER) AS source_line_number,
  CAST('{snapshot_id}' AS VARCHAR) AS source_snapshot_id,
  CAST(to_timestamp({ingested_at_epoch_seconds}) AS TIMESTAMPTZ) AS ingested_at_utc
FROM onbid_values;

-- One row per key: the object's last line that holds it (a later page is the newer answer).
CREATE OR REPLACE TEMP TABLE onbid_rows AS
SELECT * EXCLUDE (line_number, ordinal, onbid_last) FROM (
  SELECT *, row_number() OVER (PARTITION BY onbid_pbanc_no, pbct_no ORDER BY line_number DESC, ordinal DESC) AS onbid_last
  FROM onbid_built WHERE NOT (onbid_pbanc_no IS NULL OR pbct_no IS NULL)
) WHERE onbid_last = 1;
