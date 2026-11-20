CREATE OR REPLACE TEMP TABLE onbid_values AS
SELECT line_number, ordinal, json_extract(item, ['$."pbancMngNo"', '$."pbctNo"', '$."onbidPbancNo"', '$."pbancKindCd"', '$."pbancKindNm"', '$."prptDivCd"', '$."prptDivNm"', '$."dspsMthodCd"', '$."dspsMthodNm"', '$."onbidPbancNm"', '$."pbancOrgNm"', '$."pbancYmd"', '$."bidMthodCd"', '$."bidMthodNm"', '$."cptnMthodCd"', '$."cptnMthodNm"', '$."totalamtUnpcDivCd"', '$."totalamtUnpcDivNm"', '$."bidDivCd"', '$."bidDivNm"', '$."bidAmtRlsYn"', '$."pbancNsqNm"', '$."pbancRtrcnRsnCont"', '$."rltnPbancMngNo"', '$."rltnPbancNm"', '$."ptctCmsn"', '$."pbancPtctQlfcCont"', '$."anncmAlCont"', '$."atchFileList"']) AS v
FROM onbid_items;

CREATE OR REPLACE TEMP TABLE onbid_built AS
SELECT line_number, ordinal,
  CAST(py_text(v[1]) AS VARCHAR) AS pbanc_mng_no,
  CAST(onbid_whole(v[2]) AS BIGINT) AS pbct_no,
  CAST(onbid_whole(v[3]) AS BIGINT) AS onbid_pbanc_no,
  CAST(py_text(v[4]) AS VARCHAR) AS pbanc_kind_cd,
  CAST(py_text(v[5]) AS VARCHAR) AS pbanc_kind_nm,
  CAST(py_text(v[6]) AS VARCHAR) AS prpt_div_cd,
  CAST(py_text(v[7]) AS VARCHAR) AS prpt_div_nm,
  CAST(py_text(v[8]) AS VARCHAR) AS dsps_mthod_cd,
  CAST(py_text(v[9]) AS VARCHAR) AS dsps_mthod_nm,
  CAST(py_text(v[10]) AS VARCHAR) AS onbid_pbanc_nm,
  CAST(py_text(v[11]) AS VARCHAR) AS pbanc_org_nm,
  CAST(onbid_ymd(v[12]) AS DATE) AS pbanc_ymd,
  CAST(py_text(v[13]) AS VARCHAR) AS bid_mthod_cd,
  CAST(py_text(v[14]) AS VARCHAR) AS bid_mthod_nm,
  CAST(py_text(v[15]) AS VARCHAR) AS cptn_mthod_cd,
  CAST(py_text(v[16]) AS VARCHAR) AS cptn_mthod_nm,
  CAST(py_text(v[17]) AS VARCHAR) AS totalamt_unpc_div_cd,
  CAST(py_text(v[18]) AS VARCHAR) AS totalamt_unpc_div_nm,
  CAST(py_text(v[19]) AS VARCHAR) AS bid_div_cd,
  CAST(py_text(v[20]) AS VARCHAR) AS bid_div_nm,
  CAST(py_text(v[21]) AS VARCHAR) AS bid_amt_rls_yn,
  CAST(py_text(v[22]) AS VARCHAR) AS pbanc_nsq_nm,
  CAST(py_text(v[23]) AS VARCHAR) AS pbanc_rtrcn_rsn_cont,
  CAST(py_text(v[24]) AS VARCHAR) AS rltn_pbanc_mng_no,
  CAST(py_text(v[25]) AS VARCHAR) AS rltn_pbanc_nm,
  CAST(onbid_whole(v[26]) AS BIGINT) AS ptct_cmsn,
  CAST(py_text(v[27]) AS VARCHAR) AS pbanc_ptct_qlfc_cont,
  CAST(py_text(v[28]) AS VARCHAR) AS anncm_al_cont,
  CAST(onbid_json_text(v[29], []::VARCHAR[]) AS VARCHAR) AS atch_file_list_json,
  CAST('{object_key}' AS VARCHAR) AS source_record_id,
  CAST(line_number AS INTEGER) AS source_line_number,
  CAST('{snapshot_id}' AS VARCHAR) AS source_snapshot_id,
  CAST(to_timestamp({ingested_at_epoch_seconds}) AS TIMESTAMPTZ) AS ingested_at_utc
FROM onbid_values;

-- One row per key: the object's last line that holds it (a later page is the newer answer).
CREATE OR REPLACE TEMP TABLE onbid_rows AS
SELECT * EXCLUDE (line_number, ordinal, onbid_last) FROM (
  SELECT *, row_number() OVER (PARTITION BY pbanc_mng_no, pbct_no ORDER BY line_number DESC, ordinal DESC) AS onbid_last
  FROM onbid_built WHERE NOT (pbanc_mng_no IS NULL OR pbct_no IS NULL)
) WHERE onbid_last = 1;
