CREATE OR REPLACE TEMP TABLE onbid_values AS
SELECT line_number, ordinal, json_extract(item, ['$."cltrMngNo"', '$."pbctCdtnNo"', '$."sqmsList"', '$."apslEvlClgList"', '$."leasInfList"', '$."rgstPrmrInfList"', '$."dtbtRqrMtrsList"', '$."ocpyRelList"', '$."papsInf"', '$."zadrNm"', '$."cltrRadr"', '$."locVntyPscdCont"', '$."icdlCdtnCont"', '$."utlzPscdCont"', '$."cltrEtcCont"', '$."dsplVldCont"', '$."purrQlfcCont"', '$."pytnMtrsCont"', '$."frstPbancYmd"', '$."batcBidCltrClgList"', '$."crtnLstClgList"']) AS v
FROM onbid_items;

CREATE OR REPLACE TEMP TABLE onbid_built AS
SELECT line_number, ordinal,
  CAST(py_text(v[1]) AS VARCHAR) AS cltr_mng_no,
  CAST(onbid_whole(v[2]) AS BIGINT) AS pbct_cdtn_no,
  CAST(onbid_json_text(v[3], ['acpmPrptBondDclFlnm', 'acpmPrptOcpyIrpsFlnm', 'cltrInprNm']::VARCHAR[]) AS VARCHAR) AS sqms_list_json,
  CAST(onbid_json_text(v[4], ['acpmPrptBondDclFlnm', 'acpmPrptOcpyIrpsFlnm', 'cltrInprNm']::VARCHAR[]) AS VARCHAR) AS apsl_evl_clg_list_json,
  CAST(onbid_json_text(v[5], ['acpmPrptBondDclFlnm', 'acpmPrptOcpyIrpsFlnm', 'cltrInprNm']::VARCHAR[]) AS VARCHAR) AS leas_inf_list_json,
  CAST(onbid_json_text(v[6], ['acpmPrptBondDclFlnm', 'acpmPrptOcpyIrpsFlnm', 'cltrInprNm']::VARCHAR[]) AS VARCHAR) AS rgst_prmr_inf_list_json,
  CAST(onbid_json_text(v[7], ['acpmPrptBondDclFlnm', 'acpmPrptOcpyIrpsFlnm', 'cltrInprNm']::VARCHAR[]) AS VARCHAR) AS dtbt_rqr_mtrs_list_json,
  CAST(onbid_json_text(v[8], ['acpmPrptBondDclFlnm', 'acpmPrptOcpyIrpsFlnm', 'cltrInprNm']::VARCHAR[]) AS VARCHAR) AS ocpy_rel_list_json,
  CAST(onbid_json_text(v[9], ['acpmPrptBondDclFlnm', 'acpmPrptOcpyIrpsFlnm', 'cltrInprNm']::VARCHAR[]) AS VARCHAR) AS paps_inf_json,
  CAST(py_text(v[10]) AS VARCHAR) AS zadr_nm,
  CAST(py_text(v[11]) AS VARCHAR) AS cltr_radr,
  CAST(py_text(v[12]) AS VARCHAR) AS loc_vnty_pscd_cont,
  CAST(py_text(v[13]) AS VARCHAR) AS icdl_cdtn_cont,
  CAST(py_text(v[14]) AS VARCHAR) AS utlz_pscd_cont,
  CAST(py_text(v[15]) AS VARCHAR) AS cltr_etc_cont,
  CAST(py_text(v[16]) AS VARCHAR) AS dspl_vld_cont,
  CAST(py_text(v[17]) AS VARCHAR) AS purr_qlfc_cont,
  CAST(py_text(v[18]) AS VARCHAR) AS pytn_mtrs_cont,
  CAST(onbid_ymd(v[19]) AS DATE) AS frst_pbanc_ymd,
  CAST(onbid_json_text(v[20], ['acpmPrptBondDclFlnm', 'acpmPrptOcpyIrpsFlnm', 'cltrInprNm']::VARCHAR[]) AS VARCHAR) AS batc_bid_cltr_clg_list_json,
  CAST(onbid_json_text(v[21], ['acpmPrptBondDclFlnm', 'acpmPrptOcpyIrpsFlnm', 'cltrInprNm']::VARCHAR[]) AS VARCHAR) AS crtn_lst_clg_list_json,
  CAST('{object_key}' AS VARCHAR) AS source_record_id,
  CAST(line_number AS INTEGER) AS source_line_number,
  CAST('{snapshot_id}' AS VARCHAR) AS source_snapshot_id,
  CAST(to_timestamp({ingested_at_epoch_seconds}) AS TIMESTAMPTZ) AS ingested_at_utc
FROM onbid_values;

-- One row per key: the object's last line that holds it (a later page is the newer answer).
CREATE OR REPLACE TEMP TABLE onbid_rows AS
SELECT * EXCLUDE (line_number, ordinal, onbid_last) FROM (
  SELECT *, row_number() OVER (PARTITION BY cltr_mng_no ORDER BY line_number DESC, ordinal DESC) AS onbid_last
  FROM onbid_built WHERE NOT (cltr_mng_no IS NULL)
) WHERE onbid_last = 1;
