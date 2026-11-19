CREATE OR REPLACE TEMP TABLE onbid_values AS
SELECT line_number, ordinal, json_extract(item, ['$."cltrMngNo"', '$."pbctCdtnNo"', '$."onbidCltrno"', '$."onbidPbancNo"', '$."pbancMngNo"', '$."pbctNo"', '$."pbctNsq"', '$."prptDivCd"', '$."pbctStatCd"', '$."usbdNft"', '$."apslEvlAmt"', '$."lowstBidPrcIndctCont"', '$."scfbAmt"', '$."vldBddrNope"', '$."nfctBddrNope"', '$."cltrBidBgngDt"', '$."cltrBidEndDt"', '$."cltrOpbdDt"', '$."bidAmtClgCont"', '$."pbctStatNm"', '$."prptDivNm"', '$."dspsMthodCd"', '$."dspsMthodNm"', '$."cltrUsgLclsCtgrId"', '$."cltrUsgMclsCtgrId"', '$."cltrUsgSclsCtgrId"', '$."cltrUsgLclsCtgrNm"', '$."cltrUsgMclsCtgrNm"', '$."cltrUsgSclsCtgrNm"', '$."onbidCltrNm"', '$."pbctsn"', '$."orgNm"', '$."onbidPbancNm"', '$."bidMthodCd"', '$."bidMthodNm"', '$."cptnMthodCd"', '$."cptnMthodNm"', '$."totalamtUnpcDivCd"', '$."totalamtUnpcDivNm"', '$."opbdBgngDt"', '$."opbdCmptnDt"', '$."apslPrcCtrsScfbPrcRto"', '$."lowstBidCtrsScfbPrcRto"', '$."pbctTdpsCont"', '$."bidDivCd"', '$."bidDivNm"', '$."afsbRtrcnRsnCont"', '$."rtrcnRsnCont"', '$."apslPrcCtrsLowstBidRto"', '$."frstCtrsLowstBidPrcRto"', '$."feeRate"']) AS v
FROM onbid_items;

CREATE OR REPLACE TEMP TABLE onbid_built AS
SELECT line_number, ordinal,
  CAST(py_text(v[1]) AS VARCHAR) AS cltr_mng_no,
  CAST(onbid_whole(v[2]) AS BIGINT) AS pbct_cdtn_no,
  CAST(onbid_whole(v[3]) AS BIGINT) AS onbid_cltr_no,
  CAST(onbid_whole(v[4]) AS BIGINT) AS onbid_pbanc_no,
  CAST(py_text(v[5]) AS VARCHAR) AS pbanc_mng_no,
  CAST(onbid_whole(v[6]) AS BIGINT) AS pbct_no,
  CAST(py_text(v[7]) AS VARCHAR) AS pbct_nsq,
  CAST(py_text(v[8]) AS VARCHAR) AS prpt_div_cd,
  CAST(py_text(v[9]) AS VARCHAR) AS pbct_stat_cd,
  CAST(onbid_whole(v[10]) AS INTEGER) AS usbd_nft,
  CAST(onbid_whole(v[11]) AS BIGINT) AS appraisal_amount_krw,
  CAST(py_text(v[12]) AS VARCHAR) AS lowest_bid_price_raw,
  CAST(onbid_whole(v[12]) AS BIGINT) AS lowest_bid_price_krw,
  CAST(onbid_whole(v[13]) AS BIGINT) AS winning_bid_amount_krw,
  CAST(onbid_whole(v[14]) AS INTEGER) AS valid_bidder_count,
  CAST(onbid_whole(v[15]) AS INTEGER) AS invalid_bidder_count,
  CAST(onbid_kst(v[16]) AS TIMESTAMPTZ) AS bid_begins_at,
  CAST(onbid_kst(v[17]) AS TIMESTAMPTZ) AS bid_ends_at,
  CAST(onbid_kst(v[18]) AS TIMESTAMPTZ) AS opened_at,
  CAST(py_text(v[19]) AS VARCHAR) AS bid_amounts_raw,
  CAST('{object_key}' AS VARCHAR) AS source_record_id,
  CAST(line_number AS INTEGER) AS source_line_number,
  CAST('{snapshot_id}' AS VARCHAR) AS source_snapshot_id,
  CAST(to_timestamp({ingested_at_epoch_seconds}) AS TIMESTAMPTZ) AS ingested_at_utc,
  CAST(py_text(v[20]) AS VARCHAR) AS pbct_stat_nm,
  CAST(py_text(v[21]) AS VARCHAR) AS prpt_div_nm,
  CAST(py_text(v[22]) AS VARCHAR) AS dsps_mthod_cd,
  CAST(py_text(v[23]) AS VARCHAR) AS dsps_mthod_nm,
  CAST(py_text(v[24]) AS VARCHAR) AS cltr_usg_lcls_ctgr_id,
  CAST(py_text(v[25]) AS VARCHAR) AS cltr_usg_mcls_ctgr_id,
  CAST(py_text(v[26]) AS VARCHAR) AS cltr_usg_scls_ctgr_id,
  CAST(py_text(v[27]) AS VARCHAR) AS cltr_usg_lcls_ctgr_nm,
  CAST(py_text(v[28]) AS VARCHAR) AS cltr_usg_mcls_ctgr_nm,
  CAST(py_text(v[29]) AS VARCHAR) AS cltr_usg_scls_ctgr_nm,
  CAST(py_text(v[30]) AS VARCHAR) AS onbid_cltr_nm,
  CAST(py_text(v[31]) AS VARCHAR) AS pbctsn,
  CAST(py_text(v[32]) AS VARCHAR) AS org_nm,
  CAST(py_text(v[33]) AS VARCHAR) AS onbid_pbanc_nm,
  CAST(py_text(v[34]) AS VARCHAR) AS bid_mthod_cd,
  CAST(py_text(v[35]) AS VARCHAR) AS bid_mthod_nm,
  CAST(py_text(v[36]) AS VARCHAR) AS cptn_mthod_cd,
  CAST(py_text(v[37]) AS VARCHAR) AS cptn_mthod_nm,
  CAST(py_text(v[38]) AS VARCHAR) AS totalamt_unpc_div_cd,
  CAST(py_text(v[39]) AS VARCHAR) AS totalamt_unpc_div_nm,
  CAST(onbid_kst(v[40]) AS TIMESTAMPTZ) AS opbd_bgng_dt,
  CAST(onbid_kst(v[41]) AS TIMESTAMPTZ) AS opbd_cmptn_dt,
  CAST(onbid_decimal(v[42]) AS DOUBLE) AS apsl_prc_ctrs_scfb_prc_rto,
  CAST(onbid_decimal(v[43]) AS DOUBLE) AS lowst_bid_ctrs_scfb_prc_rto,
  CAST(py_text(v[44]) AS VARCHAR) AS pbct_tdps_cont,
  CAST(py_text(v[45]) AS VARCHAR) AS bid_div_cd,
  CAST(py_text(v[46]) AS VARCHAR) AS bid_div_nm,
  CAST(py_text(v[47]) AS VARCHAR) AS afsb_rtrcn_rsn_cont,
  CAST(py_text(v[48]) AS VARCHAR) AS rtrcn_rsn_cont,
  CAST(onbid_decimal(v[49]) AS DOUBLE) AS apsl_prc_ctrs_lowst_bid_rto,
  CAST(onbid_decimal(v[50]) AS DOUBLE) AS frst_ctrs_lowst_bid_prc_rto,
  CAST(onbid_decimal(v[51]) AS DOUBLE) AS fee_rate
FROM onbid_values;

-- One row per key: the object's last line that holds it (a later page is the newer answer).
CREATE OR REPLACE TEMP TABLE onbid_rows AS
SELECT * EXCLUDE (line_number, ordinal, onbid_last) FROM (
  SELECT *, row_number() OVER (PARTITION BY cltr_mng_no, pbct_cdtn_no ORDER BY line_number DESC, ordinal DESC) AS onbid_last
  FROM onbid_built WHERE CASE WHEN cltr_mng_no IS NULL OR pbct_cdtn_no IS NULL THEN error('{object_key} line ' || line_number || ': an item without cltr_mng_no and pbct_cdtn_no') ELSE true END
) WHERE onbid_last = 1;
