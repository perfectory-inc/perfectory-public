CREATE OR REPLACE TEMP TABLE onbid_values AS
SELECT line_number, ordinal, json_extract(item, ['$."cltrMngNo"', '$."pbctCdtnNo"', '$."pbancMngNo"', '$."onbidPbancNm"', '$."tdpsSbtnDcmtYn"', '$."nrnkAplyPsblYn"', '$."pbctTdpsCont"', '$."pcmtPayMtdCont"', '$."pcmtPayTermCont"', '$."bidPtctAplyYn"', '$."bidVldCrtrCont"', '$."smnsIpDpcnBidBlcktYn"', '$."tdpsSbtnDcmtCont"', '$."tdpsSbtnDcmtSbmsnTermNm"', '$."tdpsSbtnDcmtSbmsnMthodNm"', '$."bidPtctAplyDcmtCont"', '$."bidPtctAplyTermNm"', '$."bidPtctDcmtSbmsnMthodNm"', '$."bidRltnDcmtCont"', '$."bidRltnDcmtSbmsnTermNm"', '$."bidRltnDcmtSbmsnMthodNm"', '$."collbMthodDcmtCont"', '$."collbDcmtSbmsnMthodNm"', '$."collbDcmtSbmsnTermNm"', '$."subtBidDcmtCont"', '$."subtBidDcmtSbmsnMthodNm"', '$."subtBidDcmtSbmsnTermNm"', '$."qlfcLmtCdtnCont"', '$."rgnLmtCdtnCont"', '$."etcLmtCdtnCont"', '$."evlPtdbClgCont"', '$."evlPerdCont"', '$."pytnMtrsCont"', '$."bidPrcAsmcCalcwCont"', '$."minEsureAmt"', '$."minEsureRate"', '$."crtrAmt"', '$."crtrRate"', '$."rsvPrcRngCont"', '$."rsvPrcChcNum"', '$."rsvPrcEtmtNum"', '$."tplmCrrt"', '$."rsnbHghprcCfct"', '$."crtrAmtBidNm"', '$."opsnCltrBidPsblYn"', '$."bidPsblCltrQnty"', '$."ptctCmsn"', '$."prpslEvlItemClgList"', '$."prcnNsqBidRsltNm"', '$."prcnNsqLowstBidPrc"', '$."prcnBidClgList"', '$."cseqBidInfClgList"']) AS v
FROM onbid_items;

CREATE OR REPLACE TEMP TABLE onbid_built AS
SELECT line_number, ordinal,
  CAST(py_text(v[1]) AS VARCHAR) AS cltr_mng_no,
  CAST(onbid_whole(v[2]) AS BIGINT) AS pbct_cdtn_no,
  CAST(py_text(v[3]) AS VARCHAR) AS pbanc_mng_no,
  CAST(py_text(v[4]) AS VARCHAR) AS onbid_pbanc_nm,
  CAST(py_text(v[5]) AS VARCHAR) AS tdps_sbtn_dcmt_yn,
  CAST(py_text(v[6]) AS VARCHAR) AS nrnk_aply_psbl_yn,
  CAST(py_text(v[7]) AS VARCHAR) AS pbct_tdps_cont,
  CAST(py_text(v[8]) AS VARCHAR) AS pcmt_pay_mtd_cont,
  CAST(py_text(v[9]) AS VARCHAR) AS pcmt_pay_term_cont,
  CAST(py_text(v[10]) AS VARCHAR) AS bid_ptct_aply_yn,
  CAST(py_text(v[11]) AS VARCHAR) AS bid_vld_crtr_cont,
  CAST(py_text(v[12]) AS VARCHAR) AS smns_ip_dpcn_bid_blckt_yn,
  CAST(py_text(v[13]) AS VARCHAR) AS tdps_sbtn_dcmt_cont,
  CAST(py_text(v[14]) AS VARCHAR) AS tdps_sbtn_dcmt_sbmsn_term_nm,
  CAST(py_text(v[15]) AS VARCHAR) AS tdps_sbtn_dcmt_sbmsn_mthod_nm,
  CAST(py_text(v[16]) AS VARCHAR) AS bid_ptct_aply_dcmt_cont,
  CAST(py_text(v[17]) AS VARCHAR) AS bid_ptct_aply_term_nm,
  CAST(py_text(v[18]) AS VARCHAR) AS bid_ptct_dcmt_sbmsn_mthod_nm,
  CAST(py_text(v[19]) AS VARCHAR) AS bid_rltn_dcmt_cont,
  CAST(py_text(v[20]) AS VARCHAR) AS bid_rltn_dcmt_sbmsn_term_nm,
  CAST(py_text(v[21]) AS VARCHAR) AS bid_rltn_dcmt_sbmsn_mthod_nm,
  CAST(py_text(v[22]) AS VARCHAR) AS collb_mthod_dcmt_cont,
  CAST(py_text(v[23]) AS VARCHAR) AS collb_dcmt_sbmsn_mthod_nm,
  CAST(py_text(v[24]) AS VARCHAR) AS collb_dcmt_sbmsn_term_nm,
  CAST(py_text(v[25]) AS VARCHAR) AS subt_bid_dcmt_cont,
  CAST(py_text(v[26]) AS VARCHAR) AS subt_bid_dcmt_sbmsn_mthod_nm,
  CAST(py_text(v[27]) AS VARCHAR) AS subt_bid_dcmt_sbmsn_term_nm,
  CAST(py_text(v[28]) AS VARCHAR) AS qlfc_lmt_cdtn_cont,
  CAST(py_text(v[29]) AS VARCHAR) AS rgn_lmt_cdtn_cont,
  CAST(py_text(v[30]) AS VARCHAR) AS etc_lmt_cdtn_cont,
  CAST(py_text(v[31]) AS VARCHAR) AS evl_ptdb_clg_cont,
  CAST(py_text(v[32]) AS VARCHAR) AS evl_perd_cont,
  CAST(py_text(v[33]) AS VARCHAR) AS pytn_mtrs_cont,
  CAST(py_text(v[34]) AS VARCHAR) AS bid_prc_asmc_calcw_cont,
  CAST(onbid_decimal(v[35]) AS DOUBLE) AS min_esure_amt,
  CAST(onbid_decimal(v[36]) AS DOUBLE) AS min_esure_rate,
  CAST(onbid_decimal(v[37]) AS DOUBLE) AS crtr_amt,
  CAST(onbid_decimal(v[38]) AS DOUBLE) AS crtr_rate,
  CAST(py_text(v[39]) AS VARCHAR) AS rsv_prc_rng_cont,
  CAST(onbid_decimal(v[40]) AS DOUBLE) AS rsv_prc_chc_num,
  CAST(onbid_decimal(v[41]) AS DOUBLE) AS rsv_prc_etmt_num,
  CAST(onbid_decimal(v[42]) AS DOUBLE) AS tplm_crrt,
  CAST(onbid_decimal(v[43]) AS DOUBLE) AS rsnb_hghprc_cfct,
  CAST(py_text(v[44]) AS VARCHAR) AS crtr_amt_bid_nm,
  CAST(py_text(v[45]) AS VARCHAR) AS opsn_cltr_bid_psbl_yn,
  CAST(onbid_decimal(v[46]) AS DOUBLE) AS bid_psbl_cltr_qnty,
  CAST(onbid_decimal(v[47]) AS DOUBLE) AS ptct_cmsn,
  CAST(onbid_json_text(v[48], []::VARCHAR[]) AS VARCHAR) AS prpsl_evl_item_clg_list_json,
  CAST(py_text(v[49]) AS VARCHAR) AS prcn_nsq_bid_rslt_nm,
  CAST(onbid_whole(v[50]) AS BIGINT) AS prcn_nsq_lowst_bid_prc,
  CAST(onbid_json_text(v[51], []::VARCHAR[]) AS VARCHAR) AS prcn_bid_clg_list_json,
  CAST(onbid_json_text(v[52], []::VARCHAR[]) AS VARCHAR) AS cseq_bid_inf_clg_list_json,
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
