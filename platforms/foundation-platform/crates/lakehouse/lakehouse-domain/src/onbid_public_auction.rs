//! 차세대 온비드 public auction (KAMCO, data.go.kr), keyed to the provider's own PNU (root ADR-0280).
//!
//! Both tables append: a load adds the rows of keys whose content changed and, for the item list,
//! one removal marker per item condition that left a complete pass (root ADR-0275). Values only:
//! codes stay codes, and their meanings live in `data-go-kr-onbid.contract.json`.

use crate::lakehouse::{
    LakehouseColumn, LakehouseCurrentRowLatest, LakehouseLayer, LakehouseLoadUnit,
    LakehousePhysicalFormat, LakehouseServingRole, LakehouseTableContract,
    LakehouseWriteDistribution,
};

const fn column(name: &'static str, logical_type: &'static str, required: bool) -> LakehouseColumn {
    LakehouseColumn {
        name,
        logical_type,
        required,
    }
}

const SILVER_ONBID_REAL_ESTATE_ITEM_COLUMNS: &[LakehouseColumn] = &[
    // Key: one item condition (물건관리번호, 공매조건번호).
    column("cltr_mng_no", "string", true),
    column("pbct_cdtn_no", "long", true),
    // The provider's ltnoPnu when Pnu::parse accepts it; the raw text either way.
    column("pnu", "string", false),
    column("ltno_pnu_raw", "string", false),
    column("rdnm_pnu_raw", "string", false),
    column("onbid_cltr_no", "long", false),
    column("onbid_pbanc_no", "long", false),
    column("pbct_no", "long", false),
    column("pbct_nsq", "string", false),
    column("prpt_div_cd", "string", true),
    column("pvct_trgt_yn", "string", true),
    column("dsps_mthod_cd", "string", false),
    column("bid_div_cd", "string", false),
    column("bid_mthod_cd", "string", false),
    column("cptn_mthod_cd", "string", false),
    column("totalamt_unpc_div_cd", "string", false),
    column("pbct_stat_cd", "string", false),
    column("usg_lcls_ctgr_id", "string", false),
    column("usg_mcls_ctgr_id", "string", false),
    column("usg_scls_ctgr_id", "string", false),
    column("item_name", "string", false),
    column("usbd_nft", "int", false),
    column("bid_prgn_nft", "int", false),
    column("bid_begins_at", "timestamp", false),
    column("bid_ends_at", "timestamp", false),
    column("appraisal_amount_krw", "long", false),
    // lowstBidPrcIndctCont is text: usually a whole number of won, sometimes not.
    column("lowest_bid_price_raw", "string", false),
    column("lowest_bid_price_krw", "long", false),
    column("first_bid_price_krw", "long", false),
    column("land_area_m2", "double", false),
    column("building_area_m2", "double", false),
    column("share_item_yn", "string", false),
    column("location_sido_name", "string", false),
    column("location_sigungu_name", "string", false),
    column("location_emd_name", "string", false),
    column("org_name", "string", false),
    column("provider_modified_at", "timestamp", false),
    column("source_record_id", "string", true),
    column("source_line_number", "int", true),
    column("source_snapshot_id", "string", true),
    column("ingested_at_utc", "timestamp", true),
    // `removed` on a removal marker (root ADR-0275); null on every row the source holds.
    column("row_state", "string", false),
    // Root ADR-0340 §6: every other list field, named by the provider's own field
    // (`data-go-kr-onbid.fields.json` maps each); the code names (`*_nm`) are the provider's words.
    column("pbctsn", "string", false),
    column("prpt_div_nm", "string", false),
    column("dsps_mthod_nm", "string", false),
    column("bid_mthod_nm", "string", false),
    column("cptn_mthod_nm", "string", false),
    column("totalamt_unpc_div_nm", "string", false),
    column("pbct_stat_nm", "string", false),
    column("bid_div_nm", "string", false),
    column("dtbt_rqr_edtm_cont", "string", false),
    column("rent_mthod_nm", "string", false),
    column("rent_perd_cont", "string", false),
    column("thnl_img_url_adr", "string", false),
    column("crtn_yn", "string", false),
    column("cltr_usg_lcls_ctgr_nm", "string", false),
    column("cltr_usg_mcls_ctgr_nm", "string", false),
    column("cltr_usg_scls_ctgr_nm", "string", false),
    column("batc_bid_yn", "string", false),
    column("apsl_prc_ctrs_lowst_bid_rto", "double", false),
    column("frst_ctrs_lowst_bid_prc_rto", "double", false),
    column("rqst_org_nm", "string", false),
    column("eltr_grpr_use_yn", "string", false),
    column("collb_bid_psbl_yn", "string", false),
    column("twtm_gthr_bid_psbl_yn", "string", false),
    column("subt_bid_psbl_yn", "string", false),
    column("evc_rsby_trgt_cont", "string", false),
    column("fee_rate", "double", false),
];

/// Every open item condition of every real-estate list pass, as its content changes.
pub const SILVER_ONBID_REAL_ESTATE_ITEM: LakehouseTableContract = LakehouseTableContract {
    table_name: "silver.onbid_real_estate_item",
    layer: LakehouseLayer::Silver,
    physical_format: LakehousePhysicalFormat::Parquet,
    serving_role: LakehouseServingRole::Canonical,
    current_row_predicate: None,
    // A pass is one source_snapshot_id (its start, `YYYYMMDDTHHMMSSZ`), so the newest pass wins.
    current_row_latest: Some(LakehouseCurrentRowLatest {
        key: &["cltr_mng_no", "pbct_cdtn_no"],
        newest_first: &["source_snapshot_id"],
        removal_column: Some("row_state"),
        keep_ties: false,
    }),
    columns: SILVER_ONBID_REAL_ESTATE_ITEM_COLUMNS,
    partition_spec: &[],
    sort_order: &["pnu", "cltr_mng_no", "pbct_cdtn_no"],
    write_distribution: LakehouseWriteDistribution::Hash,
    quality_gates: &[
        "append_only",
        "pnu is null or passes Pnu::parse",
        "(cltr_mng_no, pbct_cdtn_no, source_snapshot_id) unique",
        "removal markers only from a complete pass",
    ],
    // One pass is one gzip JSONL Bronze object.
    load: LakehouseLoadUnit::Object {
        column: "source_record_id",
        object_prefix: None,
        object_suffix_separator: None,
    },
};

const SILVER_ONBID_REAL_ESTATE_ITEM_PHOTO_COLUMNS: &[LakehouseColumn] = &[
    // Key: one media file of one item condition, by the hash of its bytes — its content-addressed
    // R2 object name (root ADR-0298). This table is the reference index from an item to the blobs
    // the collector stored; the blobs themselves are identified by hash alone.
    column("cltr_mng_no", "string", true),
    column("pbct_cdtn_no", "long", true),
    column("sha256", "string", true),
    // "photo" (potoUrlList), "appraisal" (apslEvlClgList), and from root ADR-0340 §4
    // "location_map" (lmapUrlAdrList), "cadastral_map" (lrmUrlAdrList), "photo_360"
    // (poto360DgrUrlList) and "video" (vdoUrlAdrList): the contract's media.fields; one media store.
    column("media_kind", "string", true),
    // The order the provider lists it in, as given; its title when the provider gives one.
    column("sequence", "int", false),
    column("title", "string", false),
    // Sniffed from the bytes at download (root ADR-0298): jpg, png, gif or pdf.
    column("media_format", "string", true),
    column("size_bytes", "long", true),
    column("source_record_id", "string", true),
    column("source_line_number", "int", true),
    column("source_snapshot_id", "string", true),
    column("ingested_at_utc", "timestamp", true),
];

/// Each photo and appraisal attachment of an item, by its content hash (root ADR-0296 §3, ADR-0298).
///
/// "어느 물건이 어떤 사진 해시를 갖는가" — the reference, lineage and audit index for the
/// content-addressed media the collector mirrored, so the by-PNU gold can join photos to an item.
pub const SILVER_ONBID_REAL_ESTATE_ITEM_PHOTO: LakehouseTableContract = LakehouseTableContract {
    table_name: "silver.onbid_real_estate_item_photo",
    layer: LakehouseLayer::Silver,
    physical_format: LakehousePhysicalFormat::Parquet,
    serving_role: LakehouseServingRole::Canonical,
    current_row_predicate: None,
    // One item's one media file is one (item, hash): the media belong to the item, not to a round
    // (root ADR-0339, measured 2026-10-29: every item's rounds held the same files), so
    // `pbct_cdtn_no` only names the round whose detail answered. The newest run that recorded it
    // wins, and within a run the later line (two rounds of one item fetched in one run before root
    // ADR-0339); a re-seen photo keeps one row. "처음 본 시각" is the earliest ingested_at_utc.
    current_row_latest: Some(LakehouseCurrentRowLatest {
        key: &["cltr_mng_no", "sha256"],
        newest_first: &["source_snapshot_id", "source_line_number"],
        removal_column: None,
        keep_ties: false,
    }),
    columns: SILVER_ONBID_REAL_ESTATE_ITEM_PHOTO_COLUMNS,
    partition_spec: &[],
    sort_order: &["cltr_mng_no", "pbct_cdtn_no", "sequence"],
    write_distribution: LakehouseWriteDistribution::Hash,
    quality_gates: &[
        "append_only",
        "(cltr_mng_no, pbct_cdtn_no, sha256, source_snapshot_id) unique",
    ],
    load: LakehouseLoadUnit::Object {
        column: "source_record_id",
        object_prefix: None,
        object_suffix_separator: None,
    },
};

const SILVER_ONBID_ITEM_BID_RESULT_COLUMNS: &[LakehouseColumn] = &[
    column("cltr_mng_no", "string", true),
    column("pbct_cdtn_no", "long", true),
    column("onbid_cltr_no", "long", false),
    column("onbid_pbanc_no", "long", false),
    column("pbanc_mng_no", "string", false),
    column("pbct_no", "long", false),
    column("pbct_nsq", "string", false),
    column("prpt_div_cd", "string", false),
    column("pbct_stat_cd", "string", false),
    column("usbd_nft", "int", false),
    column("appraisal_amount_krw", "long", false),
    column("lowest_bid_price_raw", "string", false),
    column("lowest_bid_price_krw", "long", false),
    // 낙찰금액; null unless the round was won.
    column("winning_bid_amount_krw", "long", false),
    column("valid_bidder_count", "int", false),
    column("invalid_bidder_count", "int", false),
    column("bid_begins_at", "timestamp", false),
    column("bid_ends_at", "timestamp", false),
    column("opened_at", "timestamp", false),
    column("bid_amounts_raw", "string", false),
    column("source_record_id", "string", true),
    column("source_line_number", "int", true),
    column("source_snapshot_id", "string", true),
    column("ingested_at_utc", "timestamp", true),
    // Root ADR-0340 §6: every other result field, by the provider's own name. Rows loaded before
    // these columns keep them null (a result is asked once; the Bronze answer keeps the values).
    column("pbct_stat_nm", "string", false),
    column("prpt_div_nm", "string", false),
    column("dsps_mthod_cd", "string", false),
    column("dsps_mthod_nm", "string", false),
    column("cltr_usg_lcls_ctgr_id", "string", false),
    column("cltr_usg_mcls_ctgr_id", "string", false),
    column("cltr_usg_scls_ctgr_id", "string", false),
    column("cltr_usg_lcls_ctgr_nm", "string", false),
    column("cltr_usg_mcls_ctgr_nm", "string", false),
    column("cltr_usg_scls_ctgr_nm", "string", false),
    column("onbid_cltr_nm", "string", false),
    column("pbctsn", "string", false),
    column("org_nm", "string", false),
    column("onbid_pbanc_nm", "string", false),
    column("bid_mthod_cd", "string", false),
    column("bid_mthod_nm", "string", false),
    column("cptn_mthod_cd", "string", false),
    column("cptn_mthod_nm", "string", false),
    column("totalamt_unpc_div_cd", "string", false),
    column("totalamt_unpc_div_nm", "string", false),
    column("opbd_bgng_dt", "timestamp", false),
    column("opbd_cmptn_dt", "timestamp", false),
    column("apsl_prc_ctrs_scfb_prc_rto", "double", false),
    column("lowst_bid_ctrs_scfb_prc_rto", "double", false),
    column("pbct_tdps_cont", "string", false),
    column("bid_div_cd", "string", false),
    column("bid_div_nm", "string", false),
    column("afsb_rtrcn_rsn_cont", "string", false),
    column("rtrcn_rsn_cont", "string", false),
    column("apsl_prc_ctrs_lowst_bid_rto", "double", false),
    column("frst_ctrs_lowst_bid_prc_rto", "double", false),
    column("fee_rate", "double", false),
];

/// The result of each closed item condition, asked once when it leaves the item list.
pub const SILVER_ONBID_ITEM_BID_RESULT: LakehouseTableContract = LakehouseTableContract {
    table_name: "silver.onbid_item_bid_result",
    layer: LakehouseLayer::Silver,
    physical_format: LakehousePhysicalFormat::Parquet,
    serving_role: LakehouseServingRole::Canonical,
    current_row_predicate: None,
    current_row_latest: Some(LakehouseCurrentRowLatest {
        key: &["cltr_mng_no", "pbct_cdtn_no"],
        newest_first: &["source_snapshot_id"],
        removal_column: None,
        keep_ties: false,
    }),
    columns: SILVER_ONBID_ITEM_BID_RESULT_COLUMNS,
    partition_spec: &[],
    sort_order: &["cltr_mng_no", "pbct_cdtn_no"],
    write_distribution: LakehouseWriteDistribution::Hash,
    quality_gates: &[
        "append_only",
        "(cltr_mng_no, pbct_cdtn_no, source_snapshot_id) unique",
    ],
    load: LakehouseLoadUnit::Object {
        column: "source_record_id",
        object_prefix: None,
        object_suffix_separator: None,
    },
};

const SILVER_ONBID_ANNOUNCEMENT_COLUMNS: &[LakehouseColumn] = &[
    column("onbid_pbanc_no", "long", true),
    column("pbct_no", "long", true),
    column("pbanc_mng_no", "string", false),
    column("pbanc_kind_cd", "string", false),
    column("pbanc_kind_nm", "string", false),
    column("prpt_div_cd", "string", false),
    column("prpt_div_nm", "string", false),
    column("dsps_mthod_cd", "string", false),
    column("dsps_mthod_nm", "string", false),
    column("onbid_pbanc_nm", "string", false),
    column("org_nm", "string", false),
    column("rspb_sbr_nm", "string", false),
    column("pbanc_ymd", "date", false),
    column("pbct_nsq", "string", false),
    column("pbctsn", "string", false),
    column("cltr_bid_bgng_dt", "timestamp", false),
    column("cltr_bid_end_dt", "timestamp", false),
    column("cltr_opbd_dt", "timestamp", false),
    column("bid_div_cd", "string", false),
    column("bid_div_nm", "string", false),
    column("source_record_id", "string", true),
    column("source_line_number", "int", true),
    column("source_snapshot_id", "string", true),
    column("ingested_at_utc", "timestamp", true),
];

/// Each announcement round (공고목록 getPbancList2, 15157216) as its content changes.
pub const SILVER_ONBID_ANNOUNCEMENT: LakehouseTableContract = LakehouseTableContract {
    table_name: "silver.onbid_announcement",
    layer: LakehouseLayer::Silver,
    physical_format: LakehousePhysicalFormat::Parquet,
    serving_role: LakehouseServingRole::Canonical,
    current_row_predicate: None,
    current_row_latest: Some(LakehouseCurrentRowLatest {
        key: &["onbid_pbanc_no", "pbct_no"],
        newest_first: &["source_snapshot_id"],
        removal_column: None,
        keep_ties: false,
    }),
    columns: SILVER_ONBID_ANNOUNCEMENT_COLUMNS,
    partition_spec: &[],
    sort_order: &["onbid_pbanc_no", "pbct_no"],
    write_distribution: LakehouseWriteDistribution::Hash,
    quality_gates: &[
        "append_only",
        "(onbid_pbanc_no, pbct_no, source_snapshot_id) unique",
    ],
    load: LakehouseLoadUnit::Object {
        column: "source_record_id",
        object_prefix: None,
        object_suffix_separator: None,
    },
};

const SILVER_ONBID_ANNOUNCEMENT_BID_RESULT_COLUMNS: &[LakehouseColumn] = &[
    column("onbid_pbanc_no", "long", true),
    column("pbct_no", "long", true),
    column("pbanc_mng_no", "string", false),
    column("pbct_nsq", "string", false),
    column("pbctsn", "string", false),
    column("pbanc_kind_cd", "string", false),
    column("pbanc_kind_nm", "string", false),
    column("prpt_div_cd", "string", false),
    column("prpt_div_nm", "string", false),
    column("dsps_mthod_cd", "string", false),
    column("dsps_mthod_nm", "string", false),
    column("onbid_pbanc_nm", "string", false),
    column("org_nm", "string", false),
    column("rspb_sbr_nm", "string", false),
    column("pbanc_ymd", "date", false),
    column("cltr_opbd_dt", "timestamp", false),
    column("exct_stat_cd", "string", false),
    column("exct_stat_nm", "string", false),
    column("source_record_id", "string", true),
    column("source_line_number", "int", true),
    column("source_snapshot_id", "string", true),
    column("ingested_at_utc", "timestamp", true),
];

/// Each announcement round's opening state (공고 입찰결과목록 getPbancBidRsltList2, 15157222) as it changes.
pub const SILVER_ONBID_ANNOUNCEMENT_BID_RESULT: LakehouseTableContract = LakehouseTableContract {
    table_name: "silver.onbid_announcement_bid_result",
    layer: LakehouseLayer::Silver,
    physical_format: LakehousePhysicalFormat::Parquet,
    serving_role: LakehouseServingRole::Canonical,
    current_row_predicate: None,
    current_row_latest: Some(LakehouseCurrentRowLatest {
        key: &["onbid_pbanc_no", "pbct_no"],
        newest_first: &["source_snapshot_id"],
        removal_column: None,
        keep_ties: false,
    }),
    columns: SILVER_ONBID_ANNOUNCEMENT_BID_RESULT_COLUMNS,
    partition_spec: &[],
    sort_order: &["onbid_pbanc_no", "pbct_no"],
    write_distribution: LakehouseWriteDistribution::Hash,
    quality_gates: &[
        "append_only",
        "(onbid_pbanc_no, pbct_no, source_snapshot_id) unique",
    ],
    load: LakehouseLoadUnit::Object {
        column: "source_record_id",
        object_prefix: None,
        object_suffix_separator: None,
    },
};

const SILVER_ONBID_REAL_ESTATE_ITEM_DETAIL_COLUMNS: &[LakehouseColumn] = &[
    column("cltr_mng_no", "string", true),
    column("pbct_cdtn_no", "long", false),
    column("sqms_list_json", "string", false),
    column("apsl_evl_clg_list_json", "string", false),
    column("leas_inf_list_json", "string", false),
    column("rgst_prmr_inf_list_json", "string", false),
    column("dtbt_rqr_mtrs_list_json", "string", false),
    column("ocpy_rel_list_json", "string", false),
    column("paps_inf_json", "string", false),
    column("zadr_nm", "string", false),
    column("cltr_radr", "string", false),
    column("loc_vnty_pscd_cont", "string", false),
    column("icdl_cdtn_cont", "string", false),
    column("utlz_pscd_cont", "string", false),
    column("cltr_etc_cont", "string", false),
    column("dspl_vld_cont", "string", false),
    column("purr_qlfc_cont", "string", false),
    column("pytn_mtrs_cont", "string", false),
    column("frst_pbanc_ymd", "date", false),
    column("batc_bid_cltr_clg_list_json", "string", false),
    column("crtn_lst_clg_list_json", "string", false),
    column("source_record_id", "string", true),
    column("source_line_number", "int", true),
    column("source_snapshot_id", "string", true),
    column("ingested_at_utc", "timestamp", true),
];

/// Each item's detail (물건상세 getRlstDtlInf2, 15157247), once per item (root ADR-0339), as it changes: notices, the 공매재산명세서, tenancy, occupancy, rights, areas, corrections and appraisals; private persons' names are dropped (root ADR-0340 §5).
pub const SILVER_ONBID_REAL_ESTATE_ITEM_DETAIL: LakehouseTableContract = LakehouseTableContract {
    table_name: "silver.onbid_real_estate_item_detail",
    layer: LakehouseLayer::Silver,
    physical_format: LakehousePhysicalFormat::Parquet,
    serving_role: LakehouseServingRole::Canonical,
    current_row_predicate: None,
    current_row_latest: Some(LakehouseCurrentRowLatest {
        key: &["cltr_mng_no"],
        newest_first: &["source_snapshot_id"],
        removal_column: None,
        keep_ties: false,
    }),
    columns: SILVER_ONBID_REAL_ESTATE_ITEM_DETAIL_COLUMNS,
    partition_spec: &[],
    sort_order: &["cltr_mng_no"],
    write_distribution: LakehouseWriteDistribution::Hash,
    quality_gates: &["append_only", "(cltr_mng_no, source_snapshot_id) unique"],
    load: LakehouseLoadUnit::Object {
        column: "source_record_id",
        object_prefix: None,
        object_suffix_separator: None,
    },
};

const SILVER_ONBID_ITEM_BID_INFO_COLUMNS: &[LakehouseColumn] = &[
    column("cltr_mng_no", "string", true),
    column("pbct_cdtn_no", "long", false),
    column("pbanc_mng_no", "string", false),
    column("onbid_pbanc_nm", "string", false),
    column("tdps_sbtn_dcmt_yn", "string", false),
    column("nrnk_aply_psbl_yn", "string", false),
    column("pbct_tdps_cont", "string", false),
    column("pcmt_pay_mtd_cont", "string", false),
    column("pcmt_pay_term_cont", "string", false),
    column("bid_ptct_aply_yn", "string", false),
    column("bid_vld_crtr_cont", "string", false),
    column("smns_ip_dpcn_bid_blckt_yn", "string", false),
    column("tdps_sbtn_dcmt_cont", "string", false),
    column("tdps_sbtn_dcmt_sbmsn_term_nm", "string", false),
    column("tdps_sbtn_dcmt_sbmsn_mthod_nm", "string", false),
    column("bid_ptct_aply_dcmt_cont", "string", false),
    column("bid_ptct_aply_term_nm", "string", false),
    column("bid_ptct_dcmt_sbmsn_mthod_nm", "string", false),
    column("bid_rltn_dcmt_cont", "string", false),
    column("bid_rltn_dcmt_sbmsn_term_nm", "string", false),
    column("bid_rltn_dcmt_sbmsn_mthod_nm", "string", false),
    column("collb_mthod_dcmt_cont", "string", false),
    column("collb_dcmt_sbmsn_mthod_nm", "string", false),
    column("collb_dcmt_sbmsn_term_nm", "string", false),
    column("subt_bid_dcmt_cont", "string", false),
    column("subt_bid_dcmt_sbmsn_mthod_nm", "string", false),
    column("subt_bid_dcmt_sbmsn_term_nm", "string", false),
    column("qlfc_lmt_cdtn_cont", "string", false),
    column("rgn_lmt_cdtn_cont", "string", false),
    column("etc_lmt_cdtn_cont", "string", false),
    column("evl_ptdb_clg_cont", "string", false),
    column("evl_perd_cont", "string", false),
    column("pytn_mtrs_cont", "string", false),
    column("bid_prc_asmc_calcw_cont", "string", false),
    column("min_esure_amt", "double", false),
    column("min_esure_rate", "double", false),
    column("crtr_amt", "double", false),
    column("crtr_rate", "double", false),
    column("rsv_prc_rng_cont", "string", false),
    column("rsv_prc_chc_num", "double", false),
    column("rsv_prc_etmt_num", "double", false),
    column("tplm_crrt", "double", false),
    column("rsnb_hghprc_cfct", "double", false),
    column("crtr_amt_bid_nm", "string", false),
    column("opsn_cltr_bid_psbl_yn", "string", false),
    column("bid_psbl_cltr_qnty", "double", false),
    column("ptct_cmsn", "double", false),
    column("prpsl_evl_item_clg_list_json", "string", false),
    column("prcn_nsq_bid_rslt_nm", "string", false),
    column("prcn_nsq_lowst_bid_prc", "long", false),
    column("prcn_bid_clg_list_json", "string", false),
    column("cseq_bid_inf_clg_list_json", "string", false),
    column("source_record_id", "string", true),
    column("source_line_number", "int", true),
    column("source_snapshot_id", "string", true),
    column("ingested_at_utc", "timestamp", true),
];

/// Each item's bid information (물건상세 입찰정보 getCltrBidInf2, 15157251), once per item, as it changes: bid method, restrictions, documents, evaluation, earlier rounds' results and every round's schedule.
pub const SILVER_ONBID_ITEM_BID_INFO: LakehouseTableContract = LakehouseTableContract {
    table_name: "silver.onbid_item_bid_info",
    layer: LakehouseLayer::Silver,
    physical_format: LakehousePhysicalFormat::Parquet,
    serving_role: LakehouseServingRole::Canonical,
    current_row_predicate: None,
    current_row_latest: Some(LakehouseCurrentRowLatest {
        key: &["cltr_mng_no"],
        newest_first: &["source_snapshot_id"],
        removal_column: None,
        keep_ties: false,
    }),
    columns: SILVER_ONBID_ITEM_BID_INFO_COLUMNS,
    partition_spec: &[],
    sort_order: &["cltr_mng_no"],
    write_distribution: LakehouseWriteDistribution::Hash,
    quality_gates: &["append_only", "(cltr_mng_no, source_snapshot_id) unique"],
    load: LakehouseLoadUnit::Object {
        column: "source_record_id",
        object_prefix: None,
        object_suffix_separator: None,
    },
};

const SILVER_ONBID_ANNOUNCEMENT_DETAIL_COLUMNS: &[LakehouseColumn] = &[
    column("pbanc_mng_no", "string", true),
    column("pbct_no", "long", true),
    column("onbid_pbanc_no", "long", false),
    column("pbanc_kind_cd", "string", false),
    column("pbanc_kind_nm", "string", false),
    column("prpt_div_cd", "string", false),
    column("prpt_div_nm", "string", false),
    column("dsps_mthod_cd", "string", false),
    column("dsps_mthod_nm", "string", false),
    column("onbid_pbanc_nm", "string", false),
    column("pbanc_org_nm", "string", false),
    column("pbanc_ymd", "date", false),
    column("bid_mthod_cd", "string", false),
    column("bid_mthod_nm", "string", false),
    column("cptn_mthod_cd", "string", false),
    column("cptn_mthod_nm", "string", false),
    column("totalamt_unpc_div_cd", "string", false),
    column("totalamt_unpc_div_nm", "string", false),
    column("bid_div_cd", "string", false),
    column("bid_div_nm", "string", false),
    column("bid_amt_rls_yn", "string", false),
    column("pbanc_nsq_nm", "string", false),
    column("pbanc_rtrcn_rsn_cont", "string", false),
    column("rltn_pbanc_mng_no", "string", false),
    column("rltn_pbanc_nm", "string", false),
    column("ptct_cmsn", "long", false),
    column("pbanc_ptct_qlfc_cont", "string", false),
    column("anncm_al_cont", "string", false),
    column("atch_file_list_json", "string", false),
    column("source_record_id", "string", true),
    column("source_line_number", "int", true),
    column("source_snapshot_id", "string", true),
    column("ingested_at_utc", "timestamp", true),
];

/// Each announcement's detail (공고상세 getPbancDtlInf2, 15157218): full text, attachments by content hash, qualifications, as it changes.
pub const SILVER_ONBID_ANNOUNCEMENT_DETAIL: LakehouseTableContract = LakehouseTableContract {
    table_name: "silver.onbid_announcement_detail",
    layer: LakehouseLayer::Silver,
    physical_format: LakehousePhysicalFormat::Parquet,
    serving_role: LakehouseServingRole::Canonical,
    current_row_predicate: None,
    current_row_latest: Some(LakehouseCurrentRowLatest {
        key: &["pbanc_mng_no", "pbct_no"],
        newest_first: &["source_snapshot_id"],
        removal_column: None,
        keep_ties: false,
    }),
    columns: SILVER_ONBID_ANNOUNCEMENT_DETAIL_COLUMNS,
    partition_spec: &[],
    sort_order: &["pbanc_mng_no", "pbct_no"],
    write_distribution: LakehouseWriteDistribution::Hash,
    quality_gates: &[
        "append_only",
        "(pbanc_mng_no, pbct_no, source_snapshot_id) unique",
    ],
    load: LakehouseLoadUnit::Object {
        column: "source_record_id",
        object_prefix: None,
        object_suffix_separator: None,
    },
};
