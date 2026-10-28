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
    // "photo" (potoUrlList) or "appraisal" (apslEvlClgList); both share the one media store.
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
    // One item's one media file is one (item, hash); the newest run that recorded it wins, so a
    // re-seen photo keeps one row. "처음 본 시각" is the earliest ingested_at_utc across its appends.
    current_row_latest: Some(LakehouseCurrentRowLatest {
        key: &["cltr_mng_no", "pbct_cdtn_no", "sha256"],
        newest_first: &["source_snapshot_id"],
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
