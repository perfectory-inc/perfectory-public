use std::collections::BTreeMap;

use serde_json::Value as JsonValue;

use super::{string_prop, string_prop_default, Config, StorageDriver};

pub(in crate::national_data_collection_ledger_execute) fn set_job_environment(
    job: &JsonValue,
    config: &Config,
    envs: &mut BTreeMap<String, String>,
) -> String {
    envs.insert(
        "FOUNDATION_PLATFORM_BRONZE_OBJECT_STORAGE_DRIVER".to_owned(),
        config.bronze_storage_driver.as_str().to_owned(),
    );
    if config.provider_min_page_interval_ms > 0 {
        envs.insert(
            "FOUNDATION_PLATFORM_PROVIDER_MIN_PAGE_INTERVAL_MS".to_owned(),
            config.provider_min_page_interval_ms.to_string(),
        );
    } else {
        envs.remove("FOUNDATION_PLATFORM_PROVIDER_MIN_PAGE_INTERVAL_MS");
    }
    match config.bronze_storage_driver {
        StorageDriver::Local => {
            envs.insert(
                "FOUNDATION_PLATFORM_BRONZE_LOCAL_OBJECT_ROOT".to_owned(),
                config.local_object_root.to_string_lossy().to_string(),
            );
        }
        StorageDriver::R2 => {
            envs.remove("FOUNDATION_PLATFORM_BRONZE_LOCAL_OBJECT_ROOT");
            envs.entry("RUST_LOG".to_owned())
                .or_insert_with(|| "foundation_outbox_publisher=info".to_owned());
        }
    }
    if string_prop(job, "provider") == "data.go.kr" {
        return set_data_go_kr_environment(job, envs);
    }
    set_vworld_environment(job, envs)
}

fn set_data_go_kr_environment(job: &JsonValue, envs: &mut BTreeMap<String, String>) -> String {
    let endpoint_slug = string_prop(job, "endpoint_slug");
    let endpoint = string_prop(job, "endpoint");
    if endpoint_slug.starts_with("data-go-kr-real-transaction-")
        || endpoint.contains("RTMS")
        || endpoint.contains("Real")
        || endpoint.contains("Transaction")
    {
        remove_keys(envs, BUILDING_REGISTER_ENV);
        envs.insert(
            "FOUNDATION_PLATFORM_REAL_TRANSACTION_SOURCE_SLUG".to_owned(),
            string_prop(job, "source_slug"),
        );
        envs.insert(
            "FOUNDATION_PLATFORM_REAL_TRANSACTION_OPERATION".to_owned(),
            string_prop(job, "operation"),
        );
        envs.insert(
            "FOUNDATION_PLATFORM_REAL_TRANSACTION_LAWD_CD".to_owned(),
            string_prop(job, "lawd_cd"),
        );
        envs.insert(
            "FOUNDATION_PLATFORM_REAL_TRANSACTION_DEAL_YMD".to_owned(),
            string_prop(job, "deal_ymd"),
        );
        envs.insert(
            "FOUNDATION_PLATFORM_REAL_TRANSACTION_PAGE_NO".to_owned(),
            string_prop_default(job, "page_start", "1"),
        );
        envs.insert(
            "FOUNDATION_PLATFORM_REAL_TRANSACTION_NUM_OF_ROWS".to_owned(),
            string_prop(job, "num_of_rows"),
        );
        envs.insert(
            "FOUNDATION_PLATFORM_REAL_TRANSACTION_MAX_PAGES".to_owned(),
            string_prop(job, "max_pages"),
        );
        envs.insert(
            "FOUNDATION_PLATFORM_REAL_TRANSACTION_LIVE_WRITE".to_owned(),
            "1".to_owned(),
        );
        return "ingest-real-transaction".to_owned();
    }
    remove_keys(envs, REAL_TRANSACTION_ENV);
    envs.insert(
        "FOUNDATION_PLATFORM_BUILDING_REGISTER_SOURCE_SLUG".to_owned(),
        string_prop(job, "source_slug"),
    );
    envs.insert(
        "FOUNDATION_PLATFORM_BUILDING_REGISTER_OPERATION".to_owned(),
        string_prop(job, "operation"),
    );
    envs.insert(
        "FOUNDATION_PLATFORM_BUILDING_REGISTER_SIGUNGU_CD".to_owned(),
        string_prop(job, "sigungu_cd"),
    );
    envs.insert(
        "FOUNDATION_PLATFORM_BUILDING_REGISTER_BJDONG_CD".to_owned(),
        string_prop(job, "bjdong_cd"),
    );
    envs.insert(
        "FOUNDATION_PLATFORM_BUILDING_REGISTER_PAGE_NO".to_owned(),
        string_prop_default(job, "page_start", "1"),
    );
    if is_partial_page_window(job) {
        envs.insert(
            "FOUNDATION_PLATFORM_BUILDING_REGISTER_PARTIAL_PAGE_WINDOW".to_owned(),
            "1".to_owned(),
        );
    } else {
        envs.remove("FOUNDATION_PLATFORM_BUILDING_REGISTER_PARTIAL_PAGE_WINDOW");
    }
    envs.insert(
        "FOUNDATION_PLATFORM_BUILDING_REGISTER_NUM_OF_ROWS".to_owned(),
        string_prop(job, "num_of_rows"),
    );
    envs.insert(
        "FOUNDATION_PLATFORM_BUILDING_REGISTER_MAX_PAGES".to_owned(),
        string_prop(job, "max_pages"),
    );
    envs.insert(
        "FOUNDATION_PLATFORM_BUILDING_REGISTER_LIVE_WRITE".to_owned(),
        "1".to_owned(),
    );
    "ingest-building-register".to_owned()
}

fn set_vworld_environment(job: &JsonValue, envs: &mut BTreeMap<String, String>) -> String {
    remove_keys(envs, VWORLD_ENV);
    if string_prop(job, "endpoint") == "ingest-vworld-land-register" {
        envs.insert(
            "FOUNDATION_PLATFORM_VWORLD_LAND_REGISTER_SOURCE_SLUG".to_owned(),
            string_prop(job, "source_slug"),
        );
        envs.insert(
            "FOUNDATION_PLATFORM_VWORLD_LAND_REGISTER_OPERATION".to_owned(),
            string_prop(job, "operation"),
        );
        envs.insert(
            "FOUNDATION_PLATFORM_VWORLD_LAND_REGISTER_PNU".to_owned(),
            string_prop(job, "pnu_prefix"),
        );
        envs.insert(
            "FOUNDATION_PLATFORM_VWORLD_LAND_REGISTER_PAGE_NO".to_owned(),
            "1".to_owned(),
        );
        envs.insert(
            "FOUNDATION_PLATFORM_VWORLD_LAND_REGISTER_NUM_OF_ROWS".to_owned(),
            string_prop(job, "num_of_rows"),
        );
        envs.insert(
            "FOUNDATION_PLATFORM_VWORLD_LAND_REGISTER_MAX_PAGES".to_owned(),
            string_prop(job, "max_pages"),
        );
        envs.insert(
            "FOUNDATION_PLATFORM_VWORLD_LAND_REGISTER_LIVE_WRITE".to_owned(),
            "1".to_owned(),
        );
        return "ingest-vworld-land-register".to_owned();
    }
    envs.insert(
        "FOUNDATION_PLATFORM_VWORLD_CADASTRAL_SOURCE_SLUG".to_owned(),
        string_prop(job, "source_slug"),
    );
    envs.insert(
        "FOUNDATION_PLATFORM_VWORLD_CADASTRAL_DATASET".to_owned(),
        string_prop(job, "dataset"),
    );
    envs.insert(
        "FOUNDATION_PLATFORM_VWORLD_CADASTRAL_ATTR_FILTER".to_owned(),
        string_prop(job, "attr_filter"),
    );
    envs.insert(
        "FOUNDATION_PLATFORM_VWORLD_CADASTRAL_PAGE".to_owned(),
        "1".to_owned(),
    );
    envs.insert(
        "FOUNDATION_PLATFORM_VWORLD_CADASTRAL_SIZE".to_owned(),
        string_prop(job, "size"),
    );
    envs.insert(
        "FOUNDATION_PLATFORM_VWORLD_CADASTRAL_MAX_PAGES".to_owned(),
        string_prop(job, "max_pages"),
    );
    envs.insert(
        "FOUNDATION_PLATFORM_VWORLD_CADASTRAL_LIVE_WRITE".to_owned(),
        "1".to_owned(),
    );
    "ingest-vworld-cadastral".to_owned()
}

const BUILDING_REGISTER_ENV: &[&str] = &[
    "FOUNDATION_PLATFORM_BUILDING_REGISTER_SOURCE_SLUG",
    "FOUNDATION_PLATFORM_BUILDING_REGISTER_OPERATION",
    "FOUNDATION_PLATFORM_BUILDING_REGISTER_SIGUNGU_CD",
    "FOUNDATION_PLATFORM_BUILDING_REGISTER_BJDONG_CD",
    "FOUNDATION_PLATFORM_BUILDING_REGISTER_PAGE_NO",
    "FOUNDATION_PLATFORM_BUILDING_REGISTER_PARTIAL_PAGE_WINDOW",
    "FOUNDATION_PLATFORM_BUILDING_REGISTER_NUM_OF_ROWS",
    "FOUNDATION_PLATFORM_BUILDING_REGISTER_MAX_PAGES",
    "FOUNDATION_PLATFORM_BUILDING_REGISTER_LIVE_WRITE",
];
const REAL_TRANSACTION_ENV: &[&str] = &[
    "FOUNDATION_PLATFORM_REAL_TRANSACTION_SOURCE_SLUG",
    "FOUNDATION_PLATFORM_REAL_TRANSACTION_OPERATION",
    "FOUNDATION_PLATFORM_REAL_TRANSACTION_LAWD_CD",
    "FOUNDATION_PLATFORM_REAL_TRANSACTION_DEAL_YMD",
    "FOUNDATION_PLATFORM_REAL_TRANSACTION_PAGE_NO",
    "FOUNDATION_PLATFORM_REAL_TRANSACTION_NUM_OF_ROWS",
    "FOUNDATION_PLATFORM_REAL_TRANSACTION_MAX_PAGES",
    "FOUNDATION_PLATFORM_REAL_TRANSACTION_LIVE_WRITE",
];
const VWORLD_ENV: &[&str] = &[
    "FOUNDATION_PLATFORM_VWORLD_CADASTRAL_ATTR_FILTER",
    "FOUNDATION_PLATFORM_VWORLD_CADASTRAL_PNU",
    "FOUNDATION_PLATFORM_VWORLD_LAND_REGISTER_PNU",
    "FOUNDATION_PLATFORM_VWORLD_LAND_REGISTER_OPERATION",
    "FOUNDATION_PLATFORM_VWORLD_LAND_REGISTER_SOURCE_SLUG",
    "FOUNDATION_PLATFORM_VWORLD_LAND_REGISTER_PAGE_NO",
    "FOUNDATION_PLATFORM_VWORLD_LAND_REGISTER_NUM_OF_ROWS",
    "FOUNDATION_PLATFORM_VWORLD_LAND_REGISTER_MAX_PAGES",
    "FOUNDATION_PLATFORM_VWORLD_LAND_REGISTER_LIVE_WRITE",
    "FOUNDATION_PLATFORM_VWORLD_CADASTRAL_GEOM_FILTER",
    "FOUNDATION_PLATFORM_VWORLD_CADASTRAL_GRID_BBOX",
    "FOUNDATION_PLATFORM_VWORLD_CADASTRAL_GRID_ROWS",
    "FOUNDATION_PLATFORM_VWORLD_CADASTRAL_GRID_COLUMNS",
    "FOUNDATION_PLATFORM_VWORLD_CADASTRAL_ADAPTIVE_SUBDIVISION",
    "FOUNDATION_PLATFORM_VWORLD_CADASTRAL_MAX_SUBDIVISION_DEPTH",
];

fn is_partial_page_window(job: &JsonValue) -> bool {
    !string_prop(job, "page_count_total").is_empty() || string_prop(job, "job_id").contains("-p")
}

fn remove_keys(envs: &mut BTreeMap<String, String>, keys: &[&str]) {
    for key in keys {
        envs.remove(*key);
    }
}
