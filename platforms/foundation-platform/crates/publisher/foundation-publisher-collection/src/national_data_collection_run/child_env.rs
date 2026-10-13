use std::collections::BTreeMap;

use super::RunConfig;

pub(super) fn building_child_env(
    config: &RunConfig,
    dotenv: &BTreeMap<String, String>,
) -> BTreeMap<String, String> {
    let mut envs = dotenv.clone();
    envs.insert(
        "FOUNDATION_PLATFORM_BUILDING_REGISTER_SOURCE_SLUG".to_owned(),
        config.source_slug.clone(),
    );
    envs.insert(
        "FOUNDATION_PLATFORM_BUILDING_REGISTER_OPERATION".to_owned(),
        config.operation.clone(),
    );
    envs.insert(
        "FOUNDATION_PLATFORM_BUILDING_REGISTER_SIGUNGU_CD".to_owned(),
        config.sigungu_cd.clone(),
    );
    envs.insert(
        "FOUNDATION_PLATFORM_BUILDING_REGISTER_BJDONG_CD".to_owned(),
        config.bjdong_cd.clone(),
    );
    envs.insert(
        "FOUNDATION_PLATFORM_BUILDING_REGISTER_PAGE_NO".to_owned(),
        "1".to_owned(),
    );
    envs.insert(
        "FOUNDATION_PLATFORM_BUILDING_REGISTER_NUM_OF_ROWS".to_owned(),
        config.num_of_rows.to_string(),
    );
    envs.insert(
        "FOUNDATION_PLATFORM_BUILDING_REGISTER_MAX_PAGES".to_owned(),
        config.max_pages.to_string(),
    );
    envs.insert(
        "FOUNDATION_PLATFORM_BUILDING_REGISTER_LIVE_WRITE".to_owned(),
        "1".to_owned(),
    );
    envs.insert(
        "FOUNDATION_PLATFORM_BRONZE_OBJECT_STORAGE_DRIVER".to_owned(),
        "local".to_owned(),
    );
    envs.insert(
        "FOUNDATION_PLATFORM_BRONZE_LOCAL_OBJECT_ROOT".to_owned(),
        config.local_object_root.to_string_lossy().to_string(),
    );
    envs.entry("RUST_LOG".to_owned())
        .or_insert_with(|| "info".to_owned());
    envs
}

pub(super) fn vworld_child_env(
    config: &RunConfig,
    dotenv: &BTreeMap<String, String>,
) -> BTreeMap<String, String> {
    let mut envs = dotenv.clone();
    for name in VWORLD_STALE_FILTER_ENV {
        envs.remove(*name);
    }
    envs.insert(
        "FOUNDATION_PLATFORM_VWORLD_CADASTRAL_SOURCE_SLUG".to_owned(),
        config.vworld_source_slug.clone(),
    );
    envs.insert(
        "FOUNDATION_PLATFORM_VWORLD_CADASTRAL_DATASET".to_owned(),
        config.vworld_dataset.clone(),
    );
    envs.insert(
        "FOUNDATION_PLATFORM_VWORLD_CADASTRAL_ATTR_FILTER".to_owned(),
        config.vworld_attr_filter.clone(),
    );
    envs.insert(
        "FOUNDATION_PLATFORM_VWORLD_CADASTRAL_PAGE".to_owned(),
        "1".to_owned(),
    );
    envs.insert(
        "FOUNDATION_PLATFORM_VWORLD_CADASTRAL_SIZE".to_owned(),
        config.vworld_size.to_string(),
    );
    envs.insert(
        "FOUNDATION_PLATFORM_VWORLD_CADASTRAL_MAX_PAGES".to_owned(),
        config.vworld_max_pages.to_string(),
    );
    envs.insert(
        "FOUNDATION_PLATFORM_VWORLD_CADASTRAL_LIVE_WRITE".to_owned(),
        "1".to_owned(),
    );
    envs.insert(
        "FOUNDATION_PLATFORM_BRONZE_OBJECT_STORAGE_DRIVER".to_owned(),
        "local".to_owned(),
    );
    envs.insert(
        "FOUNDATION_PLATFORM_BRONZE_LOCAL_OBJECT_ROOT".to_owned(),
        config.local_object_root.to_string_lossy().to_string(),
    );
    envs.entry("RUST_LOG".to_owned())
        .or_insert_with(|| "info".to_owned());
    envs
}

const VWORLD_STALE_FILTER_ENV: &[&str] = &[
    "FOUNDATION_PLATFORM_VWORLD_CADASTRAL_ATTR_FILTER",
    "FOUNDATION_PLATFORM_VWORLD_CADASTRAL_PNU",
    "FOUNDATION_PLATFORM_VWORLD_LAND_REGISTER_PNU",
    "FOUNDATION_PLATFORM_VWORLD_CADASTRAL_GEOM_FILTER",
    "FOUNDATION_PLATFORM_VWORLD_CADASTRAL_GRID_BBOX",
    "FOUNDATION_PLATFORM_VWORLD_CADASTRAL_GRID_ROWS",
    "FOUNDATION_PLATFORM_VWORLD_CADASTRAL_GRID_COLUMNS",
    "FOUNDATION_PLATFORM_VWORLD_CADASTRAL_ADAPTIVE_SUBDIVISION",
    "FOUNDATION_PLATFORM_VWORLD_CADASTRAL_MAX_SUBDIVISION_DEPTH",
];
