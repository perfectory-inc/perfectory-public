//! Provider-native `operation -> dataset_slug` maps + the shared object-key operation-collapse rule.
//!
//! These are the single in-code SSOT (ADR 0014 D3) for the maps, and the SSOT for the collapse
//! predicate built on them (ADR 0016 Task T1.2, D-C / D-D).
//!
//! The provider-native `operation` (e.g. `getBrTitleInfo`, `getRTMSDataSvcAptTradeDev`, `ladfrlList`)
//! is **not** the canonical `dataset_slug` (e.g. `building_register_main`,
//! `real_transaction_apartment_trade`, `land_register`): a `snake_case(operation)` transform would
//! wrongly yield `get_br_title_info`. data.go.kr and the V-World NED attribute API therefore each need
//! a curated map. These functions hold them once so that the building-register, real-transaction, and
//! V-World NED / land-register producers can resolve a run's `operation` to its `dataset_slug` and
//! then feed [`crate::source_slug`] — instead of each producer hand-writing the final slug literal.
//!
//! The human-facing SSOT remains `docs/catalog/bronze-source-slug-rename.v1.md` (§1.1 / §1.2) and the
//! catalog JSON (`public-source-endpoint-catalog.v1.json`); a parity test asserts these code maps
//! equal the catalog's `(operation, dataset_slug)` pairs so the two cannot drift. (For V-World the
//! catalog's `operation` field is already the canonical snake_case dataset name; the provider-native
//! API operation that flows into the Bronze object key is distinct, so the V-World map below maps the
//! provider-native API operation onto that same canonical dataset_slug.)
//!
//! hub.go.kr / mois / factoryon / juso operations are already canonical `snake_case`
//! (`dataset_slug == operation`) in the catalog, so they need no map here — the byte-equality branch
//! of [`operation_collapses_into_slug`] covers them.
//!
//! ## Object-key operation collapse (ADR 0016 T1.2)
//!
//! Because every Bronze `source_slug` is `{providerid}__{dataset_slug}` and each of these provider
//! operations maps 1:1 onto its own unique `dataset_slug`, an `operation=` segment in the object key
//! is fully redundant with the `source=` segment: the slug already names the dataset, and the
//! operation can be recovered from it. [`operation_collapses_into_slug`] is the SINGLE predicate the
//! Bronze key compiler consults to drop that redundant `operation=` segment. The provider operation is
//! still kept in lineage (`source_partition_key`, `request_params`) — only the immutable R2 object key
//! drops it (D-D).

/// Resolves a data.go.kr building-register `getBr*` `operation` to its canonical `dataset_slug`.
///
/// Returns `None` for any operation outside the 10 owner-approved building-register operations
/// (ADR 0014 §1.1). Callers must treat `None` as a hard error rather than inventing a slug.
#[must_use]
pub fn building_register_dataset_slug(operation: &str) -> Option<&'static str> {
    match operation {
        "getBrTitleInfo" => Some("building_register_main"),
        "getBrRecapTitleInfo" => Some("building_register_master"),
        "getBrExposInfo" => Some("building_register_exclusive_unit"),
        "getBrExposPubuseAreaInfo" => Some("building_register_exclusive_common_area"),
        "getBrFlrOulnInfo" => Some("building_register_floor_overview"),
        "getBrHsprcInfo" => Some("building_register_house_price"),
        "getBrJijiguInfo" => Some("building_register_district_zone"),
        "getBrWclfInfo" => Some("building_register_sewage_facility"),
        "getBrAtchJibunInfo" => Some("building_register_sub_parcel"),
        "getBrBasisOulnInfo" => Some("building_register_basis_outline"),
        _ => None,
    }
}

/// Resolves a data.go.kr real-transaction `getRTMSDataSvc*` `operation` to its `dataset_slug`.
///
/// Returns `None` for any operation outside the 12 owner-approved real-transaction operations
/// (ADR 0014 §1.2). Callers must treat `None` as a hard error rather than inventing a slug.
#[must_use]
pub fn real_transaction_dataset_slug(operation: &str) -> Option<&'static str> {
    match operation {
        "getRTMSDataSvcAptTradeDev" => Some("real_transaction_apartment_trade"),
        "getRTMSDataSvcAptRent" => Some("real_transaction_apartment_rent"),
        "getRTMSDataSvcOffiTrade" => Some("real_transaction_officetel_trade"),
        "getRTMSDataSvcOffiRent" => Some("real_transaction_officetel_rent"),
        "getRTMSDataSvcRHTrade" => Some("real_transaction_row_house_trade"),
        "getRTMSDataSvcRHRent" => Some("real_transaction_row_house_rent"),
        "getRTMSDataSvcSHTrade" => Some("real_transaction_detached_house_trade"),
        "getRTMSDataSvcSHRent" => Some("real_transaction_detached_house_rent"),
        "getRTMSDataSvcNrgTrade" => Some("real_transaction_commercial_trade"),
        "getRTMSDataSvcInduTrade" => Some("real_transaction_industrial_trade"),
        "getRTMSDataSvcLandTrade" => Some("real_transaction_land_trade"),
        "getRTMSDataSvcSilvTrade" => Some("real_transaction_apartment_presale"),
        _ => None,
    }
}

/// Resolves a V-World NED attribute-API provider operation to its canonical `dataset_slug`.
///
/// The V-World NED attribute API exposes several land-record operations whose provider-native call id
/// (e.g. `ladfrlList`, `getLandCharacteristic`) is distinct from the catalog's canonical
/// `snake_case` dataset name. Each operation maps to its OWN unique `dataset_slug` (the map is
/// bijective — no two operations share a slug), so the `source=vworldkr__{dataset_slug}` segment
/// already uniquely names the dataset and an `operation=` key segment is redundant (ADR 0016 T1.2 /
/// D-C).
///
/// Returns `None` for any operation outside the seven owner-approved V-World NED operations; callers
/// must treat `None` as "not a known collapsible operation" rather than inventing a slug.
#[must_use]
pub fn vworld_ned_dataset_slug(operation: &str) -> Option<&'static str> {
    match operation {
        "ladfrlList" => Some("land_register"),
        "getLandCharacteristic" => Some("land_characteristic"),
        "getIndvdLandPriceAttr" => Some("land_individual_price"),
        "getPossessionAttr" => Some("land_ownership"),
        "getLandUseAttr" => Some("land_use_plan"),
        "getLandMoveAttr" => Some("land_transfer_history"),
        "ldaregList" => Some("land_right_registration"),
        _ => None,
    }
}

/// Returns `true` when `operation` 1:1-maps to the dataset portion of `source_slug`.
///
/// When it does, the `operation=` segment is redundant with `source=` and must be dropped from the
/// immutable Bronze object key (ADR 0016 T1.2 / D-D). The provider operation is still kept in lineage
/// (`source_partition_key` / `request_params`); only the object key drops it.
///
/// This is the SINGLE shared collapse rule the Bronze key compiler consults, consolidating what used
/// to be two byte-equality-only copies of `operation_is_redundant_with_slug`. A collapse fires when
/// the operation resolves to the slug's `dataset_slug` through ANY of:
/// - **byte-equality** (`operation == dataset_slug`) — hub.go.kr / mois / factoryon / juso operations
///   are already canonical `snake_case` in the catalog, so they collapse directly;
/// - the data.go.kr building-register map ([`building_register_dataset_slug`]);
/// - the data.go.kr real-transaction map ([`real_transaction_dataset_slug`]);
/// - the V-World NED attribute map ([`vworld_ned_dataset_slug`]).
///
/// Each map is bijective, so a collapse can NEVER merge two distinct operations onto one key: the
/// resolved `dataset_slug` is exactly the slug's dataset, which uniquely identifies the operation.
#[must_use]
pub fn operation_collapses_into_slug(operation: &str, source_slug: &str) -> bool {
    let Some((_, dataset)) = source_slug.split_once("__") else {
        return false;
    };
    if dataset == operation {
        return true;
    }
    resolve_collapsible_dataset_slug(operation).is_some_and(|resolved| resolved == dataset)
}

/// Resolves a provider operation to its canonical `dataset_slug` through any of the curated maps
/// (building-register, real-transaction, V-World NED), or `None` if no map covers it. Used by
/// [`operation_collapses_into_slug`] after the byte-equality fast path.
fn resolve_collapsible_dataset_slug(operation: &str) -> Option<&'static str> {
    building_register_dataset_slug(operation)
        .or_else(|| real_transaction_dataset_slug(operation))
        .or_else(|| vworld_ned_dataset_slug(operation))
}

/// Canonical (fixed) provider page size for a Bronze page lane, keyed by the provider `operation`.
///
/// Pinned because the immutable Bronze object key carries the page as a `page-NNNNNN` leaf: if the
/// provider page size changed between runs, "page 1" would map to a different slice of provider rows
/// and silently collide different bytes onto the same physical key. Enforced at plan time (ADR 0016
/// acceptance #7 / D-A). Values are evidence-based (live API probe or official doc):
/// - building-register `getBr*` -> 100 (provider hard cap; numOfRows=1500 echoes back 100)
/// - real-transaction `getRTMSDataSvc*` -> 1000 (no provider cap; chosen fixed canonical)
/// - V-World NED land ops -> 1000 (provider max; numOfRows=1500 rejected "유효한 범위 1~1000")
/// - V-World cadastral `GetFeature` -> 1000 (V-World 2D Data API doc: size max 1000)
///
/// Returns `None` for operations with no pinned canonical (synthetic/future operations are not
/// enforced). All five production page-lane operation families above are pinned.
#[must_use]
pub fn canonical_page_size(operation: &str) -> Option<u32> {
    if building_register_dataset_slug(operation).is_some() {
        return Some(100);
    }
    if real_transaction_dataset_slug(operation).is_some()
        || vworld_ned_dataset_slug(operation).is_some()
    {
        return Some(1000);
    }
    match operation {
        "GetFeature" => Some(1000),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        building_register_dataset_slug, canonical_page_size, operation_collapses_into_slug,
        real_transaction_dataset_slug, vworld_ned_dataset_slug,
    };
    use crate::source_slug;

    #[test]
    fn building_register_map_resolves_known_operations() {
        assert_eq!(
            building_register_dataset_slug("getBrTitleInfo"),
            Some("building_register_main")
        );
        assert_eq!(
            building_register_dataset_slug("getBrExposInfo"),
            Some("building_register_exclusive_unit")
        );
        assert_eq!(building_register_dataset_slug("getBrUnknownInfo"), None);
    }

    #[test]
    fn real_transaction_map_resolves_known_operations() {
        assert_eq!(
            real_transaction_dataset_slug("getRTMSDataSvcAptTradeDev"),
            Some("real_transaction_apartment_trade")
        );
        assert_eq!(
            real_transaction_dataset_slug("getRTMSDataSvcSilvTrade"),
            Some("real_transaction_apartment_presale")
        );
        assert_eq!(real_transaction_dataset_slug("getRTMSDataSvcUnknown"), None);
    }

    #[test]
    fn building_register_map_feeds_the_generator() -> Result<(), Box<dyn std::error::Error>> {
        let dataset_slug =
            building_register_dataset_slug("getBrTitleInfo").ok_or("expected a dataset_slug")?;
        assert_eq!(
            source_slug("data.go.kr", dataset_slug)?,
            "datagokr__building_register_main"
        );
        Ok(())
    }

    #[test]
    fn real_transaction_map_feeds_the_generator() -> Result<(), Box<dyn std::error::Error>> {
        let dataset_slug = real_transaction_dataset_slug("getRTMSDataSvcAptTradeDev")
            .ok_or("expected a dataset_slug")?;
        assert_eq!(
            source_slug("data.go.kr", dataset_slug)?,
            "datagokr__real_transaction_apartment_trade"
        );
        Ok(())
    }

    #[test]
    fn vworld_ned_map_resolves_known_operations() {
        assert_eq!(vworld_ned_dataset_slug("ladfrlList"), Some("land_register"));
        assert_eq!(
            vworld_ned_dataset_slug("getLandCharacteristic"),
            Some("land_characteristic")
        );
        assert_eq!(
            vworld_ned_dataset_slug("ldaregList"),
            Some("land_right_registration")
        );
        assert_eq!(vworld_ned_dataset_slug("getUnknownAttr"), None);
    }

    /// The V-World NED map is bijective: every operation maps to its own unique `dataset_slug`, so a
    /// slug can never be claimed by two operations (the precondition for collapsing `operation=`).
    #[test]
    fn vworld_ned_map_is_bijective() -> Result<(), Box<dyn std::error::Error>> {
        let operations = [
            "ladfrlList",
            "getLandCharacteristic",
            "getIndvdLandPriceAttr",
            "getPossessionAttr",
            "getLandUseAttr",
            "getLandMoveAttr",
            "ldaregList",
        ];
        let mut slugs = Vec::with_capacity(operations.len());
        for operation in operations {
            slugs.push(vworld_ned_dataset_slug(operation).ok_or("known operation must resolve")?);
        }
        slugs.sort_unstable();
        let unique = slugs.len();
        slugs.dedup();
        assert_eq!(
            slugs.len(),
            unique,
            "V-World NED dataset_slugs must be unique"
        );
        Ok(())
    }

    #[test]
    fn vworld_ned_map_feeds_the_generator() -> Result<(), Box<dyn std::error::Error>> {
        let dataset_slug =
            vworld_ned_dataset_slug("getLandCharacteristic").ok_or("expected a dataset_slug")?;
        assert_eq!(
            source_slug("VWorld", dataset_slug)?,
            "vworldkr__land_characteristic"
        );
        Ok(())
    }

    // ---- operation_collapses_into_slug: the single shared collapse rule (ADR 0016 T1.2) ----

    /// data.go.kr building-register: the `getBr*` operation is 1:1 with the slug's dataset, so it
    /// collapses even though `operation != dataset_slug` byte-wise (the byte-equality copies missed
    /// this — the bug this task fixes).
    #[test]
    fn collapses_building_register_operation_via_map() {
        assert!(operation_collapses_into_slug(
            "getBrTitleInfo",
            "datagokr__building_register_main"
        ));
    }

    /// data.go.kr real-transaction: same map-driven collapse for the `getRTMS*` operations.
    #[test]
    fn collapses_real_transaction_operation_via_map() {
        assert!(operation_collapses_into_slug(
            "getRTMSDataSvcInduTrade",
            "datagokr__real_transaction_industrial_trade"
        ));
    }

    /// V-World NED / land-register provider operation collapses against its 1:1 dataset slug (D-C).
    #[test]
    fn collapses_vworld_ned_operation_via_map() {
        assert!(operation_collapses_into_slug(
            "getLandCharacteristic",
            "vworldkr__land_characteristic"
        ));
        assert!(operation_collapses_into_slug(
            "ladfrlList",
            "vworldkr__land_register"
        ));
    }

    /// hub.go.kr / canonical-snake_case operations collapse through the byte-equality fast path.
    #[test]
    fn collapses_canonical_snake_case_operation_by_byte_equality() {
        assert!(operation_collapses_into_slug(
            "building_register_main",
            "hubgokr__building_register_main"
        ));
    }

    /// A collapse must NOT fire when the operation does not resolve to THIS slug's dataset — that
    /// would be the dangerous case (dropping `operation=` where it still carries identity).
    #[test]
    fn does_not_collapse_when_operation_maps_to_a_different_slug() {
        // `getBrTitleInfo` -> `building_register_main`, but the slug's dataset is `_master`.
        assert!(!operation_collapses_into_slug(
            "getBrTitleInfo",
            "datagokr__building_register_master"
        ));
        // Unknown operation, slug dataset is unrelated.
        assert!(!operation_collapses_into_slug(
            "getUnknownThing",
            "datagokr__building_register_main"
        ));
        // A malformed slug with no `__` separator never collapses.
        assert!(!operation_collapses_into_slug(
            "getBrTitleInfo",
            "datagokr-building-register-main"
        ));
    }

    // ---- canonical_page_size: the pinned per-source page-size SSOT (ADR 0016 D-A) ----

    /// building-register `getBr*` operations pin to 100 (provider hard cap).
    #[test]
    fn canonical_page_size_pins_building_register_to_100() {
        assert_eq!(canonical_page_size("getBrTitleInfo"), Some(100));
    }

    /// real-transaction `getRTMSDataSvc*` operations pin to 1000 (chosen fixed canonical).
    #[test]
    fn canonical_page_size_pins_real_transaction_to_1000() {
        assert_eq!(canonical_page_size("getRTMSDataSvcInduTrade"), Some(1000));
    }

    /// V-World NED land operations pin to 1000 (provider max).
    #[test]
    fn canonical_page_size_pins_vworld_ned_to_1000() {
        assert_eq!(canonical_page_size("ladfrlList"), Some(1000));
        assert_eq!(canonical_page_size("getLandCharacteristic"), Some(1000));
    }

    /// V-World cadastral `GetFeature` pins to 1000 (V-World 2D Data API doc: size max 1000).
    #[test]
    fn canonical_page_size_pins_cadastral_get_feature_to_1000() {
        assert_eq!(canonical_page_size("GetFeature"), Some(1000));
    }

    /// Synthetic/unknown operations have no pinned canonical and are not enforced.
    #[test]
    fn canonical_page_size_is_none_for_unknown_operation() {
        assert_eq!(canonical_page_size("getTradeInfo"), None);
    }
}
