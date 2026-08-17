use super::{json_text, write_resolution_jsonl};

use std::{env, fs, path::PathBuf};

use lakehouse_application::{
    resolve_industrial_complex_addresses, IndustrialComplexAddressGranularity,
    IndustrialComplexAddressResolutionError, IndustrialComplexAddressResolutionInput,
    ResolutionTier, ResolvedAdministrativeCode, ResolvedComplexAddress, SourceComplexRecord,
    SourceNoticeRecord,
};
use serde_json::json;
use std::collections::BTreeSet;
use uuid::Uuid;

const COMPLEX_DATASET: &str = "industrylandorkr__industrial_complex_list";
const NOTICE_DATASET: &str = "industrylandorkr__industrial_complex_notice";

/// Returns the ten-digit code a resolution ended up with, or `None` when it has none.
fn code_of(resolved: &ResolvedComplexAddress) -> Option<&str> {
    match &resolved.administrative_code {
        ResolvedAdministrativeCode::Sourced { code, .. } => Some(code.as_str()),
        ResolvedAdministrativeCode::Missing(_) => None,
    }
}

fn authority(codes: &[&str]) -> BTreeSet<String> {
    codes.iter().map(|code| (*code).to_owned()).collect()
}

fn complex(code: &str, administrative_code: Option<&str>) -> SourceComplexRecord {
    SourceComplexRecord {
        official_complex_code: code.to_owned(),
        administrative_code: administrative_code.map(str::to_owned),
        address_text: Some("서울특별시 구로구 구로동 일원".to_owned()),
        source_dataset: COMPLEX_DATASET.to_owned(),
    }
}

fn notice(code: &str, administrative_code: &str, seq: &str) -> SourceNoticeRecord {
    SourceNoticeRecord {
        official_complex_code: code.to_owned(),
        administrative_code: Some(administrative_code.to_owned()),
        notice_record_id: format!("ref_seq={seq}"),
    }
}

fn resolve(
    targets: &[&str],
    complexes: &[SourceComplexRecord],
    notices: &[SourceNoticeRecord],
    authority: &BTreeSet<String>,
) -> Result<
    lakehouse_application::IndustrialComplexAddressResolution,
    IndustrialComplexAddressResolutionError,
> {
    let targets: Vec<String> = targets.iter().map(|code| (*code).to_owned()).collect();
    resolve_industrial_complex_addresses(&IndustrialComplexAddressResolutionInput {
        target_complex_codes: &targets,
        source_complexes: complexes,
        source_notices: notices,
        sigungu_authority: authority,
        notice_dataset: NOTICE_DATASET,
    })
}

#[test]
fn a_code_the_authority_knows_resolves_from_the_source_itself() -> anyhow::Result<()> {
    let resolution = resolve(
        &["111010"],
        &[complex("111010", Some("1153000000"))],
        &[],
        &authority(&["11530"]),
    )?;

    assert_eq!(resolution.unresolved.len(), 0);
    let resolved = &resolution.resolved[0];
    assert_eq!(code_of(resolved), Some("1153000000"));
    assert_eq!(
        resolved.resolution_tier,
        ResolutionTier::SourceCodeInAuthority
    );
    assert_eq!(resolved.address_source_dataset, COMPLEX_DATASET);
    // Every code the source publishes is district-granularity, and the value says so rather than
    // letting a reader assume a dong.
    assert_eq!(
        resolved.administrative_code,
        ResolvedAdministrativeCode::Sourced {
            code: "1153000000".to_owned(),
            granularity: IndustrialComplexAddressGranularity::Sigungu,
        }
    );
    Ok(())
}

/// The heuristic tier. It is labelled as one, and its provenance points at the notice that decided
/// it rather than at the complex list, which did not.
#[test]
fn a_superseded_code_falls_back_to_the_modal_code_among_its_own_notices() -> anyhow::Result<()> {
    let resolution = resolve(
        &["129030"],
        &[complex("129030", Some("1226000000"))],
        &[
            notice("129030", "2917000000", "1"),
            notice("129030", "2917000000", "2"),
            notice("129030", "4677000000", "3"),
            // A different complex's notices must not vote in this one's resolution.
            notice("999999", "1111100000", "4"),
        ],
        &authority(&["29170", "46770", "11111"]),
    )?;

    let resolved = &resolution.resolved[0];
    assert_eq!(code_of(resolved), Some("2917000000"));
    assert_eq!(resolved.resolution_tier, ResolutionTier::ModalNoticeCode);
    assert_eq!(resolved.address_source_dataset, NOTICE_DATASET);
    assert_eq!(resolved.address_source_record_id, "ref_seq=1");
    // The observation is what the migration table is made of.
    assert_eq!(
        resolution.learned_code_migrations.get("1226000000"),
        Some(&"2917000000".to_owned())
    );
    Ok(())
}

/// The third tier exists only because the second one produced observations. A complex with a
/// superseded code and no notices of its own is resolved by what the notices of OTHER complexes
/// with the same superseded code showed.
#[test]
fn a_complex_without_notices_is_resolved_through_the_learned_migration() -> anyhow::Result<()> {
    let resolution = resolve(
        &["100001", "100002"],
        &[
            complex("100001", Some("1226000000")),
            complex("100002", Some("1226000000")),
        ],
        &[notice("100001", "2917000000", "1")],
        &authority(&["29170"]),
    )?;

    assert_eq!(resolution.unresolved.len(), 0);
    let learned = resolution
        .resolved
        .iter()
        .find(|resolved| resolved.official_complex_code == "100002")
        .expect("the second complex must resolve");
    assert_eq!(code_of(learned), Some("2917000000"));
    assert_eq!(
        learned.resolution_tier,
        ResolutionTier::LearnedCodeMigration
    );
    // A migration is not a notice: the provenance points at the complex list entry it rewrote.
    assert_eq!(learned.address_source_dataset, COMPLEX_DATASET);
    Ok(())
}

/// A learned table with a contradiction in it is not evidence. Silently picking a winner would put
/// roughly half the affected complexes in the wrong district, so the run stops.
#[test]
fn a_contradictory_learned_migration_stops_the_whole_run() {
    let error = resolve(
        &["100001", "100002"],
        &[
            complex("100001", Some("1226000000")),
            complex("100002", Some("1226000000")),
        ],
        &[
            notice("100001", "2917000000", "1"),
            notice("100002", "4677000000", "2"),
        ],
        &authority(&["29170", "46770"]),
    )
    .expect_err("a contradictory migration must fail the run");

    assert!(
        matches!(
            error,
            IndustrialComplexAddressResolutionError::ContradictoryLearnedMigration {
                ref superseded,
                ..
            } if superseded == "1226000000"
        ),
        "{error}"
    );
}

#[test]
fn an_empty_authority_is_refused_rather_than_trusted() {
    let error = resolve(
        &["111010"],
        &[complex("111010", Some("1153000000"))],
        &[],
        &BTreeSet::new(),
    )
    .expect_err("an empty authority must fail");

    assert!(
        matches!(
            error,
            IndustrialComplexAddressResolutionError::EmptyAuthority
        ),
        "{error}"
    );
}

/// The eup/myeon/dong layer uses ten-digit codes. Accepting it as the district authority would make
/// every membership test miss and silently unresolve the whole country.
#[test]
fn a_non_district_authority_code_is_refused() {
    let error = resolve(
        &["111010"],
        &[complex("111010", Some("1153000000"))],
        &[],
        &authority(&["1153010200"]),
    )
    .expect_err("a ten-digit authority code must fail");

    assert!(
        matches!(
            error,
            IndustrialComplexAddressResolutionError::InvalidAuthorityCode(_)
        ),
        "{error}"
    );
}

/// A complex is unresolved only when there is no location to record at all. Missing words, not a
/// missing code, is what stops a complex now (root ADR-0035).
#[test]
fn every_way_a_complex_can_stay_unresolved_is_named() -> anyhow::Result<()> {
    let mut without_text = complex("400004", Some("1153000000"));
    without_text.address_text = Some("   ".to_owned());
    let resolution = resolve(
        &["400001", "400004"],
        &[without_text],
        &[],
        &authority(&["11530"]),
    )?;

    let reasons: Vec<(&str, &str)> = resolution
        .unresolved
        .iter()
        .map(|unresolved| {
            (
                unresolved.official_complex_code.as_str(),
                unresolved.reason.wire_name(),
            )
        })
        .collect();
    assert_eq!(
        reasons,
        vec![
            ("400001", "missing_from_source_list"),
            ("400004", "address_text_absent"),
        ]
    );
    Ok(())
}

/// The two ways a complex ends up with words but no district: the source never stated a code, and
/// the source stated one no authority knows and no notice migrates. Both resolve — `446400` is the
/// second — and both say which one they were.
#[test]
fn a_complex_with_words_but_no_district_code_resolves_and_says_why() -> anyhow::Result<()> {
    let resolution = resolve(
        &["400002", "400003"],
        &[
            complex("400002", None),
            complex("400003", Some("1287000000")),
        ],
        &[],
        &authority(&["11530"]),
    )?;

    assert_eq!(resolution.unresolved.len(), 0);
    assert_eq!(resolution.resolved.len(), 2);
    for resolved in &resolution.resolved {
        assert_eq!(code_of(resolved), None);
        assert_eq!(resolved.resolution_tier, ResolutionTier::AddressTextOnly);
        // The words and their provenance survive; only the code is gone.
        assert_eq!(resolved.address_text, "서울특별시 구로구 구로동 일원");
        assert_eq!(resolved.address_source_dataset, COMPLEX_DATASET);
    }
    assert_eq!(
        resolution
            .missing_administrative_codes()
            .into_iter()
            .map(|(code, reason)| (code, reason.wire_name()))
            .collect::<Vec<_>>(),
        vec![
            ("400002", "source_code_absent"),
            ("400003", "source_code_unknown_and_no_notice_evidence"),
        ]
    );
    assert_eq!(resolution.tier_counts().get("address_text_only"), Some(&2));
    Ok(())
}

#[test]
fn the_source_list_may_not_carry_one_complex_twice() {
    let error = resolve(
        &["111010"],
        &[
            complex("111010", Some("1153000000")),
            complex("111010", Some("4111700000")),
        ],
        &[],
        &authority(&["11530", "41117"]),
    )
    .expect_err("a duplicated source complex must fail");

    assert!(
        matches!(
            error,
            IndustrialComplexAddressResolutionError::DuplicateSourceComplex(ref code)
                if code == "111010"
        ),
        "{error}"
    );
}

/// The complexes arrive from two datasets — the bulk list, and the per-complex detail endpoint for
/// the ones the list omits. Attributing a detail-sourced address to the list would point the
/// provenance at an object that does not contain it.
#[test]
fn a_detail_sourced_complex_keeps_its_own_dataset_as_provenance() -> anyhow::Result<()> {
    let mut from_detail = complex("244460", Some("4415000000"));
    from_detail.source_dataset = "industrylandorkr__industrial_complex_detail".to_owned();

    let resolution = resolve(&["244460"], &[from_detail], &[], &authority(&["44150"]))?;

    assert_eq!(
        resolution.resolved[0].address_source_dataset,
        "industrylandorkr__industrial_complex_detail"
    );
    Ok(())
}

/// The address text is the source's own wording. Normalizing it would export an address no source
/// ever published.
#[test]
fn the_address_text_is_carried_through_verbatim() -> anyhow::Result<()> {
    let multi_district = "서울특별시 구로구 구로동, 금천구 가산동 일원, 인천광역시 부평구 청천동";
    let mut record = complex("111010", Some("1153000000"));
    record.address_text = Some(multi_district.to_owned());

    let resolution = resolve(&["111010"], &[record], &[], &authority(&["11530"]))?;

    assert_eq!(resolution.resolved[0].address_text, multi_district);
    Ok(())
}

#[test]
fn json_text_reads_strings_numbers_and_treats_null_and_blank_alike() {
    let record = json!({
        "danji_cd": "111010",
        "ref_seq": 19922,
        "addr_nm": serde_json::Value::Null,
        "blank": "   ",
    });

    assert_eq!(json_text(&record, "danji_cd").as_deref(), Some("111010"));
    assert_eq!(json_text(&record, "ref_seq").as_deref(), Some("19922"));
    assert_eq!(json_text(&record, "addr_nm"), None);
    assert_eq!(json_text(&record, "blank"), None);
    assert_eq!(json_text(&record, "absent"), None);
}

fn temp_path(name: &str) -> PathBuf {
    env::temp_dir().join(format!("{name}-{}", Uuid::new_v4()))
}

fn resolved_line() -> ResolvedComplexAddress {
    ResolvedComplexAddress {
        official_complex_code: "111010".to_owned(),
        administrative_code: ResolvedAdministrativeCode::Sourced {
            code: "1153000000".to_owned(),
            granularity: IndustrialComplexAddressGranularity::Sigungu,
        },
        address_text: "서울특별시 구로구 구로동 일원".to_owned(),
        address_source_dataset: COMPLEX_DATASET.to_owned(),
        address_source_record_id: "danji_cd=111010".to_owned(),
        resolution_tier: ResolutionTier::SourceCodeInAuthority,
    }
}

fn codeless_line() -> ResolvedComplexAddress {
    ResolvedComplexAddress {
        official_complex_code: "446400".to_owned(),
        administrative_code: ResolvedAdministrativeCode::Missing(
            lakehouse_application::MissingAdministrativeCodeReason::SourceCodeUnknownAndNoNoticeEvidence,
        ),
        address_text: "전라남도 신안군 지도읍".to_owned(),
        address_source_dataset: COMPLEX_DATASET.to_owned(),
        address_source_record_id: "danji_cd=446400".to_owned(),
        resolution_tier: ResolutionTier::AddressTextOnly,
    }
}

#[test]
fn the_written_resolution_carries_the_granularity_and_the_tier() -> anyhow::Result<()> {
    let path = temp_path("industrial-complex-address-resolution").join("addresses.jsonl");

    write_resolution_jsonl(&path, &[resolved_line()])?;

    let written = fs::read_to_string(&path)?;
    let line = serde_json::from_str::<serde_json::Value>(written.trim())?;
    assert_eq!(line["official_complex_code"], "111010");
    assert_eq!(line["administrative_code"], "1153000000");
    assert_eq!(line["administrative_code_granularity"], "sigungu");
    assert_eq!(line["address_source_dataset"], COMPLEX_DATASET);
    assert_eq!(
        line["address_source_record_id"],
        "industrylandorkr:danji_cd=111010"
    );
    assert_eq!(line["resolution_tier"], "source_code_in_authority");
    fs::remove_dir_all(path.parent().expect("parent"))?;
    Ok(())
}

/// A complex with no district code is written with neither key rather than with `""`. An absent key
/// makes the reader decide; a blank one lets it slide through as if a source had said something
/// (root ADR-0035).
#[test]
fn a_resolution_without_a_code_omits_both_code_keys() -> anyhow::Result<()> {
    let path = temp_path("industrial-complex-address-resolution-codeless").join("addresses.jsonl");

    write_resolution_jsonl(&path, &[codeless_line()])?;

    let written = fs::read_to_string(&path)?;
    let line = serde_json::from_str::<serde_json::Value>(written.trim())?;
    let object = line.as_object().expect("a resolution line is an object");
    assert!(!object.contains_key("administrative_code"), "{written}");
    assert!(
        !object.contains_key("administrative_code_granularity"),
        "{written}"
    );
    assert_eq!(line["official_complex_code"], "446400");
    assert_eq!(line["address_text"], "전라남도 신안군 지도읍");
    assert_eq!(line["resolution_tier"], "address_text_only");
    fs::remove_dir_all(path.parent().expect("parent"))?;
    Ok(())
}

#[test]
fn an_existing_resolution_is_never_overwritten() -> anyhow::Result<()> {
    let path = temp_path("industrial-complex-address-resolution-append-only").join("a.jsonl");
    write_resolution_jsonl(&path, &[resolved_line()])?;
    let first = fs::read_to_string(&path)?;

    let error = write_resolution_jsonl(&path, &[resolved_line()])
        .expect_err("a rerun must not replace the resolution");

    assert!(format!("{error:#}").contains("append-only"), "{error:#}");
    assert_eq!(fs::read_to_string(&path)?, first);
    fs::remove_dir_all(path.parent().expect("parent"))?;
    Ok(())
}

#[test]
fn an_empty_resolution_is_not_written_at_all() {
    let path = temp_path("industrial-complex-address-resolution-empty").join("a.jsonl");

    let error =
        write_resolution_jsonl(&path, &[]).expect_err("an empty resolution must not be written");

    assert!(format!("{error:#}").contains("zero addresses"), "{error:#}");
    assert!(!path.exists());
}
