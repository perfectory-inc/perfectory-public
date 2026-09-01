//! A parcel that never had a kind is not changing one.
//!
//! `catalog.parcel.kind_changed.v1` declares `previous_kind` required. Every parcel loaded from
//! the canonical boundary source starts without a kind — the vocabulary describes land use inside
//! an industrial complex and a person decides it (root ADR-0070) — so the first staff edit has no
//! previous value to report. Reporting one would mean inventing it.

use catalog_domain::{Parcel, ParcelKind, ParcelKindEdit};
use chrono::{TimeZone, Utc};
use foundation_shared_kernel::ids::ParcelId;
use foundation_shared_kernel::pnu::Pnu;
use uuid::Uuid;

fn parcel(kind: Option<ParcelKind>) -> Result<Parcel, Box<dyn std::error::Error>> {
    let at = Utc
        .with_ymd_and_hms(2026, 9, 1, 0, 0, 0)
        .single()
        .ok_or("fixture timestamp must be unambiguous")?;
    Ok(Parcel {
        id: ParcelId::new(Uuid::nil()),
        // 저장소가 예약한 99999 범위. 실제로 배정될 수 있는 PNU 를 공개 저장소의
        // 픽스처에 적으면 실물 필지를 가리키게 된다 (public-fixture-safety).
        pnu: Pnu::parse("9999900000100000000".to_owned())?,
        kind,
        area_m2: None,
        created_at: at,
        updated_at: at,
        version: 1,
    })
}

#[test]
fn a_first_kind_is_assigned_not_changed() -> Result<(), Box<dyn std::error::Error>> {
    let loaded = parcel(None)?;

    match loaded.kind_edit_event(ParcelKind::Factory) {
        ParcelKindEdit::Assigned(event) => {
            assert_eq!(event.assigned_kind, "factory");
            assert_eq!(event.pnu, loaded.pnu);
            assert_eq!(event.schema_version, 1);
        }
        ParcelKindEdit::Changed(_) => {
            return Err("a parcel with no kind has no previous kind to report a change from".into())
        }
    }
    Ok(())
}

#[test]
fn replacing_a_kind_still_reports_the_previous_one() -> Result<(), Box<dyn std::error::Error>> {
    let decided = parcel(Some(ParcelKind::Support))?;

    match decided.kind_edit_event(ParcelKind::Factory) {
        ParcelKindEdit::Changed(event) => {
            assert_eq!(event.previous_kind, "support");
            assert_eq!(event.new_kind, "factory");
        }
        ParcelKindEdit::Assigned(_) => {
            return Err(
                "a parcel that had a kind is changing it, and v1 readers expect that".into(),
            )
        }
    }
    Ok(())
}

#[test]
fn a_loaded_parcel_needs_neither_a_kind_nor_an_area() -> Result<(), Box<dyn std::error::Error>> {
    // The boundary source carries `PNU` and `JIBUN`; `silver.parcel_boundaries` has no area
    // column. If either field were required here, a loader could not build the first row —
    // which is why the table held nothing while 39,861,511 parcels sat in canonical Silver.
    let loaded = parcel(None)?;

    assert!(loaded.kind.is_none());
    assert!(loaded.area_m2.is_none());
    Ok(())
}
