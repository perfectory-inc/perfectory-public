//! A parcel's identifier is derived from its PNU and stays derived.
//!
//! The catalog projection is loaded from a lakehouse holding 39,861,511 parcels, and a projection
//! is by definition something you may rebuild. A generated identifier would mint a second row for
//! a parcel that has not changed, so the load could not be repeated — which is the same reason
//! warehouse practice hashes a surrogate key from the natural key instead of counting.

use catalog_domain::parcel_id_for_pnu;
use foundation_shared_kernel::pnu::Pnu;
use std::collections::HashSet;

fn pnu(raw: &str) -> Result<Pnu, Box<dyn std::error::Error>> {
    Ok(Pnu::parse(raw.to_owned())?)
}

#[test]
fn the_same_pnu_always_derives_the_same_id() -> Result<(), Box<dyn std::error::Error>> {
    let once = parcel_id_for_pnu(&pnu("9999900000100000000")?);
    let again = parcel_id_for_pnu(&pnu("9999900000100000000")?);

    assert_eq!(once, again);
    Ok(())
}

#[test]
fn different_pnus_derive_different_ids() -> Result<(), Box<dyn std::error::Error>> {
    let mut seen = HashSet::new();
    for suffix in 0..512_u32 {
        let raw = format!("9999900000{}{:08}", 1, suffix);
        assert_eq!(raw.len(), 19, "fixture PNU must be nineteen digits");
        assert!(
            seen.insert(parcel_id_for_pnu(&pnu(&raw)?)),
            "two PNUs derived the same identifier, which a UNIQUE(pnu) target would not catch"
        );
    }
    Ok(())
}

#[test]
fn the_derived_id_is_a_uuid_v8_with_the_standard_variant() -> Result<(), Box<dyn std::error::Error>>
{
    // Not decoration: a value in a `uuid` column that is not a well-formed UUID is a value some
    // client library will reject at read time, which is a failure a long way from this function.
    let derived = parcel_id_for_pnu(&pnu("9999900000100000042")?);
    let uuid = derived.as_uuid();
    let bytes = uuid.as_bytes();

    assert_eq!(bytes[6] >> 4, 0x8, "version nibble must be 8");
    assert_eq!(bytes[8] >> 6, 0b10, "variant bits must be RFC 9562");
    Ok(())
}

#[test]
fn the_namespace_is_part_of_the_contract() -> Result<(), Box<dyn std::error::Error>> {
    // Pinned by value, not recomputed. Recomputing it here would assert that the function equals
    // itself; this asserts that the identifiers already written stay the identifiers this function
    // produces. Changing the namespace string changes every parcel id ever derived, and this test
    // is where that is noticed.
    let derived = parcel_id_for_pnu(&pnu("9999900000100000000")?);

    assert_eq!(
        derived.as_uuid().to_string(),
        "c465db45-68b4-8c85-a1e4-8a0dbc2d6512",
        "the derivation changed; every parcel id already written no longer matches its PNU"
    );
    Ok(())
}
