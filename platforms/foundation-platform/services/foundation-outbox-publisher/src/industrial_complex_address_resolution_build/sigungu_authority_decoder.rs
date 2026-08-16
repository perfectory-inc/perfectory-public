//! Reads the current city/county/district codes out of a dBase attribute table.
//!
//! The district authority ships as the `.dbf` beside a V-World boundary shapefile. Only the code
//! column is read: the geometry is never opened, because a polygon is not evidence for which
//! district a complex belongs to (root ADR-0020) — the codes are, and they are plain text in the
//! attribute table.
//!
//! dBase III/5 layout, which is all this needs to know: a 32-byte header carrying the record count,
//! the header length, and the record length; then 32-byte field descriptors terminated by `0x0D`;
//! then fixed-width records, each prefixed by a one-byte deletion flag.

use std::collections::BTreeSet;

use anyhow::{bail, Context as _};

/// Column carrying the administrative code in the V-World boundary attribute tables.
const CODE_COLUMN: &str = "BJCD";

/// Length of a city/county/district code.
const SIGUNGU_CODE_LEN: usize = 5;

/// Length of a legal-division code in its full form.
const ADMINISTRATIVE_CODE_LEN: usize = 10;

/// The five trailing digits a district code carries in its full ten-digit form.
const SIGUNGU_CODE_SUFFIX: &str = "00000";

/// Fixed size of the dBase file header and of one field descriptor.
const DBF_HEADER_LEN: usize = 32;

/// Terminator byte that ends the field-descriptor block.
const DBF_FIELD_TERMINATOR: u8 = 0x0D;

/// Deletion flag written on a record that has been marked deleted.
const DBF_DELETED_FLAG: u8 = b'*';

/// Where the code column sits inside one fixed-width record.
struct DbfField {
    offset: usize,
    length: usize,
}

/// Reads the set of current five-digit district codes out of a dBase attribute table.
///
/// # Errors
/// Returns an error when the bytes are not a readable dBase table, when the code column is absent,
/// when a value is not a five-digit code, or when the table yields no codes at all.
pub(super) fn decode_sigungu_authority(bytes: &[u8]) -> anyhow::Result<BTreeSet<String>> {
    if bytes.len() < DBF_HEADER_LEN {
        bail!(
            "the district authority is not a dBase table: {} bytes",
            bytes.len()
        );
    }
    let record_count = u32::from_le_bytes(header_slice(bytes, 4, 4)?.try_into()?) as usize;
    let header_len = u16::from_le_bytes(header_slice(bytes, 8, 2)?.try_into()?) as usize;
    let record_len = u16::from_le_bytes(header_slice(bytes, 10, 2)?.try_into()?) as usize;
    if header_len < DBF_HEADER_LEN || record_len == 0 {
        bail!(
            "the district authority declares an unusable dBase layout: header {header_len}, \
             record {record_len}"
        );
    }

    let field = code_field(bytes, header_len)?;
    let mut codes = BTreeSet::new();
    for index in 0..record_count {
        let start = header_len + index * record_len;
        let end = start + record_len;
        let Some(record) = bytes.get(start..end) else {
            bail!(
                "the district authority declares {record_count} records but ends after {index}; \
                 the file is truncated"
            );
        };
        if record.first() == Some(&DBF_DELETED_FLAG) {
            continue;
        }
        let raw = record
            .get(field.offset..field.offset + field.length)
            .with_context(|| {
                format!("dBase record {index} is shorter than its {CODE_COLUMN} column")
            })?;
        let value = std::str::from_utf8(raw)
            .with_context(|| format!("dBase record {index} {CODE_COLUMN} is not ASCII"))?
            .trim();
        if value.is_empty() {
            continue;
        }
        let Some(district) = district_prefix(value) else {
            bail!(
                "the district authority carries a non-district {CODE_COLUMN} at record {index}: \
                 {value:?}; this table must be the city/county/district layer"
            );
        };
        codes.insert(district.to_owned());
    }
    if codes.is_empty() {
        bail!("the district authority decoded zero codes; without it nothing can be checked");
    }
    Ok(codes)
}

/// Returns the five-digit district prefix of an authority code, or `None` if it is not one.
///
/// The V-World district layer writes the code in its full ten-digit form (`1111000000`), while other
/// exports of the same layer write the bare five digits. Both name a district. A ten-digit code
/// whose last five digits are not zeros names an eup/myeon/dong, which is a different layer, and
/// accepting it here would widen the authority into something that answers a different question.
fn district_prefix(value: &str) -> Option<&str> {
    if !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    match value.len() {
        SIGUNGU_CODE_LEN => Some(value),
        ADMINISTRATIVE_CODE_LEN if value.ends_with(SIGUNGU_CODE_SUFFIX) => {
            Some(&value[0..SIGUNGU_CODE_LEN])
        }
        _ => None,
    }
}

fn code_field(bytes: &[u8], header_len: usize) -> anyhow::Result<DbfField> {
    let mut offset = DBF_HEADER_LEN;
    // The deletion flag occupies the first byte of every record, so field offsets start at one.
    let mut record_offset = 1_usize;
    let mut observed = Vec::new();
    while offset + DBF_HEADER_LEN <= header_len {
        let descriptor = bytes
            .get(offset..offset + DBF_HEADER_LEN)
            .context("dBase field descriptor block is truncated")?;
        if descriptor[0] == DBF_FIELD_TERMINATOR {
            break;
        }
        let name = descriptor
            .iter()
            .take(11)
            .take_while(|byte| **byte != 0)
            .map(|byte| char::from(*byte))
            .collect::<String>();
        let length = usize::from(descriptor[16]);
        observed.push(name.clone());
        if name == CODE_COLUMN {
            return Ok(DbfField {
                offset: record_offset,
                length,
            });
        }
        record_offset += length;
        offset += DBF_HEADER_LEN;
    }
    bail!(
        "the district authority has no {CODE_COLUMN} column; columns present: {}",
        observed.join(", ")
    )
}

fn header_slice(bytes: &[u8], start: usize, length: usize) -> anyhow::Result<&[u8]> {
    bytes
        .get(start..start + length)
        .context("dBase header is truncated")
}

#[cfg(test)]
mod tests {
    use super::decode_sigungu_authority;

    /// Builds a minimal dBase III table with the given columns and rows.
    fn dbf(columns: &[(&str, usize)], rows: &[Vec<&str>]) -> Vec<u8> {
        let header_len = 32 + columns.len() * 32 + 1;
        let record_len = 1 + columns.iter().map(|(_, length)| *length).sum::<usize>();
        let mut bytes = vec![0_u8; 32];
        bytes[0] = 0x03;
        bytes[4..8].copy_from_slice(&(rows.len() as u32).to_le_bytes());
        bytes[8..10].copy_from_slice(&(header_len as u16).to_le_bytes());
        bytes[10..12].copy_from_slice(&(record_len as u16).to_le_bytes());
        for (name, length) in columns {
            let mut descriptor = vec![0_u8; 32];
            descriptor[..name.len()].copy_from_slice(name.as_bytes());
            descriptor[11] = b'C';
            descriptor[16] = u8::try_from(*length).expect("column length fits a byte");
            bytes.extend_from_slice(&descriptor);
        }
        bytes.push(0x0D);
        for row in rows {
            bytes.push(b' ');
            for ((_, length), value) in columns.iter().zip(row) {
                let mut cell = vec![b' '; *length];
                cell[..value.len()].copy_from_slice(value.as_bytes());
                bytes.extend_from_slice(&cell);
            }
        }
        bytes
    }

    /// The V-World district layer writes the code in its full ten-digit form, so the decoder has to
    /// reduce it to the district prefix rather than store `1111000000` as if it were a dong.
    #[test]
    fn reads_the_code_column_past_the_columns_before_it() -> anyhow::Result<()> {
        let bytes = dbf(
            // The UFID column exists here only to put the code column at a non-zero offset. Its
            // value is a synthetic marker rather than a real-looking feature id: a long digit run
            // in a committed fixture reads as an assignable PNU.
            &[("UFID", 34), ("BJCD", 10), ("DIVI", 6)],
            &[
                vec!["SYNTHETIC-UFID-A", "1111000000", "G01010"],
                vec!["SYNTHETIC-UFID-B", "1114000000", "G01010"],
            ],
        );

        let codes = decode_sigungu_authority(&bytes)?;

        assert_eq!(codes.len(), 2);
        assert!(codes.contains("11110"));
        assert!(codes.contains("11140"));
        Ok(())
    }

    /// The same layer is exported elsewhere with the bare five digits; both name a district.
    #[test]
    fn a_bare_five_digit_district_code_is_accepted() -> anyhow::Result<()> {
        let bytes = dbf(&[("BJCD", 10)], &[vec!["11110"]]);

        let codes = decode_sigungu_authority(&bytes)?;

        assert_eq!(codes.len(), 1);
        assert!(codes.contains("11110"));
        Ok(())
    }

    /// The authority is what makes "is this code current?" answerable. A table without the code
    /// column would otherwise decode to an empty set and silently reject every complex.
    #[test]
    fn a_table_without_the_code_column_names_the_columns_it_does_have() {
        let bytes = dbf(&[("UFID", 34), ("NAME", 10)], &[vec!["x", "서울"]]);

        let error =
            decode_sigungu_authority(&bytes).expect_err("a table without BJCD must be rejected");

        let message = format!("{error:#}");
        assert!(message.contains("BJCD"), "{message}");
        assert!(message.contains("UFID"), "{message}");
    }

    /// The eup/myeon/dong boundary layer uses the same column name with ten-digit values. Feeding
    /// it in where the district layer belongs must fail rather than silently widen the authority.
    #[test]
    fn a_ten_digit_code_layer_is_rejected_as_the_district_authority() {
        let bytes = dbf(&[("BJCD", 10)], &[vec!["1153010200"]]);

        let error = decode_sigungu_authority(&bytes)
            .expect_err("a legal-dong layer must not pass as the district authority");

        assert!(format!("{error:#}").contains("1153010200"), "{error:#}");
    }

    #[test]
    fn a_deleted_record_is_skipped() -> anyhow::Result<()> {
        let mut bytes = dbf(&[("BJCD", 10)], &[vec!["1111000000"], vec!["1114000000"]]);
        let header_len = 32 + 32 + 1;
        let record_len = 1 + 10;
        bytes[header_len + record_len] = b'*';

        let codes = decode_sigungu_authority(&bytes)?;

        assert_eq!(codes.len(), 1);
        assert!(codes.contains("11110"));
        Ok(())
    }

    #[test]
    fn an_empty_table_is_rejected() {
        let bytes = dbf(&[("BJCD", 10)], &[]);

        let error = decode_sigungu_authority(&bytes).expect_err("an empty authority must fail");

        assert!(format!("{error:#}").contains("zero codes"), "{error:#}");
    }

    #[test]
    fn a_truncated_table_is_rejected() {
        let mut bytes = dbf(&[("BJCD", 10)], &[vec!["1111000000"], vec!["1114000000"]]);
        bytes.truncate(bytes.len() - 6);

        let error = decode_sigungu_authority(&bytes).expect_err("a truncated table must fail");

        assert!(format!("{error:#}").contains("truncated"), "{error:#}");
    }

    #[test]
    fn a_file_that_is_not_a_dbase_table_is_rejected() {
        let error =
            decode_sigungu_authority(b"not a dbf").expect_err("a non-dBase file must be rejected");

        assert!(format!("{error:#}").contains("dBase"), "{error:#}");
    }
}
