//! Attribute-only access using the same dBase decoder and forward seek adapter as SHP/DBF.

use std::io::Read;

use anyhow::{bail, Context};
use shapefile::dbase::{self, encoding::EncodingRs, Record};

use crate::{encoding_from_cpg, ForwardOnlySeek};

/// Visits DBF records without reading, projecting, or buffering the companion geometry.
/// The caller selects the ZIP member and declares its measured encoding and field names.
///
/// # Errors
/// Refuses schema drift, decoding errors, visitor failures, and truncated record streams.
pub fn for_each_dbf_record(
    source: impl Read,
    encoding_label: &str,
    expected_fields: &[&str],
    mut visitor: impl FnMut(Record) -> anyhow::Result<()>,
) -> anyhow::Result<u64> {
    let encoding = encoding_from_cpg(encoding_label)?;
    let mut reader = dbase::ReaderBuilder::new()
        .with_encoding(EncodingRs::from(encoding))
        .build(ForwardOnlySeek::new(source))
        .context("failed to open attribute-only DBF stream")?;
    let fields: Vec<&str> = reader.fields().iter().map(dbase::FieldInfo::name).collect();
    if fields != expected_fields {
        bail!("dbf_attribute_schema_mismatch: expected {expected_fields:?}, got {fields:?}");
    }
    let mut count = 0_u64;
    // The header counts physical records, including deleted ones. The upstream iterator
    // consumes all of them and yields only live records. Its read_exact and our forward-only
    // seek reject truncation even while skipping a deleted record.
    for record in reader.iter_records() {
        visitor(record.context("failed to decode DBF attributes")?)?;
        count += 1;
    }
    Ok(count)
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;

    fn fixture() -> anyhow::Result<Vec<u8>> {
        let mut bytes = Cursor::new(Vec::new());
        {
            let field = dbase::FieldName::try_from("A1").map_err(|e| anyhow::anyhow!(e))?;
            let mut writer =
                dbase::TableWriterBuilder::with_encoding(EncodingRs::from(encoding_rs::EUC_KR))
                    .add_character_field(field, 19)
                    .build_with_dest(&mut bytes);
            for pnu in ["9999938029104450003", "9999938029104450004"] {
                let mut row = Record::default();
                row.insert("A1".into(), dbase::FieldValue::Character(Some(pnu.into())));
                writer.write_record(&row)?;
            }
        }
        Ok(bytes.into_inner())
    }

    #[test]
    fn deleted_records_are_consumed_without_counting_as_live_rows() -> anyhow::Result<()> {
        let mut bytes = fixture()?;
        let records_start = usize::from(u16::from_le_bytes([bytes[8], bytes[9]]));
        bytes[records_start] = b'*';
        let mut visited = 0;
        let count = for_each_dbf_record(&bytes[..], "EUC-KR", &["A1"], |_| {
            visited += 1;
            Ok(())
        })?;
        assert_eq!((count, visited), (1, 1));
        Ok(())
    }

    #[test]
    fn truncated_live_and_deleted_records_both_refuse() -> anyhow::Result<()> {
        for deleted in [false, true] {
            let mut bytes = fixture()?;
            let records_start = usize::from(u16::from_le_bytes([bytes[8], bytes[9]]));
            let record_bytes = usize::from(u16::from_le_bytes([bytes[10], bytes[11]]));
            if deleted {
                bytes[records_start + record_bytes] = b'*';
            }
            bytes.truncate(records_start + record_bytes + 3);
            assert!(for_each_dbf_record(&bytes[..], "EUC-KR", &["A1"], |_| Ok(())).is_err());
        }
        Ok(())
    }

    #[test]
    fn shifted_schema_refuses_before_visiting_rows() -> anyhow::Result<()> {
        let bytes = fixture()?;
        let mut visited = false;
        let error = for_each_dbf_record(&bytes[..], "EUC-KR", &["A2"], |_| {
            visited = true;
            Ok(())
        })
        .err()
        .context("drift must refuse")?;
        assert!(error.to_string().contains("dbf_attribute_schema_mismatch"));
        assert!(!visited);
        Ok(())
    }
}
