//! Reader for the dBase attribute table that ships beside every ESRI shapefile.
//!
//! dBase III/5 layout, which is all this needs to know: a 32-byte header carrying the record count,
//! the header length, and the record length; then 32-byte field descriptors terminated by `0x0D`;
//! then fixed-width records, each prefixed by a one-byte deletion flag.
//!
//! This module knows the file format and nothing about what any column means. Deciding which column
//! answers which question, and how its text is encoded, belongs to the caller: the district
//! authority reads ASCII codes out of `BJCD`, the industrial-complex boundary reader reads
//! `DAN_ID`, and neither is a fact about dBase.

use anyhow::{bail, Context as _};

/// Fixed size of the dBase file header and of one field descriptor.
const DBF_HEADER_LEN: usize = 32;

/// Terminator byte that ends the field-descriptor block.
const DBF_FIELD_TERMINATOR: u8 = 0x0D;

/// Deletion flag written on a record that has been marked deleted.
const DBF_DELETED_FLAG: u8 = b'*';

/// How many bytes of a field descriptor hold the column name.
const DBF_FIELD_NAME_LEN: usize = 11;

/// Where the field length sits inside a field descriptor.
const DBF_FIELD_LENGTH_OFFSET: usize = 16;

/// Where one column sits inside a fixed-width record.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DbaseField {
    /// Column name as the table declares it.
    pub(crate) name: String,
    /// Byte offset of the column inside one record, counted past the deletion flag.
    pub(crate) offset: usize,
    /// Byte length of the column.
    pub(crate) length: usize,
}

/// A dBase attribute table opened over its raw bytes.
#[derive(Debug)]
pub(crate) struct DbaseTable<'a> {
    bytes: &'a [u8],
    header_len: usize,
    record_len: usize,
    record_count: usize,
    fields: Vec<DbaseField>,
}

impl<'a> DbaseTable<'a> {
    /// Opens a dBase table over its raw bytes.
    ///
    /// # Errors
    /// Returns an error when the bytes are too short to be a dBase table, when the declared layout
    /// is unusable, or when the field-descriptor block is truncated.
    pub(crate) fn open(bytes: &'a [u8]) -> anyhow::Result<Self> {
        if bytes.len() < DBF_HEADER_LEN {
            bail!("not a dBase table: {} bytes", bytes.len());
        }
        let record_count = u32::from_le_bytes(header_slice(bytes, 4, 4)?.try_into()?) as usize;
        let header_len = u16::from_le_bytes(header_slice(bytes, 8, 2)?.try_into()?) as usize;
        let record_len = u16::from_le_bytes(header_slice(bytes, 10, 2)?.try_into()?) as usize;
        if header_len < DBF_HEADER_LEN || record_len == 0 {
            bail!(
                "the dBase table declares an unusable layout: header {header_len}, \
                 record {record_len}"
            );
        }

        let fields = read_fields(bytes, header_len)?;
        Ok(Self {
            bytes,
            header_len,
            record_len,
            record_count,
            fields,
        })
    }

    /// Number of records the table declares, deleted ones included.
    pub(crate) const fn record_count(&self) -> usize {
        self.record_count
    }

    /// Every column the table declares, in file order.
    pub(crate) fn fields(&self) -> &[DbaseField] {
        &self.fields
    }

    /// Returns the named column, or `None` when the table does not declare it.
    pub(crate) fn field(&self, name: &str) -> Option<&DbaseField> {
        self.fields.iter().find(|field| field.name == name)
    }

    /// Returns record `index`, or `None` when that record is marked deleted.
    ///
    /// # Errors
    /// Returns an error when the file ends before the record the header promised.
    pub(crate) fn record(&self, index: usize) -> anyhow::Result<Option<DbaseRecord<'_>>> {
        let start = self.header_len + index * self.record_len;
        let end = start + self.record_len;
        let Some(bytes) = self.bytes.get(start..end) else {
            bail!(
                "the dBase table declares {} records but ends after {index}; the file is truncated",
                self.record_count
            );
        };
        if bytes.first() == Some(&DBF_DELETED_FLAG) {
            return Ok(None);
        }
        Ok(Some(DbaseRecord { bytes }))
    }
}

/// One live record of a dBase table.
#[derive(Debug)]
pub(crate) struct DbaseRecord<'a> {
    bytes: &'a [u8],
}

impl DbaseRecord<'_> {
    /// Returns the raw bytes of one column, or `None` when the record is shorter than the column.
    ///
    /// The bytes are returned undecoded: a `.cpg` sidecar, not this file format, decides which
    /// character encoding they carry.
    pub(crate) fn raw(&self, field: &DbaseField) -> Option<&[u8]> {
        self.bytes.get(field.offset..field.offset + field.length)
    }
}

fn read_fields(bytes: &[u8], header_len: usize) -> anyhow::Result<Vec<DbaseField>> {
    let mut fields = Vec::new();
    let mut offset = DBF_HEADER_LEN;
    // The deletion flag occupies the first byte of every record, so field offsets start at one.
    let mut record_offset = 1_usize;
    while offset + DBF_HEADER_LEN <= header_len {
        let descriptor = bytes
            .get(offset..offset + DBF_HEADER_LEN)
            .context("dBase field descriptor block is truncated")?;
        if descriptor[0] == DBF_FIELD_TERMINATOR {
            break;
        }
        let name = descriptor
            .iter()
            .take(DBF_FIELD_NAME_LEN)
            .take_while(|byte| **byte != 0)
            .map(|byte| char::from(*byte))
            .collect::<String>();
        let length = usize::from(descriptor[DBF_FIELD_LENGTH_OFFSET]);
        fields.push(DbaseField {
            name,
            offset: record_offset,
            length,
        });
        record_offset += length;
        offset += DBF_HEADER_LEN;
    }
    Ok(fields)
}

fn header_slice(bytes: &[u8], start: usize, length: usize) -> anyhow::Result<&[u8]> {
    bytes
        .get(start..start + length)
        .context("dBase header is truncated")
}

#[cfg(test)]
pub(crate) mod test_support {
    /// Builds a minimal dBase III table with the given columns and rows.
    ///
    /// Values are written as raw bytes so a test can hand it text in whatever encoding the provider
    /// it stands in for actually uses.
    pub(crate) fn dbase_bytes(columns: &[(&str, usize)], rows: &[Vec<&[u8]>]) -> Vec<u8> {
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
            descriptor[16] = u8::try_from(*length).unwrap_or(u8::MAX);
            bytes.extend_from_slice(&descriptor);
        }
        bytes.push(0x0D);
        for row in rows {
            bytes.push(b' ');
            for ((_, length), value) in columns.iter().zip(row) {
                let mut cell = vec![b' '; *length];
                cell[..value.len()].copy_from_slice(value);
                bytes.extend_from_slice(&cell);
            }
        }
        bytes
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Context as _;

    use super::{test_support::dbase_bytes, DbaseTable};

    #[test]
    fn reads_columns_and_their_record_offsets() -> anyhow::Result<()> {
        let bytes = dbase_bytes(&[("A", 3), ("B", 2)], &[vec![b"xyz", b"pq"]]);

        let table = DbaseTable::open(&bytes)?;

        let names = table
            .fields()
            .iter()
            .map(|field| field.name.clone())
            .collect::<Vec<_>>();
        assert_eq!(names, vec!["A".to_owned(), "B".to_owned()]);
        let b = table.field("B").context("column B must be present")?;
        let record = table.record(0)?.context("record 0 must be live")?;
        assert_eq!(record.raw(b), Some(&b"pq"[..]));
        Ok(())
    }

    /// The reader hands back raw bytes, so a column carrying non-ASCII text survives intact for a
    /// caller that knows the `.cpg` encoding.
    #[test]
    fn a_non_ascii_column_is_returned_undecoded() -> anyhow::Result<()> {
        let bytes = dbase_bytes(&[("NAME", 4)], &[vec![&[0xB0, 0xE6, 0xB1, 0xE2]]]);

        let table = DbaseTable::open(&bytes)?;

        let field = table.field("NAME").context("column NAME must be present")?;
        let record = table.record(0)?.context("record 0 must be live")?;
        assert_eq!(record.raw(field), Some(&[0xB0, 0xE6, 0xB1, 0xE2][..]));
        Ok(())
    }

    #[test]
    fn a_deleted_record_reads_as_absent() -> anyhow::Result<()> {
        let mut bytes = dbase_bytes(&[("A", 2)], &[vec![b"aa"], vec![b"bb"]]);
        let header_len = 32 + 32 + 1;
        bytes[header_len + 3] = b'*';

        let table = DbaseTable::open(&bytes)?;

        assert!(table.record(0)?.is_some());
        assert!(table.record(1)?.is_none());
        Ok(())
    }

    #[test]
    fn a_truncated_table_is_rejected() {
        let mut bytes = dbase_bytes(&[("A", 2)], &[vec![b"aa"], vec![b"bb"]]);
        bytes.truncate(bytes.len() - 2);
        let table = DbaseTable::open(&bytes).expect("the header is intact");

        let error = table.record(1).expect_err("a truncated record must fail");

        assert!(format!("{error:#}").contains("truncated"), "{error:#}");
    }

    #[test]
    fn a_file_that_is_not_a_dbase_table_is_rejected() {
        let error = DbaseTable::open(b"not a dbf").expect_err("a non-dBase file must be rejected");

        assert!(format!("{error:#}").contains("dBase"), "{error:#}");
    }
}
