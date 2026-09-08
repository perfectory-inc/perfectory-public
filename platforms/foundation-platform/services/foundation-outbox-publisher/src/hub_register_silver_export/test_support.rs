use super::*;
use std::io::{Cursor, SeekFrom, Write};

struct Fragmented(Cursor<Vec<u8>>);

impl Read for Fragmented {
    fn read(&mut self, target: &mut [u8]) -> std::io::Result<usize> {
        let count = target.len().min(7);
        self.0.read(&mut target[..count])
    }
}

impl Seek for Fragmented {
    fn seek(&mut self, position: SeekFrom) -> std::io::Result<u64> {
        self.0.seek(position)
    }
}

pub(super) fn convert_fixture(
    zip: Vec<u8>,
    mut layout: Layout,
    rows_per_part: u64,
) -> anyhow::Result<Value> {
    let temp = std::env::temp_dir().join(format!("hub-price-{}", Uuid::new_v4()));
    std::fs::create_dir_all(&temp)?;
    let result = (|| {
        layout.rows_per_part = rows_per_part;
        let config = Config {
            input_object_key: layout.selected()?.object_key.clone(),
            output_prefix: OutputSink::R2Object(format!(
                "silver-handoff/{}",
                layout.source.trim_start_matches("bronze/source=")
            )),
            source_snapshot_id: "SYNTHETIC-test".into(),
            summary_path: Some(temp.join("summary.json")),
        };
        let runtime = tokio::runtime::Builder::new_current_thread().build()?;
        let result = convert(Fragmented(Cursor::new(zip)), &config, &layout, |output| {
            runtime.block_on(open_sink(
                &OutputSink::LocalPath(temp.join(output_name(output))),
                None,
            ))
        });
        let manifest = temp.join(output_name(
            &config.output(&format!("{}/manifest.json", config.object_stem()?)),
        ));
        if result.is_err() {
            assert!(!manifest.exists(), "failed conversion published a manifest");
            assert!(
                !temp.join("summary.json").exists(),
                "failed conversion published summary"
            );
        }
        let report = result?;
        let mut value = serde_json::to_value(&report)?;
        let persisted: Value = serde_json::from_slice(&std::fs::read(manifest)?)?;
        assert_eq!(persisted, value);
        assert_eq!(
            serde_json::from_slice::<Value>(&std::fs::read(temp.join("summary.json"))?)?,
            value
        );
        let mut rows = Vec::<Value>::new();
        for part in &report.parts {
            let file = std::fs::File::open(temp.join(&part.object_key))?;
            let mut text = String::new();
            flate2::read::GzDecoder::new(file).read_to_string(&mut text)?;
            let decoded = text
                .lines()
                .map(serde_json::from_str::<Value>)
                .collect::<Result<Vec<_>, _>>()?;
            assert_eq!(decoded.len() as u64, part.rows);
            rows.extend(decoded);
        }
        value["decoded_rows"] = json!(rows);
        Ok(value)
    })();
    std::fs::remove_dir_all(temp)?;
    result
}

pub(super) fn fixture_named(text: &str, member: &str) -> anyhow::Result<Vec<u8>> {
    let mut archive = zip::ZipWriter::new(Cursor::new(Vec::new()));
    archive.start_file(
        member,
        zip::write::SimpleFileOptions::default()
            .compression_method(zip::CompressionMethod::Deflated)
            .large_file(true),
    )?;
    archive.write_all(text.as_bytes())?;
    Ok(archive.finish()?.into_inner())
}
