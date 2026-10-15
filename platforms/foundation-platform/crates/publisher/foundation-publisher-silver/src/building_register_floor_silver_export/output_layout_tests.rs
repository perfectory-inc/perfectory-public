use super::{export_handoff, ExportConfig, OutputFormat, SourceSelector};
use anyhow::Context;
use std::{fs, path::Path};

fn config(root: &Path) -> ExportConfig {
    ExportConfig {
        bronze_local_object_root: root.to_path_buf(),
        source_selector: SourceSelector::Exact("floor-test".into()),
        exact_inputs: None,
        committed_inputs: None,
        reuse_completed: false,
        output_path: root.join("silver/rows.jsonl"),
        proposal_input_path: Some(root.join("proposals/rows.jsonl")),
        summary_path: Some(root.join("audit/ready.json")),
        source_snapshot_id: "floor-layout-test".into(),
        valid_from_utc: chrono::Utc::now(),
        ingested_at_utc: chrono::Utc::now(),
        max_rows: None,
        chunk_rows: None,
        output_format: OutputFormat::Jsonl,
        title_source_slug: None,
    }
}

#[tokio::test]
async fn colliding_paths_fail_before_touching_inputs_or_outputs() -> anyhow::Result<()> {
    for case in 0..7 {
        let root = tempfile::tempdir()?;
        let input = root.path().join("bronze/source=floor-test/input.json");
        fs::create_dir_all(input.parent().context("input parent")?)?;
        fs::write(&input, b"source sentinel")?;
        let sentinel = root.path().join("silver/keep.jsonl");
        fs::create_dir_all(sentinel.parent().context("output parent")?)?;
        fs::write(&sentinel, b"output sentinel")?;
        let mut config = config(root.path());
        match case {
            0 => {
                config.output_path = sentinel.clone();
                config.proposal_input_path = Some(sentinel.clone());
            }
            1 => config.output_path = input.clone(),
            2 => config.proposal_input_path = Some(input.clone()),
            3 => {
                config.output_path = root.path().to_path_buf();
                config.chunk_rows = Some(1);
            }
            4 => {
                config.output_path = root.path().join("silver");
                config.proposal_input_path = Some(root.path().join("silver/proposals"));
                config.chunk_rows = Some(1);
            }
            5 => {
                config.output_path = root.path().join("silver");
                config.summary_path = Some(root.path().join("silver/part-000001.jsonl"));
                config.chunk_rows = Some(1);
            }
            6 => {
                config.output_path = root.path().join("silver/new/rows.jsonl");
                config.proposal_input_path = Some(root.path().join("silver/new"));
                config.chunk_rows = Some(1);
            }
            _ => unreachable!(),
        }
        let error = export_handoff(&config)
            .await
            .err()
            .context("overlap must fail")?;
        assert!(
            error.to_string().contains("overlap"),
            "case {case}: {error:#}"
        );
        assert_eq!(fs::read(&input)?, b"source sentinel", "case {case}");
        assert_eq!(fs::read(&sentinel)?, b"output sentinel", "case {case}");
        assert!(!config.summary_path.as_ref().context("summary")?.exists());
    }
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn filesystem_aliases_cannot_hide_output_collisions() -> anyhow::Result<()> {
    use std::os::unix::fs::symlink;
    for hard_link in [false, true] {
        let root = tempfile::tempdir()?;
        let input = root.path().join("bronze/source=floor-test/input.json");
        fs::create_dir_all(input.parent().context("input parent")?)?;
        fs::write(&input, b"source sentinel")?;
        let mut config = config(root.path());
        if hard_link {
            fs::hard_link(&input, root.path().join("alias.jsonl"))?;
            config.output_path = root.path().join("alias.jsonl");
        } else {
            fs::create_dir(root.path().join("silver"))?;
            symlink(root.path().join("silver"), root.path().join("alias"))?;
            config.proposal_input_path = Some(root.path().join("alias/rows.jsonl"));
        }
        let error = export_handoff(&config)
            .await
            .err()
            .context("alias must fail")?;
        assert!(error.to_string().contains("overlap"), "{error:#}");
        assert_eq!(fs::read(&input)?, b"source sentinel");
        assert!(!config.summary_path.as_ref().context("summary")?.exists());
    }
    Ok(())
}
