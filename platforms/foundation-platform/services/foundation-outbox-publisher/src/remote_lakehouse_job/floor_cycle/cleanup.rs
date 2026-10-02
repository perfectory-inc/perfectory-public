//! Stop only the one-off containers owned by this systemd invocation.
use std::{
    io::Read as _,
    path::Path,
    process::{Command, Stdio},
};

use anyhow::{ensure, Context};

const MAX_LIST_BYTES: u64 = 4096;

pub(super) fn project_id(
    lookup: &mut impl FnMut(&str) -> Option<String>,
) -> anyhow::Result<String> {
    let invocation = lookup("INVOCATION_ID").context("FLOOR requires systemd INVOCATION_ID")?;
    ensure!(
        invocation.len() == 32
            && invocation
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
        "invalid systemd INVOCATION_ID"
    );
    Ok(format!("foundation-floor-{invocation}"))
}

pub(super) fn run() -> anyhow::Result<()> {
    let project = project_id(&mut |name| std::env::var(name).ok())?;
    cleanup_with(Path::new("docker"), &project)
}

fn container_ids(raw: &[u8]) -> anyhow::Result<Vec<String>> {
    ensure!(
        raw.len() as u64 <= MAX_LIST_BYTES,
        "FLOOR container list exceeds its bound"
    );
    let text = std::str::from_utf8(raw).context("invalid Docker container list")?;
    text.lines()
        .map(|line| {
            ensure!(
                (line.len() == 12 || line.len() == 64)
                    && line
                        .bytes()
                        .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
                "invalid Docker container ID"
            );
            Ok(line.to_owned())
        })
        .collect()
}

fn list(docker: &Path, project: &str) -> anyhow::Result<Vec<String>> {
    list_ids(
        docker,
        &[
            "ps",
            "--all",
            "--quiet",
            "--no-trunc",
            "--filter",
            &format!("label=com.docker.compose.project={project}"),
            "--filter",
            "label=com.docker.compose.oneoff=True",
        ],
    )
}

fn networks(docker: &Path, project: &str) -> anyhow::Result<Vec<String>> {
    list_ids(
        docker,
        &[
            "network",
            "ls",
            "--quiet",
            "--no-trunc",
            "--filter",
            &format!("label=com.docker.compose.project={project}"),
        ],
    )
}

fn list_ids(docker: &Path, arguments: &[&str]) -> anyhow::Result<Vec<String>> {
    let mut child = Command::new(docker)
        .args(arguments)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .context("cannot list FLOOR containers")?;
    let mut bytes = Vec::new();
    let read = child
        .stdout
        .take()
        .context("missing Docker stdout")?
        .take(MAX_LIST_BYTES + 1)
        .read_to_end(&mut bytes);
    if read.is_err() || bytes.len() as u64 > MAX_LIST_BYTES {
        let _ = child.kill();
    }
    let status = child
        .wait()
        .context("cannot wait for Docker container list")?;
    read.context("cannot read Docker container list")?;
    ensure!(status.success(), "Docker container listing failed");
    container_ids(&bytes)
}

fn cleanup_with(docker: &Path, project: &str) -> anyhow::Result<()> {
    cleanup_containers(docker, project)?;
    let ids = networks(docker, project)?;
    if ids.is_empty() {
        return Ok(());
    }
    let removed = Command::new(docker)
        .args(["network", "rm"])
        .args(&ids)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .status()
        .context("cannot remove FLOOR networks")?;
    ensure!(
        networks(docker, project)?.is_empty(),
        "FLOOR networks remain after cleanup (rm success={})",
        removed.success()
    );
    Ok(())
}

fn cleanup_containers(docker: &Path, project: &str) -> anyhow::Result<()> {
    let ids = list(docker, project)?;
    if ids.is_empty() {
        return Ok(());
    }
    let stopped = Command::new(docker)
        .args(["stop", "--time", "30"])
        .args(&ids)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .status()
        .context("cannot stop FLOOR containers")?;
    let remaining = list(docker, project)?;
    // --rm can remove a container between listing and stopping. A failed command is
    // harmless only when the exact invocation has no containers left.
    ensure!(
        stopped.success() || remaining.is_empty(),
        "FLOOR container stop failed"
    );
    if remaining.is_empty() {
        return Ok(());
    }
    let removed = Command::new(docker)
        .arg("rm")
        .args(&remaining)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .status()
        .context("cannot remove FLOOR containers")?;
    let after = list(docker, project)?;
    ensure!(
        after.is_empty(),
        "FLOOR containers remain after cleanup (rm success={})",
        removed.success()
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn invocation_is_an_exact_systemd_identity() -> anyhow::Result<()> {
        let id = "a".repeat(32);
        assert_eq!(
            project_id(&mut |_| Some(id.clone()))?,
            format!("foundation-floor-{id}")
        );
        for invalid in ["", "../other", "ABCDEF", &"A".repeat(32), &"a".repeat(33)] {
            assert!(project_id(&mut |_| Some(invalid.into())).is_err());
        }
        assert!(project_id(&mut |_| None).is_err());
        Ok(())
    }

    #[test]
    fn docker_ids_are_bounded_and_cannot_be_options() -> anyhow::Result<()> {
        assert!(container_ids(b"")?.is_empty());
        assert_eq!(container_ids(b"abcdef123456\n")?, ["abcdef123456"]);
        for invalid in [b"--all\n".as_slice(), b" \n", b"ABCDEF123456\n", b"123\n"] {
            assert!(container_ids(invalid).is_err());
        }
        assert!(container_ids(&vec![b'a'; 4097]).is_err());
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn leftover_network_is_removed_without_containers_and_failure_is_reported() -> anyhow::Result<()>
    {
        use std::os::unix::fs::PermissionsExt as _;
        let directory = tempfile::tempdir()?;
        let docker = directory.path().join("docker");
        let marker = directory.path().join("network");
        let quoted = super::super::super::shell_quote(&marker.to_string_lossy());
        let id = "a".repeat(64);
        for removable in [true, false] {
            std::fs::write(&marker, b"exists")?;
            let removal = if removable {
                format!("rm -- {quoted}")
            } else {
                "exit 1".into()
            };
            std::fs::write(&docker, format!(
                "#!/bin/sh\ncase \"$*\" in\n'ps --all --quiet --no-trunc --filter label=com.docker.compose.project=foundation-floor-test --filter label=com.docker.compose.oneoff=True') exit 0 ;;\n'network ls --quiet --no-trunc --filter label=com.docker.compose.project=foundation-floor-test') if [ -f {quoted} ]; then echo {id}; fi ;;\n'network rm {id}') {removal} ;;\n*) exit 99 ;;\nesac\n"
            ))?;
            std::fs::set_permissions(&docker, std::fs::Permissions::from_mode(0o755))?;
            assert_eq!(
                cleanup_with(&docker, "foundation-floor-test").is_ok(),
                removable
            );
            assert_eq!(marker.exists(), !removable);
        }
        Ok(())
    }
}
