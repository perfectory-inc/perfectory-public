//! How much more memory this process may allocate, read from the kernel at run time.
//!
//! A command that sizes its working set (the HUB export's worker count) must follow the limit of
//! whatever runs it, not a number written beside the code: the Silver lane unit's
//! `MemoryMax=16G` (`infra/systemd/foundation-silver-refresh@.service`, counted by
//! `tools/host-memory-budget.contract.json`), a container's `--memory`, or a host with no cap.
//! Linux publishes that limit as the process's cgroup: `memory.max` (cgroup v2) or
//! `memory.limit_in_bytes` (v1) on the cgroup `/proc/self/cgroup` names, and on every ancestor,
//! since a slice's cap bounds everything under it. A limit is a share of the whole cgroup, so what
//! is left is the limit minus the cgroup's current usage. The host's `MemAvailable` bounds it too.
use std::path::{Path, PathBuf};

const CGROUP_V2_ROOT: &str = "/sys/fs/cgroup";
const CGROUP_V1_MEMORY_ROOT: &str = "/sys/fs/cgroup/memory";
/// cgroup v1 writes "no limit" as a page-rounded `i64::MAX`; anything this large is no limit.
const CGROUP_V1_UNLIMITED: u64 = 1 << 62;

/// What workers may take of the memory left: the rest absorbs what a caller does not count — the
/// allocator's per-thread arenas keeping freed buffers, decoders, readers and the process itself.
const WORKER_SHARE_NUMERATOR: u64 = 3;
const WORKER_SHARE_DENOMINATOR: u64 = 4;

/// How many workers run at once on `cores` cores with `available` bytes left.
///
/// As many as the cores, while `held_once` (memory the caller holds however many workers run) and
/// `per_worker` bytes for each fit in three quarters of what is left (`available_bytes`). At least
/// one; one when the memory is unknown (`None`, a platform that publishes no limit), the one count
/// that cannot hold more than a sequential run always did. Every command that runs work side by
/// side sizes it here, so the rule is one (the HUB export's parts, the land lanes' regions).
#[must_use]
pub fn workers_that_fit(
    cores: usize,
    available: Option<u64>,
    held_once: u64,
    per_worker: u64,
) -> usize {
    let Some(available) = available else {
        return 1;
    };
    let share = available / WORKER_SHARE_DENOMINATOR * WORKER_SHARE_NUMERATOR;
    let by_memory = share.saturating_sub(held_once) / per_worker.max(1);
    usize::try_from(by_memory)
        .unwrap_or(usize::MAX)
        .min(cores)
        .max(1)
}

/// The most memory this process's cgroup has held since it was made (cgroup v2 `memory.peak`).
///
/// A command sizes parallel work from what one unit of it actually took. `None` without it
/// (cgroup v1, a kernel before 5.19, not Linux).
#[must_use]
pub fn cgroup_peak_bytes() -> Option<u64> {
    cgroup_peak_bytes_from(&|path| std::fs::read_to_string(path).ok())
}

/// [`cgroup_peak_bytes`] over an injected file reader.
pub fn cgroup_peak_bytes_from(read: &dyn Fn(&Path) -> Option<String>) -> Option<u64> {
    let text = read(Path::new("/proc/self/cgroup"))?;
    let path = text.lines().find_map(|line| line.strip_prefix("0::"))?;
    if path.split('/').any(|segment| segment == "..") {
        return None;
    }
    let file = Path::new(CGROUP_V2_ROOT)
        .join(path.trim_start_matches('/'))
        .join("memory.peak");
    read(&file)?.trim().parse::<u64>().ok()
}

/// The bytes this process may still allocate: the smallest headroom of its cgroup chain and the
/// host's available memory. `None` when the platform publishes neither (not Linux).
#[must_use]
pub fn available_bytes() -> Option<u64> {
    available_bytes_from(&|path| std::fs::read_to_string(path).ok())
}

/// [`available_bytes`] over an injected file reader, so the parsing is tested without a kernel.
pub fn available_bytes_from(read: &dyn Fn(&Path) -> Option<String>) -> Option<u64> {
    let cgroup = read(Path::new("/proc/self/cgroup")).and_then(|text| cgroup_headroom(&text, read));
    let host = read(Path::new("/proc/meminfo")).and_then(|text| mem_available(&text));
    match (cgroup, host) {
        (Some(cgroup), Some(host)) => Some(cgroup.min(host)),
        (one, other) => one.or(other),
    }
}

/// The smallest `limit − usage` over the process's cgroup and its ancestors; `None` when no level
/// sets a limit.
fn cgroup_headroom(self_cgroup: &str, read: &dyn Fn(&Path) -> Option<String>) -> Option<u64> {
    // cgroup v2 has one hierarchy, written as `0::<path>`.
    if let Some(path) = self_cgroup
        .lines()
        .find_map(|line| line.strip_prefix("0::"))
    {
        return smallest_headroom(
            Path::new(CGROUP_V2_ROOT),
            path,
            "memory.max",
            "memory.current",
            read,
        );
    }
    // cgroup v1 has a memory hierarchy, written as `<id>:<controllers>:<path>`.
    let path = self_cgroup.lines().find_map(|line| {
        let mut fields = line.splitn(3, ':');
        let _id = fields.next()?;
        let controllers = fields.next()?;
        controllers
            .split(',')
            .any(|controller| controller == "memory")
            .then(|| fields.next())
            .flatten()
    })?;
    smallest_headroom(
        Path::new(CGROUP_V1_MEMORY_ROOT),
        path,
        "memory.limit_in_bytes",
        "memory.usage_in_bytes",
        read,
    )
}

fn smallest_headroom(
    root: &Path,
    cgroup_path: &str,
    limit_file: &str,
    usage_file: &str,
    read: &dyn Fn(&Path) -> Option<String>,
) -> Option<u64> {
    let mut directory: PathBuf = root.to_path_buf();
    let mut levels = vec![directory.clone()];
    for segment in cgroup_path.split('/').filter(|segment| !segment.is_empty()) {
        // A path that climbs is not a cgroup this process is in; read nothing outside the root.
        if segment == ".." {
            return None;
        }
        directory.push(segment);
        levels.push(directory.clone());
    }
    levels
        .iter()
        .filter_map(|level| {
            let limit = read(&level.join(limit_file))?
                .trim()
                .parse::<u64>()
                .ok()
                .filter(|limit| *limit < CGROUP_V1_UNLIMITED)?;
            let usage = read(&level.join(usage_file))
                .and_then(|text| text.trim().parse::<u64>().ok())
                .unwrap_or(0);
            Some(limit.saturating_sub(usage))
        })
        .min()
}

/// `MemAvailable` of `/proc/meminfo`, in bytes.
fn mem_available(meminfo: &str) -> Option<u64> {
    let line = meminfo
        .lines()
        .find_map(|line| line.strip_prefix("MemAvailable:"))?;
    let mut fields = line.split_whitespace();
    let value = fields.next()?.parse::<u64>().ok()?;
    match fields.next() {
        Some("kB") => value.checked_mul(1024),
        None => Some(value),
        Some(_) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    const MIB: u64 = 1024 * 1024;

    #[test]
    fn workers_fit_the_cores_and_three_quarters_of_the_memory_left() {
        assert_eq!(workers_that_fit(20, Some(256 * GIB), 0, 300 * MIB), 20);
        // 16 GiB left: 12 GiB share, 300 MiB each → 40, bounded by 8 cores.
        assert_eq!(workers_that_fit(8, Some(16 * GIB), 0, 300 * MIB), 8);
        // 1 GiB left: 768 MiB share − 168 MiB held = 600 MiB → 2 of 300 MiB.
        assert_eq!(workers_that_fit(20, Some(GIB), 168 * MIB, 300 * MIB), 2);
        assert_eq!(workers_that_fit(20, Some(MIB), 0, 300 * MIB), 1);
        assert_eq!(workers_that_fit(20, None, 0, MIB), 1);
        assert_eq!(workers_that_fit(0, Some(GIB), 0, MIB), 1);
    }

    #[test]
    fn the_peak_is_the_own_cgroups_memory_peak() {
        let files = BTreeMap::from([
            (
                "/proc/self/cgroup".to_owned(),
                "0::/system.slice/lane.service\n".to_owned(),
            ),
            (
                "/sys/fs/cgroup/system.slice/lane.service/memory.peak".to_owned(),
                "310378496\n".to_owned(),
            ),
        ]);
        let read = |path: &Path| {
            files
                .get(&path.to_string_lossy().replace('\\', "/"))
                .cloned()
        };
        assert_eq!(cgroup_peak_bytes_from(&read), Some(310_378_496));
        let v1 = |path: &Path| {
            (path == Path::new("/proc/self/cgroup")).then(|| "4:memory:/lane\n".to_owned())
        };
        assert_eq!(cgroup_peak_bytes_from(&v1), None);
    }

    const GIB: u64 = 1024 * 1024 * 1024;

    fn files(entries: &[(&str, String)]) -> impl Fn(&Path) -> Option<String> {
        let map: BTreeMap<PathBuf, String> = entries
            .iter()
            .map(|(path, text)| (PathBuf::from(path), text.clone()))
            .collect();
        move |path| map.get(path).cloned()
    }

    fn unit(path: &str) -> String {
        format!("{CGROUP_V2_ROOT}/system.slice/foundation-silver-refresh@x.service/{path}")
    }

    #[test]
    fn a_systemd_unit_cap_leaves_its_limit_minus_its_usage() {
        let read = files(&[
            (
                "/proc/self/cgroup",
                "0::/system.slice/foundation-silver-refresh@x.service\n".to_owned(),
            ),
            (&unit("memory.max"), format!("{}\n", 16 * GIB)),
            (&unit("memory.current"), format!("{GIB}\n")),
            (
                &format!("{CGROUP_V2_ROOT}/system.slice/memory.max"),
                "max\n".to_owned(),
            ),
            ("/proc/meminfo", "MemAvailable:   104857600 kB\n".to_owned()),
        ]);
        assert_eq!(available_bytes_from(&read), Some(15 * GIB));
    }

    #[test]
    fn a_tighter_ancestor_or_host_wins() {
        let read = files(&[
            ("/proc/self/cgroup", "0::/a.slice/b.service\n".to_owned()),
            (
                &format!("{CGROUP_V2_ROOT}/a.slice/b.service/memory.max"),
                format!("{}", 16 * GIB),
            ),
            (
                &format!("{CGROUP_V2_ROOT}/a.slice/memory.max"),
                format!("{}", 8 * GIB),
            ),
            (
                &format!("{CGROUP_V2_ROOT}/a.slice/memory.current"),
                format!("{}", 2 * GIB),
            ),
            ("/proc/meminfo", "MemAvailable:   104857600 kB\n".to_owned()),
        ]);
        assert_eq!(available_bytes_from(&read), Some(6 * GIB));
        let read = files(&[
            ("/proc/self/cgroup", "0::/\n".to_owned()),
            ("/sys/fs/cgroup/memory.max", format!("{}", 16 * GIB)),
            ("/proc/meminfo", "MemAvailable:   4194304 kB\n".to_owned()),
        ]);
        assert_eq!(available_bytes_from(&read), Some(4 * GIB));
    }

    #[test]
    fn no_limit_falls_back_to_the_host_and_then_to_unknown() {
        let read = files(&[
            ("/proc/self/cgroup", "0::/user.slice\n".to_owned()),
            ("/sys/fs/cgroup/user.slice/memory.max", "max\n".to_owned()),
            (
                "/proc/meminfo",
                "MemTotal: 1 kB\nMemAvailable: 2048 kB\n".to_owned(),
            ),
        ]);
        assert_eq!(available_bytes_from(&read), Some(2 * 1024 * 1024));
        assert_eq!(available_bytes_from(&files(&[])), None);
    }

    #[test]
    fn cgroup_v1_reads_the_memory_hierarchy_and_its_unlimited_marker() {
        let read = files(&[
            (
                "/proc/self/cgroup",
                "5:cpu,cpuacct:/x\n4:memory:/docker/abc\n".to_owned(),
            ),
            (
                "/sys/fs/cgroup/memory/docker/abc/memory.limit_in_bytes",
                format!("{}", 4 * GIB),
            ),
            (
                "/sys/fs/cgroup/memory/docker/abc/memory.usage_in_bytes",
                format!("{GIB}"),
            ),
            (
                "/sys/fs/cgroup/memory/memory.limit_in_bytes",
                // v1's "no limit": i64::MAX rounded down to a 4 KiB page.
                ((u64::MAX >> 1) & !4095).to_string(),
            ),
        ]);
        assert_eq!(available_bytes_from(&read), Some(3 * GIB));
    }

    #[test]
    fn a_climbing_cgroup_path_reads_nothing() {
        let read = files(&[
            ("/proc/self/cgroup", "0::/../escape\n".to_owned()),
            ("/sys/fs/cgroup/memory.max", format!("{GIB}")),
        ]);
        assert_eq!(available_bytes_from(&read), None);
    }
}
