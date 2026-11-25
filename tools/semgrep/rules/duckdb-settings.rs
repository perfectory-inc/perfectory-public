// Fixture for tools/semgrep/rules/duckdb-settings.yml (`opengrep scan --test`). Not compiled.

fn a_copy_of_the_configuration(profile: &ExecutionProfile) -> anyhow::Result<Connection> {
    // ruleid: duckdb-memory-and-threads-come-from-engine-config
    let config = Config::default()
        // ruleid: duckdb-memory-and-threads-come-from-engine-config
        .max_memory(&format!("{}MiB", profile.memory_mib / 2))?
        // ruleid: duckdb-memory-and-threads-come-from-engine-config
        .threads(i64::from(profile.cpu_slots))?;
    Ok(Connection::open_in_memory_with_flags(config)?)
}

fn a_lanes_own_number(connection: &Connection, limit: &str) -> anyhow::Result<()> {
    // ruleid: duckdb-memory-and-threads-come-from-engine-config
    connection.execute("SET memory_limit = ?", [limit])?;
    // ruleid: duckdb-memory-and-threads-come-from-engine-config
    connection.execute_batch("set threads = 4")?;
    // ruleid: duckdb-memory-and-threads-come-from-engine-config
    connection.execute_batch("PRAGMA memory_limit='384MiB'")?;
    Ok(())
}

fn a_setting_by_name(config: Config) -> anyhow::Result<Config> {
    // ruleid: duckdb-memory-and-threads-come-from-engine-config
    Ok(config.with("memory_limit", "600MiB")?)
}

fn through_the_one_function(spill: &Path) -> anyhow::Result<Connection> {
    // ok: duckdb-memory-and-threads-come-from-engine-config
    open_engine(None, Some(spill))
}

fn reading_what_duckdb_holds(connection: &Connection) -> anyhow::Result<String> {
    // ok: duckdb-memory-and-threads-come-from-engine-config
    Ok(connection.query_row("SELECT current_setting('memory_limit')", [], |row| row.get(0))?)
}

fn another_libraries_config() -> PublisherConfig {
    // ok: duckdb-memory-and-threads-come-from-engine-config
    PublisherConfig::default()
}

fn a_spill_ceiling_of_its_own(connection: &Connection, bytes: u64) -> anyhow::Result<()> {
    // ruleid: duckdb-memory-and-threads-come-from-engine-config
    connection.execute("SET max_temp_directory_size = ?", [format!("{bytes}B")])?;
    Ok(())
}
