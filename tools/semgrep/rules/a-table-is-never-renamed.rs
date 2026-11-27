// Fixture for tools/semgrep/rules/a-table-is-never-renamed.yml (`opengrep scan --test`). Not compiled.

async fn switch_by_rename(catalog: &impl Catalog, old: &TableIdent, new: &TableIdent) {
    // ruleid: a-table-is-never-renamed
    catalog.rename_table(old, new).await?;
}

fn a_duckdb_stage_table(name: &str) -> String {
    // ok: a-table-is-never-renamed
    format!("ALTER TABLE {name} RENAME TO {name}_whole;")
}

async fn switch_by_create(reconciler: &Reconciler, contract: &LakehouseTableContract) {
    // ok: a-table-is-never-renamed
    reconciler.create_table(contract).await?;
}
