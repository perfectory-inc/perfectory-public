//! `reconcile-lakehouse-catalog`: the lakehouse catalog's reconciler (root ADR-0624 decision 10-4,
//! P8, A4, A6), run as the operator by `infra/aws-lakekeeper` (`reconciler.tf`). It makes what the
//! declaration names:
//!
//! 1. bootstraps Lakekeeper once (the operator becomes its admin);
//! 2. makes the warehouse, handing Lakekeeper the R2 credential, or checks the one there;
//! 3. makes the declared namespaces;
//! 4. makes the declared tables from their Rust table contracts;
//! 5. registers each lane's principal and grants it what the declaration says on each table.
//!
//! It removes nothing. What it would have to remove or change is drift: a namespace, table or
//! grant the declaration does not hold, a declared table whose schema is not its contract's, a
//! warehouse whose profile is not the declaration's (its key prefix cannot change). Drift fails
//! the run in `fail` mode (prod) and is only printed in `report` mode (dev, whose probes make
//! tables of their own).
//!
//! Each printed line is one JSON record. No token, secret or credential is printed.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    sync::Arc,
    time::Duration,
};

use anyhow::{bail, ensure, Context};
use foundation_publisher_core::{
    lakehouse_data_commit::{contract_layout, LakehouseDataCommitter},
    lakehouse_rest_catalog,
};
use iceberg::{spec::Schema, Catalog, NamespaceIdent};
use lakehouse_domain::naming::naming_registry;
use lakehouse_domain::{industrial_complex_lakehouse_contracts, LakehouseTableContract};
use reqwest::{Method, StatusCode};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::public_data_control_support::required_env_value;

const CATALOG_URL_ENV: &str = "LAKEHOUSE_RECONCILE_CATALOG_URL";
const TOKEN_URI_ENV: &str = "LAKEHOUSE_RECONCILE_TOKEN_URI";
const SCOPE_ENV: &str = "LAKEHOUSE_RECONCILE_SCOPE";
const CLIENT_ID_ENV: &str = "LAKEHOUSE_RECONCILE_CLIENT_ID";
const CLIENT_SECRET_ENV: &str = "LAKEHOUSE_RECONCILE_CLIENT_SECRET";
const DECLARATION_ENV: &str = "LAKEHOUSE_RECONCILE_DECLARATION";
const R2_CREDENTIAL_ENV: &str = "LAKEHOUSE_RECONCILE_R2_CREDENTIAL";

/// What the catalog holds: the environment contract's warehouse and reconciler blocks, with the
/// lanes named by their Lakekeeper user ids (`oidc~<client id>`).
#[derive(Debug, Deserialize)]
struct Declaration {
    warehouse: Warehouse,
    tables: Vec<String>,
    lanes: BTreeMap<String, BTreeMap<String, Vec<String>>>,
    operator: String,
    drift: DriftMode,
}

#[derive(Debug, Deserialize)]
struct Warehouse {
    bucket: String,
    region: String,
    key_prefix: String,
    layout: Value,
    delete_profile: Value,
    namespaces: Vec<String>,
}

#[derive(Debug, Deserialize, PartialEq, Eq, Clone, Copy)]
#[serde(rename_all = "lowercase")]
enum DriftMode {
    Fail,
    Report,
}

/// A lane's grant on one table: (privilege, Lakekeeper user id).
type Grant = (String, String);

/// The declared tables' contracts, refusing a declaration that names a table with no contract, a
/// table outside the declared namespaces, or a grant on a table it does not declare.
fn validate(declaration: &Declaration) -> anyhow::Result<Vec<&'static LakehouseTableContract>> {
    let contracts = industrial_complex_lakehouse_contracts();
    let mut declared = Vec::with_capacity(declaration.tables.len());
    for table in &declaration.tables {
        let namespace = table.split_once('.').map(|(namespace, _)| namespace);
        ensure!(
            namespace.is_some_and(|namespace| declaration
                .warehouse
                .namespaces
                .iter()
                .any(|declared| declared == namespace)),
            "{table}: not in a declared namespace {:?}",
            declaration.warehouse.namespaces
        );
        let contract = contracts
            .iter()
            .find(|contract| contract.table_name == table)
            .with_context(|| format!("{table}: no table contract declares it"))?;
        // G3 (root ADR-0626 §7): the reconciler is the only creator, so a name outside the rules is
        // never created, an old name on the rename work list included: a switched table takes its
        // final name. Prevents the smoke tables production held in its catalog namespaces (F10).
        naming_registry()
            .map_err(|error| anyhow::anyhow!(error))?
            .check_table_name(table)
            .with_context(|| {
                format!(
                    "{table}: the reconciler creates only names that follow the naming registry"
                )
            })?;
        declared.push(contract);
    }
    for (lane, grants) in &declaration.lanes {
        for (privilege, tables) in grants {
            ensure!(
                matches!(privilege.as_str(), "select" | "write"),
                "{lane}: a lane holds select or write, not {privilege}"
            );
            for table in tables {
                ensure!(
                    declaration.tables.contains(table),
                    "{lane}: {privilege} on {table}, which is not a declared table"
                );
            }
        }
    }
    Ok(declared)
}

/// The grants the declaration gives on `table`.
fn declared_grants(declaration: &Declaration, table: &str) -> BTreeSet<Grant> {
    declaration
        .lanes
        .iter()
        .flat_map(|(lane, grants)| {
            grants
                .iter()
                .filter(|(_, tables)| tables.iter().any(|name| name == table))
                .map(move |(privilege, _)| (privilege.clone(), lane.clone()))
        })
        .collect()
}

/// The grants to make (declared, not live) and the drift (live, not declared). The operator's own
/// assignments (its ownership of what it created) are neither.
fn grant_changes(
    live: &BTreeSet<Grant>,
    declared: &BTreeSet<Grant>,
    operator: &str,
) -> (Vec<Grant>, Vec<Grant>) {
    let missing = declared.difference(live).cloned().collect();
    let extra = live
        .difference(declared)
        .filter(|(_, user)| user != operator)
        .cloned()
        .collect();
    (missing, extra)
}

/// A live table's schema against its contract's: by column name, type and required, in order
/// (field ids are the catalog's to assign).
fn schema_drift(live: &Schema, declared: &Schema) -> Option<String> {
    let shape = |schema: &Schema| -> Vec<String> {
        schema
            .as_struct()
            .fields()
            .iter()
            .map(|field| {
                let required = if field.required { " required" } else { "" };
                format!("{} {}{required}", field.name, field.field_type)
            })
            .collect()
    };
    let (live, declared) = (shape(live), shape(declared));
    (live != declared).then(|| format!("columns {live:?}, contract {declared:?}"))
}

/// Every declared field of `declared` that the live object `live` does not hold as it is.
fn object_drift(what: &str, live: &Value, declared: &Value) -> Vec<String> {
    let Some(fields) = declared.as_object() else {
        return vec![format!("{what}: the declaration is not an object")];
    };
    fields
        .iter()
        .filter(|(key, value)| live.get(key.as_str()) != Some(value))
        .map(|(key, value)| {
            format!(
                "{what}.{key}: {}, declared {value}",
                live.get(key.as_str()).unwrap_or(&Value::Null)
            )
        })
        .collect()
}

/// The live warehouse (`GET /management/v1/warehouse/{id}`) against the declaration.
fn warehouse_drift(live: &Value, declared: &Warehouse) -> Vec<String> {
    let profile = &live["storage-profile"];
    let mut drift = object_drift(
        "storage-profile",
        profile,
        &json!({
            "bucket": declared.bucket,
            "region": declared.region,
            "key-prefix": declared.key_prefix,
        }),
    );
    drift.extend(object_drift(
        "storage-profile.storage-layout",
        &profile["storage-layout"],
        &declared.layout,
    ));
    drift.extend(object_drift(
        "delete-profile",
        &live["delete-profile"],
        &declared.delete_profile,
    ));
    drift
}

/// The body that makes the warehouse: the dev rig's warehouse probe's, which T1-T36 ran against.
/// The API wants sts-enabled although upstream's R2 example leaves it out.
fn warehouse_request(declared: &Warehouse, credential: &str) -> anyhow::Result<Value> {
    let parts: Vec<&str> = credential.split(':').collect();
    let [account, key_id, secret_key, token] = parts.as_slice() else {
        bail!(
            "{R2_CREDENTIAL_ENV} is not <account id>:<access key id>:<secret access key>:<token>"
        );
    };
    ensure!(
        parts.iter().all(|part| !part.is_empty()),
        "{R2_CREDENTIAL_ENV} has an empty part"
    );
    Ok(json!({
        "warehouse-name": declared.bucket,
        "delete-profile": declared.delete_profile,
        "storage-profile": {
            "type": "s3",
            "bucket": declared.bucket,
            "region": declared.region,
            "sts-enabled": true,
            "flavor": "s3-compat",
            "endpoint": format!("https://{account}.r2.cloudflarestorage.com"),
            "key-prefix": declared.key_prefix,
            "storage-layout": declared.layout,
        },
        "storage-credential": {
            "type": "s3",
            "credential-type": "cloudflare-r2",
            "account-id": account,
            "access-key-id": key_id,
            "secret-access-key": secret_key,
            "token": token,
        },
    }))
}

/// Lakekeeper's management API as the operator. A refusal names the status and the error type,
/// never the request (a warehouse request holds the credential).
struct Management {
    http: reqwest::Client,
    base: String,
    jwt: String,
}

impl Management {
    async fn call(
        &self,
        method: Method,
        path: &str,
        body: Option<&Value>,
        expected: &[StatusCode],
    ) -> anyhow::Result<Value> {
        let mut request = self
            .http
            .request(method.clone(), format!("{}{path}", self.base))
            .bearer_auth(&self.jwt);
        if let Some(body) = body {
            request = request.json(body);
        }
        let response = request
            .send()
            .await
            .with_context(|| format!("{method} {path}: no answer"))?;
        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        let answer: Value = serde_json::from_str(&text).unwrap_or(Value::Null);
        ensure!(
            expected.contains(&status),
            "{method} {path}: {status} {}",
            answer["error"]["type"].as_str().unwrap_or("")
        );
        Ok(answer)
    }
}

struct Settings {
    catalog_url: String,
    token_uri: String,
    scope: String,
    client_id: String,
    client_secret: String,
    declaration: String,
}

impl Settings {
    fn from_env() -> anyhow::Result<Self> {
        Ok(Self {
            catalog_url: required_env_value(CATALOG_URL_ENV)?,
            token_uri: required_env_value(TOKEN_URI_ENV)?,
            scope: required_env_value(SCOPE_ENV)?,
            client_id: required_env_value(CLIENT_ID_ENV)?,
            client_secret: required_env_value(CLIENT_SECRET_ENV)?,
            declaration: required_env_value(DECLARATION_ENV)?,
        })
    }

    /// iceberg-rust's catalog properties: its OAuth2 client, and no storage key.
    fn catalog_properties(&self, warehouse: &str) -> HashMap<String, String> {
        [
            ("uri", format!("{}/catalog", self.catalog_url)),
            ("warehouse", warehouse.to_owned()),
            (
                "credential",
                format!("{}:{}", self.client_id, self.client_secret),
            ),
            ("oauth2-server-uri", self.token_uri.clone()),
            ("scope", self.scope.clone()),
            ("s3.disable-config-load", "true".to_owned()),
            ("s3.disable-ec2-metadata", "true".to_owned()),
        ]
        .into_iter()
        .map(|(key, value)| (key.to_owned(), value))
        .collect()
    }

    async fn token(&self, http: &reqwest::Client) -> anyhow::Result<String> {
        let response = http
            .post(&self.token_uri)
            .form(&[
                ("grant_type", "client_credentials"),
                ("client_id", self.client_id.as_str()),
                ("client_secret", self.client_secret.as_str()),
                ("scope", self.scope.as_str()),
            ])
            .send()
            .await
            .context("the token endpoint did not answer")?;
        let status = response.status();
        ensure!(status.is_success(), "the token endpoint answered {status}");
        let answer: Value = response.json().await?;
        answer["access_token"]
            .as_str()
            .map(str::to_owned)
            .context("the token endpoint's answer holds no access token")
    }
}

const OK: &[StatusCode] = &[StatusCode::OK];
const DONE: &[StatusCode] = &[StatusCode::OK, StatusCode::CREATED, StatusCode::NO_CONTENT];

pub(crate) async fn run() -> anyhow::Result<()> {
    let settings = Settings::from_env()?;
    let declaration: Declaration = serde_json::from_str(&settings.declaration)
        .with_context(|| format!("{DECLARATION_ENV} is not a declaration"))?;
    let contracts = validate(&declaration)?;
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(60))
        .build()?;
    let api = Management {
        jwt: settings.token(&http).await?,
        http,
        base: settings.catalog_url.clone(),
    };
    let mut drift: Vec<String> = Vec::new();

    let info = api
        .call(Method::GET, "/management/v1/info", None, OK)
        .await?;
    if info["bootstrapped"] != Value::Bool(true) {
        let request = json!({"accept-terms-of-use": true, "is-operator": true});
        api.call(
            Method::POST,
            "/management/v1/bootstrap",
            Some(&request),
            DONE,
        )
        .await?;
        println!("{}", json!({"made": "bootstrap"}));
    }

    let warehouse = &declaration.warehouse;
    let listed = api
        .call(Method::GET, "/management/v1/warehouse", None, OK)
        .await?;
    let found = listed["warehouses"]
        .as_array()
        .into_iter()
        .flatten()
        .find(|item| item["name"] == warehouse.bucket.as_str())
        .and_then(|item| item["id"].as_str().map(str::to_owned));
    let warehouse_id = if let Some(id) = found {
        let live = api
            .call(
                Method::GET,
                &format!("/management/v1/warehouse/{id}"),
                None,
                OK,
            )
            .await?;
        drift.extend(warehouse_drift(&live, warehouse));
        id
    } else {
        let credential = required_env_value(R2_CREDENTIAL_ENV)?;
        let request = warehouse_request(warehouse, &credential)?;
        let made = api
            .call(
                Method::POST,
                "/management/v1/warehouse",
                Some(&request),
                DONE,
            )
            .await?;
        let id = made["warehouse-id"]
            .as_str()
            .or_else(|| made["id"].as_str())
            .context("the new warehouse's answer holds no id")?
            .to_owned();
        println!(
            "{}",
            json!({"made": "warehouse", "name": warehouse.bucket, "id": id})
        );
        id
    };

    let catalog: Arc<dyn Catalog> =
        lakehouse_rest_catalog::open(settings.catalog_properties(&warehouse.bucket)).await?;
    for name in &warehouse.namespaces {
        let namespace = NamespaceIdent::new(name.clone());
        if !catalog.namespace_exists(&namespace).await? {
            catalog.create_namespace(&namespace, HashMap::new()).await?;
            println!("{}", json!({"made": "namespace", "name": name}));
        }
    }
    for namespace in catalog.list_namespaces(None).await? {
        let name = namespace.to_url_string();
        if !warehouse.namespaces.contains(&name) {
            drift.push(format!("namespace {name}: not declared"));
        }
    }

    let committer = LakehouseDataCommitter::new(Arc::clone(&catalog));
    for contract in &contracts {
        if committer.create_table_if_absent(contract).await? {
            println!("{}", json!({"made": "table", "name": contract.table_name}));
        }
    }
    let mut table_ids = BTreeMap::new();
    for name in &warehouse.namespaces {
        for ident in catalog
            .list_tables(&NamespaceIdent::new(name.clone()))
            .await?
        {
            let table = format!("{name}.{}", ident.name());
            let Some(contract) = contracts.iter().find(|c| c.table_name == table) else {
                drift.push(format!("table {table}: not declared"));
                continue;
            };
            let loaded = catalog.load_table(&ident).await?;
            let (schema, _, _) = contract_layout(contract)?;
            if let Some(found) = schema_drift(loaded.metadata().current_schema(), &schema) {
                drift.push(format!("table {table}: {found}"));
            }
            table_ids.insert(table, loaded.metadata().uuid().to_string());
        }
    }

    for lane in declaration.lanes.keys() {
        let request =
            json!({"id": lane, "name": lane, "user-type": "application", "update-if-exists": true});
        api.call(Method::POST, "/management/v1/user", Some(&request), DONE)
            .await?;
    }
    for (table, id) in &table_ids {
        let path =
            format!("/management/v1/permissions/warehouse/{warehouse_id}/table/{id}/assignments");
        let answer = api.call(Method::GET, &path, None, OK).await?;
        let live: BTreeSet<Grant> = answer["assignments"]
            .as_array()
            .into_iter()
            .flatten()
            .map(|item| {
                let who = item["user"]
                    .as_str()
                    .map_or_else(|| format!("role:{}", item["role"]), str::to_owned);
                (item["type"].as_str().unwrap_or("").to_owned(), who)
            })
            .collect();
        let (missing, extra) = grant_changes(
            &live,
            &declared_grants(&declaration, table),
            &declaration.operator,
        );
        if !missing.is_empty() {
            let writes: Vec<Value> = missing
                .iter()
                .map(|(kind, user)| json!({"type": kind, "user": user}))
                .collect();
            api.call(Method::POST, &path, Some(&json!({"writes": writes})), DONE)
                .await?;
            println!(
                "{}",
                json!({"made": "grants", "table": table, "grants": missing})
            );
        }
        drift.extend(
            extra
                .iter()
                .map(|(kind, user)| format!("grant {kind} on {table} to {user}: not declared")),
        );
    }

    println!(
        "{}",
        json!({"reconciled": warehouse.bucket, "tables": table_ids.len(), "drift": drift, "mode": format!("{:?}", declaration.drift)})
    );
    if declaration.drift == DriftMode::Fail && !drift.is_empty() {
        bail!("{} drift item(s) and the drift mode is fail", drift.len());
    }
    Ok(())
}

#[cfg(test)]
mod tests;
