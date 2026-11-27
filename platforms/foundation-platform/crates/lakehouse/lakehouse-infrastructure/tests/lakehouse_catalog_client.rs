//! The lane's Lakekeeper client (root ADR-0624 §2) is read all-or-none, and its secret never prints.

use std::{collections::BTreeMap, error::Error};

use lakehouse_infrastructure::{LakehouseCatalogClient, LakehouseCatalogConfigError};

const ID: &str = "FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_CLIENT_ID";
const SECRET: &str = "FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_CLIENT_SECRET";
const TOKEN_URI: &str = "FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_CLIENT_TOKEN_URI";
const SCOPE: &str = "FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_CLIENT_SCOPE";

fn vars(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
    pairs
        .iter()
        .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
        .collect()
}

#[test]
fn none_set_is_no_client() {
    assert_eq!(LakehouseCatalogClient::from_vars(&vars(&[])), Ok(None));
}

#[test]
fn all_four_make_the_client() -> Result<(), Box<dyn Error>> {
    let client = LakehouseCatalogClient::from_vars(&vars(&[
        (ID, "fixture-client"),
        (SECRET, "fixture-secret-value"),
        (TOKEN_URI, "https://issuer.invalid/oauth2/token"),
        (SCOPE, "catalog-fixture/catalog"),
    ]))?
    .ok_or("the four make a client")?;
    assert_eq!(client.client_id, "fixture-client");
    assert_eq!(client.scope, "catalog-fixture/catalog");
    let shown = format!("{client:?}");
    assert!(!shown.contains("fixture-secret-value"), "{shown}");
    Ok(())
}

#[test]
fn a_part_of_the_four_is_refused_with_the_first_missing_name() {
    assert_eq!(
        LakehouseCatalogClient::from_vars(&vars(&[
            (ID, "fixture-client"),
            (SECRET, "fixture-secret-value"),
            (SCOPE, "catalog-fixture/catalog"),
        ])),
        Err(LakehouseCatalogConfigError::PartialClient(TOKEN_URI))
    );
}
