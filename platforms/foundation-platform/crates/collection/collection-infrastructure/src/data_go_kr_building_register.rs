//! HTTP client wrapper for data.go.kr building-register pages.

use std::collections::BTreeMap;

use collection_application::{
    BuildingRegisterPageRequest, PublicDataBronzePageRequest, PublicDataFixedQueryParam,
    PublicDataPartitionField,
};
use collection_domain::CollectionError;
use serde_json::Value as JsonValue;

use crate::{DataGoKrRequestPolicy, DataGoKrServiceApiClient, DataGoKrServiceApiConfig};

const BUILDING_REGISTER_USER_AGENT: &str = "foundation-platform-building-register-ingestor/1.0";

/// Configuration for the data.go.kr building-register client.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DataGoKrBuildingRegisterConfig {
    /// Base URI for the building-register hub service.
    pub base_uri: String,
    /// Decoded public-data portal service key.
    pub service_key: String,
}

/// Raw page fetched from the data.go.kr building-register API.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DataGoKrBuildingRegisterPage {
    /// Raw response body bytes, suitable for Bronze storage.
    pub raw_payload: Vec<u8>,
    /// Parsed JSON response body, suitable for metadata and schema profiling.
    pub payload: JsonValue,
}

/// `reqwest` backed client for data.go.kr building-register pages.
#[derive(Clone, Debug)]
pub struct DataGoKrBuildingRegisterClient {
    inner: DataGoKrServiceApiClient,
}

impl DataGoKrBuildingRegisterClient {
    /// Creates a building-register client from explicit configuration.
    ///
    /// # Errors
    ///
    /// Returns `CollectionError` when the base URI is invalid or credentials are empty.
    pub fn new(config: &DataGoKrBuildingRegisterConfig) -> Result<Self, CollectionError> {
        Self::new_with_policy(config, DataGoKrRequestPolicy::default())
    }

    /// Creates a building-register client from explicit configuration and request policy.
    ///
    /// # Errors
    ///
    /// Returns `CollectionError` when the base URI is invalid, credentials are empty, or the HTTP
    /// client cannot be built.
    pub fn new_with_policy(
        config: &DataGoKrBuildingRegisterConfig,
        policy: DataGoKrRequestPolicy,
    ) -> Result<Self, CollectionError> {
        let inner = DataGoKrServiceApiClient::new_with_policy(
            &DataGoKrServiceApiConfig {
                base_uri: config.base_uri.clone(),
                service_key: config.service_key.clone(),
                user_agent: BUILDING_REGISTER_USER_AGENT.to_owned(),
            },
            policy,
        )?;
        Ok(Self { inner })
    }

    /// Fetches one JSON page from the building-register API.
    ///
    /// # Errors
    ///
    /// Returns `CollectionError` when the HTTP request fails, JSON parsing fails, or the provider
    /// envelope reports a non-success result code.
    pub async fn fetch_page(
        &self,
        request: &BuildingRegisterPageRequest,
    ) -> Result<DataGoKrBuildingRegisterPage, CollectionError> {
        let page = self
            .inner
            .fetch_page(&building_register_public_data_request(request))
            .await?;
        Ok(DataGoKrBuildingRegisterPage {
            raw_payload: page.raw_payload,
            payload: page.payload,
        })
    }
}

fn building_register_public_data_request(
    request: &BuildingRegisterPageRequest,
) -> PublicDataBronzePageRequest {
    PublicDataBronzePageRequest {
        operation: request.operation.clone(),
        partition_fields: vec![
            PublicDataPartitionField {
                name: "sigungu".to_owned(),
                value: request.sigungu_cd.clone(),
            },
            PublicDataPartitionField {
                name: "bjdong".to_owned(),
                value: request.bjdong_cd.clone(),
            },
        ],
        query_params: BTreeMap::from([
            ("sigunguCd".to_owned(), request.sigungu_cd.clone()),
            ("bjdongCd".to_owned(), request.bjdong_cd.clone()),
        ]),
        format_query_param: Some(PublicDataFixedQueryParam {
            name: "_type".to_owned(),
            value: "json".to_owned(),
        }),
        page_param_name: "pageNo".to_owned(),
        size_param_name: "numOfRows".to_owned(),
        page_no: request.page_no,
        num_of_rows: request.num_of_rows,
    }
}
