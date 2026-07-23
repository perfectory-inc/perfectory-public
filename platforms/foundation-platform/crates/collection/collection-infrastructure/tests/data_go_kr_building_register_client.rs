//! Contract tests for the data.go.kr building-register client.

use collection_application::BuildingRegisterPageRequest;
use std::time::Duration;

use collection_infrastructure::{
    DataGoKrBuildingRegisterClient, DataGoKrBuildingRegisterConfig, DataGoKrRequestPolicy,
};
use serde_json::json;
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[tokio::test]
async fn building_register_client_fetches_raw_json_page() -> TestResult {
    let server = MockServer::start().await;
    let payload = json!({
        "response": {
            "header": {
                "resultCode": "00",
                "resultMsg": "NORMAL SERVICE."
            },
            "body": {
                "items": {
                    "item": [
                        {
                            "mgmBldrgstPk": "11680-10300-1"
                        }
                    ]
                },
                "numOfRows": 100,
                "pageNo": 1,
                "totalCount": 1
            }
        }
    });
    let body = serde_json::to_vec(&payload)?;

    Mock::given(method("GET"))
        .and(path("/getBrTitleInfo"))
        .and(query_param("serviceKey", "decoded-service-key"))
        .and(query_param("sigunguCd", "11680"))
        .and(query_param("bjdongCd", "10300"))
        .and(query_param("pageNo", "1"))
        .and(query_param("numOfRows", "100"))
        .and(query_param("_type", "json"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "application/json")
                .set_body_bytes(body.clone()),
        )
        .expect(1)
        .mount(&server)
        .await;

    let client = DataGoKrBuildingRegisterClient::new(&DataGoKrBuildingRegisterConfig {
        base_uri: server.uri(),
        service_key: "decoded-service-key".to_owned(),
    })?;

    let page = client
        .fetch_page(&BuildingRegisterPageRequest {
            operation: "getBrTitleInfo".to_owned(),
            sigungu_cd: "11680".to_owned(),
            bjdong_cd: "10300".to_owned(),
            page_no: 1,
            num_of_rows: 100,
        })
        .await?;

    assert_eq!(page.raw_payload, body);
    assert_eq!(page.payload["response"]["header"]["resultCode"], "00");
    Ok(())
}

#[tokio::test]
async fn building_register_client_treats_base_uri_path_as_service_root() -> TestResult {
    let server = MockServer::start().await;
    let payload = json!({
        "response": {
            "header": {
                "resultCode": "00",
                "resultMsg": "NORMAL SERVICE."
            },
            "body": {
                "items": {
                    "item": []
                },
                "totalCount": 0
            }
        }
    });

    Mock::given(method("GET"))
        .and(path("/1613000/BldRgstHubService/getBrTitleInfo"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "application/json")
                .set_body_json(payload),
        )
        .expect(1)
        .mount(&server)
        .await;

    let client = DataGoKrBuildingRegisterClient::new(&DataGoKrBuildingRegisterConfig {
        base_uri: format!("{}/1613000/BldRgstHubService", server.uri()),
        service_key: "decoded-service-key".to_owned(),
    })?;

    client
        .fetch_page(&BuildingRegisterPageRequest {
            operation: "getBrTitleInfo".to_owned(),
            sigungu_cd: "11680".to_owned(),
            bjdong_cd: "10300".to_owned(),
            page_no: 1,
            num_of_rows: 100,
        })
        .await?;

    Ok(())
}

#[tokio::test]
async fn building_register_client_retries_transient_http_status_before_succeeding() -> TestResult {
    let server = MockServer::start().await;
    let payload = normal_payload();

    Mock::given(method("GET"))
        .and(path("/getBrTitleInfo"))
        .respond_with(ResponseTemplate::new(503))
        .up_to_n_times(1)
        .expect(1)
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/getBrTitleInfo"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "application/json")
                .set_body_json(payload),
        )
        .expect(1)
        .mount(&server)
        .await;

    let client = DataGoKrBuildingRegisterClient::new_with_policy(
        &DataGoKrBuildingRegisterConfig {
            base_uri: server.uri(),
            service_key: "decoded-service-key".to_owned(),
        },
        DataGoKrRequestPolicy::new(3, Duration::from_secs(5), Duration::ZERO, Duration::ZERO)?,
    )?;

    let page = client
        .fetch_page(&BuildingRegisterPageRequest {
            operation: "getBrTitleInfo".to_owned(),
            sigungu_cd: "11680".to_owned(),
            bjdong_cd: "10300".to_owned(),
            page_no: 1,
            num_of_rows: 100,
        })
        .await?;

    assert_eq!(page.payload["response"]["header"]["resultCode"], "00");
    Ok(())
}

#[tokio::test]
async fn building_register_client_does_not_retry_provider_domain_failures() -> TestResult {
    let server = MockServer::start().await;
    let payload = json!({
        "response": {
            "header": {
                "resultCode": "03",
                "resultMsg": "NODATA_ERROR"
            }
        }
    });

    Mock::given(method("GET"))
        .and(path("/getBrTitleInfo"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "application/json")
                .set_body_json(payload),
        )
        .expect(1)
        .mount(&server)
        .await;

    let client = DataGoKrBuildingRegisterClient::new_with_policy(
        &DataGoKrBuildingRegisterConfig {
            base_uri: server.uri(),
            service_key: "decoded-service-key".to_owned(),
        },
        DataGoKrRequestPolicy::new(3, Duration::from_secs(5), Duration::ZERO, Duration::ZERO)?,
    )?;

    let Err(error) = client
        .fetch_page(&BuildingRegisterPageRequest {
            operation: "getBrTitleInfo".to_owned(),
            sigungu_cd: "11680".to_owned(),
            bjdong_cd: "10300".to_owned(),
            page_no: 1,
            num_of_rows: 100,
        })
        .await
    else {
        return Err("expected provider domain failure".into());
    };

    assert!(
        error.to_string().contains("resultCode=03"),
        "unexpected error: {error}"
    );
    Ok(())
}

fn normal_payload() -> serde_json::Value {
    json!({
        "response": {
            "header": {
                "resultCode": "00",
                "resultMsg": "NORMAL SERVICE."
            },
            "body": {
                "items": {
                    "item": [
                        {
                            "mgmBldrgstPk": "11680-10300-1"
                        }
                    ]
                },
                "numOfRows": 100,
                "pageNo": 1,
                "totalCount": 1
            }
        }
    })
}
