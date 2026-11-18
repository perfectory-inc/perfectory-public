//! The audit chain routes (root ADR-0569 §7): who may read them, the `seq` pages, `me`, and that an
//! export ends with the digest and is itself recorded.

use super::*;
use catalog_application::ports::{
    AuditAppend, AuditDigestHead, AuditEvent, AuditEventFilter, AuditEventStore, AuditImportStatus,
    AuditStoreError,
};

const STAFF: &str = "00000000-0000-4000-8000-0000000000aa";

/// Grants exactly the audit-reading capability, to the synthetic staff account `STAFF`.
struct AuditReader;

#[async_trait]
impl IdentityAuthorization for AuditReader {
    async fn authorize(
        &self,
        _bearer: &str,
        required_principal_kind: RequiredPrincipalKind,
        resource: &str,
        action: &str,
        _resource_id: Option<&str>,
        trace_id: &str,
    ) -> Result<AuthorizedPrincipal, IdentityAuthorizationError> {
        if required_principal_kind == RequiredPrincipalKind::Staff
            && (resource, action) == ("foundation.operations", "audit_read")
        {
            return Ok(AuthorizedPrincipal {
                principal_id: uuid::Uuid::parse_str(STAFF)
                    .map_err(|_| IdentityAuthorizationError::Forbidden)?,
                trace_id: trace_id.to_owned(),
            });
        }
        Err(IdentityAuthorizationError::Forbidden)
    }
}

/// A staff member who may operate jobs but not read the audit chain.
struct OperatorOnly;

#[async_trait]
impl IdentityAuthorization for OperatorOnly {
    async fn authorize(
        &self,
        _bearer: &str,
        _required_principal_kind: RequiredPrincipalKind,
        resource: &str,
        action: &str,
        _resource_id: Option<&str>,
        trace_id: &str,
    ) -> Result<AuthorizedPrincipal, IdentityAuthorizationError> {
        if (resource, action) == ("foundation.operations", "operate") {
            return Ok(AuthorizedPrincipal {
                principal_id: uuid::Uuid::nil(),
                trace_id: trace_id.to_owned(),
            });
        }
        Err(IdentityAuthorizationError::Forbidden)
    }
}

fn event(seq: i64) -> AuditEvent {
    AuditEvent {
        seq,
        event_id: uuid::Uuid::from_u128(u128::try_from(seq).unwrap_or_default()),
        occurred_at: chrono::DateTime::from_timestamp(1_763_510_400 + seq, 0).unwrap_or_default(),
        recorded_at: chrono::DateTime::from_timestamp(1_763_510_400 + seq, 0).unwrap_or_default(),
        source: "dawneer".to_owned(),
        source_event_id: format!("operator_action:{seq}:requested"),
        action: "operator_action.pause".to_owned(),
        operation: "execute".to_owned(),
        actor_kind: "staff".to_owned(),
        actor_staff_id: uuid::Uuid::parse_str(STAFF).ok(),
        actor_subject: None,
        actor_external: None,
        actor_verified: true,
        on_behalf_of: None,
        object_type: "job".to_owned(),
        object_id: "shape_ledger_check".to_owned(),
        before: None,
        after: Some(serde_json::json!({"note": "a,b"})),
        reason: Some("a stop, for the night".to_owned()),
        applied_via_kind: Some("api".to_owned()),
        applied_via_ref: Some("x".to_owned()),
        result: "requested".to_owned(),
        error: None,
        change_id: None,
        prev_hash: "00".repeat(32),
        row_hash: format!("{seq:064x}"),
    }
}

/// Holds rows 1..=`rows` and answers pages the way the database does: newest first, before a seq.
#[derive(Default)]
struct Chain {
    rows: i64,
    asked: Mutex<Vec<AuditEventFilter>>,
    appended: Mutex<Vec<AuditAppend>>,
    down: bool,
}

#[async_trait]
impl AuditEventStore for Chain {
    async fn page(&self, filter: &AuditEventFilter) -> Result<Vec<AuditEvent>, AuditStoreError> {
        if self.down {
            return Err(AuditStoreError::Unavailable("synthetic outage".to_owned()));
        }
        self.asked.lock().await.push(filter.clone());
        let top = filter.before_seq.map_or(self.rows, |before| before - 1);
        Ok((1..=top)
            .rev()
            .take(usize::try_from(filter.limit).unwrap_or_default())
            .map(event)
            .collect())
    }

    async fn append(&self, event: &AuditAppend) -> Result<Option<i64>, AuditStoreError> {
        self.appended.lock().await.push(event.clone());
        Ok(Some(self.rows + 1))
    }

    async fn import_status(&self) -> Result<Vec<AuditImportStatus>, AuditStoreError> {
        Ok(vec![AuditImportStatus {
            source: "deploy".to_owned(),
            cursor: "all".to_owned(),
            state: "running".to_owned(),
            advanced_at: chrono::DateTime::from_timestamp(1_763_510_400, 0).unwrap_or_default(),
        }])
    }

    async fn latest_digest(&self) -> Result<Option<AuditDigestHead>, AuditStoreError> {
        Ok(Some(AuditDigestHead {
            from_seq: 1,
            to_seq: self.rows,
            head_hash: format!("{:064x}", self.rows),
            digest_sha256: "ab".repeat(32),
            rekor_log_index: Some(42),
            made_at: chrono::DateTime::from_timestamp(1_763_510_400, 0).unwrap_or_default(),
        }))
    }
}

async fn get_with(
    authorization: Arc<dyn IdentityAuthorization>,
    chain: Arc<Chain>,
    uri: &str,
) -> Result<(StatusCode, axum::http::HeaderMap, Vec<u8>), Box<dyn Error>> {
    let state = AppState::bootstrap_for_test_with_identity_authorization(authorization)?
        .with_audit_event_store(chain);
    let request = Request::builder()
        .method(Method::GET)
        .uri(uri)
        .header(header::AUTHORIZATION, "Bearer synthetic-staff-token")
        .body(Body::empty())?;
    let response = router(Arc::new(state)).oneshot(request).await?;
    let status = response.status();
    let headers = response.headers().clone();
    let bytes = to_bytes(response.into_body(), usize::MAX).await?;
    Ok((status, headers, bytes.to_vec()))
}

#[tokio::test]
async fn only_the_audit_reading_capability_reads_the_chain() -> Result<(), Box<dyn Error>> {
    for uri in [
        "/data-catalog/v1/audit/events",
        "/data-catalog/v1/audit/events/export",
        "/data-catalog/v1/audit/import-status",
    ] {
        let chain = Arc::new(Chain {
            rows: 3,
            ..Chain::default()
        });
        let (status, _, _) = get_with(Arc::new(OperatorOnly), chain.clone(), uri).await?;
        assert_eq!(status, StatusCode::FORBIDDEN, "{uri}");
        assert!(chain.asked.lock().await.is_empty(), "{uri} read the chain");
        assert!(
            chain.appended.lock().await.is_empty(),
            "{uri} wrote the chain"
        );
    }
    Ok(())
}

#[tokio::test]
async fn pages_go_back_by_seq_and_me_is_the_signed_in_account() -> Result<(), Box<dyn Error>> {
    let chain = Arc::new(Chain {
        rows: 5,
        ..Chain::default()
    });
    let (status, _, body) = get_with(
        Arc::new(AuditReader),
        chain.clone(),
        "/data-catalog/v1/audit/events?actor=me&limit=2&object_type=job&object_id=shape",
    )
    .await?;
    assert_eq!(status, StatusCode::OK);
    let page: serde_json::Value = serde_json::from_slice(&body)?;
    let seqs: Vec<i64> = page["events"]
        .as_array()
        .map(|events| events.iter().filter_map(|e| e["seq"].as_i64()).collect())
        .unwrap_or_default();
    assert_eq!(seqs, vec![5, 4]);
    assert_eq!(page["next_before_seq"], 4);
    let asked = chain.asked.lock().await.clone();
    assert_eq!(
        asked[0].actor_staff_id.map(|id| id.to_string()).as_deref(),
        Some(STAFF)
    );
    assert_eq!(asked[0].object_type.as_deref(), Some("job"));
    assert_eq!(asked[0].object_id_prefix.as_deref(), Some("shape"));

    let (_, _, body) = get_with(
        Arc::new(AuditReader),
        chain,
        "/data-catalog/v1/audit/events?before_seq=2&limit=2",
    )
    .await?;
    let page: serde_json::Value = serde_json::from_slice(&body)?;
    assert_eq!(page["events"].as_array().map(Vec::len), Some(1));
    assert!(
        page["next_before_seq"].is_null(),
        "the last page names a next one"
    );
    Ok(())
}

#[tokio::test]
async fn a_bad_filter_is_refused_before_the_chain_is_read() -> Result<(), Box<dyn Error>> {
    for query in [
        "limit=0",
        "limit=501",
        "actor=someone",
        "change_id=nope",
        "from=2026-11-19T00:00:00Z&to=2026-11-18T00:00:00Z",
    ] {
        let chain = Arc::new(Chain {
            rows: 3,
            ..Chain::default()
        });
        let (status, _, _) = get_with(
            Arc::new(AuditReader),
            chain.clone(),
            &format!("/data-catalog/v1/audit/events?{query}"),
        )
        .await?;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{query}");
        assert!(chain.asked.lock().await.is_empty(), "{query}");
    }
    Ok(())
}

#[tokio::test]
async fn an_export_is_oldest_first_ends_with_the_digest_and_is_recorded(
) -> Result<(), Box<dyn Error>> {
    let chain = Arc::new(Chain {
        rows: 1_200,
        ..Chain::default()
    });
    let (status, headers, body) = get_with(
        Arc::new(AuditReader),
        chain.clone(),
        "/data-catalog/v1/audit/events/export?source=dawneer",
    )
    .await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        headers
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok()),
        Some("application/x-ndjson")
    );
    let text = String::from_utf8(body)?;
    let lines: Vec<serde_json::Value> = text
        .lines()
        .map(serde_json::from_str)
        .collect::<Result<_, _>>()?;
    assert_eq!(lines.len(), 1_201, "every row and the digest line");
    assert_eq!(lines[0]["seq"], 1);
    assert_eq!(lines[1_199]["seq"], 1_200);
    assert_eq!(lines[1_200]["digest"]["to_seq"], 1_200);
    assert_eq!(lines[1_200]["digest"]["rekor_log_index"], 42);
    let appended = chain.appended.lock().await.clone();
    assert_eq!(appended.len(), 1);
    assert_eq!(appended[0].action, "audit.exported");
    assert_eq!(appended[0].source, "audit");
    assert_eq!(
        appended[0]
            .actor_staff_id
            .map(|id| id.to_string())
            .as_deref(),
        Some(STAFF)
    );
    let after = appended[0].after.clone().unwrap_or_default();
    assert_eq!(after["rows"], 1_200);
    assert_eq!(after["source"], "dawneer");
    Ok(())
}

#[tokio::test]
async fn a_csv_export_has_one_header_line_and_quotes_what_it_must() -> Result<(), Box<dyn Error>> {
    let chain = Arc::new(Chain {
        rows: 2,
        ..Chain::default()
    });
    let (status, headers, body) = get_with(
        Arc::new(AuditReader),
        chain,
        "/data-catalog/v1/audit/events/export?format=csv",
    )
    .await?;
    assert_eq!(status, StatusCode::OK);
    assert!(headers.contains_key("x-audit-digest"));
    let text = String::from_utf8(body)?;
    let lines: Vec<&str> = text.lines().collect();
    assert_eq!(lines.len(), 3);
    assert!(lines[0].starts_with("seq,event_id,occurred_at"));
    assert!(lines[1].starts_with("1,"));
    assert!(lines[1].contains("\"a stop, for the night\""));
    Ok(())
}

#[tokio::test]
async fn an_unreadable_chain_is_a_503() -> Result<(), Box<dyn Error>> {
    let chain = Arc::new(Chain {
        down: true,
        ..Chain::default()
    });
    let (status, _, _) = get_with(
        Arc::new(AuditReader),
        chain,
        "/data-catalog/v1/audit/events",
    )
    .await?;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    Ok(())
}
