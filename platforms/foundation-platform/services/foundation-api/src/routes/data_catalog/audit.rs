//! The audit record, read by account, object and time, and exported (root ADR-0569 §7, ADR-0584 §3).
//!
//! Every route is staff-only as `foundation.operations:audit_read`. The order is always `seq`, the
//! record's own: a filter by time selects rows but never reorders them, so an event imported late
//! is still found where the record put it. A page's `seq` cursor only pages the screen; an export's
//! cursor is the snapshot it was read in, so the next export from it misses no row committed late.
//! An export is itself recorded, as `audit.exported`.

use std::sync::Arc;

use axum::extract::{Extension, Query, State};
use axum::http::{header, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use catalog_application::ports::{
    AuditAppend, AuditDigest, AuditEvent, AuditEventFilter, AuditExport, AuditStoreError,
};
use chrono::{DateTime, Utc};
use foundation_contracts::data_catalog::{
    AuditDigestView, AuditEventPage, AuditEventView, AuditImportSource, AuditImportStatusPage,
};
use foundation_contracts::error::ApiErrorResponse;
use serde::Deserialize;
use serde_json::json;
use utoipa::IntoParams;
use uuid::Uuid;

use super::{ApiError, AppState};
use crate::identity_authorization::AuthorizedPrincipal;

/// Rows one page answers by default and at most.
const PAGE_DEFAULT: u32 = 100;
const PAGE_MAX: u32 = 500;
/// Rows one export answers at most. The chain grows by fewer than 100 rows a day (root ADR-0569
/// §5), so this is more than a year; a wider export is refused rather than cut short.
const EXPORT_MAX: u32 = 50_000;

/// Which events to read. Every filter given must hold.
#[derive(Debug, Default, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub(super) struct AuditEventQuery {
    /// The staff account that acted: its id, or `me` for the signed-in account.
    actor: Option<String>,
    /// The source's id of the actor, e.g. `github:123` or `unix:1000@ai-server`.
    actor_external: Option<String>,
    /// The kind of what was changed, e.g. `job`.
    object_type: Option<String>,
    /// The start of the id of what was changed.
    object_id: Option<String>,
    /// Where the event came from, e.g. `dawneer`.
    source: Option<String>,
    /// The action, e.g. `operator_action.run_now`.
    action: Option<String>,
    /// The result, e.g. `failed`.
    result: Option<String>,
    /// The change every event of it shares.
    change_id: Option<String>,
    /// Happened at or after (RFC 3339).
    from: Option<DateTime<Utc>>,
    /// Happened before (RFC 3339).
    to: Option<DateTime<Utc>>,
    /// Only rows before this `seq`: the `next_before_seq` of the page before.
    before_seq: Option<i64>,
    /// Rows per page, 1 to 500 (default 100).
    limit: Option<u32>,
}

/// What to export, and as what.
#[derive(Debug, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub(super) struct AuditExportQuery {
    /// `jsonl` (JSON Lines, the default) or `csv`.
    format: Option<String>,
    /// The staff account that acted: its id, or `me` for the signed-in account.
    actor: Option<String>,
    /// The source's id of the actor.
    actor_external: Option<String>,
    /// The kind of what was changed.
    object_type: Option<String>,
    /// The start of the id of what was changed.
    object_id: Option<String>,
    /// Where the event came from.
    source: Option<String>,
    /// The action.
    action: Option<String>,
    /// The result.
    result: Option<String>,
    /// The change every event of it shares.
    change_id: Option<String>,
    /// Happened at or after (RFC 3339).
    from: Option<DateTime<Utc>>,
    /// Happened before (RFC 3339).
    to: Option<DateTime<Utc>>,
    /// Only rows this snapshot did not see: the `cursor` of the export before.
    after: Option<String>,
}

fn parse_uuid(name: &str, value: &str) -> Result<Uuid, ApiError> {
    Uuid::parse_str(value).map_err(|_| ApiError::BadRequest(format!("{name} is not a UUID")))
}

fn word(name: &str, value: Option<&String>) -> Result<Option<String>, ApiError> {
    match value.map(|value| value.trim()) {
        None | Some("") => Ok(None),
        Some(value) if value.len() <= 300 && !value.chars().any(char::is_control) => {
            Ok(Some(value.to_owned()))
        }
        Some(_) => Err(ApiError::BadRequest(format!(
            "{name} must be one line of at most 300 bytes"
        ))),
    }
}

impl AuditEventQuery {
    fn filter(&self, principal: &AuthorizedPrincipal) -> Result<AuditEventFilter, ApiError> {
        let limit = self.limit.unwrap_or(PAGE_DEFAULT);
        if limit == 0 || limit > PAGE_MAX {
            return Err(ApiError::BadRequest(format!(
                "limit must be between 1 and {PAGE_MAX}"
            )));
        }
        if let (Some(from), Some(to)) = (self.from, self.to) {
            if from >= to {
                return Err(ApiError::BadRequest("from must be before to".to_owned()));
            }
        }
        let actor_staff_id = match self.actor.as_deref().map(str::trim) {
            None | Some("") => None,
            Some("me") => Some(principal.principal_id),
            Some(id) => Some(parse_uuid("actor", id)?),
        };
        let change_id = word("change_id", self.change_id.as_ref())?
            .map(|id| parse_uuid("change_id", &id))
            .transpose()?;
        Ok(AuditEventFilter {
            actor_staff_id,
            actor_external: word("actor_external", self.actor_external.as_ref())?,
            object_type: word("object_type", self.object_type.as_ref())?,
            object_id_prefix: word("object_id", self.object_id.as_ref())?,
            source: word("source", self.source.as_ref())?,
            action: word("action", self.action.as_ref())?,
            result: word("result", self.result.as_ref())?,
            change_id,
            occurred_from: self.from,
            occurred_to: self.to,
            before_seq: self.before_seq,
            after_snapshot: None,
            limit,
        })
    }
}

/// A `pg_snapshot` as text, `xmin:xmax:xip,...` (PostgreSQL 17 「Snapshot Information
/// Functions」): checked here so that a bad cursor is a 400, not a database error.
fn snapshot_cursor(value: Option<&String>) -> Result<Option<String>, ApiError> {
    let Some(value) = word("after", value)? else {
        return Ok(None);
    };
    let mut parts = value.splitn(3, ':');
    let number = |part: &str| !part.is_empty() && part.bytes().all(|byte| byte.is_ascii_digit());
    let well_formed = matches!(
        (parts.next(), parts.next(), parts.next()),
        (Some(xmin), Some(xmax), Some(xip))
            if number(xmin) && number(xmax) && (xip.is_empty() || xip.split(',').all(number))
    );
    if well_formed {
        Ok(Some(value))
    } else {
        Err(ApiError::BadRequest(
            "after must be the cursor of an earlier export".to_owned(),
        ))
    }
}

fn store_unavailable(error: &AuditStoreError) -> ApiError {
    tracing::error!(error = %error, "audit chain unavailable");
    ApiError::Unavailable("the audit chain is unavailable; ask again".to_owned())
}

fn view(event: AuditEvent) -> AuditEventView {
    AuditEventView {
        seq: event.seq,
        event_id: event.event_id.to_string(),
        occurred_at: event.occurred_at,
        recorded_at: event.recorded_at,
        source: event.source,
        source_event_id: event.source_event_id,
        action: event.action,
        operation: event.operation,
        actor_kind: event.actor_kind,
        actor_staff_id: event.actor_staff_id.map(|id| id.to_string()),
        actor_subject: event.actor_subject,
        actor_external: event.actor_external,
        actor_verified: event.actor_verified,
        on_behalf_of: event.on_behalf_of,
        object_type: event.object_type,
        object_id: event.object_id,
        before: event.before,
        after: event.after,
        reason: event.reason,
        applied_via_kind: event.applied_via_kind,
        applied_via_ref: event.applied_via_ref,
        result: event.result,
        error: event.error,
        change_id: event.change_id.map(|id| id.to_string()),
        txid: event.txid,
    }
}

fn digest_view(digest: AuditDigest) -> AuditDigestView {
    AuditDigestView {
        digest_no: digest.digest_no,
        from_snapshot: digest.from_snapshot,
        to_snapshot: digest.to_snapshot,
        row_count: digest.row_count,
        rows_sha256: digest.rows_sha256,
        previous_digest_sha256: digest.previous_digest_sha256,
        digest_sha256: digest.digest_sha256,
        rekor_log_index: digest.rekor_log_index,
        made_at: digest.made_at,
    }
}

/// Reads one page of the audit chain, newest first.
#[utoipa::path(
    get,
    path = "/data-catalog/v1/audit/events",
    operation_id = "listAuditEvents",
    params(AuditEventQuery),
    responses(
        (status = 200, body = AuditEventPage),
        (status = 400, description = "A limit outside 1..=500, an actor or change id that is not a UUID (or `me`), a filter longer than one line of 300 bytes, or a `from` not before `to`", body = ApiErrorResponse),
        (status = 401, description = "No staff identity"),
        (status = 403, description = "Principal may not read the audit chain"),
        (status = 503, description = "The audit chain could not be read", body = ApiErrorResponse),
    )
)]
pub(super) async fn audit_events(
    State(state): State<Arc<AppState>>,
    Extension(principal): Extension<AuthorizedPrincipal>,
    Query(query): Query<AuditEventQuery>,
) -> Result<Json<AuditEventPage>, ApiError> {
    let filter = query.filter(&principal)?;
    let events = state
        .audit_events
        .page(&filter)
        .await
        .map_err(|error| store_unavailable(&error))?;
    let full = u32::try_from(events.len()).is_ok_and(|count| count == filter.limit);
    let next_before_seq = events.last().filter(|_| full).map(|event| event.seq);
    Ok(Json(AuditEventPage {
        events: events.into_iter().map(view).collect(),
        next_before_seq,
    }))
}

/// One CSV field: quoted when it holds a comma, a quote or a line break.
fn csv_field(value: &str) -> String {
    if value.contains([',', '"', '\n', '\r']) {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_owned()
    }
}

const CSV_COLUMNS: [&str; 25] = [
    "seq",
    "event_id",
    "occurred_at",
    "recorded_at",
    "source",
    "source_event_id",
    "action",
    "operation",
    "actor_kind",
    "actor_staff_id",
    "actor_subject",
    "actor_external",
    "actor_verified",
    "on_behalf_of",
    "object_type",
    "object_id",
    "before",
    "after",
    "reason",
    "applied_via_kind",
    "applied_via_ref",
    "result",
    "error",
    "change_id",
    "txid",
];

fn csv_row(event: &AuditEventView) -> String {
    let text = |value: &Option<String>| value.clone().unwrap_or_default();
    let json = |value: &Option<serde_json::Value>| {
        value.as_ref().map(ToString::to_string).unwrap_or_default()
    };
    let fields = [
        event.seq.to_string(),
        event.event_id.clone(),
        event.occurred_at.to_rfc3339(),
        event.recorded_at.to_rfc3339(),
        event.source.clone(),
        event.source_event_id.clone(),
        event.action.clone(),
        event.operation.clone(),
        event.actor_kind.clone(),
        text(&event.actor_staff_id),
        text(&event.actor_subject),
        text(&event.actor_external),
        event.actor_verified.to_string(),
        text(&event.on_behalf_of),
        event.object_type.clone(),
        event.object_id.clone(),
        json(&event.before),
        json(&event.after),
        text(&event.reason),
        text(&event.applied_via_kind),
        text(&event.applied_via_ref),
        event.result.clone(),
        text(&event.error),
        text(&event.change_id),
        event.txid.clone(),
    ];
    fields
        .iter()
        .map(|field| csv_field(field))
        .collect::<Vec<_>>()
        .join(",")
}

// Each row carries its `seq` and `txid`. A JSON Lines export ends with one `{"digest": ...}` line
// for each digest whose range reaches the rows' transactions (with its Rekor entry), then one
// `{"cursor": ..., "uncovered": n}` line: the snapshot the rows were read in, to pass as `after` to
// the next export, and how many rows no digest covers yet. A CSV export carries the cursor and the
// count in the `X-Audit-Cursor` and `X-Audit-Uncovered` headers. The export itself is recorded as
// `audit.exported`, with the filters it used.
/// Exports every event the filters select, oldest first, as JSON Lines or CSV.
#[utoipa::path(
    get,
    path = "/data-catalog/v1/audit/events/export",
    operation_id = "exportAuditEvents",
    params(AuditExportQuery),
    responses(
        (status = 200, description = "`application/x-ndjson` or `text/csv`, oldest first", content_type = "application/x-ndjson", body = String),
        (status = 400, description = "An unknown format, an actor or change id that is not a UUID (or `me`), an `after` that is not an export's cursor, a filter longer than one line of 300 bytes, or a `from` not before `to`", body = ApiErrorResponse),
        (status = 401, description = "No staff identity"),
        (status = 403, description = "Principal may not read the audit chain"),
        (status = 422, description = "More than 50,000 events; narrow the filters", body = ApiErrorResponse),
        (status = 503, description = "The audit chain could not be read, or the export could not be recorded", body = ApiErrorResponse),
    )
)]
pub(super) async fn export_audit_events(
    State(state): State<Arc<AppState>>,
    Extension(principal): Extension<AuthorizedPrincipal>,
    Query(query): Query<AuditExportQuery>,
) -> Result<Response, ApiError> {
    let csv = match query.format.as_deref().map(str::trim) {
        None | Some("" | "jsonl") => false,
        Some("csv") => true,
        Some(_) => {
            return Err(ApiError::BadRequest(
                "format must be jsonl or csv".to_owned(),
            ))
        }
    };
    let page = AuditEventQuery {
        actor: query.actor,
        actor_external: query.actor_external,
        object_type: query.object_type,
        object_id: query.object_id,
        source: query.source,
        action: query.action,
        result: query.result,
        change_id: query.change_id,
        from: query.from,
        to: query.to,
        before_seq: None,
        limit: Some(PAGE_MAX),
    };
    let mut filter = page.filter(&principal)?;
    filter.after_snapshot = snapshot_cursor(query.after.as_ref())?;
    let AuditExport {
        events,
        cursor,
        digests,
        uncovered,
    } = state
        .audit_events
        .export(&filter, EXPORT_MAX)
        .await
        .map_err(|error| store_unavailable(&error))?;
    if events.len() > EXPORT_MAX as usize {
        return Err(ApiError::Unprocessable(format!(
            "more than {EXPORT_MAX} events; narrow the filters"
        )));
    }
    let events: Vec<AuditEventView> = events.into_iter().map(view).collect();

    let export_id = Uuid::new_v4();
    record_export(
        &state, &principal, &filter, &events, csv, export_id, &cursor,
    )
    .await?;

    let trailer = ExportTrailer {
        digests: digests.into_iter().map(digest_view).collect(),
        cursor,
        uncovered,
    };
    let (content_type, extension, body) = export_body(&events, &trailer, csv)?;
    let mut response = (StatusCode::OK, body).into_response();
    let headers = response.headers_mut();
    headers.insert(header::CONTENT_TYPE, HeaderValue::from_static(content_type));
    if let Ok(disposition) = HeaderValue::from_str(&format!(
        "attachment; filename=\"audit-{export_id}.{extension}\""
    )) {
        headers.insert(header::CONTENT_DISPOSITION, disposition);
    }
    if let Ok(cursor) = HeaderValue::from_str(&trailer.cursor) {
        headers.insert("x-audit-cursor", cursor);
    }
    headers.insert("x-audit-uncovered", HeaderValue::from(trailer.uncovered));
    Ok(response)
}

/// What follows an export's rows.
struct ExportTrailer {
    digests: Vec<AuditDigestView>,
    cursor: String,
    uncovered: i64,
}

/// Records the export in the chain as `audit.exported`, with the filters it used. An export that
/// cannot be recorded is not given.
async fn record_export(
    state: &AppState,
    principal: &AuthorizedPrincipal,
    filter: &AuditEventFilter,
    events: &[AuditEventView],
    csv: bool,
    export_id: Uuid,
    cursor: &str,
) -> Result<(), ApiError> {
    let filters = json!({
        "format": if csv { "csv" } else { "jsonl" },
        "actor_staff_id": filter.actor_staff_id.map(|id| id.to_string()),
        "actor_external": filter.actor_external,
        "object_type": filter.object_type,
        "object_id": filter.object_id_prefix,
        "source": filter.source,
        "action": filter.action,
        "result": filter.result,
        "change_id": filter.change_id.map(|id| id.to_string()),
        "from": filter.occurred_from,
        "to": filter.occurred_to,
        "after": filter.after_snapshot,
        "cursor": cursor,
        "rows": events.len(),
        "from_seq": events.first().map(|event| event.seq),
        "to_seq": events.last().map(|event| event.seq),
    });
    let filters = match filters {
        serde_json::Value::Object(map) => serde_json::Value::Object(
            map.into_iter()
                .filter(|(_, value)| !value.is_null())
                .collect(),
        ),
        other => other,
    };
    // An export that cannot be recorded is not given: GCP records a Data Access read the same way.
    state
        .audit_events
        .append(&AuditAppend {
            event_id: export_id,
            occurred_at: Utc::now(),
            source: "audit".to_owned(),
            source_event_id: format!("export:{export_id}"),
            action: "audit.exported".to_owned(),
            operation: "access".to_owned(),
            actor_kind: "staff".to_owned(),
            actor_staff_id: Some(principal.principal_id),
            object_type: "audit".to_owned(),
            object_id: "events".to_owned(),
            after: Some(filters),
            reason: None,
            applied_via: Some(("api".to_owned(), export_id.to_string())),
            result: "succeeded".to_owned(),
            error: None,
            change_id: None,
        })
        .await
        .map_err(|error| store_unavailable(&error))?;
    Ok(())
}

/// The export body: a CSV table, or JSON Lines that end with the digests and the cursor.
fn export_body(
    events: &[AuditEventView],
    trailer: &ExportTrailer,
    csv: bool,
) -> Result<(&'static str, &'static str, String), ApiError> {
    Ok(if csv {
        let mut body = CSV_COLUMNS.join(",");
        body.push('\n');
        for event in events {
            body.push_str(&csv_row(event));
            body.push('\n');
        }
        ("text/csv; charset=utf-8", "csv", body)
    } else {
        let mut body = String::new();
        for event in events {
            body.push_str(&serde_json::to_string(event).map_err(|error| {
                ApiError::Internal(format!("an audit event did not serialize: {error}"))
            })?);
            body.push('\n');
        }
        for digest in &trailer.digests {
            body.push_str(&json!({ "digest": digest }).to_string());
            body.push('\n');
        }
        body.push_str(
            &json!({ "cursor": trailer.cursor, "uncovered": trailer.uncovered }).to_string(),
        );
        body.push('\n');
        ("application/x-ndjson", "jsonl", body)
    })
}

/// Each importer's cursor and the newest digest: whether the chain is being fed and witnessed.
#[utoipa::path(
    get,
    path = "/data-catalog/v1/audit/import-status",
    operation_id = "auditImportStatus",
    responses(
        (status = 200, body = AuditImportStatusPage),
        (status = 401, description = "No staff identity"),
        (status = 403, description = "Principal may not read the audit chain"),
        (status = 503, description = "The audit chain could not be read", body = ApiErrorResponse),
    )
)]
pub(super) async fn audit_import_status(
    State(state): State<Arc<AppState>>,
) -> Result<Json<AuditImportStatusPage>, ApiError> {
    let sources = state
        .audit_events
        .import_status()
        .await
        .map_err(|error| store_unavailable(&error))?;
    let latest_digest = state
        .audit_events
        .latest_digest()
        .await
        .map_err(|error| store_unavailable(&error))?
        .map(digest_view);
    Ok(Json(AuditImportStatusPage {
        sources: sources
            .into_iter()
            .map(|source| AuditImportSource {
                source: source.source,
                cursor: source.cursor,
                state: source.state,
                advanced_at: source.advanced_at,
            })
            .collect(),
        latest_digest,
    }))
}

#[cfg(test)]
mod tests {
    use super::csv_field;

    #[test]
    fn a_csv_field_is_quoted_only_when_it_must_be() {
        assert_eq!(csv_field("plain"), "plain");
        assert_eq!(csv_field("a,b"), "\"a,b\"");
        assert_eq!(csv_field("say \"hi\""), "\"say \"\"hi\"\"\"");
        assert_eq!(csv_field("two\nlines"), "\"two\nlines\"");
    }
}
