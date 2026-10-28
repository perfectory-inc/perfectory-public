//! Industrial complex upsert by official code. Moved out of `unit_of_work.rs` unchanged.

use super::*;

pub(super) async fn upsert_industrial_complexes_by_official_code(
    pool: &PgPool,
    commands: &[UpsertIndustrialComplexCommand],
) -> Result<Vec<UpsertIndustrialComplexOutcome>, CatalogError> {
    if commands.is_empty() {
        return Ok(Vec::new());
    }

    let mut tx = pool.begin().await.map_err(map_sqlx)?;
    let mut outcomes = Vec::with_capacity(commands.len());

    for command in commands {
        // By the derived id, archived or not (root ADR-0250): a code names one complex for good, so
        // a source that still lists an archived complex refreshes that row and leaves it archived
        // rather than minting a second one beside it.
        let existing_row = sqlx::query(&format!(
            "SELECT {INDUSTRIAL_COMPLEX_COLUMNS}
             FROM catalog.industrial_complex
             WHERE id = $1
             FOR UPDATE"
        ))
        .bind(ComplexId::for_official_complex_code(&command.official_complex_code).as_uuid())
        .fetch_optional(&mut *tx)
        .await
        .map_err(map_sqlx)?;

        let outcome = if let Some(row) = existing_row {
            upsert_existing_industrial_complex(&mut tx, command, &row).await?
        } else {
            UpsertIndustrialComplexOutcome {
                complex: insert_industrial_complex_from_upsert(&mut tx, command).await?,
                effect: UpsertIndustrialComplexEffect::Inserted,
            }
        };
        outcomes.push(outcome);
    }

    tx.commit().await.map_err(map_sqlx)?;
    Ok(outcomes)
}

pub(super) async fn upsert_existing_industrial_complex(
    tx: &mut Transaction<'_, Postgres>,
    command: &UpsertIndustrialComplexCommand,
    row: &sqlx::postgres::PgRow,
) -> Result<UpsertIndustrialComplexOutcome, CatalogError> {
    let existing = row_to_complex(row)?;
    let changed_fields = changed_industrial_complex_fields(&existing, command);
    if changed_fields.is_empty() {
        return Ok(UpsertIndustrialComplexOutcome {
            complex: existing,
            effect: UpsertIndustrialComplexEffect::Unchanged,
        });
    }

    let updated = update_industrial_complex_from_upsert(tx, &existing, command).await?;
    let event =
        CatalogEvent::IndustrialComplexUpdated(updated.updated_fields_event(changed_fields));
    insert_outbox_event(tx, &event).await?;
    Ok(UpsertIndustrialComplexOutcome {
        complex: updated,
        effect: UpsertIndustrialComplexEffect::Updated,
    })
}

pub(super) async fn update_industrial_complex_from_upsert(
    tx: &mut Transaction<'_, Postgres>,
    existing: &IndustrialComplex,
    command: &UpsertIndustrialComplexCommand,
) -> Result<IndustrialComplex, CatalogError> {
    let area_i64 = u64_to_i64(command.area_m2)?;
    let updated_row = sqlx::query(&format!(
        "UPDATE catalog.industrial_complex
         SET name = $2,
             kind = $3,
             primary_bjdong_code = $4,
             area_m2 = $5,
             status = $6,
             sido_code = $7,
             sigungu_code = $8,
             address_text = $9,
             management_agency_name = $10,
             developer_name = $11,
             designated_date = $12,
             completion_date = $13,
             construction_start_date = $14,
             development_progress_percent = $15::numeric,
             lot_sales_status = $16,
             business_period_raw = $17,
             business_period_start_month = $18,
             business_period_end_month = $19,
             designation_basis_law_raw = $20,
             development_method_raw = $21,
             development_purpose_raw = $22,
             invited_industries_raw = $23,
             kind_raw = $24,
             status_raw = $25,
             lot_sales_status_raw = $26,
             development_stage = $27,
             updated_at = now(),
             version = version + 1
         WHERE id = $1
         RETURNING {INDUSTRIAL_COMPLEX_COLUMNS}"
    ))
    .bind(existing.id.as_uuid())
    .bind(&command.name)
    .bind(command.kind.wire_name())
    .bind(command.primary_bjdong_code.as_deref())
    .bind(area_i64)
    .bind(command.status.map(IndustrialComplexStatus::wire_name))
    .bind(command.sido_code.as_deref())
    .bind(command.sigungu_code.as_deref())
    .bind(command.address_text.as_deref())
    .bind(command.management_agency_name.as_deref())
    .bind(command.developer_name.as_deref())
    .bind(command.designated_date)
    .bind(command.completion_date)
    .bind(command.construction_start_date)
    .bind(command.development_progress_percent.as_deref())
    .bind(
        command
            .lot_sales_status
            .map(IndustrialComplexLotSalesStatus::wire_name),
    )
    .bind(command.business_period_raw.as_deref())
    .bind(command.business_period_start_month.as_deref())
    .bind(command.business_period_end_month.as_deref())
    .bind(command.designation_basis_law_raw.as_deref())
    .bind(command.development_method_raw.as_deref())
    .bind(command.development_purpose_raw.as_deref())
    .bind(command.invited_industries_raw.as_deref())
    .bind(command.kind_raw.as_deref())
    .bind(command.status_raw.as_deref())
    .bind(command.lot_sales_status_raw.as_deref())
    .bind(
        command
            .development_stage
            .map(IndustrialComplexDevelopmentStage::wire_name),
    )
    .fetch_one(&mut **tx)
    .await;

    match updated_row {
        Ok(row) => row_to_complex(&row),
        Err(sqlx::Error::Database(db)) if is_unique_violation_code(db.code().as_deref()) => {
            Err(map_industrial_complex_unique_violation(
                db.constraint(),
                command.official_complex_code.as_str(),
            ))
        }
        Err(error) => Err(map_sqlx(error)),
    }
}

pub(super) async fn insert_industrial_complex_from_upsert(
    tx: &mut Transaction<'_, Postgres>,
    command: &UpsertIndustrialComplexCommand,
) -> Result<IndustrialComplex, CatalogError> {
    let now = Utc::now();
    let area_i64 = u64_to_i64(command.area_m2)?;
    let inserted_row = sqlx::query(&format!(
        "INSERT INTO catalog.industrial_complex
         (id, official_complex_code, name, kind, primary_bjdong_code,
          area_m2, status, sido_code, sigungu_code, address_text, management_agency_name,
          developer_name, designated_date, construction_start_date, completion_date,
          lot_sales_status, business_period_raw, business_period_start_month,
          business_period_end_month, development_progress_percent, designation_basis_law_raw,
          development_method_raw, development_purpose_raw, invited_industries_raw,
          created_at, updated_at, version, kind_raw, status_raw, lot_sales_status_raw,
          development_stage)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17,
                 $18, $19, $20::numeric, $21, $22, $23, $24, $25, $26, 1, $27, $28, $29, $30)
         RETURNING {INDUSTRIAL_COMPLEX_COLUMNS}"
    ))
    .bind(ComplexId::for_official_complex_code(&command.official_complex_code).as_uuid())
    .bind(&command.official_complex_code)
    .bind(&command.name)
    .bind(command.kind.wire_name())
    .bind(command.primary_bjdong_code.as_deref())
    .bind(area_i64)
    .bind(command.status.map(IndustrialComplexStatus::wire_name))
    .bind(command.sido_code.as_deref())
    .bind(command.sigungu_code.as_deref())
    .bind(command.address_text.as_deref())
    .bind(command.management_agency_name.as_deref())
    .bind(command.developer_name.as_deref())
    .bind(command.designated_date)
    .bind(command.construction_start_date)
    .bind(command.completion_date)
    .bind(
        command
            .lot_sales_status
            .map(IndustrialComplexLotSalesStatus::wire_name),
    )
    .bind(command.business_period_raw.as_deref())
    .bind(command.business_period_start_month.as_deref())
    .bind(command.business_period_end_month.as_deref())
    .bind(command.development_progress_percent.as_deref())
    .bind(command.designation_basis_law_raw.as_deref())
    .bind(command.development_method_raw.as_deref())
    .bind(command.development_purpose_raw.as_deref())
    .bind(command.invited_industries_raw.as_deref())
    .bind(now)
    .bind(now)
    .bind(command.kind_raw.as_deref())
    .bind(command.status_raw.as_deref())
    .bind(command.lot_sales_status_raw.as_deref())
    .bind(
        command
            .development_stage
            .map(IndustrialComplexDevelopmentStage::wire_name),
    )
    .fetch_one(&mut **tx)
    .await;

    let inserted = match inserted_row {
        Ok(row) => row_to_complex(&row)?,
        Err(sqlx::Error::Database(db)) if is_unique_violation_code(db.code().as_deref()) => {
            return Err(map_industrial_complex_unique_violation(
                db.constraint(),
                command.official_complex_code.as_str(),
            ));
        }
        Err(error) => return Err(map_sqlx(error)),
    };
    let event = CatalogEvent::IndustrialComplexCreatedV3(inserted.created_event());
    insert_outbox_event(tx, &event).await?;
    Ok(inserted)
}

pub(super) fn changed_industrial_complex_fields(
    existing: &IndustrialComplex,
    command: &UpsertIndustrialComplexCommand,
) -> Vec<String> {
    let mut fields = Vec::new();
    if existing.name != command.name {
        fields.push("name".to_owned());
    }
    if existing.kind != command.kind {
        fields.push("kind".to_owned());
    }
    if existing.primary_bjdong_code != command.primary_bjdong_code {
        fields.push("primary_bjdong_code".to_owned());
    }
    if existing.area_m2 != command.area_m2 {
        fields.push("area_m2".to_owned());
    }
    if existing.status != command.status {
        fields.push("status".to_owned());
    }
    if existing.sido_code != command.sido_code {
        fields.push("sido_code".to_owned());
    }
    if existing.sigungu_code != command.sigungu_code {
        fields.push("sigungu_code".to_owned());
    }
    if existing.address_text != command.address_text {
        fields.push("address_text".to_owned());
    }
    if existing.management_agency_name != command.management_agency_name {
        fields.push("management_agency_name".to_owned());
    }
    if existing.developer_name != command.developer_name {
        fields.push("developer_name".to_owned());
    }
    if existing.designated_date != command.designated_date {
        fields.push("designated_date".to_owned());
    }
    if existing.completion_date != command.completion_date {
        fields.push("completion_date".to_owned());
    }
    if existing.construction_start_date != command.construction_start_date {
        fields.push("construction_start_date".to_owned());
    }
    // Compared as text, which is how the row carries it. `59.90` and `59.9` are the same number and
    // different text, and the comparison would call a re-load a change; the producer emits two
    // decimal places for every value, and Postgres renders `numeric(5,2)` the same way, so both
    // sides of this comparison are already normalized.
    if existing.development_progress_percent != command.development_progress_percent {
        fields.push("development_progress_percent".to_owned());
    }
    if existing.lot_sales_status != command.lot_sales_status {
        fields.push("lot_sales_status".to_owned());
    }
    if existing.business_period_raw != command.business_period_raw {
        fields.push("business_period_raw".to_owned());
    }
    if existing.business_period_start_month != command.business_period_start_month {
        fields.push("business_period_start_month".to_owned());
    }
    if existing.business_period_end_month != command.business_period_end_month {
        fields.push("business_period_end_month".to_owned());
    }
    if existing.designation_basis_law_raw != command.designation_basis_law_raw {
        fields.push("designation_basis_law_raw".to_owned());
    }
    if existing.development_method_raw != command.development_method_raw {
        fields.push("development_method_raw".to_owned());
    }
    if existing.development_purpose_raw != command.development_purpose_raw {
        fields.push("development_purpose_raw".to_owned());
    }
    if existing.invited_industries_raw != command.invited_industries_raw {
        fields.push("invited_industries_raw".to_owned());
    }
    if existing.development_stage != command.development_stage {
        fields.push("development_stage".to_owned());
    }
    if existing.kind_raw != command.kind_raw {
        fields.push("kind_raw".to_owned());
    }
    if existing.status_raw != command.status_raw {
        fields.push("status_raw".to_owned());
    }
    if existing.lot_sales_status_raw != command.lot_sales_status_raw {
        fields.push("lot_sales_status_raw".to_owned());
    }
    fields
}

pub(super) fn map_industrial_complex_unique_violation(
    constraint: Option<&str>,
    official_complex_code: &str,
) -> CatalogError {
    match constraint {
        Some(
            "industrial_complex_official_complex_code_idx"
            | "industrial_complex_official_complex_code_key"
            | "industrial_complex_active_official_code_idx"
            // The id is derived from the code (root ADR-0250), so an archived complex with the
            // same code collides on the key rather than on the active-code index.
            | "industrial_complex_pkey",
        ) => CatalogError::ComplexOfficialCodeConflict(official_complex_code.to_owned()),
        Some(other) => CatalogError::Infrastructure(format!(
            "unexpected industrial_complex unique constraint violation: {other}"
        )),
        None => CatalogError::Infrastructure(
            "unexpected industrial_complex unique constraint violation without constraint name"
                .to_owned(),
        ),
    }
}
