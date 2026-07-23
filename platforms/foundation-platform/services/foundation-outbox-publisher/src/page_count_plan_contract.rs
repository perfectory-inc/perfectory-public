use anyhow::{bail, Context};

pub(crate) fn required_pages_for(
    provider_total_count: u64,
    effective_page_size: u64,
) -> anyhow::Result<u64> {
    if provider_total_count < 1 {
        return Ok(1);
    }
    let pages = provider_total_count
        .checked_add(effective_page_size - 1)
        .context("required_pages calculation overflow")?
        / effective_page_size;
    Ok(pages)
}

pub(crate) fn building_register_endpoint_slug(operation: &str) -> anyhow::Result<String> {
    if !is_get_br_operation(operation) {
        bail!("building-register operation must be a getBr* API operation: {operation}");
    }
    Ok(format!("data-go-kr-building-register-{operation}"))
}

pub(crate) fn building_register_job_id(operation: &str, sigungu: &str, bjdong: &str) -> String {
    if operation == "getBrTitleInfo" {
        format!("building-register-{sigungu}-{bjdong}")
    } else {
        format!("building-register-{operation}-{sigungu}-{bjdong}")
    }
}

pub(crate) fn is_valid_building_job_id(job_id: &str) -> bool {
    if let Some(scope) = job_id.strip_prefix("building-register-") {
        if is_legal_dong_scope(scope) {
            return true;
        }
        if let Some(rest) = scope.strip_prefix("getBr") {
            return rest
                .split_once('-')
                .is_some_and(|(operation_tail, legal_dong)| {
                    is_ascii_alphanumeric(operation_tail) && is_legal_dong_scope(legal_dong)
                });
        }
    }
    false
}

pub(crate) fn is_valid_vworld_job_id(job_id: &str) -> bool {
    ["vworld-cadastral-", "vworld-land-register-"]
        .iter()
        .any(|prefix| job_id.strip_prefix(prefix).is_some_and(is_legal_dong_scope))
}

fn is_legal_dong_scope(scope: &str) -> bool {
    scope.split_once('-').is_some_and(|(sigungu, bjdong)| {
        sigungu.len() == 5
            && bjdong.len() == 5
            && sigungu.bytes().all(|byte| byte.is_ascii_digit())
            && bjdong.bytes().all(|byte| byte.is_ascii_digit())
    })
}

fn is_get_br_operation(operation: &str) -> bool {
    operation
        .strip_prefix("getBr")
        .is_some_and(|tail| !tail.is_empty() && is_ascii_alphanumeric(tail))
}

fn is_ascii_alphanumeric(value: &str) -> bool {
    value.bytes().all(|byte| byte.is_ascii_alphanumeric())
}
