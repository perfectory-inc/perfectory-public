use serde::Deserialize;
use serde_json::Value;

/// The signals that mark an answer as a block, from the contract.
///
/// The site answers a block with HTTP 200 and a JSON body whose `data.ipcheck` is false, or with
/// a redirect or an HTML page where JSON belongs; status codes alone miss it.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct BlockSignals {
    /// HTTP statuses read as a block (redirects, 403, 429).
    pub http_statuses: Vec<u16>,
    /// JSON pointer of the flag that is `false` when the site blocks (`/data/ipcheck`).
    pub ipcheck_pointer: String,
    /// JSON pointer of the payload that must be an object in a good answer (`/data`).
    pub data_pointer: String,
    /// JSON pointer of the site's own status field (`/status`).
    pub status_pointer: String,
    /// The value the site's own status field holds in a good answer.
    pub ok_status: i64,
}

/// Why an answer is a block.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Blocked {
    /// An HTTP status the contract lists.
    Status(u16),
    /// An HTML page where JSON belongs.
    Html,
    /// The site's flag says the session is blocked.
    IpCheck,
    /// A JSON answer without the payload object.
    NoData,
}

/// Why an answer failed without being a block.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Failed {
    /// An HTTP status that is neither 200 nor a block.
    Status(u16),
    /// A body that is not JSON.
    NotJson,
    /// The site's own status field is not the good value.
    SiteStatus(Option<i64>),
}

/// One site answer, read.
#[derive(Clone, Debug, PartialEq)]
pub enum Answer {
    /// The payload object.
    Data(Value),
    /// The session is blocked: wait, renew it and ask again.
    Blocked(Blocked),
    /// A failure to retry a bounded number of times, then record.
    Failed(Failed),
}

/// Reads one answer the way the contract says. A block is checked before anything else, so a
/// blocked session is never mistaken for a broken request.
#[must_use]
pub fn classify(signals: &BlockSignals, http_status: u16, body: &str) -> Answer {
    if signals.http_statuses.contains(&http_status) {
        return Answer::Blocked(Blocked::Status(http_status));
    }
    if body.trim_start().starts_with('<') {
        return Answer::Blocked(Blocked::Html);
    }
    let Ok(json) = serde_json::from_str::<Value>(body) else {
        return Answer::Failed(if http_status == 200 {
            Failed::NotJson
        } else {
            Failed::Status(http_status)
        });
    };
    if json.pointer(&signals.ipcheck_pointer) == Some(&Value::Bool(false)) {
        return Answer::Blocked(Blocked::IpCheck);
    }
    if http_status != 200 {
        return Answer::Failed(Failed::Status(http_status));
    }
    let site_status = json
        .pointer(&signals.status_pointer)
        .and_then(Value::as_i64);
    if site_status != Some(signals.ok_status) {
        return Answer::Failed(Failed::SiteStatus(site_status));
    }
    match json.pointer(&signals.data_pointer) {
        Some(data @ Value::Object(_)) => Answer::Data(data.clone()),
        _ => Answer::Blocked(Blocked::NoData),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // The shapes the site answered with on 2026-10-21; the values are synthetic.
    fn signals() -> BlockSignals {
        BlockSignals {
            http_statuses: vec![301, 302, 303, 307, 403, 429],
            ipcheck_pointer: "/data/ipcheck".to_owned(),
            data_pointer: "/data".to_owned(),
            status_pointer: "/status".to_owned(),
            ok_status: 200,
        }
    }

    #[test]
    fn a_good_answer_gives_its_payload() {
        let answer = classify(&signals(), 200, r#"{"status":200,"data":{"rows":[1]}}"#);
        assert_eq!(answer, Answer::Data(serde_json::json!({"rows": [1]})));
    }

    #[test]
    fn the_sites_200_block_is_a_block_not_data() {
        let body = r#"{"status":200,"message":"blocked","data":{"ipcheck":false}}"#;
        assert_eq!(
            classify(&signals(), 200, body),
            Answer::Blocked(Blocked::IpCheck)
        );
    }

    #[test]
    fn a_listed_status_or_an_html_page_is_a_block() {
        assert_eq!(
            classify(&signals(), 302, ""),
            Answer::Blocked(Blocked::Status(302))
        );
        assert_eq!(
            classify(&signals(), 200, "  <html>notice</html>"),
            Answer::Blocked(Blocked::Html)
        );
        assert_eq!(
            classify(&signals(), 200, r#"{"status":200,"data":null}"#),
            Answer::Blocked(Blocked::NoData)
        );
    }

    #[test]
    fn other_failures_are_failures() {
        assert_eq!(
            classify(&signals(), 500, "oops"),
            Answer::Failed(Failed::Status(500))
        );
        assert_eq!(
            classify(&signals(), 200, "not json"),
            Answer::Failed(Failed::NotJson)
        );
        assert_eq!(
            classify(&signals(), 200, r#"{"status":500,"data":{}}"#),
            Answer::Failed(Failed::SiteStatus(Some(500)))
        );
    }
}
