use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct TraceContext {
    pub trace_id: String,
    pub tenant_id: String,
    pub human_user_id: String,
    pub product_id: String,
}
