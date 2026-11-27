//! The bytes this process landed in R2: the body of every write R2 accepted (a whole-object put, a
//! conditional put, a multipart part). A refused write (`412` of a create-only put, an error) adds
//! nothing, and a server-side copy has no body. The publisher prints the sum when it exits, so the host
//! and AWS copies of one collector report the same measure (W101c overlap, root ADR-0616 §7).
//!
//! Every R2 request goes through the one client of `R2ObjectStorage`, which carries this interceptor.
//! Parts of a multipart upload count when R2 accepts them, also when the upload is not completed.

use std::sync::atomic::{AtomicU64, Ordering};

use aws_sdk_s3::config::{
    interceptors::{BeforeTransmitInterceptorContextRef, FinalizerInterceptorContextRef},
    ConfigBag, Intercept, RuntimeComponents,
};
use aws_sdk_s3::error::BoxError;
use aws_smithy_types::config_bag::{Storable, StoreReplace};

static WRITTEN: AtomicU64 = AtomicU64::new(0);

/// The bytes this process has landed in R2 so far.
#[must_use]
pub fn r2_bytes_written() -> u64 {
    WRITTEN.load(Ordering::Relaxed)
}

/// The body length of a request, when it is a write: a `PUT` with a body. Signed streaming bodies
/// (`aws-chunked`) carry the payload's own length in `x-amz-decoded-content-length`.
fn body_of_write(method: &str, decoded_length: Option<&str>, length: Option<&str>) -> u64 {
    if !method.eq_ignore_ascii_case("PUT") {
        return 0;
    }
    decoded_length
        .or(length)
        .and_then(|value| value.trim().parse().ok())
        .unwrap_or(0)
}

/// The body of the attempt in flight, from the request (gone by the time the response is read).
#[derive(Clone, Debug)]
struct AttemptBody(u64);

impl Storable for AttemptBody {
    type Storer = StoreReplace<Self>;
}

#[derive(Debug, Default)]
pub(super) struct R2WrittenBytesInterceptor;

impl Intercept for R2WrittenBytesInterceptor {
    fn name(&self) -> &'static str {
        "R2WrittenBytesInterceptor"
    }

    fn read_before_transmit(
        &self,
        context: &BeforeTransmitInterceptorContextRef<'_>,
        _runtime_components: &RuntimeComponents,
        cfg: &mut ConfigBag,
    ) -> Result<(), BoxError> {
        let request = context.request();
        let headers = request.headers();
        let body = body_of_write(
            request.method(),
            headers.get("x-amz-decoded-content-length"),
            headers.get("content-length"),
        );
        cfg.interceptor_state().store_put(AttemptBody(body));
        Ok(())
    }

    fn read_after_attempt(
        &self,
        context: &FinalizerInterceptorContextRef<'_>,
        _runtime_components: &RuntimeComponents,
        cfg: &mut ConfigBag,
    ) -> Result<(), BoxError> {
        let accepted = context
            .response()
            .is_some_and(|response| response.status().is_success());
        if let (true, Some(AttemptBody(body))) = (accepted, cfg.load::<AttemptBody>()) {
            WRITTEN.fetch_add(*body, Ordering::Relaxed);
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "written_tests.rs"]
mod tests;
