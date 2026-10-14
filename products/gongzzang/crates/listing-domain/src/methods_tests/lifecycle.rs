use shared_kernel::listing_status::ListingStatus;

use super::fixtures::{later, sample_draft, t0};

// ── Full lifecycle (happy path) ─────────────────────────────────────────────

#[test]
fn full_lifecycle_draft_to_sold_bumps_version_each_step() {
    let mut l = sample_draft(t0());
    assert_eq!(l.version, 1);
    l.submit_for_review(later(1)).unwrap();
    assert_eq!(l.version, 2);
    l.approve(later(2)).unwrap();
    assert_eq!(l.version, 3);
    l.mark_sold(later(3)).unwrap();
    assert_eq!(l.version, 4);
    assert_eq!(l.status, ListingStatus::Sold);
}

#[test]
fn rejected_then_revise_then_resubmit_works() {
    let mut l = sample_draft(t0());
    l.submit_for_review(later(1)).unwrap();
    l.reject(later(2)).unwrap();
    assert_eq!(l.status, ListingStatus::Rejected);
    l.revise_after_rejection(later(3)).unwrap();
    assert_eq!(l.status, ListingStatus::Draft);
    l.submit_for_review(later(4)).unwrap();
    assert_eq!(l.status, ListingStatus::PendingReview);
}
