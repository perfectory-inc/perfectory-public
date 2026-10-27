//! 법원경매정보 (courtauction.go.kr) collection rules that hold without I/O (root ADR-0289, ADR-0290).
//!
//! Every value these rules use comes from `courtauction-go-kr.contract.json`: the types here carry
//! no defaults. The collector reads the contract, builds these rules and asks them what to do; the
//! transport, the queue and Bronze stay outside.

/// What a site answer is: data, a block, or a failure (root ADR-0289 §3).
pub mod answer;

/// The row fingerprint that says whether a list row changed (root ADR-0290 §2).
pub mod change;

/// The adaptive number of requests in flight per endpoint group (root ADR-0289 §4).
pub mod pace;

/// The PNU a court auction object assembles from its legal-dong code and lot (root ADR-0290 §8).
pub mod pnu;

/// When a case's detail is due again (root ADR-0290 §3).
pub mod recrawl;

pub use answer::{classify, Answer, BlockSignals, Blocked, Failed};
pub use change::{fingerprint, Fingerprint};
pub use pace::{Pace, PaceRule};
pub use pnu::{assemble, PnuAssembly, PnuUnresolved};
pub use recrawl::{due, CaseView, RecrawlRule};
