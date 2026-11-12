//! Use case for checking one person against the 부동산중개업 roster (root ADR-0550 §4, §5).
//!
//! The roster loaded from Silver answers first. On a miss the live `VWorld` register is asked, when
//! the deployment has it, because the daily file trails the register by a day or two (0.16% of
//! the people of one 시군구 on 2026-11-11). A live answer is not written back: the next file
//! reaches the roster through Silver.

use std::sync::Arc;

use async_trait::async_trait;
use catalog_domain::real_estate_broker::{
    BrokerMatch, BrokerSource, BrokerVerification, BrokerVerifyQuery,
};

/// A source that can say whether one person is registered.
#[async_trait]
pub trait BrokerRegister: Send + Sync {
    /// The person the question names, or none.
    ///
    /// # Errors
    /// The source could not answer.
    async fn find(
        &self,
        query: &BrokerVerifyQuery,
    ) -> Result<Option<BrokerMatch>, BrokerRegisterError>;
}

/// A register that could not answer.
#[derive(Debug, thiserror::Error)]
#[error("the broker register is unavailable: {0}")]
pub struct BrokerRegisterError(pub String);

/// Checks one person: the roster, then the live register on a miss.
#[derive(Clone)]
pub struct VerifyRealEstateBroker {
    roster: Arc<dyn BrokerRegister>,
    live: Option<Arc<dyn BrokerRegister>>,
}

impl VerifyRealEstateBroker {
    /// Creates the use case; `live` is none where the deployment holds no `VWorld` key.
    #[must_use]
    pub const fn new(
        roster: Arc<dyn BrokerRegister>,
        live: Option<Arc<dyn BrokerRegister>>,
    ) -> Self {
        Self { roster, live }
    }

    /// Answers one question.
    ///
    /// # Errors
    /// The roster could not answer. A live register that cannot answer is a miss it did not
    /// confirm (`checked` lacks it), not an error.
    pub async fn execute(
        &self,
        query: &BrokerVerifyQuery,
    ) -> Result<BrokerVerification, BrokerRegisterError> {
        if let Some(found) = self.roster.find(query).await? {
            return Ok(BrokerVerification {
                found: Some(found),
                checked: vec![BrokerSource::Registry],
            });
        }
        let mut checked = vec![BrokerSource::Registry];
        let Some(live) = &self.live else {
            return Ok(BrokerVerification {
                found: None,
                checked,
            });
        };
        match live.find(query).await {
            Ok(found) => {
                checked.push(BrokerSource::VworldLive);
                Ok(BrokerVerification { found, checked })
            }
            Err(_) => Ok(BrokerVerification {
                found: None,
                checked,
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use catalog_domain::real_estate_broker::BrokerCredential;

    struct Fixed(Result<Option<BrokerMatch>, ()>);

    #[async_trait]
    impl BrokerRegister for Fixed {
        async fn find(
            &self,
            _: &BrokerVerifyQuery,
        ) -> Result<Option<BrokerMatch>, BrokerRegisterError> {
            self.0
                .clone()
                .map_err(|()| BrokerRegisterError("down".to_owned()))
        }
    }

    fn found() -> BrokerMatch {
        BrokerMatch {
            member_kind_name: Some("공인중개사".to_owned()),
            position_name: Some("대표".to_owned()),
            registration_no: "99999-2026-00001".to_owned(),
            office_name: None,
            office_status_name: None,
            data_reference_date: None,
        }
    }

    fn query() -> BrokerVerifyQuery {
        BrokerVerifyQuery {
            name_key: "홍길동".to_owned(),
            credential: BrokerCredential::License("202699999".to_owned()),
        }
    }

    fn verify(roster: Fixed, live: Option<Fixed>) -> VerifyRealEstateBroker {
        VerifyRealEstateBroker::new(
            Arc::new(roster),
            live.map(|live| Arc::new(live) as Arc<dyn BrokerRegister>),
        )
    }

    #[tokio::test]
    async fn the_roster_answers_first_and_the_live_register_only_on_a_miss() {
        let hit = verify(Fixed(Ok(Some(found()))), Some(Fixed(Err(()))))
            .execute(&query())
            .await;
        assert_eq!(
            hit.ok().map(|answer| answer.checked),
            Some(vec![BrokerSource::Registry])
        );

        let live = verify(Fixed(Ok(None)), Some(Fixed(Ok(Some(found())))))
            .execute(&query())
            .await;
        let live = live.ok();
        assert!(live.as_ref().is_some_and(|answer| answer.found.is_some()));
        assert_eq!(
            live.map(|answer| answer.checked),
            Some(vec![BrokerSource::Registry, BrokerSource::VworldLive])
        );
    }

    #[tokio::test]
    async fn a_live_register_that_cannot_answer_leaves_an_unconfirmed_miss() {
        for live in [None, Some(Fixed(Err(())))] {
            let answer = verify(Fixed(Ok(None)), live).execute(&query()).await.ok();
            assert_eq!(
                answer.map(|answer| (answer.found, answer.checked)),
                Some((None, vec![BrokerSource::Registry]))
            );
        }
        assert!(verify(Fixed(Err(())), None)
            .execute(&query())
            .await
            .is_err());
    }
}
