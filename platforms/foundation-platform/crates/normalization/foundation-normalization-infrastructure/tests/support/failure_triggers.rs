use sqlx::PgPool;
use uuid::Uuid;

pub const FORCED_APPLICATION_INSERT_FAILURE: &str =
    "forced normalization application insert failure";
pub const FORCED_OUTBOX_FAILURE: &str = "forced normalization outbox failure";
pub const FORCED_PROPOSAL_UPDATE_FAILURE: &str = "forced normalization proposal update failure";

pub struct ProposalUpdateFailureTrigger {
    trigger_name: String,
    function_name: String,
}

impl ProposalUpdateFailureTrigger {
    pub async fn install(
        pool: &PgPool,
        proposal_id: Uuid,
        next_status: &str,
    ) -> Result<Self, sqlx::Error> {
        let suffix = Uuid::new_v4().simple().to_string();
        let trigger_name = format!("test_fail_normalization_update_{suffix}");
        let function_name = format!("test_fail_normalization_update_fn_{suffix}");
        let sql = format!(
            "CREATE FUNCTION catalog.{function_name}() RETURNS trigger
             LANGUAGE plpgsql AS $$
             BEGIN
                 IF NEW.id = '{proposal_id}'::uuid AND NEW.status = '{next_status}' THEN
                     IF NEW.status IN ('applied', 'rolled_back') AND NOT EXISTS (
                         SELECT 1
                         FROM catalog.normalization_application application
                         JOIN catalog.outbox_event outbox
                           ON outbox.event_id = application.outbox_event_id
                         JOIN catalog.industrial_complex canonical
                           ON canonical.id = application.target_id
                         WHERE application.proposal_id = NEW.id
                           AND application.target_kind = 'industrial_complex'
                           AND (
                               (NEW.status = 'applied' AND application.rollback_of IS NULL)
                               OR
                               (NEW.status = 'rolled_back' AND application.rollback_of IS NOT NULL)
                           )
                           AND canonical.name = application.after_snapshot->>'name'
                           AND canonical.area_m2 =
                               (application.after_snapshot->>'area_m2')::bigint
                           AND canonical.version =
                               (application.after_snapshot->>'version')::bigint
                     ) THEN
                         RAISE EXCEPTION
                             'normalization status failure reached before Catalog and ledger writes'
                             USING ERRCODE = 'P0001';
                     END IF;
                     RAISE EXCEPTION '{FORCED_PROPOSAL_UPDATE_FAILURE}' USING ERRCODE = 'P0001';
                 END IF;
                 RETURN NEW;
             END;
             $$;
             CREATE TRIGGER {trigger_name}
             BEFORE UPDATE ON catalog.normalization_proposal
             FOR EACH ROW EXECUTE FUNCTION catalog.{function_name}();"
        );
        sqlx::raw_sql(sql.as_str()).execute(pool).await?;
        Ok(Self {
            trigger_name,
            function_name,
        })
    }

    pub async fn remove(self, pool: &PgPool) -> Result<(), sqlx::Error> {
        let sql = format!(
            "DROP TRIGGER IF EXISTS {} ON catalog.normalization_proposal;
             DROP FUNCTION IF EXISTS catalog.{}();",
            self.trigger_name, self.function_name
        );
        sqlx::raw_sql(sql.as_str()).execute(pool).await?;
        Ok(())
    }
}

pub struct ApplicationInsertFailureTrigger {
    trigger_name: String,
    function_name: String,
}

impl ApplicationInsertFailureTrigger {
    pub async fn install(pool: &PgPool, application_id: Uuid) -> Result<Self, sqlx::Error> {
        let suffix = Uuid::new_v4().simple().to_string();
        let trigger_name = format!("test_fail_normalization_application_{suffix}");
        let function_name = format!("test_fail_normalization_application_fn_{suffix}");
        let sql = format!(
            "CREATE FUNCTION catalog.{function_name}() RETURNS trigger
             LANGUAGE plpgsql AS $$
             BEGIN
                 IF NEW.id = '{application_id}'::uuid THEN
                     IF NEW.target_kind <> 'industrial_complex'
                        OR NEW.target_id IS NULL
                        OR NEW.outbox_event_id IS NULL
                        OR NOT EXISTS (
                            SELECT 1
                            FROM catalog.outbox_event outbox
                            WHERE outbox.event_id = NEW.outbox_event_id
                        )
                        OR NOT EXISTS (
                            SELECT 1
                            FROM catalog.industrial_complex canonical
                            WHERE canonical.id = NEW.target_id
                              AND canonical.name = NEW.after_snapshot->>'name'
                              AND canonical.area_m2 =
                                  (NEW.after_snapshot->>'area_m2')::bigint
                              AND canonical.version =
                                  (NEW.after_snapshot->>'version')::bigint
                        ) THEN
                         RAISE EXCEPTION
                             'normalization ledger failure reached before Catalog writes'
                             USING ERRCODE = 'P0001';
                     END IF;
                     RAISE EXCEPTION '{FORCED_APPLICATION_INSERT_FAILURE}' USING ERRCODE = 'P0001';
                 END IF;
                 RETURN NEW;
             END;
             $$;
             CREATE TRIGGER {trigger_name}
             BEFORE INSERT ON catalog.normalization_application
             FOR EACH ROW EXECUTE FUNCTION catalog.{function_name}();"
        );
        sqlx::raw_sql(sql.as_str()).execute(pool).await?;
        Ok(Self {
            trigger_name,
            function_name,
        })
    }

    pub async fn remove(self, pool: &PgPool) -> Result<(), sqlx::Error> {
        let sql = format!(
            "DROP TRIGGER IF EXISTS {} ON catalog.normalization_application;
             DROP FUNCTION IF EXISTS catalog.{}();",
            self.trigger_name, self.function_name
        );
        sqlx::raw_sql(sql.as_str()).execute(pool).await?;
        Ok(())
    }
}

pub struct OutboxFailureTrigger {
    trigger_name: String,
    function_name: String,
}

impl OutboxFailureTrigger {
    pub async fn install(
        pool: &PgPool,
        payload_key: &str,
        payload_value: &str,
    ) -> Result<Self, sqlx::Error> {
        let suffix = Uuid::new_v4().simple().to_string();
        let trigger_name = format!("test_fail_normalization_outbox_{suffix}");
        let function_name = format!("test_fail_normalization_outbox_fn_{suffix}");
        let sql = format!(
            "CREATE FUNCTION catalog.{function_name}() RETURNS trigger
             LANGUAGE plpgsql AS $$
             BEGIN
                 IF NEW.payload->>'{payload_key}' = '{payload_value}' THEN
                     RAISE EXCEPTION '{FORCED_OUTBOX_FAILURE}' USING ERRCODE = 'P0001';
                 END IF;
                 RETURN NEW;
             END;
             $$;
             CREATE TRIGGER {trigger_name}
             BEFORE INSERT ON catalog.outbox_event
             FOR EACH ROW EXECUTE FUNCTION catalog.{function_name}();"
        );
        sqlx::raw_sql(sql.as_str()).execute(pool).await?;
        Ok(Self {
            trigger_name,
            function_name,
        })
    }

    pub async fn remove(self, pool: &PgPool) -> Result<(), sqlx::Error> {
        let sql = format!(
            "DROP TRIGGER IF EXISTS {} ON catalog.outbox_event;
             DROP FUNCTION IF EXISTS catalog.{}();",
            self.trigger_name, self.function_name
        );
        sqlx::raw_sql(sql.as_str()).execute(pool).await?;
        Ok(())
    }
}
