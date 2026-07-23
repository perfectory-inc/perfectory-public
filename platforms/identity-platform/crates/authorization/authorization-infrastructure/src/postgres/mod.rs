//! PostgreSQL authorization adapters.

mod effective_role_reader;
mod identity_bootstrap_uow;
mod role_grant_uow;

fn is_unique_constraint_violation(error: &sqlx::Error, constraint: &str) -> bool {
    let sqlx::Error::Database(database_error) = error else {
        return false;
    };
    database_error.code().as_deref() == Some("23505")
        && database_error.constraint() == Some(constraint)
}

fn is_foreign_key_constraint_violation(error: &sqlx::Error, constraint: &str) -> bool {
    let sqlx::Error::Database(database_error) = error else {
        return false;
    };
    database_error.code().as_deref() == Some("23503")
        && database_error.constraint() == Some(constraint)
}

pub use effective_role_reader::PgEffectiveRoleReader;
pub use identity_bootstrap_uow::PgIdentityBootstrapUnitOfWork;
pub use role_grant_uow::PgRoleGrantUnitOfWork;

#[cfg(test)]
pub(super) fn test_database_error(code: &'static str, constraint: &'static str) -> sqlx::Error {
    use std::borrow::Cow;
    use std::error::Error;
    use std::fmt::{Display, Formatter};

    use sqlx::error::{DatabaseError, ErrorKind};

    #[derive(Debug)]
    struct TestDatabaseError {
        code: &'static str,
        constraint: &'static str,
    }

    impl Display for TestDatabaseError {
        fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("test database error")
        }
    }

    impl Error for TestDatabaseError {}

    impl DatabaseError for TestDatabaseError {
        fn message(&self) -> &'static str {
            "test database error"
        }

        fn code(&self) -> Option<Cow<'_, str>> {
            Some(Cow::Borrowed(self.code))
        }

        fn as_error(&self) -> &(dyn Error + Send + Sync + 'static) {
            self
        }

        fn as_error_mut(&mut self) -> &mut (dyn Error + Send + Sync + 'static) {
            self
        }

        fn into_error(self: Box<Self>) -> Box<dyn Error + Send + Sync + 'static> {
            self
        }

        fn constraint(&self) -> Option<&str> {
            Some(self.constraint)
        }

        fn kind(&self) -> ErrorKind {
            if self.code == "23505" {
                ErrorKind::UniqueViolation
            } else {
                ErrorKind::Other
            }
        }
    }

    sqlx::Error::Database(Box::new(TestDatabaseError { code, constraint }))
}
