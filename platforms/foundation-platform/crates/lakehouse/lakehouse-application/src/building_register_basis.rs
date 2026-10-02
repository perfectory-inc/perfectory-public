//! 기본개요의 명시적 상위 대장 관계. 이름이나 필지로 원천 관계를 덮어쓰지 않는다.

use std::collections::HashMap;

use crate::building_register_unit_silver_plan::BuildingRegisterUnitSilverPlanError;

#[derive(Debug, Eq, PartialEq)]
struct BasisEntry {
    parent_pk: Option<Box<str>>,
    kind: Box<str>,
    source_line: u64,
    conflicting: bool,
}

/// 한 호실의 명시적 상위 관계를 확인한 결과.
#[derive(Debug, Eq, PartialEq)]
pub enum BuildingRegisterUnitParent<'a> {
    /// 원천에 상위 관계가 없다. 사유를 보존하고 소속은 미확정으로 둔다.
    Absent(&'static str),
    /// 종류 4 → 종류 3인 고유 원천 관계. 표제부 존재·종류 검사는 호출자가 이어서 한다.
    Linked(&'a str),
    /// 명시적 관계의 오류. 이름 추론으로 대체해서는 안 된다.
    Rejected(&'static str),
}

/// 같은 입력 집합의 기본개요를 PK로 조회하는 인덱스. PK는 불투명 문자열이다.
#[derive(Debug)]
pub struct BuildingRegisterBasisIndex {
    entries: HashMap<Box<str>, BasisEntry>,
    bronze_object_key: String,
    input_sha256: String,
}

impl BuildingRegisterBasisIndex {
    /// 검산한 입력 집합과 기본개요 원본을 고정한다.
    ///
    /// # Errors
    /// 원본 키나 SHA-256이 유효하지 않으면 거부한다.
    pub fn new(
        bronze_object_key: &str,
        input_sha256: &str,
    ) -> Result<Self, BuildingRegisterUnitSilverPlanError> {
        if bronze_object_key.trim().is_empty()
            || input_sha256.len() != 64
            || !input_sha256
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return Err(BuildingRegisterUnitSilverPlanError::InvalidInput(
                "basis source requires a Bronze key and lowercase SHA-256".to_owned(),
            ));
        }
        Ok(Self {
            entries: HashMap::new(),
            bronze_object_key: bronze_object_key.to_owned(),
            input_sha256: input_sha256.to_owned(),
        })
    }

    /// `mart_djy_01`의 0=자신 PK, 1=부모 PK, 4=대장종류를 보존한다.
    /// 동일 관계 중복은 원본의 가장 작은 행 번호를 사용한다.
    ///
    /// # Errors
    /// 잘린 행, 빈 PK, 원본 행 번호 0은 거부한다.
    pub fn insert_hub_line(
        &mut self,
        line: &str,
        source_line: u64,
    ) -> Result<(), BuildingRegisterUnitSilverPlanError> {
        let fields: Vec<_> = line.split('|').collect();
        if fields.len() < 30 || fields[0].trim().is_empty() || source_line == 0 {
            return Err(BuildingRegisterUnitSilverPlanError::InvalidInput(format!(
                "invalid basic-outline row at line {source_line}"
            )));
        }
        let parent = fields[1].trim();
        let incoming = BasisEntry {
            parent_pk: (!parent.is_empty()).then(|| parent.into()),
            kind: fields[4].trim().into(),
            source_line,
            conflicting: false,
        };
        self.entries
            .entry(fields[0].trim().into())
            .and_modify(|current| {
                current.conflicting |=
                    current.parent_pk != incoming.parent_pk || current.kind != incoming.kind;
                current.source_line = current.source_line.min(source_line);
            })
            .or_insert(incoming);
        Ok(())
    }

    /// 고유한 종류 4 → 종류 3 관계만 반환한다.
    #[must_use]
    pub fn resolve_unit(&self, unit_pk: &str) -> BuildingRegisterUnitParent<'_> {
        use BuildingRegisterUnitParent::{Absent, Linked, Rejected};
        let Some(unit) = self.entries.get(unit_pk) else {
            return Absent("basis_unit_missing");
        };
        if unit.conflicting {
            return Rejected("basis_unit_conflict");
        }
        if unit.kind.as_ref() != "4" {
            return Rejected("basis_unit_kind_mismatch");
        }
        let Some(parent_pk) = unit.parent_pk.as_deref() else {
            return Absent("basis_parent_key_missing");
        };
        if let Err(reason) = validate_parent_identity(unit_pk, parent_pk) {
            return Rejected(reason);
        }
        let Some(parent) = self.entries.get(parent_pk) else {
            return Rejected("basis_parent_missing");
        };
        if let Err(reason) = validate_parent(parent) {
            return Rejected(reason);
        }
        Linked(parent_pk)
    }

    /// 호실 기본개요를 진술한 원본 행. 충돌도 원본 파일에서 다시 조사할 수 있다.
    #[must_use]
    pub fn source_record_id(&self, unit_pk: &str) -> Option<String> {
        self.entries
            .get(unit_pk)
            .map(|entry| format!("{}#line-{:06}", self.bronze_object_key, entry.source_line))
    }

    /// 세 입력의 객체 키·크기·내용 체크섬으로 계산한 digest.
    #[must_use]
    pub fn input_sha256(&self) -> &str {
        &self.input_sha256
    }
}

fn validate_parent_identity(unit_pk: &str, parent_pk: &str) -> Result<(), &'static str> {
    if unit_pk == parent_pk {
        return Err("building_link_self_reference");
    }
    Ok(())
}

fn validate_parent(parent: &BasisEntry) -> Result<(), &'static str> {
    if parent.conflicting {
        return Err("basis_parent_conflict");
    }
    if parent.kind.as_ref() != "3" {
        return Err("basis_parent_kind_mismatch");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn line(pk: &str, parent: &str, kind: &str) -> String {
        let mut fields = vec![""; 30];
        fields[0] = pk;
        fields[1] = parent;
        fields[4] = kind;
        fields.join("|")
    }

    fn index() -> Result<BuildingRegisterBasisIndex, BuildingRegisterUnitSilverPlanError> {
        BuildingRegisterBasisIndex::new("bronze/basis.zip", &"a".repeat(64))
    }

    #[test]
    fn opaque_parent_keys_need_no_name_or_parcel_match(
    ) -> Result<(), BuildingRegisterUnitSilverPlanError> {
        let mut index = index()?;
        index.insert_hub_line(&line("unit-001", "building-002", "4"), 12)?;
        index.insert_hub_line(&line("building-002", "", "3"), 5)?;
        assert_eq!(
            index.resolve_unit("unit-001"),
            BuildingRegisterUnitParent::Linked("building-002")
        );
        assert_eq!(
            index.source_record_id("unit-001").as_deref(),
            Some("bronze/basis.zip#line-000012")
        );
        Ok(())
    }

    #[test]
    fn absent_parent_facts_carry_distinct_reasons(
    ) -> Result<(), BuildingRegisterUnitSilverPlanError> {
        let mut index = index()?;
        assert_eq!(
            index.resolve_unit("missing"),
            BuildingRegisterUnitParent::Absent("basis_unit_missing")
        );
        index.insert_hub_line(&line("blank", "", "4"), 1)?;
        assert_eq!(
            index.resolve_unit("blank"),
            BuildingRegisterUnitParent::Absent("basis_parent_key_missing")
        );
        for (pk, parent, kind, reason) in [
            ("wrong-child", "", "3", "basis_unit_kind_mismatch"),
            ("self", "self", "4", "building_link_self_reference"),
            ("orphan", "missing", "4", "basis_parent_missing"),
            ("wrong-parent", "blank", "4", "basis_parent_kind_mismatch"),
        ] {
            index.insert_hub_line(&line(pk, parent, kind), 2)?;
            assert_eq!(
                index.resolve_unit(pk),
                BuildingRegisterUnitParent::Rejected(reason)
            );
        }
        Ok(())
    }

    #[test]
    fn duplicate_conflicts_are_permanent_and_order_independent(
    ) -> Result<(), BuildingRegisterUnitSilverPlanError> {
        for reverse in [false, true] {
            let mut index = index()?;
            let mut observations = [("u", "a", "4", 8), ("u", "b", "4", 3), ("u", "a", "4", 9)];
            if reverse {
                observations.reverse();
            }
            for (pk, parent, kind, number) in observations {
                index.insert_hub_line(&line(pk, parent, kind), number)?;
            }
            assert_eq!(
                index.resolve_unit("u"),
                BuildingRegisterUnitParent::Rejected("basis_unit_conflict")
            );
            assert_eq!(
                index.source_record_id("u").as_deref(),
                Some("bronze/basis.zip#line-000003")
            );
        }
        let mut index = index()?;
        index.insert_hub_line(&line("u", "a", "4"), 5)?;
        index.insert_hub_line(&line("u", "a", "4"), 2)?;
        index.insert_hub_line(&line("a", "", "3"), 1)?;
        assert_eq!(
            index.resolve_unit("u"),
            BuildingRegisterUnitParent::Linked("a")
        );
        index.insert_hub_line(&line("a", "", "2"), 6)?;
        index.insert_hub_line(&line("a", "", "3"), 7)?;
        assert_eq!(
            index.resolve_unit("u"),
            BuildingRegisterUnitParent::Rejected("basis_parent_conflict")
        );
        Ok(())
    }

    #[test]
    fn malformed_input_is_not_an_absent_relationship(
    ) -> Result<(), BuildingRegisterUnitSilverPlanError> {
        let mut index = index()?;
        assert!(index.insert_hub_line("u|a|4", 1).is_err());
        assert!(index.insert_hub_line(&line("", "a", "4"), 1).is_err());
        assert!(index.insert_hub_line(&line("u", "a", "4"), 0).is_err());
        Ok(())
    }
}
