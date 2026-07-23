//! Package ownership smoke tests for the Lakehouse domain.

use lakehouse_domain::{
    industrial_complex_lakehouse_contracts, IndustrialComplexGoldPointer,
    LakehouseMaintenancePolicy, LakehouseStorageNamespace, SparkRunSummary,
};

#[test]
fn final_domain_package_owns_each_lakehouse_contract_family() {
    let _ = industrial_complex_lakehouse_contracts();
    let _ = std::mem::size_of::<IndustrialComplexGoldPointer>();
    let _ = std::mem::size_of::<LakehouseMaintenancePolicy>();
    let _ = std::mem::size_of::<LakehouseStorageNamespace>();
    let _ = std::mem::size_of::<SparkRunSummary>();
}
