use kalanjiyam::{intent, project_name};

#[test]
fn library_contract_starts_small() {
    assert_eq!(project_name(), "kalanjiyam");
    assert!(intent().starts_with("A distributed ACID-compliant"));
}
