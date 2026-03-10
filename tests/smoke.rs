use kalanjiyam::{intent, project_name};

#[test]
fn library_and_binary_contract_start_small() {
    assert_eq!(project_name(), "kalanjiyam");
    assert!(intent().starts_with("A distributed ACID-compliant"));
}
