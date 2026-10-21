use std::collections::BTreeMap;

use dbt_adapter::relation::RelationObject;
use dbt_adapter_core::AdapterType;
use dbt_schemas::dbt_types::RelationType;
use minijinja::Value;

use crate::macro_test_harness::MacroTestHarness;

fn harness() -> MacroTestHarness {
    MacroTestHarness::for_adapter(AdapterType::RedisAdbc)
        .load_all_macros()
        .with_stub_functions()
        .build()
        .expect("Redis harness should build")
}

#[test]
fn temporary_relations_are_unqualified() {
    let harness = harness();
    let relation = harness.relation("redis", "smoke_schema", "trips", Some(RelationType::Table));
    let rendered = harness
        .render(
            "{{ make_temp_relation(base_relation) }}",
            BTreeMap::from([
                ("base_relation", RelationObject::new(relation).into_value()),
                (
                    "model",
                    Value::from_serialize(BTreeMap::from([("batch", Value::from(()))])),
                ),
            ]),
        )
        .expect("temporary relation should render");
    assert_eq!(rendered.trim(), "\"trips__dbt_tmp\"");
}

#[test]
fn limit_wraps_queries_with_existing_limits() {
    let harness = harness();
    let rendered = harness
        .render(
            "{{ get_limit_subquery_sql('select 1 limit 10', 2) }}",
            BTreeMap::<String, Value>::new(),
        )
        .expect("limit should render");
    assert_eq!(
        rendered.trim(),
        "select * from (select 1 limit 10) as dbt_limit_subq limit 2"
    );
    let rendered = harness
        .render(
            "{{ get_limit_subquery_sql('select 1 limit 10', none) }}",
            BTreeMap::<String, Value>::new(),
        )
        .expect("unlimited query should render");
    assert_eq!(rendered.trim(), "select 1 limit 10");
}

#[test]
fn driver_binding_and_timestamp_syntax_dispatches() {
    let rendered = harness()
        .render(
            "{{ get_binding_char() }}|{{ current_timestamp() }}|{{ snapshot_get_time() }}",
            BTreeMap::<String, Value>::new(),
        )
        .expect("Redis SQL syntax should dispatch");
    assert_eq!(rendered.trim(), "%s|current_timestamp|localtimestamp");
}
