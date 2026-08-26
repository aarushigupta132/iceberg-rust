// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::collections::HashMap;
use std::sync::Arc;

use super::tests::make_v2_table_with_nested;
use super::{AddColumn, UpdateSchemaAction};
use crate::TableUpdate;
use crate::spec::{PrimitiveType, Type};
use crate::table::Table;
use crate::transaction::Transaction;
use crate::transaction::action::TransactionAction;

const SUPPORTED_PREFIXES: [&str; 3] = [
    "write.metadata.metrics.column.",
    "write.parquet.bloom-filter-enabled.column.",
    "write.parquet.stats-enabled.column.",
];

fn with_properties(table: Table, properties: HashMap<String, String>) -> Table {
    let metadata = table
        .metadata()
        .clone()
        .into_builder(None)
        .set_properties(properties)
        .unwrap()
        .build()
        .unwrap()
        .metadata;
    table.with_metadata(Arc::new(metadata))
}

async fn updates(table: &Table, action: UpdateSchemaAction) -> Vec<TableUpdate> {
    let mut commit = Arc::new(action).commit(table).await.unwrap();
    commit.take_updates()
}

fn property_removals(updates: &[TableUpdate]) -> Option<&[String]> {
    updates.iter().find_map(|update| match update {
        TableUpdate::RemoveProperties { removals } => Some(removals.as_slice()),
        _ => None,
    })
}

fn apply_updates(table: &Table, updates: &[TableUpdate]) -> Table {
    let mut builder = table.metadata().clone().into_builder(None);
    for update in updates {
        builder = update.clone().apply(builder).unwrap();
    }
    table
        .clone()
        .with_metadata(Arc::new(builder.build().unwrap().metadata))
}

#[tokio::test]
async fn deletes_exact_supported_column_properties() {
    let mut properties = HashMap::from([
        ("unrelated".to_string(), "keep".to_string()),
        (
            "write.parquet.bloom-filter-fpp.column.z".to_string(),
            "0.01".to_string(),
        ),
    ]);
    for prefix in SUPPORTED_PREFIXES {
        properties.insert(format!("{prefix}z"), "root".to_string());
        properties.insert(format!("{prefix}person.name"), "nested".to_string());
        properties.insert(format!("{prefix}person.age"), "keep".to_string());
    }
    let table = with_properties(make_v2_table_with_nested(), properties);
    let action = Transaction::new(&table)
        .update_schema()
        .delete_column("z")
        .delete_column("person.name");
    let updates = updates(&table, action).await;

    let mut expected = vec![
        "write.metadata.metrics.column.person.name".to_string(),
        "write.metadata.metrics.column.z".to_string(),
        "write.parquet.bloom-filter-enabled.column.person.name".to_string(),
        "write.parquet.bloom-filter-enabled.column.z".to_string(),
        "write.parquet.stats-enabled.column.person.name".to_string(),
        "write.parquet.stats-enabled.column.z".to_string(),
    ];
    expected.sort();
    assert_eq!(property_removals(&updates), Some(expected.as_slice()));
    assert!(
        !property_removals(&updates)
            .unwrap()
            .iter()
            .any(|key| key.contains("bloom-filter-fpp") || key.ends_with("person.age"))
    );
}

#[tokio::test]
async fn skips_property_update_when_deleted_column_has_no_configuration() {
    let table = with_properties(
        make_v2_table_with_nested(),
        HashMap::from([(
            "write.metadata.metrics.column.person.age".to_string(),
            "full".to_string(),
        )]),
    );
    let action = Transaction::new(&table).update_schema().delete_column("z");
    let updates = updates(&table, action).await;

    assert_eq!(property_removals(&updates), None);
}

#[tokio::test]
async fn parent_deletion_only_removes_the_exact_parent_configuration() {
    let prefix = "write.metadata.metrics.column.";
    let table = with_properties(
        make_v2_table_with_nested(),
        HashMap::from([
            (format!("{prefix}person"), "full".to_string()),
            (format!("{prefix}person.name"), "counts".to_string()),
        ]),
    );
    let action = Transaction::new(&table)
        .update_schema()
        .delete_column("person");
    let updates = updates(&table, action).await;

    assert_eq!(
        property_removals(&updates),
        Some([format!("{prefix}person")].as_slice())
    );
}

#[tokio::test]
async fn same_name_replacement_drops_the_deleted_fields_configuration() {
    let prefix = "write.metadata.metrics.column.";
    let table = with_properties(
        make_v2_table_with_nested(),
        HashMap::from([(format!("{prefix}z"), "full".to_string())]),
    );
    let action = Transaction::new(&table)
        .update_schema()
        .delete_column("z")
        .add_column(AddColumn::optional(
            "z",
            Type::Primitive(PrimitiveType::String),
        ));
    let updates = updates(&table, action).await;

    assert_eq!(
        property_removals(&updates),
        Some([format!("{prefix}z")].as_slice())
    );
}

#[tokio::test]
async fn renames_exact_supported_root_and_nested_properties() {
    let unsupported_key = "write.parquet.bloom-filter-fpp.column.z";
    let mut properties = HashMap::from([(unsupported_key.to_string(), "0.01".to_string())]);
    for prefix in SUPPORTED_PREFIXES {
        properties.insert(format!("{prefix}z"), "root".to_string());
        properties.insert(format!("{prefix}person.name"), "nested".to_string());
    }
    let table = with_properties(make_v2_table_with_nested(), properties);
    let action = Transaction::new(&table)
        .update_schema()
        .rename_column("z", "payload")
        .rename_column("person", "profile")
        .rename_column("person.name", "full_name");
    let updates = updates(&table, action).await;
    let committed = apply_updates(&table, &updates);

    for prefix in SUPPORTED_PREFIXES {
        assert_eq!(
            committed
                .metadata()
                .properties()
                .get(&format!("{prefix}payload"))
                .map(String::as_str),
            Some("root")
        );
        assert_eq!(
            committed
                .metadata()
                .properties()
                .get(&format!("{prefix}profile.full_name"))
                .map(String::as_str),
            Some("nested")
        );
        assert!(
            !committed
                .metadata()
                .properties()
                .contains_key(&format!("{prefix}z"))
        );
        assert!(
            !committed
                .metadata()
                .properties()
                .contains_key(&format!("{prefix}person.name"))
        );
    }
    assert_eq!(
        committed
            .metadata()
            .properties()
            .get(unsupported_key)
            .map(String::as_str),
        Some("0.01")
    );
}

#[tokio::test]
async fn swaps_column_property_ownership() {
    let prefix = "write.metadata.metrics.column.";
    let table = with_properties(
        make_v2_table_with_nested(),
        HashMap::from([
            (format!("{prefix}z"), "z-value".to_string()),
            (format!("{prefix}person"), "person-value".to_string()),
        ]),
    );
    let action = Transaction::new(&table)
        .update_schema()
        .rename_column("z", "person")
        .rename_column("person", "z");
    let updates = updates(&table, action).await;
    let committed = apply_updates(&table, &updates);

    assert_eq!(
        committed
            .metadata()
            .properties()
            .get(&format!("{prefix}person"))
            .map(String::as_str),
        Some("z-value")
    );
    assert_eq!(
        committed
            .metadata()
            .properties()
            .get(&format!("{prefix}z"))
            .map(String::as_str),
        Some("person-value")
    );
}

#[tokio::test]
async fn rename_wins_when_the_destination_column_is_deleted() {
    let prefix = "write.metadata.metrics.column.";
    let table = with_properties(
        make_v2_table_with_nested(),
        HashMap::from([
            (format!("{prefix}person.name"), "source".to_string()),
            (format!("{prefix}person.age"), "destination".to_string()),
        ]),
    );
    let action = Transaction::new(&table)
        .update_schema()
        .delete_column("person.age")
        .rename_column("person.name", "age");
    let updates = updates(&table, action).await;

    assert_eq!(
        property_removals(&updates),
        Some(
            [
                format!("{prefix}person.age"),
                format!("{prefix}person.name"),
            ]
            .as_slice()
        )
    );
    let committed = apply_updates(&table, &updates);
    assert_eq!(
        committed
            .metadata()
            .properties()
            .get(&format!("{prefix}person.age"))
            .map(String::as_str),
        Some("source")
    );
    assert!(
        !committed
            .metadata()
            .properties()
            .contains_key(&format!("{prefix}person.name"))
    );
}

#[tokio::test]
async fn parent_rename_does_not_rewrite_descendant_properties() {
    let prefix = "write.metadata.metrics.column.";
    let table = with_properties(
        make_v2_table_with_nested(),
        HashMap::from([
            (format!("{prefix}person"), "parent".to_string()),
            (format!("{prefix}person.name"), "descendant".to_string()),
        ]),
    );
    let action = Transaction::new(&table)
        .update_schema()
        .rename_column("person", "profile");
    let updates = updates(&table, action).await;
    let committed = apply_updates(&table, &updates);

    assert_eq!(
        committed
            .metadata()
            .properties()
            .get(&format!("{prefix}profile"))
            .map(String::as_str),
        Some("parent")
    );
    assert_eq!(
        committed
            .metadata()
            .properties()
            .get(&format!("{prefix}person.name"))
            .map(String::as_str),
        Some("descendant")
    );
    assert!(
        !committed
            .metadata()
            .properties()
            .contains_key(&format!("{prefix}profile.name"))
    );
}
