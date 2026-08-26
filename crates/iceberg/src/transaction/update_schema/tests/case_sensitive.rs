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
use crate::spec::{ListType, NestedField, NestedFieldRef, PrimitiveType, Schema, StructType, Type};
use crate::table::Table;
use crate::transaction::Transaction;
use crate::transaction::action::TransactionAction;
use crate::{Error, ErrorKind, TableUpdate};

const COLUMN_PROPERTY_PREFIXES: [&str; 3] = [
    "write.metadata.metrics.column.",
    "write.parquet.bloom-filter-enabled.column.",
    "write.parquet.stats-enabled.column.",
];

async fn updated_schema(table: &Table, action: UpdateSchemaAction) -> Schema {
    let mut commit = Arc::new(action).commit(table).await.unwrap();
    match commit.take_updates().remove(0) {
        TableUpdate::AddSchema { schema } => schema,
        update => panic!("expected AddSchema, got {update:?}"),
    }
}

async fn commit_error(table: &Table, action: UpdateSchemaAction) -> Error {
    match Arc::new(action).commit(table).await {
        Err(error) => error,
        Ok(_) => panic!("schema update should fail"),
    }
}

fn replace_current_schema(table: &Table, schema: Schema) -> Table {
    let metadata = table
        .metadata()
        .clone()
        .into_builder(None)
        .add_schema(schema)
        .unwrap()
        .set_current_schema(-1)
        .unwrap()
        .build()
        .unwrap()
        .metadata;
    table.clone().with_metadata(Arc::new(metadata))
}

fn with_extra_fields(table: &Table, fields: impl IntoIterator<Item = NestedFieldRef>) -> Table {
    let mut all_fields = table
        .metadata()
        .current_schema()
        .as_struct()
        .fields()
        .to_vec();
    all_fields.extend(fields);
    let schema = Schema::builder()
        .with_fields(all_fields)
        .with_identifier_field_ids(table.metadata().current_schema().identifier_field_ids())
        .build()
        .unwrap();
    replace_current_schema(table, schema)
}

fn with_properties(table: &Table, properties: HashMap<String, String>) -> Table {
    let metadata = table
        .metadata()
        .clone()
        .into_builder(None)
        .set_properties(properties)
        .unwrap()
        .build()
        .unwrap()
        .metadata;
    table.clone().with_metadata(Arc::new(metadata))
}

#[tokio::test]
async fn defaults_to_case_sensitive_resolution() {
    let table = crate::transaction::tests::make_v2_table();
    let error = commit_error(
        &table,
        Transaction::new(&table)
            .update_schema()
            .rename_column("Z", "payload"),
    )
    .await;

    assert_eq!(error.kind(), ErrorKind::PreconditionFailed);
    assert_eq!(error.message(), "Cannot rename missing column: Z");
}

#[tokio::test]
async fn resolves_existing_columns_case_insensitively() {
    let table = make_v2_table_with_nested();
    let schema = updated_schema(
        &table,
        Transaction::new(&table)
            .update_schema()
            .case_sensitive(false)
            .add_column(
                AddColumn::builder()
                    .parent("PERSON")
                    .name("email")
                    .field_type(Type::Primitive(PrimitiveType::String))
                    .build(),
            )
            .delete_column("TAGS")
            .rename_column("PROPS.DATA", "payload"),
    )
    .await;

    assert!(schema.field_by_name("person.email").is_some());
    assert!(schema.field_by_name("tags").is_none());
    assert_eq!(schema.field_by_id(14).unwrap().name, "payload");
}

#[tokio::test]
async fn applies_case_sensitivity_toggles_in_order() {
    let table = crate::transaction::tests::make_v2_table();
    let schema = updated_schema(
        &table,
        Transaction::new(&table)
            .update_schema()
            .case_sensitive(false)
            .rename_column("Z", "payload")
            .case_sensitive(true)
            .rename_column("y", "coordinate"),
    )
    .await;
    assert!(schema.field_by_name("payload").is_some());
    assert!(schema.field_by_name("coordinate").is_some());

    let error = commit_error(
        &table,
        Transaction::new(&table)
            .update_schema()
            .case_sensitive(false)
            .case_sensitive(true)
            .rename_column("Z", "payload"),
    )
    .await;
    assert_eq!(error.message(), "Cannot rename missing column: Z");
}

#[tokio::test]
async fn applies_case_insensitive_conflict_checks() {
    let table = make_v2_table_with_nested();
    let root_error = commit_error(
        &table,
        Transaction::new(&table)
            .update_schema()
            .case_sensitive(false)
            .add_column(AddColumn::optional(
                "Z",
                Type::Primitive(PrimitiveType::Boolean),
            )),
    )
    .await;
    assert_eq!(
        root_error.message(),
        "Cannot add column, name already exists: Z"
    );

    let nested_error = commit_error(
        &table,
        Transaction::new(&table)
            .update_schema()
            .case_sensitive(false)
            .add_column(
                AddColumn::builder()
                    .parent("PERSON")
                    .name("NAME")
                    .field_type(Type::Primitive(PrimitiveType::Boolean))
                    .build(),
            ),
    )
    .await;
    assert_eq!(
        nested_error.message(),
        "Cannot add column, name already exists: PERSON.NAME"
    );
}

#[tokio::test]
async fn rejects_unrelated_case_insensitive_name_collisions() {
    let base = crate::transaction::tests::make_v2_table();
    let table = with_extra_fields(&base, [
        NestedField::optional(
            4,
            "items",
            Type::List(ListType {
                element_field: NestedField::list_element(
                    5,
                    Type::Struct(StructType::new(vec![
                        NestedField::optional(6, "Code", Type::Primitive(PrimitiveType::String))
                            .into(),
                    ])),
                    false,
                )
                .into(),
            }),
        )
        .into(),
        NestedField::optional(7, "ITEMS.code", Type::Primitive(PrimitiveType::String)).into(),
        NestedField::optional(8, "unrelated", Type::Primitive(PrimitiveType::String)).into(),
    ]);

    let error = commit_error(
        &table,
        Transaction::new(&table)
            .update_schema()
            .case_sensitive(false)
            .rename_column("UNRELATED", "changed"),
    )
    .await;

    assert_eq!(error.kind(), ErrorKind::PreconditionFailed);
    assert!(
        error
            .message()
            .contains("multiple fields match: items.code")
    );
}

#[tokio::test]
async fn validates_the_final_schema_using_the_final_mode() {
    let table = with_extra_fields(&crate::transaction::tests::make_v2_table(), [
        NestedField::optional(4, "Foo", Type::Primitive(PrimitiveType::String)).into(),
        NestedField::optional(5, "bar", Type::Primitive(PrimitiveType::String)).into(),
    ]);

    let error = commit_error(
        &table,
        Transaction::new(&table)
            .update_schema()
            .case_sensitive(false)
            .rename_column("BAR", "foo"),
    )
    .await;
    assert_eq!(error.kind(), ErrorKind::PreconditionFailed);
    assert!(error.message().contains("multiple fields match: foo"));

    let schema = updated_schema(
        &table,
        Transaction::new(&table)
            .update_schema()
            .case_sensitive(false)
            .rename_column("BAR", "foo")
            .case_sensitive(true),
    )
    .await;
    assert_eq!(schema.field_by_name("Foo").unwrap().id, 4);
    assert_eq!(schema.field_by_name("foo").unwrap().id, 5);
}

#[tokio::test]
async fn final_mode_controls_case_collisions_between_pending_additions() {
    let table = crate::transaction::tests::make_v2_table();
    let additions = |table: &Table| {
        Transaction::new(table)
            .update_schema()
            .case_sensitive(false)
            .add_column(AddColumn::optional(
                "NewColumn",
                Type::Primitive(PrimitiveType::String),
            ))
            .add_column(AddColumn::optional(
                "newcolumn",
                Type::Primitive(PrimitiveType::String),
            ))
    };

    let error = commit_error(&table, additions(&table)).await;
    assert_eq!(error.kind(), ErrorKind::PreconditionFailed);
    assert!(error.message().contains("multiple fields match: newcolumn"));

    let schema = updated_schema(&table, additions(&table).case_sensitive(true)).await;
    assert_eq!(schema.field_by_name("NewColumn").unwrap().id, 4);
    assert_eq!(schema.field_by_name("newcolumn").unwrap().id, 5);
}

#[tokio::test]
async fn sensitivity_only_action_is_a_no_op() {
    let table = crate::transaction::tests::make_v2_table();
    let mut commit = Arc::new(
        Transaction::new(&table)
            .update_schema()
            .case_sensitive(false),
    )
    .commit(&table)
    .await
    .unwrap();

    assert!(commit.take_updates().is_empty());
    assert!(commit.take_requirements().is_empty());
}

#[tokio::test]
async fn sensitivity_only_action_validates_an_ambiguous_schema() {
    let table = with_extra_fields(&crate::transaction::tests::make_v2_table(), [
        NestedField::optional(4, "Foo", Type::Primitive(PrimitiveType::String)).into(),
        NestedField::optional(5, "foo", Type::Primitive(PrimitiveType::String)).into(),
    ]);
    let error = commit_error(
        &table,
        Transaction::new(&table)
            .update_schema()
            .case_sensitive(false),
    )
    .await;

    assert_eq!(error.kind(), ErrorKind::PreconditionFailed);
    assert!(error.message().contains("multiple fields match: foo"));
}

#[tokio::test]
async fn replays_case_sensitivity_against_refreshed_metadata() {
    let table = crate::transaction::tests::make_v2_table();
    let action = Arc::new(
        Transaction::new(&table)
            .update_schema()
            .case_sensitive(false)
            .rename_column("Z", "payload"),
    );

    let mut initial = action.clone().commit(&table).await.unwrap();
    let initial_schema = match initial.take_updates().remove(0) {
        TableUpdate::AddSchema { schema } => schema,
        update => panic!("expected AddSchema, got {update:?}"),
    };
    assert_eq!(initial_schema.field_by_name("payload").unwrap().id, 3);

    let mut concurrent_fields = table
        .metadata()
        .current_schema()
        .as_struct()
        .fields()
        .to_vec();
    let mut renamed_concurrently = (*concurrent_fields[2]).clone();
    renamed_concurrently.name = "Z".to_string();
    concurrent_fields[2] = Arc::new(renamed_concurrently);
    concurrent_fields.push(
        NestedField::optional(4, "concurrent", Type::Primitive(PrimitiveType::Boolean)).into(),
    );
    let concurrent_schema = Schema::builder()
        .with_fields(concurrent_fields)
        .with_identifier_field_ids(table.metadata().current_schema().identifier_field_ids())
        .build()
        .unwrap();
    let refreshed = replace_current_schema(&table, concurrent_schema);

    let mut replayed = action.commit(&refreshed).await.unwrap();
    let replayed_schema = match replayed.take_updates().remove(0) {
        TableUpdate::AddSchema { schema } => schema,
        update => panic!("expected AddSchema, got {update:?}"),
    };
    assert_eq!(replayed_schema.field_by_name("payload").unwrap().id, 3);
    assert_eq!(replayed_schema.field_by_name("concurrent").unwrap().id, 4);
}

#[tokio::test]
async fn replay_rejects_a_new_unrelated_name_collision() {
    let table = crate::transaction::tests::make_v2_table();
    let action = Arc::new(
        Transaction::new(&table)
            .update_schema()
            .case_sensitive(false)
            .rename_column("Z", "payload"),
    );
    action.clone().commit(&table).await.unwrap();

    let refreshed = with_extra_fields(&table, [
        NestedField::optional(4, "Concurrent", Type::Primitive(PrimitiveType::String)).into(),
        NestedField::optional(5, "concurrent", Type::Primitive(PrimitiveType::String)).into(),
    ]);
    let error = match action.commit(&refreshed).await {
        Err(error) => error,
        Ok(_) => panic!("replayed schema update should fail"),
    };

    assert_eq!(error.kind(), ErrorKind::PreconditionFailed);
    assert!(
        error
            .message()
            .contains("multiple fields match: concurrent")
    );
}

#[tokio::test]
async fn migrates_canonical_column_properties_for_case_insensitive_selectors() {
    let mut properties = HashMap::new();
    for prefix in COLUMN_PROPERTY_PREFIXES {
        properties.insert(format!("{prefix}z"), "configured".to_string());
    }
    let table = with_properties(&crate::transaction::tests::make_v2_table(), properties);
    let mut commit = Arc::new(
        Transaction::new(&table)
            .update_schema()
            .case_sensitive(false)
            .rename_column("Z", "payload"),
    )
    .commit(&table)
    .await
    .unwrap();
    let updates = commit.take_updates();

    let removals = updates.iter().find_map(|update| match update {
        TableUpdate::RemoveProperties { removals } => Some(removals),
        _ => None,
    });
    let property_updates = updates.iter().find_map(|update| match update {
        TableUpdate::SetProperties { updates } => Some(updates),
        _ => None,
    });
    for prefix in COLUMN_PROPERTY_PREFIXES {
        assert!(
            removals.unwrap().contains(&format!("{prefix}z")),
            "the canonical source property should be removed"
        );
        assert_eq!(
            property_updates
                .unwrap()
                .get(&format!("{prefix}payload"))
                .map(String::as_str),
            Some("configured")
        );
    }
}
