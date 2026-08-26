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

use std::collections::HashSet;
use std::sync::Arc;

use super::ordered_tests::table_with_struct_map_key;
use super::tests::make_v2_table_with_nested;
use super::{AddColumn, UpdateSchemaAction};
use crate::spec::{NestedField, PrimitiveType, Schema, Type};
use crate::table::Table;
use crate::transaction::Transaction;
use crate::transaction::action::TransactionAction;
use crate::{Error, ErrorKind, TableRequirement, TableUpdate};

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

#[tokio::test]
async fn renames_root_and_nested_columns_using_original_paths() {
    let table = make_v2_table_with_nested();
    let original_z = table
        .metadata()
        .current_schema()
        .field_by_name("z")
        .unwrap();
    let schema = updated_schema(
        &table,
        Transaction::new(&table)
            .update_schema()
            .rename_column("person", "profile")
            .rename_column("person.name", "full_name")
            .rename_column("z", "payload")
            .rename_column("z", "record"),
    )
    .await;

    assert_eq!(schema.field_by_name("profile").unwrap().id, 4);
    assert_eq!(schema.field_by_name("profile.full_name").unwrap().id, 5);
    let record = schema.field_by_name("record").unwrap();
    let mut expected = (**original_z).clone();
    expected.name = "record".to_string();
    assert_eq!(record.as_ref(), &expected);
    assert!(schema.field_by_name("payload").is_none());
}

#[tokio::test]
async fn rename_lookup_never_switches_to_pending_or_added_names() {
    let table = crate::transaction::tests::make_v2_table();

    let pending_name = commit_error(
        &table,
        Transaction::new(&table)
            .update_schema()
            .rename_column("z", "payload")
            .rename_column("payload", "record"),
    )
    .await;
    assert_eq!(
        pending_name.message(),
        "Cannot rename missing column: payload"
    );

    let added_name = commit_error(
        &table,
        Transaction::new(&table)
            .update_schema()
            .add_column(AddColumn::optional(
                "later",
                Type::Primitive(PrimitiveType::String),
            ))
            .rename_column("later", "renamed"),
    )
    .await;
    assert_eq!(added_name.message(), "Cannot rename missing column: later");

    let missing = commit_error(
        &table,
        Transaction::new(&table)
            .update_schema()
            .rename_column("", "renamed"),
    )
    .await;
    assert_eq!(missing.message(), "Invalid column name: (empty)");
}

#[tokio::test]
async fn no_op_renames_still_mark_updates_for_conflict_checks() {
    let table = crate::transaction::tests::make_v2_table();

    let mut no_op = Arc::new(
        Transaction::new(&table)
            .update_schema()
            .rename_column("z", "z"),
    )
    .commit(&table)
    .await
    .unwrap();
    assert!(no_op.take_updates().is_empty());
    assert!(no_op.take_requirements().is_empty());

    let rename_then_delete = commit_error(
        &table,
        Transaction::new(&table)
            .update_schema()
            .rename_column("z", "z")
            .delete_column("z"),
    )
    .await;
    assert!(
        rename_then_delete
            .message()
            .contains("column that has updates: z")
    );

    let nested = make_v2_table_with_nested();
    let delete_then_rename = commit_error(
        &nested,
        Transaction::new(&nested)
            .update_schema()
            .delete_column("person.name")
            .rename_column("person.name", "full_name"),
    )
    .await;
    assert_eq!(
        delete_then_rename.message(),
        "Cannot rename a column that will be deleted: name"
    );
}

#[tokio::test]
async fn rejects_sibling_name_collisions() {
    let table = make_v2_table_with_nested();
    for (name, new_name) in [("z", "y"), ("person.name", "age")] {
        let error = commit_error(
            &table,
            Transaction::new(&table)
                .update_schema()
                .rename_column(name, new_name),
        )
        .await;
        assert_eq!(error.kind(), ErrorKind::PreconditionFailed);
        assert_eq!(error.message(), "Cannot apply schema update");
    }
}

#[tokio::test]
async fn accepts_empty_and_dotted_leaf_names() {
    let table = make_v2_table_with_nested();
    let empty = updated_schema(
        &table,
        Transaction::new(&table)
            .update_schema()
            .rename_column("z", ""),
    )
    .await;
    assert_eq!(empty.field_by_id(3).unwrap().name, "");

    let dotted = updated_schema(
        &table,
        Transaction::new(&table)
            .update_schema()
            .rename_column("person.name", "full.name"),
    )
    .await;
    assert_eq!(dotted.field_by_id(5).unwrap().name, "full.name");
}

#[tokio::test]
async fn preserves_identifier_field_ids() {
    let table = crate::transaction::tests::make_v2_table();
    let schema = updated_schema(
        &table,
        Transaction::new(&table)
            .update_schema()
            .rename_column("x", "identifier"),
    )
    .await;

    assert_eq!(schema.field_by_name("identifier").unwrap().id, 1);
    assert_eq!(
        schema.identifier_field_ids().collect::<HashSet<_>>(),
        HashSet::from([1, 2])
    );
}

#[tokio::test]
async fn rejects_map_key_renames() {
    let table = make_v2_table_with_nested();
    let direct = commit_error(
        &table,
        Transaction::new(&table)
            .update_schema()
            .rename_column("props.key", "lookup"),
    )
    .await;
    assert!(direct.message().contains("Cannot update map keys"));

    let no_op = commit_error(
        &table,
        Transaction::new(&table)
            .update_schema()
            .rename_column("props.key", "key"),
    )
    .await;
    assert!(no_op.message().contains("Cannot update map keys"));

    let struct_key_table = table_with_struct_map_key();
    let nested = commit_error(
        &struct_key_table,
        Transaction::new(&struct_key_table)
            .update_schema()
            .rename_column("indexed.key.part", "renamed"),
    )
    .await;
    assert!(nested.message().contains("Cannot alter map keys"));
}

#[tokio::test]
async fn swaps_sibling_names_while_preserving_field_ids() {
    let table = make_v2_table_with_nested();
    let schema = updated_schema(
        &table,
        Transaction::new(&table)
            .update_schema()
            .rename_column("z", "person")
            .rename_column("person", "z"),
    )
    .await;

    assert_eq!(schema.field_by_name("person").unwrap().id, 3);
    assert_eq!(schema.field_by_name("z").unwrap().id, 4);
}

#[tokio::test]
async fn renames_under_deleted_ancestors_are_dropped() {
    let table = make_v2_table_with_nested();
    for action in [
        Transaction::new(&table)
            .update_schema()
            .delete_column("person")
            .rename_column("person.name", "full_name"),
        Transaction::new(&table)
            .update_schema()
            .rename_column("person.name", "full_name")
            .delete_column("person"),
    ] {
        let schema = updated_schema(&table, action).await;
        assert!(schema.field_by_name("person").is_none());
        assert!(schema.field_by_id(5).is_none());
    }
}

#[tokio::test]
async fn collection_pseudo_field_renames_are_structural_no_ops() {
    let table = make_v2_table_with_nested();
    let mut commit = Arc::new(
        Transaction::new(&table)
            .update_schema()
            .rename_column("tags.element", "item")
            .rename_column("props.value", "entry"),
    )
    .commit(&table)
    .await
    .unwrap();

    assert!(commit.take_updates().is_empty());
    assert!(commit.take_requirements().is_empty());
    assert_eq!(
        table
            .metadata()
            .current_schema()
            .field_by_name("tags.element")
            .unwrap()
            .name,
        "element"
    );
    assert_eq!(
        table
            .metadata()
            .current_schema()
            .field_by_name("props.value")
            .unwrap()
            .name,
        "value"
    );
}

#[tokio::test]
async fn replays_rename_against_refreshed_metadata() {
    let table = crate::transaction::tests::make_v2_table();
    let action = Arc::new(
        Transaction::new(&table)
            .update_schema()
            .rename_column("z", "payload"),
    );

    let mut initial_commit = action.clone().commit(&table).await.unwrap();
    let initial_schema = match initial_commit.take_updates().remove(0) {
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
    concurrent_fields.push(
        NestedField::optional(4, "concurrent", Type::Primitive(PrimitiveType::Boolean)).into(),
    );
    let concurrent_schema = Schema::builder()
        .with_fields(concurrent_fields)
        .with_identifier_field_ids(table.metadata().current_schema().identifier_field_ids())
        .build()
        .unwrap();
    let metadata = table
        .metadata()
        .clone()
        .into_builder(None)
        .add_schema(concurrent_schema)
        .unwrap()
        .set_current_schema(-1)
        .unwrap()
        .build()
        .unwrap()
        .metadata;
    let refreshed = table.with_metadata(Arc::new(metadata));

    let mut replayed = action.commit(&refreshed).await.unwrap();
    let schema = match replayed.take_updates().remove(0) {
        TableUpdate::AddSchema { schema } => schema,
        update => panic!("expected AddSchema, got {update:?}"),
    };
    assert_eq!(schema.field_by_name("payload").unwrap().id, 3);
    assert_eq!(schema.field_by_name("concurrent").unwrap().id, 4);
    assert_eq!(replayed.take_requirements(), vec![
        TableRequirement::UuidMatch {
            uuid: refreshed.metadata().uuid(),
        },
        TableRequirement::LastAssignedFieldIdMatch {
            last_assigned_field_id: 4,
        },
        TableRequirement::CurrentSchemaIdMatch {
            current_schema_id: refreshed.metadata().current_schema_id(),
        },
    ]);
}
