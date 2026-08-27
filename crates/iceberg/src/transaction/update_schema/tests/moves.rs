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

use std::sync::Arc;

use super::ordered_tests::table_with_struct_map_key;
use super::tests::make_v2_table_with_nested;
use super::{AddColumn, UpdateSchemaAction};
use crate::spec::{NestedField, PrimitiveType, Schema, Type};
use crate::table::Table;
use crate::transaction::Transaction;
use crate::transaction::action::TransactionAction;
use crate::{Error, ErrorKind, TableUpdate};

fn update(table: &Table) -> UpdateSchemaAction {
    Transaction::new(table).update_schema()
}

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

async fn assert_no_op(table: &Table, action: Arc<UpdateSchemaAction>) {
    let mut commit = action.commit(table).await.unwrap();
    assert!(commit.take_updates().is_empty());
    assert!(commit.take_requirements().is_empty());
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

fn root_names(schema: &Schema) -> Vec<&str> {
    schema
        .as_struct()
        .fields()
        .iter()
        .map(|field| field.name.as_str())
        .collect()
}

fn struct_names<'a>(schema: &'a Schema, name: &str) -> Vec<&'a str> {
    let field = schema.field_by_name(name).unwrap();
    let Type::Struct(struct_type) = field.field_type.as_ref() else {
        panic!("{name} should be a struct")
    };
    struct_type
        .fields()
        .iter()
        .map(|field| field.name.as_str())
        .collect()
}

#[tokio::test]
async fn applies_root_moves_in_call_order_and_preserves_field_ids() {
    let table = make_v2_table_with_nested();
    let schema = updated_schema(
        &table,
        update(&table)
            .move_column_first("props")
            .move_column_first("z")
            .move_column_after("x", "y")
            .move_column_before("props", "person"),
    )
    .await;

    assert_eq!(root_names(&schema), [
        "z", "y", "x", "props", "person", "tags"
    ]);
    assert_eq!(schema.field_by_name("x").unwrap().id, 1);
    assert_eq!(schema.field_by_name("y").unwrap().id, 2);
    assert_eq!(schema.field_by_name("z").unwrap().id, 3);
    assert_eq!(schema.identifier_field_ids().collect::<Vec<_>>().len(), 2);
}

#[tokio::test]
async fn moves_fields_in_structs_list_elements_and_map_values() {
    let table = make_v2_table_with_nested();
    let schema = updated_schema(
        &table,
        update(&table)
            .move_column_first("person.age")
            .move_column_first("tags.element.value")
            .add_column(
                AddColumn::builder()
                    .parent("props")
                    .name("note")
                    .field_type(Type::Primitive(PrimitiveType::String))
                    .build(),
            )
            .move_column_before("props.value.note", "props.data"),
    )
    .await;

    assert_eq!(struct_names(&schema, "person"), ["age", "name"]);
    assert_eq!(struct_names(&schema, "tags.element"), ["value", "key"]);
    assert_eq!(struct_names(&schema, "props.value"), ["note", "data"]);
}

#[tokio::test]
async fn additions_are_appended_before_moves_and_replacements_win_name_lookup() {
    let table = make_v2_table_with_nested();
    let schema = updated_schema(
        &table,
        update(&table)
            .delete_column("z")
            .add_column(AddColumn::optional(
                "z",
                Type::Primitive(PrimitiveType::String),
            ))
            .add_column(AddColumn::optional(
                "extra",
                Type::Primitive(PrimitiveType::Boolean),
            ))
            .delete_column("person.name")
            .add_column(
                AddColumn::builder()
                    .parent("person")
                    .name("name")
                    .field_type(Type::Primitive(PrimitiveType::String))
                    .build(),
            )
            .move_column_before("extra", "z")
            .move_column_first("z")
            .move_column_after("extra", "x")
            .move_column_after("person.name", "person.age"),
    )
    .await;

    assert_eq!(root_names(&schema), [
        "z", "x", "extra", "y", "person", "tags", "props"
    ]);
    assert_eq!(schema.field_by_name("z").unwrap().id, 15);
    assert_eq!(schema.field_by_name("extra").unwrap().id, 16);
    assert_eq!(schema.field_by_name("person.name").unwrap().id, 17);
    assert!(schema.field_by_id(3).is_none());
    assert!(schema.field_by_id(5).is_none());
    assert_eq!(struct_names(&schema, "person"), ["age", "name"]);
}

#[tokio::test]
async fn validates_missing_self_and_cross_struct_moves() {
    let table = make_v2_table_with_nested();
    for (action, message) in [
        (
            update(&table).move_column_first("missing"),
            "Cannot move missing column: missing",
        ),
        (
            update(&table).move_column_before("z", "missing"),
            "Cannot move z before missing column: missing",
        ),
        (
            update(&table).move_column_after("z", "missing"),
            "Cannot move z after missing column: missing",
        ),
        (
            update(&table).move_column_before("tags.element", "missing"),
            "Cannot move tags.element before missing column: missing",
        ),
        (
            update(&table).move_column_before("z", "z"),
            "Cannot move z before itself",
        ),
        (
            update(&table).move_column_after("z", "z"),
            "Cannot move z after itself",
        ),
        (
            update(&table).move_column_before("z", "person.name"),
            "Cannot move field z to a different struct",
        ),
        (
            update(&table).move_column_after("person.name", "tags.element.key"),
            "Cannot move field person.name to a different struct",
        ),
    ] {
        let error = commit_error(&table, action).await;
        assert_eq!(error.kind(), ErrorKind::PreconditionFailed);
        assert_eq!(error.message(), message);
    }
}

#[tokio::test]
async fn rejects_collection_pseudo_fields_and_map_key_descendants() {
    let table = make_v2_table_with_nested();
    for action in [
        update(&table).move_column_first("tags.element"),
        update(&table).move_column_before("props.key", "props.value"),
    ] {
        let error = commit_error(&table, action).await;
        assert!(
            error
                .message()
                .starts_with("Cannot move fields in non-struct type:")
        );
    }

    let struct_key = table_with_struct_map_key();
    let key_error = commit_error(
        &struct_key,
        update(&struct_key).move_column_first("indexed.key.part"),
    )
    .await;
    assert_eq!(key_error.message(), "Cannot alter map keys: indexed");
}

#[tokio::test]
async fn move_before_add_fails_and_already_satisfied_moves_are_no_ops() {
    let table = make_v2_table_with_nested();
    let before_add = commit_error(
        &table,
        update(&table)
            .move_column_first("new_col")
            .add_column(AddColumn::optional(
                "new_col",
                Type::Primitive(PrimitiveType::String),
            )),
    )
    .await;
    assert_eq!(before_add.message(), "Cannot move missing column: new_col");

    assert_no_op(&table, Arc::new(update(&table).move_column_first("x"))).await;
    assert_no_op(
        &table,
        Arc::new(update(&table).move_column_before("x", "y")),
    )
    .await;
    assert_no_op(&table, Arc::new(update(&table).move_column_after("y", "x"))).await;
}

#[tokio::test]
async fn resolves_case_insensitively_and_replays_against_refreshed_metadata() {
    let table = make_v2_table_with_nested();
    let added = updated_schema(
        &table,
        update(&table)
            .case_sensitive(false)
            .add_column(AddColumn::optional(
                "Anchor",
                Type::Primitive(PrimitiveType::String),
            ))
            .add_column(AddColumn::optional(
                "Added",
                Type::Primitive(PrimitiveType::String),
            ))
            .move_column_before("ADDED", "ANCHOR"),
    )
    .await;
    let names = root_names(&added);
    assert_eq!(&names[names.len() - 2..], ["Added", "Anchor"]);

    let action = Arc::new(update(&table).case_sensitive(false).move_column_first("Z"));
    let mut first = action.clone().commit(&table).await.unwrap();
    let first = match first.take_updates().remove(0) {
        TableUpdate::AddSchema { schema } => schema,
        update => panic!("expected AddSchema, got {update:?}"),
    };
    assert_eq!(root_names(&first)[0], "z");

    let mut fields = table
        .metadata()
        .current_schema()
        .as_struct()
        .fields()
        .to_vec();
    fields.push(
        NestedField::optional(15, "concurrent", Type::Primitive(PrimitiveType::Boolean)).into(),
    );
    let concurrent = Schema::builder()
        .with_fields(fields)
        .with_identifier_field_ids(table.metadata().current_schema().identifier_field_ids())
        .build()
        .unwrap();
    let refreshed = replace_current_schema(&table, concurrent);
    let mut replayed = action.clone().commit(&refreshed).await.unwrap();
    let replayed = match replayed.take_updates().remove(0) {
        TableUpdate::AddSchema { schema } => schema,
        update => panic!("expected AddSchema, got {update:?}"),
    };
    assert_eq!(root_names(&replayed)[0], "z");
    assert!(replayed.field_by_name("concurrent").is_some());

    let already_applied = replace_current_schema(&table, first);
    assert_no_op(&already_applied, action).await;
}
