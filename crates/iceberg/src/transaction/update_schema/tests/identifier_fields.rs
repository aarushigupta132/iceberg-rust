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

use super::tests::make_v2_table_with_nested;
use super::{AddColumn, UpdateSchemaAction};
use crate::spec::{NestedField, PrimitiveType, Schema, StructType, Type};
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

fn identifier_ids(schema: &Schema) -> HashSet<i32> {
    schema.identifier_field_ids().collect()
}

fn required_without_default(name: &str, field_type: Type) -> AddColumn {
    AddColumn::builder()
        .name(name)
        .required(true)
        .field_type(field_type)
        .build()
}

fn table_with_required_nested_fields() -> Table {
    let table = make_v2_table_with_nested();
    let mut fields = table
        .metadata()
        .current_schema()
        .as_struct()
        .fields()
        .to_vec();
    fields.push(
        NestedField::required(
            15,
            "parent",
            Type::Struct(StructType::new(vec![
                NestedField::required(16, "id", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::required(17, "value", Type::Primitive(PrimitiveType::Int)).into(),
            ])),
        )
        .into(),
    );
    let schema = Schema::builder()
        .with_fields(fields)
        .with_identifier_field_ids(table.metadata().current_schema().identifier_field_ids())
        .build()
        .unwrap();
    replace_current_schema(&table, schema)
}

#[tokio::test]
async fn replaces_deduplicates_and_clears_identifier_fields() {
    let table = make_v2_table_with_nested();
    let schema = updated_schema(&table, update(&table).set_identifier_fields(["z", "z"])).await;
    assert_eq!(identifier_ids(&schema), HashSet::from([3]));

    let schema = updated_schema(
        &table,
        update(&table)
            .set_identifier_fields(["z"])
            .set_identifier_fields(["x"]),
    )
    .await;
    assert_eq!(identifier_ids(&schema), HashSet::from([1]));

    assert_no_op(
        &table,
        Arc::new(update(&table).set_identifier_fields(["x", "y"])),
    )
    .await;

    for action in [
        update(&table)
            .set_identifier_fields(std::iter::empty::<&str>())
            .delete_column("x")
            .delete_column("y"),
        update(&table)
            .delete_column("x")
            .delete_column("y")
            .set_identifier_fields(std::iter::empty::<&str>()),
    ] {
        let schema = updated_schema(&table, action).await;
        assert!(identifier_ids(&schema).is_empty());
        assert!(schema.field_by_name("x").is_none());
        assert!(schema.field_by_name("y").is_none());
    }
}

#[tokio::test]
async fn resolves_existing_future_and_nested_added_fields() {
    let table = make_v2_table_with_nested();
    let schema = updated_schema(
        &table,
        update(&table)
            .allow_incompatible_changes()
            .set_identifier_fields(["future"])
            .add_column(required_without_default(
                "future",
                Type::Primitive(PrimitiveType::String),
            )),
    )
    .await;
    assert_eq!(
        identifier_ids(&schema),
        HashSet::from([schema.field_by_name("future").unwrap().id])
    );

    let schema = updated_schema(
        &table,
        update(&table)
            .allow_incompatible_changes()
            .add_column(required_without_default(
                "parent_added",
                Type::Struct(StructType::new(vec![
                    NestedField::required(100, "id", Type::Primitive(PrimitiveType::String)).into(),
                ])),
            ))
            .set_identifier_fields(["parent_added.id"]),
    )
    .await;
    assert_eq!(
        identifier_ids(&schema),
        HashSet::from([schema.field_by_name("parent_added.id").unwrap().id])
    );
}

#[tokio::test]
async fn rejects_missing_or_invalid_identifier_fields() {
    let table = make_v2_table_with_nested();
    let missing = commit_error(&table, update(&table).set_identifier_fields(["missing"])).await;
    assert_eq!(
        missing.message(),
        "Cannot add field missing as an identifier field: not found in current schema or added columns"
    );

    for name in [
        "person",
        "person.name",
        "person.age",
        "tags.element.key",
        "props.value.data",
    ] {
        let error = commit_error(&table, update(&table).set_identifier_fields([name])).await;
        assert_eq!(error.kind(), ErrorKind::PreconditionFailed, "{name}");
        assert_eq!(error.message(), "Cannot apply schema update", "{name}");
    }

    for primitive in [PrimitiveType::Float, PrimitiveType::Double] {
        let error = commit_error(
            &table,
            update(&table)
                .allow_incompatible_changes()
                .add_column(required_without_default(
                    "candidate",
                    Type::Primitive(primitive),
                ))
                .set_identifier_fields(["candidate"]),
        )
        .await;
        assert_eq!(error.message(), "Cannot apply schema update");
    }
}

#[tokio::test]
async fn replacement_controls_deletion_and_delete_readd_identity() {
    let table = make_v2_table_with_nested();
    let selected_delete = commit_error(&table, update(&table).delete_column("x")).await;
    assert_eq!(
        selected_delete.message(),
        "Cannot delete identifier field x"
    );

    let schema = updated_schema(
        &table,
        update(&table)
            .delete_column("x")
            .delete_column("y")
            .set_identifier_fields(["z"]),
    )
    .await;
    assert_eq!(identifier_ids(&schema), HashSet::from([3]));

    let schema = updated_schema(
        &table,
        update(&table)
            .set_identifier_fields(["z"])
            .make_column_optional("x"),
    )
    .await;
    assert!(!schema.field_by_name("x").unwrap().required);
    assert_eq!(identifier_ids(&schema), HashSet::from([3]));

    let old_selection = commit_error(
        &table,
        update(&table)
            .set_identifier_fields(["z"])
            .delete_column("z")
            .allow_incompatible_changes()
            .add_column(required_without_default(
                "z",
                Type::Primitive(PrimitiveType::String),
            )),
    )
    .await;
    assert_eq!(old_selection.message(), "Cannot delete identifier field z");

    let schema = updated_schema(
        &table,
        update(&table)
            .delete_column("z")
            .allow_incompatible_changes()
            .add_column(required_without_default(
                "z",
                Type::Primitive(PrimitiveType::String),
            ))
            .set_identifier_fields(["z"]),
    )
    .await;
    let replacement = schema.field_by_name("z").unwrap();
    assert_ne!(replacement.id, 3);
    assert_eq!(identifier_ids(&schema), HashSet::from([replacement.id]));

    let nested = table_with_required_nested_fields();
    let ancestor = commit_error(
        &nested,
        update(&nested)
            .set_identifier_fields(["parent.id"])
            .delete_column("parent"),
    )
    .await;
    assert_eq!(
        ancestor.message(),
        "Cannot delete field parent as it will delete nested identifier field id"
    );

    let schema = updated_schema(
        &nested,
        update(&nested)
            .delete_column("parent")
            .allow_incompatible_changes()
            .add_column(required_without_default(
                "parent",
                Type::Struct(StructType::new(vec![
                    NestedField::required(100, "id", Type::Primitive(PrimitiveType::String)).into(),
                ])),
            ))
            .set_identifier_fields(["parent.id"]),
    )
    .await;
    let replacement = schema.field_by_name("parent.id").unwrap();
    assert_ne!(replacement.id, 16);
    assert_eq!(identifier_ids(&schema), HashSet::from([replacement.id]));

    let schema = updated_schema(
        &table,
        update(&table)
            .allow_incompatible_changes()
            .add_column(
                AddColumn::builder()
                    .parent("tags")
                    .name("id")
                    .required(true)
                    .field_type(Type::Primitive(PrimitiveType::String))
                    .build(),
            )
            .delete_column("tags")
            .add_column(required_without_default(
                "tags",
                Type::Struct(StructType::new(vec![
                    NestedField::required(
                        100,
                        "element",
                        Type::Struct(StructType::new(vec![
                            NestedField::required(
                                101,
                                "id",
                                Type::Primitive(PrimitiveType::String),
                            )
                            .into(),
                        ])),
                    )
                    .into(),
                ])),
            ))
            .case_sensitive(false)
            .set_identifier_fields(["TAGS.ELEMENT.ID"]),
    )
    .await;
    let replacement = schema.field_by_name("tags.element.id").unwrap();
    assert_ne!(replacement.id, 15);
    assert_eq!(identifier_ids(&schema), HashSet::from([replacement.id]));
}

#[tokio::test]
async fn identifiers_follow_root_nested_and_case_insensitive_renames() {
    let table = make_v2_table_with_nested();
    let schema = updated_schema(
        &table,
        update(&table)
            .case_sensitive(false)
            .set_identifier_fields(["X"])
            .rename_column("X", "identifier"),
    )
    .await;
    assert_eq!(identifier_ids(&schema), HashSet::from([1]));
    assert_eq!(schema.field_by_name("identifier").unwrap().id, 1);

    let nested = table_with_required_nested_fields();
    let schema = updated_schema(
        &nested,
        update(&nested)
            .case_sensitive(false)
            .set_identifier_fields(["PARENT.ID"])
            .rename_column("PARENT", "human")
            .rename_column("PARENT.ID", "key")
            .move_column_after("PARENT.ID", "PARENT.VALUE"),
    )
    .await;
    let identifier = schema.field_by_name("human.key").unwrap();
    assert_eq!(identifier.id, 16);
    assert_eq!(identifier_ids(&schema), HashSet::from([16]));
    let Type::Struct(parent) = schema.field_by_name("human").unwrap().field_type.as_ref() else {
        panic!("human should remain a struct")
    };
    assert_eq!(parent.fields()[1].id, 16);
}

#[tokio::test]
async fn captures_case_sensitivity_when_identifier_fields_are_set() {
    let table = make_v2_table_with_nested();
    let schema = updated_schema(
        &table,
        update(&table)
            .case_sensitive(false)
            .set_identifier_fields(["Z"])
            .case_sensitive(true),
    )
    .await;
    assert_eq!(identifier_ids(&schema), HashSet::from([3]));

    let exact_error = commit_error(
        &table,
        update(&table)
            .set_identifier_fields(["Z"])
            .case_sensitive(false),
    )
    .await;
    assert!(exact_error.message().contains("not found"));

    let schema = updated_schema(
        &table,
        update(&table)
            .case_sensitive(false)
            .set_identifier_fields(["FUTURE"])
            .case_sensitive(true)
            .allow_incompatible_changes()
            .add_column(required_without_default(
                "Future",
                Type::Primitive(PrimitiveType::String),
            )),
    )
    .await;
    assert_eq!(
        identifier_ids(&schema),
        HashSet::from([schema.field_by_name("Future").unwrap().id])
    );

    let schema = updated_schema(
        &table,
        update(&table)
            .delete_column("z")
            .allow_incompatible_changes()
            .add_column(required_without_default(
                "Z",
                Type::Primitive(PrimitiveType::String),
            ))
            .case_sensitive(false)
            .set_identifier_fields(["Z"]),
    )
    .await;
    let replacement = schema.field_by_name("Z").unwrap();
    assert_ne!(replacement.id, 3);
    assert_eq!(identifier_ids(&schema), HashSet::from([replacement.id]));

    let ambiguity = commit_error(
        &table,
        update(&table)
            .allow_incompatible_changes()
            .add_column(required_without_default(
                "Z",
                Type::Primitive(PrimitiveType::String),
            ))
            .case_sensitive(false)
            .set_identifier_fields(["z"]),
    )
    .await;
    assert!(ambiguity.message().contains("multiple fields match: z"));
}

#[tokio::test]
async fn replays_identifier_selection_against_refreshed_metadata() {
    let table = make_v2_table_with_nested();
    let action = Arc::new(update(&table).set_identifier_fields(["z"]));

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
    assert!(replayed.field_by_name("concurrent").is_some());
    assert_eq!(identifier_ids(&replayed), HashSet::from([3]));

    let already_applied = replace_current_schema(&table, replayed);
    assert_no_op(&already_applied, action).await;
}
