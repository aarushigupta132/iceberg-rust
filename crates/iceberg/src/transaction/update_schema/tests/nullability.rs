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

use super::tests::make_v2_table_with_nested;
use super::{AddColumn, UpdateSchemaAction};
use crate::spec::{Literal, NestedField, PrimitiveType, Schema, Type};
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

fn required_without_default(name: &str) -> AddColumn {
    AddColumn::builder()
        .name(name)
        .required(true)
        .field_type(Type::Primitive(PrimitiveType::String))
        .build()
}

#[tokio::test]
async fn changes_nullability_and_preserves_pending_metadata_updates() {
    let table = make_v2_table_with_nested();
    let schema = updated_schema(
        &table,
        update(&table)
            .update_column_doc("person.name", Some("full name".to_string()))
            .update_column_default("person.name", Some(Literal::string("unknown")))
            .rename_column("person.name", "full_name")
            .allow_incompatible_changes()
            .require_column("person.name")
            .make_column_optional("person.age")
            .update_column_type("person.age", PrimitiveType::Long),
    )
    .await;

    assert!(schema.field_by_name("person.name").is_none());
    let name = schema.field_by_name("person.full_name").unwrap();
    assert!(name.required);
    assert_eq!(name.id, 5);
    assert_eq!(name.doc.as_deref(), Some("full name"));
    assert_eq!(name.initial_default, None);
    assert_eq!(name.write_default, Some(Literal::string("unknown")));

    let age = schema.field_by_name("person.age").unwrap();
    assert!(!age.required);
    assert_eq!(age.id, 6);
    assert_eq!(
        age.field_type.as_ref(),
        &Type::Primitive(PrimitiveType::Long)
    );
}

#[tokio::test]
async fn requires_explicit_permission_for_incompatible_changes() {
    let table = make_v2_table_with_nested();
    let required = commit_error(&table, update(&table).require_column("person.name")).await;
    assert_eq!(required.kind(), ErrorKind::PreconditionFailed);
    assert_eq!(
        required.message(),
        "Cannot change column nullability: person.name: optional -> required"
    );

    let required_add = commit_error(
        &table,
        update(&table).add_column(required_without_default("required_col")),
    )
    .await;
    assert_eq!(
        required_add.message(),
        "Incompatible change: cannot add required column without an initial default: required_col"
    );

    let schema = updated_schema(
        &table,
        update(&table)
            .allow_incompatible_changes()
            .require_column("person.name")
            .add_column(required_without_default("required_col")),
    )
    .await;
    assert!(schema.field_by_name("person.name").unwrap().required);
    assert!(schema.field_by_name("required_col").unwrap().required);
}

#[tokio::test]
async fn incompatible_permission_applies_only_to_subsequent_operations() {
    let table = make_v2_table_with_nested();
    for action in [
        update(&table)
            .require_column("person.name")
            .allow_incompatible_changes(),
        update(&table)
            .add_column(required_without_default("required_col"))
            .allow_incompatible_changes(),
    ] {
        let error = commit_error(&table, action).await;
        assert_eq!(error.kind(), ErrorKind::PreconditionFailed);
    }

    assert_no_op(
        &table,
        Arc::new(update(&table).allow_incompatible_changes()),
    )
    .await;
}

#[tokio::test]
async fn an_initial_default_but_not_a_write_default_makes_an_add_safe_to_require() {
    let table = make_v2_table_with_nested();
    let schema = updated_schema(
        &table,
        update(&table)
            .add_column(
                AddColumn::builder()
                    .name("defaulted")
                    .field_type(Type::Primitive(PrimitiveType::Int))
                    .initial_default(Literal::int(7))
                    .write_default(Literal::int(8))
                    .build(),
            )
            .require_column("defaulted"),
    )
    .await;
    let defaulted = schema.field_by_name("defaulted").unwrap();
    assert!(defaulted.required);
    assert_eq!(defaulted.initial_default, Some(Literal::int(7)));
    assert_eq!(defaulted.write_default, Some(Literal::int(8)));

    let write_only = commit_error(
        &table,
        update(&table)
            .add_column(AddColumn::optional(
                "write_only",
                Type::Primitive(PrimitiveType::String),
            ))
            .update_column_default("write_only", Some(Literal::string("unknown")))
            .require_column("write_only"),
    )
    .await;
    assert_eq!(
        write_only.message(),
        "Cannot change column nullability: write_only: optional -> required"
    );
}

#[tokio::test]
async fn validates_missing_deleted_and_exact_no_op_targets_in_java_order() {
    let table = make_v2_table_with_nested();
    let missing = commit_error(&table, update(&table).make_column_optional("missing")).await;
    assert_eq!(missing.message(), "Cannot update missing column: missing");

    assert_no_op(&table, Arc::new(update(&table).require_column("z"))).await;

    let deleted = updated_schema(
        &table,
        update(&table).delete_column("z").require_column("z"),
    )
    .await;
    assert!(deleted.field_by_name("z").is_none());

    let incompatible_before_deleted = commit_error(
        &table,
        update(&table)
            .delete_column("person.name")
            .require_column("person.name"),
    )
    .await;
    assert_eq!(
        incompatible_before_deleted.message(),
        "Cannot change column nullability: person.name: optional -> required"
    );

    let deleted_after_opt_in = commit_error(
        &table,
        update(&table)
            .delete_column("person.name")
            .allow_incompatible_changes()
            .require_column("person.name"),
    )
    .await;
    assert_eq!(
        deleted_after_opt_in.message(),
        "Cannot update a column that will be deleted: name"
    );
}

#[tokio::test]
async fn updates_collection_nullability_and_rejects_map_key_changes() {
    let table = make_v2_table_with_nested();
    let schema = updated_schema(
        &table,
        update(&table)
            .make_column_optional("tags.element")
            .make_column_optional("props.value"),
    )
    .await;
    assert!(!schema.field_by_name("tags.element").unwrap().required);
    assert!(!schema.field_by_name("props.value").unwrap().required);

    let map_key = commit_error(&table, update(&table).make_column_optional("props.key")).await;
    assert_eq!(map_key.message(), "Cannot update map keys: props");
}

#[tokio::test]
async fn incompatible_permission_does_not_override_identifier_validation() {
    let table = make_v2_table_with_nested();
    let error = commit_error(
        &table,
        update(&table)
            .allow_incompatible_changes()
            .make_column_optional("x"),
    )
    .await;
    assert_eq!(error.kind(), ErrorKind::PreconditionFailed);
    assert_eq!(error.message(), "Cannot apply schema update");
}

#[tokio::test]
async fn resolves_case_insensitively_and_replays_against_refreshed_metadata() {
    let table = make_v2_table_with_nested();
    let action = Arc::new(
        update(&table)
            .case_sensitive(false)
            .make_column_optional("PERSON.AGE"),
    );
    let mut first = action.clone().commit(&table).await.unwrap();
    let first = match first.take_updates().remove(0) {
        TableUpdate::AddSchema { schema } => schema,
        update => panic!("expected AddSchema, got {update:?}"),
    };
    assert!(!first.field_by_name("person.age").unwrap().required);

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
    assert!(!replayed.field_by_name("person.age").unwrap().required);

    let already_applied = replace_current_schema(&table, first);
    assert_no_op(&already_applied, action).await;
}
