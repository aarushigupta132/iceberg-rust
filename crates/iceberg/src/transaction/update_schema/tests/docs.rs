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
use crate::spec::{Literal, PrimitiveType, Schema, Type};
use crate::table::Table;
use crate::transaction::Transaction;
use crate::transaction::action::TransactionAction;
use crate::{Error, TableUpdate};

fn update(table: &Table) -> UpdateSchemaAction {
    Transaction::new(table).update_schema()
}

fn doc(value: &str) -> Option<String> {
    Some(value.to_string())
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

async fn assert_no_op(table: &Table, action: UpdateSchemaAction) {
    let mut commit = Arc::new(action).commit(table).await.unwrap();
    assert!(commit.take_updates().is_empty());
    assert!(commit.take_requirements().is_empty());
}

fn field_doc<'a>(schema: &'a Schema, name: &str) -> Option<&'a str> {
    schema.field_by_name(name).unwrap().doc.as_deref()
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

fn documented_table() -> Table {
    let table = make_v2_table_with_nested();
    let mut fields = table
        .metadata()
        .current_schema()
        .as_struct()
        .fields()
        .to_vec();
    let mut documented = (*fields[2]).clone();
    documented.doc = doc("original");
    fields[2] = Arc::new(documented);
    let schema = Schema::builder()
        .with_fields(fields)
        .with_identifier_field_ids(table.metadata().current_schema().identifier_field_ids())
        .build()
        .unwrap();
    replace_current_schema(&table, schema)
}

#[tokio::test]
async fn sets_replaces_and_clears_docs_without_changing_other_metadata() {
    let table = documented_table();
    let original = table
        .metadata()
        .current_schema()
        .field_by_name("z")
        .unwrap();

    let replaced = updated_schema(
        &table,
        update(&table).update_column_doc("z", doc("replacement")),
    )
    .await;
    let mut expected = (**original).clone();
    expected.doc = doc("replacement");
    assert_eq!(replaced.field_by_name("z").unwrap().as_ref(), &expected);

    let cleared = updated_schema(&table, update(&table).update_column_doc("z", None)).await;
    assert_eq!(field_doc(&cleared, "z"), None);
    assert_no_op(
        &table,
        update(&table).update_column_doc("z", doc("original")),
    )
    .await;
}

#[tokio::test]
async fn validates_targets_before_no_ops_and_keeps_reverted_updates_marked() {
    let table = documented_table();
    let missing = commit_error(&table, update(&table).update_column_doc("missing", None)).await;
    assert_eq!(missing.message(), "Cannot update missing column: missing");

    let deleted = commit_error(
        &table,
        update(&table)
            .case_sensitive(false)
            .delete_column("Z")
            .update_column_doc("Z", doc("original")),
    )
    .await;
    assert_eq!(
        deleted.message(),
        "Cannot update a column that will be deleted: z"
    );

    let deleted_schema = updated_schema(
        &table,
        update(&table)
            .update_column_doc("z", doc("original"))
            .delete_column("z"),
    )
    .await;
    assert!(deleted_schema.field_by_name("z").is_none());

    let reverted = commit_error(
        &table,
        update(&table)
            .update_column_doc("z", doc("temporary"))
            .update_column_doc("z", doc("original"))
            .delete_column("z"),
    )
    .await;
    assert_eq!(
        reverted.message(),
        "Cannot delete a column that has updates: z"
    );
}

#[tokio::test]
async fn merges_with_renames_using_only_the_original_selector() {
    let table = documented_table();
    for action in [
        update(&table)
            .rename_column("z", "payload")
            .update_column_doc("z", doc("renamed")),
        update(&table)
            .update_column_doc("z", doc("renamed"))
            .rename_column("z", "payload"),
    ] {
        let schema = updated_schema(&table, action).await;
        let field = schema.field_by_name("payload").unwrap();
        assert_eq!(field.id, 3);
        assert_eq!(field.doc.as_deref(), Some("renamed"));
    }

    let error = commit_error(
        &table,
        update(&table)
            .rename_column("z", "payload")
            .update_column_doc("payload", None),
    )
    .await;
    assert_eq!(error.message(), "Cannot update missing column: payload");
}

#[tokio::test]
async fn updates_pending_additions_with_ordered_case_resolution() {
    let table = make_v2_table_with_nested();
    let schema = updated_schema(
        &table,
        update(&table)
            .case_sensitive(false)
            .add_column(AddColumn::required(
                "Added",
                Type::Primitive(PrimitiveType::Int),
                Literal::int(7),
            ))
            .update_column_doc("ADDED", doc("first"))
            .add_column(
                AddColumn::builder()
                    .parent("PERSON")
                    .name("note")
                    .field_type(Type::Primitive(PrimitiveType::String))
                    .build(),
            )
            .update_column_doc("PERSON.NOTE", doc("nested"))
            .case_sensitive(true)
            .update_column_doc("added", doc("final")),
    )
    .await;
    let added = schema.field_by_name("Added").unwrap();
    assert_eq!(added.doc.as_deref(), Some("final"));
    assert_eq!(added.initial_default, Some(Literal::int(7)));
    assert_eq!(added.write_default, Some(Literal::int(7)));
    assert_eq!(field_doc(&schema, "person.note"), Some("nested"));

    let toggled = commit_error(
        &table,
        update(&table)
            .add_column(AddColumn::optional(
                "MixedCase",
                Type::Primitive(PrimitiveType::String),
            ))
            .case_sensitive(false)
            .update_column_doc("MIXEDCASE", None),
    )
    .await;
    assert_eq!(toggled.message(), "Cannot update missing column: MIXEDCASE");

    let replacement = commit_error(
        &table,
        update(&table)
            .delete_column("z")
            .add_column(AddColumn::optional(
                "z",
                Type::Primitive(PrimitiveType::String),
            ))
            .update_column_doc("z", doc("replacement")),
    )
    .await;
    assert_eq!(
        replacement.message(),
        "Cannot update a column that will be deleted: z"
    );
}

#[tokio::test]
async fn handles_collection_docs_like_java() {
    let table = make_v2_table_with_nested();
    assert_no_op(&table, update(&table).update_column_doc("props.key", None)).await;

    let key_change = commit_error(
        &table,
        update(&table).update_column_doc("props.key", doc("key")),
    )
    .await;
    assert_eq!(key_change.message(), "Cannot update map keys: props");

    let struct_key = table_with_struct_map_key();
    let nested_key_change = commit_error(
        &struct_key,
        update(&struct_key).update_column_doc("indexed.key.part", doc("part")),
    )
    .await;
    assert_eq!(
        nested_key_change.message(),
        "Cannot alter map keys: indexed"
    );

    let schema = updated_schema(
        &table,
        update(&table)
            .update_column_doc("tags.element", doc("discarded"))
            .update_column_doc("props.value", doc("discarded"))
            .update_column_doc("tags.element.key", doc("list child"))
            .update_column_doc("props.value.data", doc("map child")),
    )
    .await;
    assert_eq!(field_doc(&schema, "tags.element"), None);
    assert_eq!(field_doc(&schema, "props.value"), None);
    assert_eq!(field_doc(&schema, "tags.element.key"), Some("list child"));
    assert_eq!(field_doc(&schema, "props.value.data"), Some("map child"));
}

#[tokio::test]
async fn drops_doc_updates_below_deleted_ancestors() {
    let table = make_v2_table_with_nested();
    let schema = updated_schema(
        &table,
        update(&table)
            .delete_column("person")
            .update_column_doc("person.name", doc("dropped")),
    )
    .await;
    assert!(schema.field_by_name("person").is_none());
    assert!(schema.field_by_id(5).is_none());
}

#[tokio::test]
async fn replays_docs_against_refreshed_metadata_and_detects_no_ops() {
    let table = documented_table();
    let action = Arc::new(update(&table).update_column_doc("z", doc("requested")));
    let mut first = action.clone().commit(&table).await.unwrap();
    let first_schema = match first.take_updates().remove(0) {
        TableUpdate::AddSchema { schema } => schema,
        update => panic!("expected AddSchema, got {update:?}"),
    };

    let already_applied = replace_current_schema(&table, first_schema);
    assert_no_op(&already_applied, Arc::into_inner(action).unwrap()).await;
}
