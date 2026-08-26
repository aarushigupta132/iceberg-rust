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
use crate::catalog::MockCatalog;
use crate::spec::{MapType, NestedField, PrimitiveType, Schema, StructType, Type};
use crate::table::Table;
use crate::transaction::action::TransactionAction;
use crate::transaction::{ApplyTransactionAction, Transaction};
use crate::{Error, ErrorKind, TableRequirement, TableUpdate};

async fn commit_error(table: &Table, action: UpdateSchemaAction) -> Error {
    match Arc::new(action).commit(table).await {
        Err(error) => error,
        Ok(_) => panic!("schema update should fail"),
    }
}

async fn updated_schema(table: &Table, action: UpdateSchemaAction) -> Schema {
    let mut commit = Arc::new(action).commit(table).await.unwrap();
    match commit.take_updates().remove(0) {
        TableUpdate::AddSchema { schema } => schema,
        update => panic!("expected AddSchema, got {update:?}"),
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

fn table_with_nested_identifier() -> Table {
    let table = crate::transaction::tests::make_v2_table();
    let mut fields = table
        .metadata()
        .current_schema()
        .as_struct()
        .fields()
        .to_vec();
    fields.push(
        NestedField::required(
            4,
            "identity",
            Type::Struct(StructType::new(vec![
                NestedField::required(5, "code", Type::Primitive(PrimitiveType::String)).into(),
            ])),
        )
        .into(),
    );
    let schema = Schema::builder()
        .with_fields(fields)
        .with_identifier_field_ids([5])
        .build()
        .unwrap();
    replace_current_schema(&table, schema)
}

fn table_with_struct_map_key() -> Table {
    let table = crate::transaction::tests::make_v2_table();
    let mut fields = table
        .metadata()
        .current_schema()
        .as_struct()
        .fields()
        .to_vec();
    fields.push(
        NestedField::optional(
            4,
            "indexed",
            Type::Map(MapType {
                key_field: NestedField::map_key_element(
                    5,
                    Type::Struct(StructType::new(vec![
                        NestedField::required(7, "part", Type::Primitive(PrimitiveType::Int))
                            .into(),
                    ])),
                )
                .into(),
                value_field: NestedField::map_value_element(
                    6,
                    Type::Primitive(PrimitiveType::String),
                    false,
                )
                .into(),
            }),
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
async fn replays_operations_against_latest_metadata() {
    let table = crate::transaction::tests::make_v2_table();
    let action = Arc::new(Transaction::new(&table).update_schema().add_column(
        AddColumn::optional("added", Type::Primitive(PrimitiveType::String)),
    ));

    let mut initial_commit = action.clone().commit(&table).await.unwrap();
    let initial_schema = match initial_commit.take_updates().remove(0) {
        TableUpdate::AddSchema { schema } => schema,
        update => panic!("expected AddSchema, got {update:?}"),
    };
    assert_eq!(initial_schema.field_by_name("added").unwrap().id, 4);

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
    let refreshed = replace_current_schema(&table, concurrent_schema);

    let mut replayed_commit = action.commit(&refreshed).await.unwrap();
    let replayed_schema = match replayed_commit.take_updates().remove(0) {
        TableUpdate::AddSchema { schema } => schema,
        update => panic!("expected AddSchema, got {update:?}"),
    };
    assert_eq!(replayed_schema.field_by_name("concurrent").unwrap().id, 4);
    assert_eq!(replayed_schema.field_by_name("added").unwrap().id, 5);
    assert_eq!(replayed_commit.take_requirements(), vec![
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

#[tokio::test]
async fn add_then_delete_new_column_targets_the_base_schema() {
    let table = crate::transaction::tests::make_v2_table();
    let error = commit_error(
        &table,
        Transaction::new(&table)
            .update_schema()
            .add_column(AddColumn::optional(
                "new_col",
                Type::Primitive(PrimitiveType::Boolean),
            ))
            .delete_column("new_col"),
    )
    .await;
    assert_eq!(error.kind(), ErrorKind::PreconditionFailed);
    assert!(error.message().contains("Cannot delete missing column"));
}

#[tokio::test]
async fn repeated_deletes_still_target_the_original_field() {
    let table = crate::transaction::tests::make_v2_table();
    let deleted = updated_schema(
        &table,
        Transaction::new(&table)
            .update_schema()
            .delete_column("z")
            .delete_column("z"),
    )
    .await;
    assert!(deleted.field_by_name("z").is_none());

    let replaced = updated_schema(
        &table,
        Transaction::new(&table)
            .update_schema()
            .delete_column("z")
            .add_column(AddColumn::optional(
                "z",
                Type::Primitive(PrimitiveType::Boolean),
            ))
            .delete_column("z"),
    )
    .await;
    assert_eq!(replaced.field_by_name("z").unwrap().id, 4);
}

#[tokio::test]
async fn rejects_duplicate_pending_additions_when_building_the_schema() {
    let table = crate::transaction::tests::make_v2_table();
    let error = commit_error(
        &table,
        Transaction::new(&table)
            .update_schema()
            .add_column(AddColumn::optional(
                "new_col",
                Type::Primitive(PrimitiveType::Int),
            ))
            .add_column(AddColumn::optional(
                "new_col",
                Type::Primitive(PrimitiveType::Long),
            )),
    )
    .await;

    assert_eq!(error.kind(), ErrorKind::PreconditionFailed);
    assert!(error.message().contains("Cannot apply schema update"));
}

#[tokio::test]
async fn rejects_struct_parent_deletion_conflicts_in_both_orders() {
    let table = make_v2_table_with_nested();
    let add_then_delete = commit_error(
        &table,
        Transaction::new(&table)
            .update_schema()
            .add_column(
                AddColumn::builder()
                    .parent("person")
                    .name("email")
                    .field_type(Type::Primitive(PrimitiveType::String))
                    .build(),
            )
            .delete_column("person"),
    )
    .await;
    assert!(add_then_delete.message().contains("has additions"));

    let delete_then_add = commit_error(
        &table,
        Transaction::new(&table)
            .update_schema()
            .delete_column("person")
            .add_column(
                AddColumn::builder()
                    .parent("person")
                    .name("email")
                    .field_type(Type::Primitive(PrimitiveType::String))
                    .build(),
            ),
    )
    .await;
    assert!(
        delete_then_add
            .message()
            .contains("column that will be deleted")
    );
}

#[tokio::test]
async fn collection_ancestor_deletes_drop_nested_additions() {
    let table = make_v2_table_with_nested();
    for (parent, child, field_type) in [
        ("tags", "score", PrimitiveType::Double),
        ("props", "version", PrimitiveType::Int),
    ] {
        let delete_then_add = updated_schema(
            &table,
            Transaction::new(&table)
                .update_schema()
                .delete_column(parent)
                .add_column(
                    AddColumn::builder()
                        .parent(parent)
                        .name(child)
                        .field_type(Type::Primitive(field_type.clone()))
                        .build(),
                ),
        )
        .await;
        assert!(delete_then_add.field_by_name(parent).is_none());

        let add_then_delete = updated_schema(
            &table,
            Transaction::new(&table)
                .update_schema()
                .add_column(
                    AddColumn::builder()
                        .parent(parent)
                        .name(child)
                        .field_type(Type::Primitive(field_type))
                        .build(),
                )
                .delete_column(parent),
        )
        .await;
        assert!(add_then_delete.field_by_name(parent).is_none());
    }
}

#[tokio::test]
async fn empty_update_skips_the_catalog_commit() {
    let table = crate::transaction::tests::make_v2_table();
    let mut direct_commit = Arc::new(Transaction::new(&table).update_schema())
        .commit(&table)
        .await
        .unwrap();
    assert!(direct_commit.take_updates().is_empty());
    assert!(direct_commit.take_requirements().is_empty());

    let refreshed = table.clone();
    let mut catalog = MockCatalog::new();
    catalog
        .expect_load_table()
        .once()
        .returning_st(move |_| Box::pin(std::future::ready(Ok(refreshed.clone()))));
    catalog.expect_update_table().never();

    let tx = Transaction::new(&table);
    let tx = tx.update_schema().apply(tx).unwrap();
    let committed = tx.commit(&catalog).await.unwrap();
    assert_eq!(committed.metadata(), table.metadata());
}

#[tokio::test]
async fn multiple_schema_actions_use_staged_metadata() {
    let table = crate::transaction::tests::make_v2_table().with_metadata_location(
        "s3://bucket/test/location/metadata/00000-9c12d441-03fe-4693-9a96-a0705ddf69c1.metadata.json"
            .to_string(),
    );
    let refreshed = table.clone();
    let base = table.clone();
    let mut catalog = MockCatalog::new();
    catalog
        .expect_load_table()
        .once()
        .returning_st(move |_| Box::pin(std::future::ready(Ok(refreshed.clone()))));
    catalog
        .expect_update_table()
        .once()
        .returning_st(move |commit| Box::pin(std::future::ready(commit.apply(base.clone()))));

    let tx = Transaction::new(&table);
    let tx = tx
        .update_schema()
        .add_column(AddColumn::optional(
            "first",
            Type::Primitive(PrimitiveType::Int),
        ))
        .apply(tx)
        .unwrap();
    let tx = tx
        .update_schema()
        .add_column(AddColumn::optional(
            "second",
            Type::Primitive(PrimitiveType::String),
        ))
        .apply(tx)
        .unwrap();

    let committed = tx.commit(&catalog).await.unwrap();
    assert_eq!(
        committed
            .metadata()
            .current_schema()
            .field_by_name("first")
            .unwrap()
            .id,
        4
    );
    assert_eq!(
        committed
            .metadata()
            .current_schema()
            .field_by_name("second")
            .unwrap()
            .id,
        5
    );
}

#[tokio::test]
async fn rejects_identifier_and_identifier_ancestor_deletions() {
    let table = table_with_nested_identifier();

    let identifier_error = commit_error(
        &table,
        Transaction::new(&table)
            .update_schema()
            .delete_column("identity.code"),
    )
    .await;
    assert!(identifier_error.message().contains("identifier field"));

    let ancestor_error = commit_error(
        &table,
        Transaction::new(&table)
            .update_schema()
            .delete_column("identity"),
    )
    .await;
    assert!(
        ancestor_error
            .message()
            .contains("delete nested identifier field")
    );
}

#[tokio::test]
async fn same_name_replacement_does_not_bypass_identifier_protection() {
    let table = crate::transaction::tests::make_v2_table();
    let error = commit_error(
        &table,
        Transaction::new(&table)
            .update_schema()
            .delete_column("x")
            .add_column(AddColumn::required(
                "x",
                Type::Primitive(PrimitiveType::Long),
                crate::spec::Literal::long(0),
            )),
    )
    .await;

    assert_eq!(error.kind(), ErrorKind::PreconditionFailed);
    assert!(error.message().contains("Cannot delete identifier field"));
}

#[tokio::test]
async fn rejects_collection_pseudo_field_deletions() {
    let table = make_v2_table_with_nested();
    for (name, expected) in [
        ("tags.element", "Cannot delete element type from list"),
        ("props.key", "Cannot delete map keys"),
        ("props.value", "Cannot delete value type from map"),
    ] {
        let error = commit_error(
            &table,
            Transaction::new(&table).update_schema().delete_column(name),
        )
        .await;
        assert!(
            error.message().contains(expected),
            "unexpected error for {name}: {error}"
        );
    }

    let nested_delete_under_deleted_list = commit_error(
        &table,
        Transaction::new(&table)
            .update_schema()
            .delete_column("tags")
            .delete_column("tags.element"),
    )
    .await;
    assert!(
        nested_delete_under_deleted_list
            .message()
            .contains("Cannot delete element type from list")
    );
}

#[tokio::test]
async fn rejects_additions_to_map_keys() {
    let table = table_with_struct_map_key();
    let error = commit_error(
        &table,
        Transaction::new(&table).update_schema().add_column(
            AddColumn::builder()
                .parent("indexed.key")
                .name("extra")
                .field_type(Type::Primitive(PrimitiveType::Long))
                .build(),
        ),
    )
    .await;

    assert_eq!(error.kind(), ErrorKind::PreconditionFailed);
    assert!(error.message().contains("Cannot add fields to map keys"));

    let under_deleted_map = commit_error(
        &table,
        Transaction::new(&table)
            .update_schema()
            .delete_column("indexed")
            .add_column(
                AddColumn::builder()
                    .parent("indexed.key")
                    .name("extra")
                    .field_type(Type::Primitive(PrimitiveType::Long))
                    .build(),
            ),
    )
    .await;
    assert!(
        under_deleted_map
            .message()
            .contains("Cannot add fields to map keys")
    );
}

#[tokio::test]
async fn handles_dotted_names_like_java_overloads() {
    let table = make_v2_table_with_nested();
    let empty_root_error = commit_error(
        &table,
        Transaction::new(&table)
            .update_schema()
            .add_column(AddColumn::optional("", Type::Primitive(PrimitiveType::Int))),
    )
    .await;
    assert_eq!(empty_root_error.message(), "Invalid column name: (empty)");

    let root_error = commit_error(
        &table,
        Transaction::new(&table)
            .update_schema()
            .add_column(AddColumn::optional(
                "dot.field",
                Type::Primitive(PrimitiveType::Int),
            )),
    )
    .await;
    assert!(root_error.message().contains("ambiguous name"));

    let schema = updated_schema(
        &table,
        Transaction::new(&table).update_schema().add_column(
            AddColumn::builder()
                .parent("person")
                .name("dot.field")
                .field_type(Type::Primitive(PrimitiveType::Int))
                .build(),
        ),
    )
    .await;
    assert_eq!(schema.field_by_name("person.dot.field").unwrap().id, 15);

    let empty_leaf_schema = updated_schema(
        &table,
        Transaction::new(&table).update_schema().add_column(
            AddColumn::builder()
                .parent("person")
                .name("")
                .field_type(Type::Primitive(PrimitiveType::Int))
                .build(),
        ),
    )
    .await;
    assert_eq!(empty_leaf_schema.field_by_name("person.").unwrap().id, 15);

    let collision = commit_error(
        &table,
        Transaction::new(&table).update_schema().add_column(
            AddColumn::builder()
                .parent("tags")
                .name("element.key")
                .field_type(Type::Primitive(PrimitiveType::String))
                .build(),
        ),
    )
    .await;
    assert!(collision.message().contains("already exists"));
}

#[tokio::test]
async fn canonicalizes_collection_parent_paths() {
    let table = make_v2_table_with_nested();
    let list_schema = updated_schema(
        &table,
        Transaction::new(&table).update_schema().add_column(
            AddColumn::builder()
                .parent("tags")
                .name("score")
                .field_type(Type::Primitive(PrimitiveType::Double))
                .build(),
        ),
    )
    .await;
    assert_eq!(list_schema.name_by_field_id(15), Some("tags.element.score"));

    let map_schema = updated_schema(
        &table,
        Transaction::new(&table).update_schema().add_column(
            AddColumn::builder()
                .parent("props")
                .name("version")
                .field_type(Type::Primitive(PrimitiveType::Int))
                .build(),
        ),
    )
    .await;
    assert_eq!(map_schema.name_by_field_id(15), Some("props.value.version"));
}
