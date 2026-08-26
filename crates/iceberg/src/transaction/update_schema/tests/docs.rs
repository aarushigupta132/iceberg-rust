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

use super::ordered_tests::{replace_current_schema, table_with_struct_map_key};
use super::tests::make_v2_table_with_nested;
use super::{AddColumn, UpdateSchemaAction};
use crate::catalog::MockCatalog;
use crate::spec::{Literal, PrimitiveType, Schema, Type};
use crate::table::Table;
use crate::transaction::action::TransactionAction;
use crate::transaction::{ApplyTransactionAction, Transaction};
use crate::{Error, ErrorKind, TableUpdate};

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
async fn sets_clears_and_merges_docs_without_changing_other_metadata() {
    let table = crate::transaction::tests::make_v2_table();
    let original = table
        .metadata()
        .current_schema()
        .field_by_name("y")
        .unwrap();

    let updated = updated_schema(
        &table,
        Transaction::new(&table)
            .update_schema()
            .update_column_doc("y", Some("first".to_string()))
            .update_column_doc("y", Some("replacement".to_string())),
    )
    .await;
    let mut expected = (**original).clone();
    expected.doc = Some("replacement".to_string());
    assert_eq!(updated.field_by_name("y").unwrap().as_ref(), &expected);

    let updated_table = replace_current_schema(&table, updated);
    let cleared = updated_schema(
        &updated_table,
        Transaction::new(&updated_table)
            .update_schema()
            .update_column_doc("y", None),
    )
    .await;
    expected.doc = None;
    assert_eq!(cleared.field_by_name("y").unwrap().as_ref(), &expected);
    assert_eq!(
        cleared.identifier_field_ids().collect::<HashSet<_>>(),
        table
            .metadata()
            .current_schema()
            .identifier_field_ids()
            .collect::<HashSet<_>>()
    );
}

#[tokio::test]
async fn updates_new_columns_using_canonical_added_names() {
    let table = make_v2_table_with_nested();
    let action = Transaction::new(&table)
        .update_schema()
        .add_column(AddColumn::required(
            "required_col",
            Type::Primitive(PrimitiveType::Long),
            Literal::long(7),
        ))
        .update_column_doc("required_col", Some("root docs".to_string()))
        .add_column(
            AddColumn::builder()
                .parent("tags")
                .name("score")
                .field_type(Type::Primitive(PrimitiveType::Double))
                .build(),
        )
        .update_column_doc("tags.element.score", Some("list docs".to_string()))
        .add_column(
            AddColumn::builder()
                .parent("props")
                .name("version")
                .field_type(Type::Primitive(PrimitiveType::Int))
                .build(),
        )
        .update_column_doc("props.value.version", Some("map docs".to_string()));

    let schema = updated_schema(&table, action).await;
    let required = schema.field_by_name("required_col").unwrap();
    assert_eq!(required.id, 15);
    assert!(required.required);
    assert_eq!(required.initial_default, Some(Literal::long(7)));
    assert_eq!(required.write_default, Some(Literal::long(7)));
    assert_eq!(required.doc.as_deref(), Some("root docs"));
    assert_eq!(
        schema
            .field_by_name("tags.element.score")
            .unwrap()
            .doc
            .as_deref(),
        Some("list docs")
    );
    assert_eq!(
        schema
            .field_by_name("props.value.version")
            .unwrap()
            .doc
            .as_deref(),
        Some("map docs")
    );

    let short_name_error = commit_error(
        &table,
        Transaction::new(&table)
            .update_schema()
            .add_column(
                AddColumn::builder()
                    .parent("tags")
                    .name("score")
                    .field_type(Type::Primitive(PrimitiveType::Double))
                    .build(),
            )
            .update_column_doc("tags.score", Some("docs".to_string())),
    )
    .await;
    assert_eq!(short_name_error.kind(), ErrorKind::PreconditionFailed);
    assert!(
        short_name_error
            .message()
            .contains("Cannot update missing column: tags.score")
    );
}

#[tokio::test]
async fn enforces_ordered_missing_delete_and_update_conflicts() {
    let table = crate::transaction::tests::make_v2_table();

    let before_add = commit_error(
        &table,
        Transaction::new(&table)
            .update_schema()
            .update_column_doc("later", Some("docs".to_string()))
            .add_column(AddColumn::optional(
                "later",
                Type::Primitive(PrimitiveType::String),
            )),
    )
    .await;
    assert!(before_add.message().contains("missing column: later"));

    let delete_then_update = commit_error(
        &table,
        Transaction::new(&table)
            .update_schema()
            .delete_column("z")
            .update_column_doc("z", None),
    )
    .await;
    assert!(
        delete_then_update
            .message()
            .contains("column that will be deleted: z")
    );

    let update_then_delete = commit_error(
        &table,
        Transaction::new(&table)
            .update_schema()
            .update_column_doc("z", Some("docs".to_string()))
            .delete_column("z"),
    )
    .await;
    assert!(
        update_then_delete
            .message()
            .contains("column that has updates: z")
    );

    let reverted_then_delete = commit_error(
        &table,
        Transaction::new(&table)
            .update_schema()
            .update_column_doc("z", Some("temporary".to_string()))
            .update_column_doc("z", None)
            .delete_column("z"),
    )
    .await;
    assert!(
        reverted_then_delete
            .message()
            .contains("column that has updates: z")
    );

    let deleted = updated_schema(
        &table,
        Transaction::new(&table)
            .update_schema()
            .update_column_doc("z", None)
            .delete_column("z"),
    )
    .await;
    assert!(deleted.field_by_name("z").is_none());
}

#[tokio::test]
async fn existing_field_lookup_wins_over_same_name_replacement() {
    let table = crate::transaction::tests::make_v2_table();
    let error = commit_error(
        &table,
        Transaction::new(&table)
            .update_schema()
            .delete_column("z")
            .add_column(AddColumn::optional(
                "z",
                Type::Primitive(PrimitiveType::String),
            ))
            .update_column_doc("z", Some("replacement docs".to_string())),
    )
    .await;

    assert!(
        error
            .message()
            .contains("Cannot update a column that will be deleted: z")
    );
}

#[tokio::test]
async fn no_op_doc_updates_skip_schema_and_catalog_commits() {
    let table = crate::transaction::tests::make_v2_table();
    let unchanged = Transaction::new(&table)
        .update_schema()
        .update_column_doc("z", None);
    let mut direct_commit = Arc::new(unchanged).commit(&table).await.unwrap();
    assert!(direct_commit.take_updates().is_empty());
    assert!(direct_commit.take_requirements().is_empty());

    let reverted = Transaction::new(&table)
        .update_schema()
        .update_column_doc("z", Some("temporary".to_string()))
        .update_column_doc("z", None);
    let mut reverted_commit = Arc::new(reverted).commit(&table).await.unwrap();
    assert!(reverted_commit.take_updates().is_empty());
    assert!(reverted_commit.take_requirements().is_empty());

    let refreshed = table.clone();
    let mut catalog = MockCatalog::new();
    catalog
        .expect_load_table()
        .once()
        .returning_st(move |_| Box::pin(std::future::ready(Ok(refreshed.clone()))));
    catalog.expect_update_table().never();
    let tx = Transaction::new(&table);
    let tx = tx
        .update_schema()
        .update_column_doc("z", None)
        .apply(tx)
        .unwrap();
    let committed = tx.commit(&catalog).await.unwrap();
    assert_eq!(committed.metadata(), table.metadata());
}

#[tokio::test]
async fn rejects_map_key_doc_updates_but_allows_true_no_ops() {
    let table = make_v2_table_with_nested();
    let direct_key_error = commit_error(
        &table,
        Transaction::new(&table)
            .update_schema()
            .update_column_doc("props.key", Some("docs".to_string())),
    )
    .await;
    assert!(
        direct_key_error
            .message()
            .contains("Cannot update map keys")
    );

    let mut no_op = Arc::new(
        Transaction::new(&table)
            .update_schema()
            .update_column_doc("props.key", None),
    )
    .commit(&table)
    .await
    .unwrap();
    assert!(no_op.take_updates().is_empty());

    let struct_key_table = table_with_struct_map_key();
    let nested_key_error = commit_error(
        &struct_key_table,
        Transaction::new(&struct_key_table)
            .update_schema()
            .update_column_doc("indexed.key.part", Some("docs".to_string())),
    )
    .await;
    assert!(nested_key_error.message().contains("Cannot alter map keys"));

    let deleted_outer_error = commit_error(
        &struct_key_table,
        Transaction::new(&struct_key_table)
            .update_schema()
            .delete_column("indexed")
            .update_column_doc("indexed.key.part", Some("docs".to_string())),
    )
    .await;
    assert!(
        deleted_outer_error
            .message()
            .contains("Cannot alter map keys")
    );
}

#[tokio::test]
async fn collection_pseudo_field_docs_follow_java_no_op_semantics() {
    let table = make_v2_table_with_nested();
    for name in ["tags.element", "props.value"] {
        let mut commit = Arc::new(
            Transaction::new(&table)
                .update_schema()
                .update_column_doc(name, Some("docs".to_string())),
        )
        .commit(&table)
        .await
        .unwrap();
        assert!(
            commit.take_updates().is_empty(),
            "unexpected update for {name}"
        );
        assert!(
            commit.take_requirements().is_empty(),
            "unexpected requirement for {name}"
        );
    }
}

#[tokio::test]
async fn doc_updates_under_deleted_ancestors_are_dropped() {
    let table = make_v2_table_with_nested();

    let deleted_nested_error = commit_error(
        &table,
        Transaction::new(&table)
            .update_schema()
            .delete_column("person.name")
            .update_column_doc("person.name", Some("docs".to_string())),
    )
    .await;
    assert_eq!(
        deleted_nested_error.message(),
        "Cannot update a column that will be deleted: name"
    );

    let delete_then_update = updated_schema(
        &table,
        Transaction::new(&table)
            .update_schema()
            .delete_column("person")
            .update_column_doc("person.name", Some("docs".to_string())),
    )
    .await;
    assert!(delete_then_update.field_by_name("person").is_none());

    let update_then_delete = updated_schema(
        &table,
        Transaction::new(&table)
            .update_schema()
            .update_column_doc("person.name", Some("docs".to_string()))
            .delete_column("person"),
    )
    .await;
    assert!(update_then_delete.field_by_name("person").is_none());
}

#[tokio::test]
async fn empty_update_name_is_invalid() {
    let table = crate::transaction::tests::make_v2_table();
    let error = commit_error(
        &table,
        Transaction::new(&table)
            .update_schema()
            .update_column_doc("", None),
    )
    .await;
    assert_eq!(error.message(), "Invalid column name: (empty)");
}
