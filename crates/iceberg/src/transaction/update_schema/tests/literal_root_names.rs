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
use crate::spec::{Literal, PrimitiveType, Schema, Type};
use crate::table::Table;
use crate::transaction::Transaction;
use crate::transaction::action::TransactionAction;
use crate::{Error, TableUpdate};

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

#[tokio::test]
async fn ordinary_root_additions_still_reject_ambiguous_dotted_names() {
    let table = make_v2_table_with_nested();
    let error = commit_error(
        &table,
        update(&table).add_column(AddColumn::optional(
            "dot.field",
            Type::Primitive(PrimitiveType::String),
        )),
    )
    .await;

    assert_eq!(
        error.message(),
        "Cannot add column with ambiguous name: dot.field, set a parent to add a nested column"
    );
}

#[tokio::test]
async fn adds_required_literal_dotted_root_name_like_java_explicit_parent() {
    let table = make_v2_table_with_nested();
    let column = AddColumn::builder()
        .name("dot.field")
        .required(true)
        .field_type(Type::Primitive(PrimitiveType::String))
        .build()
        .with_literal_root_name();
    let schema = updated_schema(
        &table,
        update(&table)
            .allow_incompatible_changes()
            .add_column(column)
            .set_identifier_fields(["x", "dot.field"]),
    )
    .await;

    let field = schema.field_by_id(15).unwrap();
    assert_eq!(field.name, "dot.field");
    assert!(field.required);
    assert_eq!(
        schema.identifier_field_ids().collect::<HashSet<_>>(),
        HashSet::from([1, 15])
    );
}

#[tokio::test]
async fn literal_root_modifier_composes_with_builder_metadata() {
    let table = make_v2_table_with_nested();
    let column = AddColumn::builder()
        .name("metrics.value")
        .field_type(Type::Primitive(PrimitiveType::Int))
        .doc("metric")
        .write_default(Literal::long(7))
        .build()
        .with_literal_root_name();
    let schema = updated_schema(&table, update(&table).add_column(column)).await;

    let field = schema.field_by_id(15).unwrap();
    assert_eq!(field.name, "metrics.value");
    assert!(!field.required);
    assert_eq!(field.doc.as_deref(), Some("metric"));
    assert_eq!(field.initial_default, None);
    assert_eq!(field.write_default, Some(Literal::int(7)));
}
