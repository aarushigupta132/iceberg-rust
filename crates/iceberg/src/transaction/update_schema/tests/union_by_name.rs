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

use super::UpdateSchemaAction;
use super::tests::make_v2_table_with_nested;
use crate::spec::{
    FormatVersion, ListType, Literal, MapType, NestedField, NestedFieldRef, PrimitiveType, Schema,
    StructType, Type, VariantType,
};
use crate::table::Table;
use crate::transaction::Transaction;
use crate::transaction::action::TransactionAction;
use crate::{Error, ErrorKind, TableUpdate};

fn update(table: &Table) -> UpdateSchemaAction {
    Transaction::new(table).update_schema()
}

fn schema(fields: impl IntoIterator<Item = NestedFieldRef>) -> Schema {
    Schema::builder().with_fields(fields).build().unwrap()
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

fn replace_current_schema(table: &Table, schema: Schema, v3: bool) -> Table {
    let mut builder = table.metadata().clone().into_builder(None);
    if v3 {
        builder = builder.upgrade_format_version(FormatVersion::V3).unwrap();
    }
    let metadata = builder
        .add_schema(schema)
        .unwrap()
        .set_current_schema(-1)
        .unwrap()
        .build()
        .unwrap()
        .metadata;
    table.clone().with_metadata(Arc::new(metadata))
}

fn table_with_schema(schema: Schema) -> Table {
    replace_current_schema(&make_v2_table_with_nested(), schema, false)
}

fn table_with_v3_schema(schema: Schema) -> Table {
    replace_current_schema(&make_v2_table_with_nested(), schema, true)
}

#[tokio::test]
async fn assigns_fresh_ids_in_java_postorder_and_adds_dotted_root_names() {
    let table = make_v2_table_with_nested();
    let incoming = schema([
        NestedField::required(100, "new_root", Type::Primitive(PrimitiveType::String)).into(),
        NestedField::optional(101, "literal.dot", Type::Primitive(PrimitiveType::Boolean)).into(),
        NestedField::optional(
            102,
            "person",
            Type::Struct(StructType::new(vec![
                NestedField::optional(103, "name", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::required(104, "age", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::required(105, "country", Type::Primitive(PrimitiveType::String))
                    .into(),
            ])),
        )
        .into(),
    ]);

    let result = updated_schema(&table, update(&table).union_by_name(incoming)).await;
    let country = result.field_by_name("person.country").unwrap();
    let new_root = result.field_by_name("new_root").unwrap();
    assert_eq!(country.id, 15);
    assert_eq!(new_root.id, 16);
    assert!(!country.required);
    assert!(!new_root.required);
    let dotted = result.field_by_id(17).unwrap();
    assert_eq!(dotted.name, "literal.dot");
}

#[tokio::test]
async fn reconciles_fields_inside_lists_and_map_values_but_rejects_map_key_changes() {
    let table = make_v2_table_with_nested();
    let incoming = schema([
        NestedField::optional(
            100,
            "tags",
            Type::List(ListType {
                element_field: NestedField::list_element(
                    101,
                    Type::Struct(StructType::new(vec![
                        NestedField::optional(102, "key", Type::Primitive(PrimitiveType::String))
                            .into(),
                        NestedField::optional(103, "value", Type::Primitive(PrimitiveType::String))
                            .into(),
                        NestedField::required(104, "extra", Type::Primitive(PrimitiveType::Int))
                            .into(),
                    ])),
                    false,
                )
                .into(),
            }),
        )
        .into(),
        NestedField::optional(
            105,
            "props",
            Type::Map(MapType {
                key_field: NestedField::map_key_element(
                    106,
                    Type::Primitive(PrimitiveType::String),
                )
                .into(),
                value_field: NestedField::map_value_element(
                    107,
                    Type::Struct(StructType::new(vec![
                        NestedField::optional(108, "data", Type::Primitive(PrimitiveType::String))
                            .into(),
                        NestedField::optional(109, "extra", Type::Primitive(PrimitiveType::Long))
                            .into(),
                    ])),
                    false,
                )
                .into(),
            }),
        )
        .into(),
    ]);

    let result = updated_schema(&table, update(&table).union_by_name(incoming)).await;
    assert_eq!(result.field_by_name("tags.element.extra").unwrap().id, 15);
    assert_eq!(result.field_by_name("props.value.extra").unwrap().id, 16);
    assert!(!result.field_by_name("tags.element").unwrap().required);
    assert!(!result.field_by_name("props.value").unwrap().required);

    let invalid_key = schema([NestedField::optional(
        200,
        "props",
        Type::Map(MapType {
            key_field: NestedField::map_key_element(201, Type::Primitive(PrimitiveType::Uuid))
                .into(),
            value_field: NestedField::map_value_element(
                202,
                Type::Struct(StructType::new(vec![
                    NestedField::optional(203, "data", Type::Primitive(PrimitiveType::String))
                        .into(),
                ])),
                false,
            )
            .into(),
        }),
    )
    .into()]);
    let error = commit_error(&table, update(&table).union_by_name(invalid_key)).await;
    assert_eq!(error.kind(), ErrorKind::PreconditionFailed);
    assert!(error.message().contains("props.key: string -> uuid"));
}

#[tokio::test]
async fn promotes_wider_types_ignores_narrower_types_and_checks_categories() {
    let base = schema([
        NestedField::required(1, "count", Type::Primitive(PrimitiveType::Int)).into(),
        NestedField::required(2, "ratio", Type::Primitive(PrimitiveType::Double)).into(),
        NestedField::required(
            3,
            "amount",
            Type::Primitive(PrimitiveType::Decimal {
                precision: 20,
                scale: 2,
            }),
        )
        .into(),
        NestedField::optional(
            4,
            "complex",
            Type::Struct(StructType::new(vec![
                NestedField::optional(5, "value", Type::Primitive(PrimitiveType::String)).into(),
            ])),
        )
        .into(),
        NestedField::optional(6, "variant", Type::Variant(VariantType)).into(),
    ]);
    let table = table_with_v3_schema(base);
    let incoming = schema([
        NestedField::required(101, "count", Type::Primitive(PrimitiveType::Long)).into(),
        NestedField::required(102, "ratio", Type::Primitive(PrimitiveType::Float)).into(),
        NestedField::required(
            103,
            "amount",
            Type::Primitive(PrimitiveType::Decimal {
                precision: 22,
                scale: 2,
            }),
        )
        .into(),
        NestedField::optional(104, "complex", Type::Variant(VariantType)).into(),
        NestedField::optional(105, "variant", Type::Variant(VariantType)).into(),
    ]);

    let result = updated_schema(&table, update(&table).union_by_name(incoming)).await;
    assert_eq!(
        result.field_by_name("count").unwrap().field_type.as_ref(),
        &Type::Primitive(PrimitiveType::Long)
    );
    assert_eq!(
        result.field_by_name("ratio").unwrap().field_type.as_ref(),
        &Type::Primitive(PrimitiveType::Double)
    );
    assert_eq!(
        result.field_by_name("amount").unwrap().field_type.as_ref(),
        &Type::Primitive(PrimitiveType::Decimal {
            precision: 22,
            scale: 2,
        })
    );
    assert!(
        result
            .field_by_name("complex")
            .unwrap()
            .field_type
            .is_struct()
    );

    let incompatible =
        schema([
            NestedField::required(200, "count", Type::Primitive(PrimitiveType::String)).into(),
        ]);
    let error = commit_error(&table, update(&table).union_by_name(incompatible)).await;
    assert!(error.message().contains("Cannot change column type"));
}

#[tokio::test]
async fn applies_metadata_after_type_promotion_and_never_clears_existing_metadata() {
    let base = schema([
        NestedField::required(1, "metric", Type::Primitive(PrimitiveType::Int))
            .with_doc("old")
            .with_initial_default(Literal::int(1))
            .with_write_default(Literal::int(2))
            .into(),
        NestedField::optional(2, "keep", Type::Primitive(PrimitiveType::String))
            .with_doc("keep")
            .with_write_default(Literal::string("old"))
            .into(),
    ]);
    let table = table_with_v3_schema(base);
    let incoming = schema([
        NestedField::optional(101, "metric", Type::Primitive(PrimitiveType::Long))
            .with_doc("new")
            .with_initial_default(Literal::long(9))
            .with_write_default(Literal::int(7))
            .into(),
        NestedField::required(102, "keep", Type::Primitive(PrimitiveType::String)).into(),
        NestedField::required(103, "added", Type::Primitive(PrimitiveType::String))
            .with_doc("added")
            .with_initial_default(Literal::string("initial"))
            .with_write_default(Literal::string("write"))
            .into(),
        NestedField::required(
            104,
            "container",
            Type::Struct(StructType::new(vec![
                NestedField::required(105, "child", Type::Primitive(PrimitiveType::Int)).into(),
            ])),
        )
        .into(),
    ]);

    let result = updated_schema(&table, update(&table).union_by_name(incoming)).await;
    let metric = result.field_by_name("metric").unwrap();
    assert!(!metric.required);
    assert_eq!(metric.doc.as_deref(), Some("new"));
    assert_eq!(metric.initial_default, Some(Literal::long(1)));
    assert_eq!(metric.write_default, Some(Literal::long(7)));

    let keep = result.field_by_name("keep").unwrap();
    assert!(!keep.required);
    assert_eq!(keep.doc.as_deref(), Some("keep"));
    assert_eq!(keep.write_default, Some(Literal::string("old")));

    let added = result.field_by_name("added").unwrap();
    assert!(!added.required);
    assert_eq!(added.initial_default, Some(Literal::string("initial")));
    assert_eq!(added.write_default, Some(Literal::string("write")));
    assert!(!result.field_by_name("container").unwrap().required);
    assert!(result.field_by_name("container.child").unwrap().required);
}

#[tokio::test]
async fn captures_case_mode_and_rejects_incoming_case_collisions() {
    let table = table_with_schema(schema([NestedField::required(
        1,
        "Name",
        Type::Primitive(PrimitiveType::Int),
    )
    .into()]));
    let incoming = || {
        schema([NestedField::required(100, "name", Type::Primitive(PrimitiveType::Long)).into()])
    };

    let sensitive = updated_schema(&table, update(&table).union_by_name(incoming())).await;
    assert!(sensitive.field_by_name("Name").is_some());
    assert!(sensitive.field_by_name("name").is_some());

    let insensitive = updated_schema(
        &table,
        update(&table)
            .case_sensitive(false)
            .union_by_name(incoming())
            .case_sensitive(true),
    )
    .await;
    assert!(insensitive.field_by_name("name").is_none());
    assert_eq!(
        insensitive
            .field_by_name("Name")
            .unwrap()
            .field_type
            .as_ref(),
        &Type::Primitive(PrimitiveType::Long)
    );

    let collision = schema([
        NestedField::optional(200, "Foo", Type::Primitive(PrimitiveType::String)).into(),
        NestedField::optional(201, "foo", Type::Primitive(PrimitiveType::String)).into(),
    ]);
    let error = commit_error(
        &table,
        update(&table)
            .case_sensitive(false)
            .union_by_name(collision),
    )
    .await;
    assert!(error.message().contains("multiple fields match: foo"));

    let ambiguous_base = table_with_schema(schema([
        NestedField::optional(300, "Foo", Type::Primitive(PrimitiveType::String)).into(),
        NestedField::optional(301, "foo", Type::Primitive(PrimitiveType::String)).into(),
    ]));
    let error = commit_error(
        &ambiguous_base,
        update(&ambiguous_base)
            .case_sensitive(false)
            .union_by_name(Schema::builder().build().unwrap())
            .case_sensitive(true),
    )
    .await;
    assert!(error.message().contains("multiple fields match: foo"));
}

#[tokio::test]
async fn preserves_or_validates_identifier_fields_from_the_existing_schema_only() {
    let table = make_v2_table_with_nested();
    let incoming = Schema::builder()
        .with_fields([
            NestedField::optional(101, "x", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::required(102, "y", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::required(103, "z", Type::Primitive(PrimitiveType::Long)).into(),
        ])
        .with_identifier_field_ids([103])
        .build()
        .unwrap();

    let error = commit_error(&table, update(&table).union_by_name(incoming.clone())).await;
    assert_eq!(error.message(), "Cannot apply schema update");

    let result = updated_schema(
        &table,
        update(&table)
            .set_identifier_fields(["y"])
            .union_by_name(incoming),
    )
    .await;
    assert!(!result.field_by_name("x").unwrap().required);
    assert_eq!(
        result.identifier_field_ids().collect::<HashSet<_>>(),
        HashSet::from([2])
    );
}

#[tokio::test]
async fn no_ops_skip_commits_and_retry_replays_fresh_ids() {
    let table = make_v2_table_with_nested();
    let narrower_reordered = schema([
        NestedField::required(100, "z", Type::Primitive(PrimitiveType::Int)).into(),
        NestedField::required(101, "x", Type::Primitive(PrimitiveType::Long)).into(),
    ]);
    assert_no_op(
        &table,
        Arc::new(update(&table).union_by_name(narrower_reordered)),
    )
    .await;
    assert_no_op(
        &table,
        Arc::new(update(&table).union_by_name(Schema::builder().build().unwrap())),
    )
    .await;

    let incoming =
        schema([
            NestedField::optional(200, "added", Type::Primitive(PrimitiveType::String)).into(),
        ]);
    let action = Arc::new(update(&table).union_by_name(incoming));
    let mut first = action.clone().commit(&table).await.unwrap();
    let first = match first.take_updates().remove(0) {
        TableUpdate::AddSchema { schema } => schema,
        update => panic!("expected AddSchema, got {update:?}"),
    };
    assert_eq!(first.field_by_name("added").unwrap().id, 15);

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
    let refreshed = replace_current_schema(&table, concurrent, false);
    let mut replayed = action.commit(&refreshed).await.unwrap();
    let replayed = match replayed.take_updates().remove(0) {
        TableUpdate::AddSchema { schema } => schema,
        update => panic!("expected AddSchema, got {update:?}"),
    };
    assert!(replayed.field_by_name("concurrent").is_some());
    assert_eq!(replayed.field_by_name("added").unwrap().id, 16);
}
