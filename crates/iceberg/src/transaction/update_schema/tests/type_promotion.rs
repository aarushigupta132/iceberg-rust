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

use super::apply::PendingSchemaUpdate;
use super::ordered_tests::table_with_struct_map_key;
use super::tests::make_v2_table_with_nested;
use super::{AddColumn, SchemaOperation, UpdateSchemaAction};
use crate::spec::{
    ListType, Literal, MapType, NestedField, NestedFieldRef, PrimitiveType, Schema, Type,
};
use crate::table::Table;
use crate::transaction::Transaction;
use crate::transaction::action::TransactionAction;
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

fn table_with_fields(fields: impl IntoIterator<Item = NestedFieldRef>) -> Table {
    let schema = Schema::builder().with_fields(fields).build().unwrap();
    replace_current_schema(&make_v2_table_with_nested(), schema)
}

fn table_with_primitive(primitive: PrimitiveType) -> Table {
    table_with_fields([NestedField::optional(1, "value", Type::Primitive(primitive)).into()])
}

#[test]
fn allows_only_the_standard_primitive_promotions() {
    let primitives = [
        PrimitiveType::Boolean,
        PrimitiveType::Int,
        PrimitiveType::Long,
        PrimitiveType::Float,
        PrimitiveType::Double,
        PrimitiveType::Decimal {
            precision: 9,
            scale: 2,
        },
        PrimitiveType::Decimal {
            precision: 18,
            scale: 2,
        },
        PrimitiveType::Decimal {
            precision: 9,
            scale: 3,
        },
        PrimitiveType::Date,
        PrimitiveType::Time,
        PrimitiveType::Timestamp,
        PrimitiveType::Timestamptz,
        PrimitiveType::TimestampNs,
        PrimitiveType::TimestamptzNs,
        PrimitiveType::String,
        PrimitiveType::Uuid,
        PrimitiveType::Fixed(3),
        PrimitiveType::Fixed(4),
        PrimitiveType::Binary,
    ];

    for from in &primitives {
        let base_schema = Schema::builder()
            .with_fields([NestedField::optional(1, "value", Type::Primitive(from.clone())).into()])
            .build()
            .unwrap();
        for to in &primitives {
            let mut pending = PendingSchemaUpdate::new(&base_schema, 1);
            let result = pending.apply_operations(&[SchemaOperation::UpdateType {
                name: "value".to_string(),
                new_type: to.clone(),
            }]);
            let allowed = matches!(
                (from, to),
                (PrimitiveType::Int, PrimitiveType::Long)
                    | (PrimitiveType::Float, PrimitiveType::Double)
                    | (
                        PrimitiveType::Decimal {
                            precision: 9,
                            scale: 2,
                        },
                        PrimitiveType::Decimal {
                            precision: 18,
                            scale: 2,
                        },
                    )
            );

            if from == to {
                result
                    .unwrap_or_else(|error| panic!("exact update {from} -> {to} failed: {error}"));
                assert!(pending.updates().is_empty(), "{from} -> {to}");
            } else if allowed {
                result.unwrap_or_else(|error| {
                    panic!("allowed promotion {from} -> {to} failed: {error}")
                });
                let schema = pending.apply().unwrap();
                assert_eq!(
                    schema.field_by_name("value").unwrap().field_type.as_ref(),
                    &Type::Primitive(to.clone())
                );
            } else {
                let error = match result {
                    Err(error) => error,
                    Ok(()) => panic!("unexpectedly allowed {from} -> {to}"),
                };
                assert_eq!(error.kind(), ErrorKind::PreconditionFailed);
                assert_eq!(
                    error.message(),
                    format!("Cannot change column type: value: {from} -> {to}")
                );
            }
        }
    }
}

#[test]
fn preserves_field_metadata_and_casts_both_defaults() {
    let base_schema = Schema::builder()
        .with_fields([
            NestedField::required(1, "count", Type::Primitive(PrimitiveType::Int))
                .with_doc("number of records")
                .with_initial_default(Literal::int(34))
                .with_write_default(Literal::int(35))
                .into(),
            NestedField::optional(2, "ratio", Type::Primitive(PrimitiveType::Float))
                .with_doc("ratio")
                .with_initial_default(Literal::float(1.25_f32))
                .with_write_default(Literal::float(2.5_f32))
                .into(),
            NestedField::required(
                3,
                "amount",
                Type::Primitive(PrimitiveType::Decimal {
                    precision: 9,
                    scale: 2,
                }),
            )
            .with_doc("amount")
            .with_initial_default(Literal::decimal(1234))
            .with_write_default(Literal::decimal(5678))
            .into(),
        ])
        .build()
        .unwrap();
    let mut pending = PendingSchemaUpdate::new(&base_schema, 3);
    pending
        .apply_operations(&[
            SchemaOperation::UpdateType {
                name: "count".to_string(),
                new_type: PrimitiveType::Long,
            },
            SchemaOperation::UpdateType {
                name: "ratio".to_string(),
                new_type: PrimitiveType::Double,
            },
            SchemaOperation::UpdateType {
                name: "amount".to_string(),
                new_type: PrimitiveType::Decimal {
                    precision: 18,
                    scale: 2,
                },
            },
        ])
        .unwrap();
    let schema = pending.apply().unwrap();

    let count = schema.field_by_name("count").unwrap();
    assert_eq!(count.id, 1);
    assert!(count.required);
    assert_eq!(count.doc.as_deref(), Some("number of records"));
    assert_eq!(count.initial_default, Some(Literal::long(34)));
    assert_eq!(count.write_default, Some(Literal::long(35)));

    let ratio = schema.field_by_name("ratio").unwrap();
    assert_eq!(ratio.id, 2);
    assert!(!ratio.required);
    assert_eq!(ratio.doc.as_deref(), Some("ratio"));
    assert_eq!(ratio.initial_default, Some(Literal::double(1.25_f64)));
    assert_eq!(ratio.write_default, Some(Literal::double(2.5_f64)));

    let amount = schema.field_by_name("amount").unwrap();
    assert_eq!(amount.id, 3);
    assert!(amount.required);
    assert_eq!(amount.doc.as_deref(), Some("amount"));
    assert_eq!(amount.initial_default, Some(Literal::decimal(1234)));
    assert_eq!(amount.write_default, Some(Literal::decimal(5678)));

    let encoded = serde_json::to_string(&schema).unwrap();
    let decoded: Schema = serde_json::from_str(&encoded).unwrap();
    assert_eq!(
        decoded.field_by_name("count").unwrap().initial_default,
        Some(Literal::long(34))
    );
    assert_eq!(
        decoded.field_by_name("ratio").unwrap().write_default,
        Some(Literal::double(2.5_f64))
    );
    assert_eq!(
        decoded.field_by_name("amount").unwrap().write_default,
        Some(Literal::decimal(5678))
    );
}

#[tokio::test]
async fn updates_nested_and_renamed_fields_by_original_path() {
    let table = make_v2_table_with_nested();
    let schema = updated_schema(
        &table,
        Transaction::new(&table)
            .update_schema()
            .rename_column("person.age", "years")
            .update_column_type("person.age", PrimitiveType::Long),
    )
    .await;

    let years = schema.field_by_name("person.years").unwrap();
    assert_eq!(years.id, 6);
    assert!(years.required);
    assert_eq!(
        years.field_type.as_ref(),
        &Type::Primitive(PrimitiveType::Long)
    );
}

#[tokio::test]
async fn updates_list_elements_and_map_values() {
    let table = table_with_fields([
        NestedField::optional(
            1,
            "numbers",
            Type::List(ListType {
                element_field: NestedField::list_element(
                    2,
                    Type::Primitive(PrimitiveType::Int),
                    true,
                )
                .into(),
            }),
        )
        .into(),
        NestedField::optional(
            3,
            "lookup",
            Type::Map(MapType {
                key_field: NestedField::map_key_element(4, Type::Primitive(PrimitiveType::String))
                    .into(),
                value_field: NestedField::map_value_element(
                    5,
                    Type::Primitive(PrimitiveType::Float),
                    false,
                )
                .into(),
            }),
        )
        .into(),
    ]);

    let schema = updated_schema(
        &table,
        Transaction::new(&table)
            .update_schema()
            .update_column_type("numbers.element", PrimitiveType::Long)
            .update_column_type("lookup.value", PrimitiveType::Double),
    )
    .await;

    let element = schema.field_by_name("numbers.element").unwrap();
    assert_eq!(element.id, 2);
    assert!(element.required);
    assert_eq!(
        element.field_type.as_ref(),
        &Type::Primitive(PrimitiveType::Long)
    );
    let value = schema.field_by_name("lookup.value").unwrap();
    assert_eq!(value.id, 5);
    assert!(!value.required);
    assert_eq!(
        value.field_type.as_ref(),
        &Type::Primitive(PrimitiveType::Double)
    );
}

#[tokio::test]
async fn rejects_map_key_updates_during_structural_rebuild() {
    let table = table_with_fields([NestedField::optional(
        1,
        "lookup",
        Type::Map(MapType {
            key_field: NestedField::map_key_element(2, Type::Primitive(PrimitiveType::Int)).into(),
            value_field: NestedField::map_value_element(
                3,
                Type::Primitive(PrimitiveType::String),
                false,
            )
            .into(),
        }),
    )
    .into()]);

    let error = commit_error(
        &table,
        Transaction::new(&table)
            .update_schema()
            .update_column_type("lookup.key", PrimitiveType::Long),
    )
    .await;
    assert_eq!(error.kind(), ErrorKind::PreconditionFailed);
    assert_eq!(error.message(), "Cannot update map keys: lookup");

    let mut no_op = Arc::new(
        Transaction::new(&table)
            .update_schema()
            .update_column_type("lookup.key", PrimitiveType::Int),
    )
    .commit(&table)
    .await
    .unwrap();
    assert!(no_op.take_updates().is_empty());
    assert!(no_op.take_requirements().is_empty());

    let struct_key_table = table_with_struct_map_key();
    let nested = commit_error(
        &struct_key_table,
        Transaction::new(&struct_key_table)
            .update_schema()
            .update_column_type("indexed.key.part", PrimitiveType::Long),
    )
    .await;
    assert_eq!(nested.kind(), ErrorKind::PreconditionFailed);
    assert_eq!(nested.message(), "Cannot alter map keys: indexed");
}

#[tokio::test]
async fn updates_root_and_nested_pending_additions() {
    let table = make_v2_table_with_nested();
    let schema = updated_schema(
        &table,
        Transaction::new(&table)
            .update_schema()
            .add_column(AddColumn::required(
                "count",
                Type::Primitive(PrimitiveType::Int),
                Literal::int(7),
            ))
            .update_column_type("count", PrimitiveType::Long)
            .add_column(
                AddColumn::builder()
                    .parent("person")
                    .name("score")
                    .field_type(Type::Primitive(PrimitiveType::Float))
                    .build(),
            )
            .update_column_type("person.score", PrimitiveType::Double),
    )
    .await;

    let count = schema.field_by_name("count").unwrap();
    assert_eq!(
        count.field_type.as_ref(),
        &Type::Primitive(PrimitiveType::Long)
    );
    assert_eq!(count.initial_default, Some(Literal::long(7)));
    assert_eq!(count.write_default, Some(Literal::long(7)));
    let score = schema.field_by_name("person.score").unwrap();
    assert_eq!(
        score.field_type.as_ref(),
        &Type::Primitive(PrimitiveType::Double)
    );
}

#[tokio::test]
async fn resolves_pending_additions_using_the_mode_at_add_and_lookup_time() {
    let table = crate::transaction::tests::make_v2_table();
    let insensitive = updated_schema(
        &table,
        Transaction::new(&table)
            .update_schema()
            .case_sensitive(false)
            .add_column(AddColumn::optional(
                "MixedCase",
                Type::Primitive(PrimitiveType::Int),
            ))
            .update_column_type("MIXEDCASE", PrimitiveType::Long),
    )
    .await;
    assert_eq!(
        insensitive
            .field_by_name("MixedCase")
            .unwrap()
            .field_type
            .as_ref(),
        &Type::Primitive(PrimitiveType::Long)
    );

    let normalized_key = updated_schema(
        &table,
        Transaction::new(&table)
            .update_schema()
            .case_sensitive(false)
            .add_column(AddColumn::optional(
                "MixedCase",
                Type::Primitive(PrimitiveType::Int),
            ))
            .case_sensitive(true)
            .update_column_type("mixedcase", PrimitiveType::Long),
    )
    .await;
    assert_eq!(
        normalized_key
            .field_by_name("MixedCase")
            .unwrap()
            .field_type
            .as_ref(),
        &Type::Primitive(PrimitiveType::Long)
    );

    let changed_mode = commit_error(
        &table,
        Transaction::new(&table)
            .update_schema()
            .add_column(AddColumn::optional(
                "MixedCase",
                Type::Primitive(PrimitiveType::Int),
            ))
            .case_sensitive(false)
            .update_column_type("MIXEDCASE", PrimitiveType::Long),
    )
    .await;
    assert_eq!(
        changed_mode.message(),
        "Cannot update missing column: MIXEDCASE"
    );
}

#[tokio::test]
async fn base_fields_take_precedence_over_same_named_additions() {
    let table = crate::transaction::tests::make_v2_table();
    let error = commit_error(
        &table,
        Transaction::new(&table)
            .update_schema()
            .delete_column("z")
            .add_column(AddColumn::optional(
                "z",
                Type::Primitive(PrimitiveType::Int),
            ))
            .update_column_type("z", PrimitiveType::Long),
    )
    .await;

    assert_eq!(
        error.message(),
        "Cannot update a column that will be deleted: z"
    );
}

#[tokio::test]
async fn duplicate_pending_additions_are_validated_only_in_the_final_schema() {
    let table = crate::transaction::tests::make_v2_table();
    let error = commit_error(
        &table,
        Transaction::new(&table)
            .update_schema()
            .add_column(AddColumn::optional(
                "duplicate",
                Type::Primitive(PrimitiveType::Int),
            ))
            .add_column(AddColumn::optional(
                "duplicate",
                Type::Primitive(PrimitiveType::Int),
            ))
            .update_column_type("duplicate", PrimitiveType::Long),
    )
    .await;

    assert_eq!(error.kind(), ErrorKind::PreconditionFailed);
    assert_eq!(error.message(), "Cannot apply schema update");
}

#[tokio::test]
async fn checks_deletion_conflicts_before_exact_no_ops() {
    let table = make_v2_table_with_nested();

    let schema = updated_schema(
        &table,
        Transaction::new(&table)
            .update_schema()
            .update_column_type("z", PrimitiveType::Long)
            .delete_column("z"),
    )
    .await;
    assert!(schema.field_by_name("z").is_none());

    let delete_then_no_op = commit_error(
        &table,
        Transaction::new(&table)
            .update_schema()
            .delete_column("z")
            .update_column_type("z", PrimitiveType::Long),
    )
    .await;
    assert_eq!(
        delete_then_no_op.message(),
        "Cannot update a column that will be deleted: z"
    );

    let update_then_delete = commit_error(
        &table,
        Transaction::new(&table)
            .update_schema()
            .update_column_type("person.age", PrimitiveType::Long)
            .delete_column("person.age"),
    )
    .await;
    assert_eq!(
        update_then_delete.message(),
        "Cannot delete a column that has updates: person.age"
    );
}

#[tokio::test]
async fn rejects_missing_and_non_primitive_targets() {
    let table = make_v2_table_with_nested();
    let missing = commit_error(
        &table,
        Transaction::new(&table)
            .update_schema()
            .update_column_type("missing", PrimitiveType::Long),
    )
    .await;
    assert_eq!(missing.message(), "Cannot update missing column: missing");

    let nested = commit_error(
        &table,
        Transaction::new(&table)
            .update_schema()
            .update_column_type("person", PrimitiveType::Long),
    )
    .await;
    assert_eq!(nested.kind(), ErrorKind::PreconditionFailed);
    assert!(
        nested
            .message()
            .starts_with("Cannot change column type: person: struct<")
    );
}

#[tokio::test]
async fn rejects_invalid_decimal_target_precision_without_defaults() {
    let table = table_with_primitive(PrimitiveType::Decimal {
        precision: 9,
        scale: 2,
    });
    let error = commit_error(
        &table,
        Transaction::new(&table).update_schema().update_column_type(
            "value",
            PrimitiveType::Decimal {
                precision: 39,
                scale: 2,
            },
        ),
    )
    .await;

    assert_eq!(error.kind(), ErrorKind::PreconditionFailed);
    assert_eq!(
        error.message(),
        "Cannot change column type: value: decimal(9, 2) -> decimal(39, 2)"
    );
}

#[tokio::test]
async fn applies_case_sensitivity_in_operation_order() {
    let table = make_v2_table_with_nested();
    let schema = updated_schema(
        &table,
        Transaction::new(&table)
            .update_schema()
            .case_sensitive(false)
            .update_column_type("PERSON.AGE", PrimitiveType::Long),
    )
    .await;
    assert_eq!(
        schema
            .field_by_name("person.age")
            .unwrap()
            .field_type
            .as_ref(),
        &Type::Primitive(PrimitiveType::Long)
    );

    let sensitive = commit_error(
        &table,
        Transaction::new(&table)
            .update_schema()
            .case_sensitive(false)
            .case_sensitive(true)
            .update_column_type("PERSON.AGE", PrimitiveType::Long),
    )
    .await;
    assert_eq!(
        sensitive.message(),
        "Cannot update missing column: PERSON.AGE"
    );
}

#[tokio::test]
async fn case_insensitive_lookup_rejects_unrelated_ambiguity() {
    let table = table_with_fields([
        NestedField::optional(1, "target", Type::Primitive(PrimitiveType::Int)).into(),
        NestedField::optional(2, "Foo", Type::Primitive(PrimitiveType::String)).into(),
        NestedField::optional(3, "foo", Type::Primitive(PrimitiveType::String)).into(),
    ]);

    let error = commit_error(
        &table,
        Transaction::new(&table)
            .update_schema()
            .case_sensitive(false)
            .update_column_type("TARGET", PrimitiveType::Long),
    )
    .await;
    assert_eq!(error.kind(), ErrorKind::PreconditionFailed);
    assert!(error.message().contains("multiple fields match: foo"));
}

#[tokio::test]
async fn replays_type_updates_against_refreshed_metadata() {
    let table = table_with_primitive(PrimitiveType::Int);
    let action = Arc::new(
        Transaction::new(&table)
            .update_schema()
            .update_column_type("value", PrimitiveType::Long),
    );

    let mut first = action.clone().commit(&table).await.unwrap();
    let first_schema = match first.take_updates().remove(0) {
        TableUpdate::AddSchema { schema } => schema,
        update => panic!("expected AddSchema, got {update:?}"),
    };
    assert_eq!(
        first_schema
            .field_by_name("value")
            .unwrap()
            .field_type
            .as_ref(),
        &Type::Primitive(PrimitiveType::Long)
    );

    let refreshed = replace_current_schema(
        &table,
        Schema::builder()
            .with_fields([
                NestedField::optional(1, "value", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::optional(2, "concurrent", Type::Primitive(PrimitiveType::Boolean))
                    .into(),
            ])
            .build()
            .unwrap(),
    );
    let mut replayed = action.clone().commit(&refreshed).await.unwrap();
    let replayed_schema = match replayed.take_updates().remove(0) {
        TableUpdate::AddSchema { schema } => schema,
        update => panic!("expected AddSchema, got {update:?}"),
    };
    assert_eq!(
        replayed_schema
            .field_by_name("value")
            .unwrap()
            .field_type
            .as_ref(),
        &Type::Primitive(PrimitiveType::Long)
    );
    assert!(replayed_schema.field_by_name("concurrent").is_some());

    let incompatible = table_with_primitive(PrimitiveType::Float);
    let error = match action.clone().commit(&incompatible).await {
        Err(error) => error,
        Ok(_) => panic!("replayed promotion should use the refreshed source type"),
    };
    assert_eq!(
        error.message(),
        "Cannot change column type: value: float -> long"
    );

    let already_promoted = table_with_primitive(PrimitiveType::Long);
    let mut no_op = action.commit(&already_promoted).await.unwrap();
    assert!(no_op.take_updates().is_empty());
    assert!(no_op.take_requirements().is_empty());
}

#[tokio::test]
async fn exact_type_update_alone_is_a_no_op() {
    let table = crate::transaction::tests::make_v2_table();
    let mut commit = Arc::new(
        Transaction::new(&table)
            .update_schema()
            .update_column_type("z", PrimitiveType::Long),
    )
    .commit(&table)
    .await
    .unwrap();

    assert!(commit.take_updates().is_empty());
    assert!(commit.take_requirements().is_empty());
}
