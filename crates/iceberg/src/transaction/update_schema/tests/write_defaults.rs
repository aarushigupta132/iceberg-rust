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
use crate::spec::{
    FormatVersion, Literal, NestedField, PrimitiveLiteral, PrimitiveType, Schema, Type,
};
use crate::table::Table;
use crate::transaction::Transaction;
use crate::transaction::action::TransactionAction;
use crate::{Error, ErrorKind, TableRequirement, TableUpdate};

fn update(table: &Table) -> UpdateSchemaAction {
    Transaction::new(table).update_schema()
}

async fn updated_schema(table: &Table, action: UpdateSchemaAction) -> Schema {
    let mut commit = Arc::new(action).commit(table).await.unwrap();
    let updates = commit.take_updates();
    assert_eq!(updates.len(), 2);
    assert!(matches!(updates[1], TableUpdate::SetCurrentSchema { .. }));

    let requirements = commit.take_requirements();
    assert_eq!(requirements.len(), 3);
    assert!(matches!(
        requirements[0],
        TableRequirement::UuidMatch { .. }
    ));
    assert!(matches!(
        requirements[1],
        TableRequirement::LastAssignedFieldIdMatch { .. }
    ));
    assert!(matches!(
        requirements[2],
        TableRequirement::CurrentSchemaIdMatch { .. }
    ));

    match updates.into_iter().next().unwrap() {
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

fn defaulted_table() -> Table {
    let table = make_v2_table_with_nested();
    let mut fields = table
        .metadata()
        .current_schema()
        .as_struct()
        .fields()
        .to_vec();
    let mut field = (*fields[2]).clone();
    field.doc = Some("description".to_string());
    field.initial_default = Some(Literal::long(34));
    field.write_default = Some(Literal::long(35));
    fields[2] = Arc::new(field);
    let schema = Schema::builder()
        .with_fields(fields)
        .with_identifier_field_ids(table.metadata().current_schema().identifier_field_ids())
        .build()
        .unwrap();
    let metadata = table
        .metadata()
        .clone()
        .into_builder(None)
        .upgrade_format_version(FormatVersion::V3)
        .unwrap()
        .add_schema(schema)
        .unwrap()
        .set_current_schema(-1)
        .unwrap()
        .build()
        .unwrap()
        .metadata;
    table.with_metadata(Arc::new(metadata))
}

#[tokio::test]
async fn sets_replaces_and_clears_only_the_write_default() {
    let table = defaulted_table();
    let original = table
        .metadata()
        .current_schema()
        .field_by_name("z")
        .unwrap();

    let replaced = updated_schema(
        &table,
        update(&table).update_column_default("z", Some(Literal::int(123))),
    )
    .await;
    let mut expected = (**original).clone();
    expected.write_default = Some(Literal::long(123));
    assert_eq!(replaced.field_by_name("z").unwrap().as_ref(), &expected);

    let cleared = updated_schema(&table, update(&table).update_column_default("z", None)).await;
    let cleared = cleared.field_by_name("z").unwrap();
    assert_eq!(cleared.initial_default, Some(Literal::long(34)));
    assert_eq!(cleared.write_default, None);
    assert_eq!(cleared.doc.as_deref(), Some("description"));
}

#[tokio::test]
async fn updates_pending_additions_and_nested_columns_in_operation_order() {
    let table = make_v2_table_with_nested();
    let schema = updated_schema(
        &table,
        update(&table)
            .case_sensitive(false)
            .add_column(AddColumn::optional(
                "Added",
                Type::Primitive(PrimitiveType::Int),
            ))
            .update_column_default("ADDED", Some(Literal::long(7)))
            .rename_column("PERSON.AGE", "years")
            .update_column_type("PERSON.AGE", PrimitiveType::Long)
            .update_column_default("PERSON.AGE", Some(Literal::int(18))),
    )
    .await;

    let added = schema.field_by_name("Added").unwrap();
    assert_eq!(added.initial_default, None);
    assert_eq!(added.write_default, Some(Literal::int(7)));
    assert!(schema.field_by_name("person.age").is_none());
    let years = schema.field_by_name("person.years").unwrap();
    assert_eq!(
        years.field_type.as_ref(),
        &Type::Primitive(PrimitiveType::Long)
    );
    assert_eq!(years.write_default, Some(Literal::long(18)));
}

#[tokio::test]
async fn validates_targets_and_preserves_java_no_op_ordering() {
    let table = make_v2_table_with_nested();
    let missing = commit_error(
        &table,
        update(&table).update_column_default("missing", None),
    )
    .await;
    assert_eq!(missing.message(), "Cannot update missing column: missing");

    let deleted = commit_error(
        &table,
        update(&table)
            .delete_column("z")
            .update_column_default("z", Some(Literal::long(1))),
    )
    .await;
    assert_eq!(
        deleted.message(),
        "Cannot update a column that will be deleted: z"
    );

    // Java records a null update even when the current default is already null.
    let cleared_then_deleted = commit_error(
        &table,
        update(&table)
            .update_column_default("z", None)
            .delete_column("z"),
    )
    .await;
    assert_eq!(
        cleared_then_deleted.message(),
        "Cannot delete a column that has updates: z"
    );

    let with_default = defaulted_table();
    let same_default =
        Arc::new(update(&with_default).update_column_default("z", Some(Literal::int(35))));
    assert_no_op(&with_default, same_default).await;

    let deleted = updated_schema(
        &with_default,
        update(&with_default)
            .update_column_default("z", Some(Literal::int(35)))
            .delete_column("z"),
    )
    .await;
    assert!(deleted.field_by_name("z").is_none());
}

#[tokio::test]
async fn rejects_incompatible_and_map_key_defaults() {
    let table = make_v2_table_with_nested();
    let incompatible = commit_error(
        &table,
        update(&table).update_column_default("person.age", Some(Literal::string("unknown"))),
    )
    .await;
    assert_eq!(incompatible.kind(), ErrorKind::PreconditionFailed);
    assert_eq!(
        incompatible.message(),
        "Invalid default for column person.age: default is incompatible with int"
    );

    let nested = commit_error(
        &table,
        update(&table).update_column_default("person", Some(Literal::int(1))),
    )
    .await;
    assert!(
        nested
            .message()
            .starts_with("Invalid default for column person:")
    );

    let map_key = commit_error(
        &table,
        update(&table).update_column_default("props.key", None),
    )
    .await;
    assert_eq!(map_key.message(), "Cannot update map keys: props");
}

#[tokio::test]
async fn commits_signed_zero_default_changes() {
    let table = make_v2_table_with_nested();
    let mut fields = table
        .metadata()
        .current_schema()
        .as_struct()
        .fields()
        .to_vec();
    fields.extend([
        NestedField::optional(15, "float_zero", Type::Primitive(PrimitiveType::Float))
            .with_write_default(Literal::float(-0.0))
            .into(),
        NestedField::optional(16, "double_zero", Type::Primitive(PrimitiveType::Double))
            .with_write_default(Literal::double(-0.0))
            .into(),
    ]);
    let schema = Schema::builder()
        .with_fields(fields)
        .with_identifier_field_ids(table.metadata().current_schema().identifier_field_ids())
        .build()
        .unwrap();
    let table = replace_current_schema(&table, schema);

    let schema = updated_schema(
        &table,
        update(&table)
            .update_column_default("float_zero", Some(Literal::float(0.0)))
            .update_column_default("double_zero", Some(Literal::double(0.0))),
    )
    .await;

    let Some(Literal::Primitive(PrimitiveLiteral::Float(value))) =
        &schema.field_by_name("float_zero").unwrap().write_default
    else {
        panic!("expected float default");
    };
    assert_eq!(value.0.to_bits(), 0.0_f32.to_bits());

    let Some(Literal::Primitive(PrimitiveLiteral::Double(value))) =
        &schema.field_by_name("double_zero").unwrap().write_default
    else {
        panic!("expected double default");
    };
    assert_eq!(value.0.to_bits(), 0.0_f64.to_bits());
}

#[tokio::test]
async fn replays_against_refreshed_metadata_and_detects_an_applied_update() {
    let table = make_v2_table_with_nested();
    let action =
        Arc::new(update(&table).update_column_default("person.age", Some(Literal::int(18))));
    let mut first = action.clone().commit(&table).await.unwrap();
    let first = match first.take_updates().remove(0) {
        TableUpdate::AddSchema { schema } => schema,
        update => panic!("expected AddSchema, got {update:?}"),
    };

    let mut concurrent_fields = table
        .metadata()
        .current_schema()
        .as_struct()
        .fields()
        .to_vec();
    concurrent_fields.push(
        NestedField::optional(15, "concurrent", Type::Primitive(PrimitiveType::Boolean)).into(),
    );
    let concurrent = Schema::builder()
        .with_fields(concurrent_fields)
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
    assert_eq!(
        replayed.field_by_name("person.age").unwrap().write_default,
        Some(Literal::int(18))
    );

    let already_applied = replace_current_schema(&table, first);
    assert_no_op(&already_applied, action).await;
}
