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

use std::collections::HashMap;
use std::io::BufReader;
use std::sync::Arc;

use as_any::Downcast;

use crate::catalog::MockCatalog;
use crate::memory::tests::new_memory_catalog;
use crate::spec::{
    DEFAULT_SCHEMA_ID, DEFAULT_SCHEMA_NAME_MAPPING, ListType, Literal, MapType, NameMapping,
    NestedField, PrimitiveLiteral, PrimitiveType, Schema, Struct, StructType, TableMetadata, Type,
    VariantType,
};
use crate::table::Table;
use crate::transaction::Transaction;
use crate::transaction::action::{ApplyTransactionAction, TransactionAction};
use crate::transaction::tests::{make_v2_table, make_v3_minimal_table_in_catalog};
use crate::transaction::update_schema::{AddColumn, DEFAULT_FIELD_ID, UpdateSchemaAction};
use crate::{Error, ErrorKind, TableIdent, TableRequirement, TableUpdate};

// The V2 test table has:
//   last_column_id: 3
//   current schema (id=1): x(1, req, long), y(2, req, long), z(3, req, long)
//   identifier_field_ids: [1, 2]

/// Build a V2 test table that includes nested types:
///
///   last_column_id: 14
///   current schema (id=0):
///     x(1, req, long)           -- identifier
///     y(2, req, long)           -- identifier
///     z(3, req, long)
///     person(4, opt, struct)
///       name(5, opt, string)
///       age(6, req, int)
///     tags(7, opt, list<struct>)
///       element(8, req, struct)
///         key(9, opt, string)
///         value(10, opt, string)
///     props(11, opt, map<string, struct>)
///       key(12, req, string)
///       value(13, req, struct)
///         data(14, opt, string)
fn make_v2_table_with_nested() -> Table {
    let json = r#"{
        "format-version": 2,
        "table-uuid": "9c12d441-03fe-4693-9a96-a0705ddf69c2",
        "location": "s3://bucket/test/location",
        "last-sequence-number": 0,
        "last-updated-ms": 1602638573590,
        "last-column-id": 14,
        "current-schema-id": 0,
        "schemas": [
            {
                "type": "struct",
                "schema-id": 0,
                "identifier-field-ids": [1, 2],
                "fields": [
                    {"id": 1, "name": "x", "required": true, "type": "long"},
                    {"id": 2, "name": "y", "required": true, "type": "long"},
                    {"id": 3, "name": "z", "required": true, "type": "long"},
                    {"id": 4, "name": "person", "required": false, "type": {
                        "type": "struct",
                        "fields": [
                            {"id": 5, "name": "name", "required": false, "type": "string"},
                            {"id": 6, "name": "age", "required": true, "type": "int"}
                        ]
                    }},
                    {"id": 7, "name": "tags", "required": false, "type": {
                        "type": "list",
                        "element-id": 8,
                        "element": {
                            "type": "struct",
                            "fields": [
                                {"id": 9, "name": "key", "required": false, "type": "string"},
                                {"id": 10, "name": "value", "required": false, "type": "string"}
                            ]
                        },
                        "element-required": true
                    }},
                    {"id": 11, "name": "props", "required": false, "type": {
                        "type": "map",
                        "key-id": 12,
                        "key": "string",
                        "value-id": 13,
                        "value": {
                            "type": "struct",
                            "fields": [
                                {"id": 14, "name": "data", "required": false, "type": "string"}
                            ]
                        },
                        "value-required": true
                    }}
                ]
            }
        ],
        "default-spec-id": 0,
        "partition-specs": [
            {"spec-id": 0, "fields": []}
        ],
        "last-partition-id": 999,
        "default-sort-order-id": 0,
        "sort-orders": [
            {"order-id": 0, "fields": []}
        ],
        "properties": {},
        "current-snapshot-id": -1,
        "snapshots": []
    }"#;

    let reader = BufReader::new(json.as_bytes());
    let metadata = serde_json::from_reader::<_, TableMetadata>(reader).unwrap();

    Table::builder()
        .metadata(metadata)
        .metadata_location("s3://bucket/test/location/metadata/v1.json".to_string())
        .identifier(TableIdent::from_strs(["ns1", "test1"]).unwrap())
        .file_io(crate::io::FileIO::new_with_memory())
        .runtime(crate::test_utils::test_runtime())
        .build()
        .unwrap()
}

async fn apply_schema(table: &Table, action: UpdateSchemaAction) -> Schema {
    let mut commit = Arc::new(action).commit(table).await.unwrap();
    commit
        .take_updates()
        .into_iter()
        .find_map(|update| match update {
            TableUpdate::AddSchema { schema } => Some(schema),
            _ => None,
        })
        .unwrap_or_else(|| table.metadata().current_schema().as_ref().clone())
}

async fn commit_error(table: &Table, action: UpdateSchemaAction) -> Error {
    match Arc::new(action).commit(table).await {
        Ok(_) => panic!("expected schema update to fail"),
        Err(error) => error,
    }
}

fn table_with_schema(schema: Schema) -> Table {
    let table = make_v2_table();
    let custom_schema = schema
        .into_builder()
        .with_reassigned_field_ids(table.metadata().last_column_id() + 1)
        .build()
        .unwrap();
    let mut fields = table
        .metadata()
        .current_schema()
        .as_struct()
        .fields()
        .to_vec();
    fields.extend_from_slice(custom_schema.as_struct().fields());
    let schema = Schema::builder()
        .with_fields(fields)
        .with_identifier_field_ids(table.metadata().current_schema().identifier_field_ids())
        .build()
        .unwrap();
    let metadata = table
        .metadata()
        .clone()
        .into_builder(None)
        .add_current_schema(schema)
        .unwrap()
        .build()
        .unwrap()
        .metadata;
    table.with_metadata(Arc::new(metadata))
}

fn table_with_nested_identifier_candidate() -> Table {
    table_with_schema(
        Schema::builder()
            .with_fields([NestedField::required(
                1,
                "parent",
                Type::Struct(StructType::new(vec![
                    NestedField::required(2, "id", Type::Primitive(PrimitiveType::String)).into(),
                ])),
            )
            .into()])
            .build()
            .unwrap(),
    )
}

// -----------------------------------------------------------------------
// Existing root-level tests
// -----------------------------------------------------------------------

#[test]
fn test_assign_fresh_ids_variant() {
    // Variant carries no sub-fields, so fresh-id assignment only renames the field
    // itself and leaves the type untouched.
    let mut next_id = 10;
    let field = NestedField::optional(1, "data", Type::Variant(VariantType));
    let assigned = super::apply::assign_fresh_ids(&field, &mut next_id).unwrap();

    assert_eq!(assigned.id, 11);
    assert_eq!(*assigned.field_type, Type::Variant(VariantType));
    assert_eq!(next_id, 11);
}

#[tokio::test]
async fn test_fresh_ids_are_assigned_to_siblings_before_nested_fields() {
    let table = make_v2_table();
    let schema = apply_schema(
        &table,
        Transaction::new(&table)
            .update_schema()
            .add_column(AddColumn::optional(
                "outer",
                Type::Struct(StructType::new(vec![
                    NestedField::optional(
                        20,
                        "a",
                        Type::Struct(StructType::new(vec![
                            NestedField::optional(21, "c", Type::Primitive(PrimitiveType::Int))
                                .into(),
                        ])),
                    )
                    .into(),
                    NestedField::optional(22, "b", Type::Primitive(PrimitiveType::Int)).into(),
                ])),
            )),
    )
    .await;

    assert_eq!(schema.field_by_name("outer").unwrap().id, 4);
    assert_eq!(schema.field_by_name("outer.a").unwrap().id, 5);
    assert_eq!(schema.field_by_name("outer.b").unwrap().id, 6);
    assert_eq!(schema.field_by_name("outer.a.c").unwrap().id, 7);
}

#[tokio::test]
async fn test_added_collection_fields_are_rebuilt_canonically() {
    let table = make_v2_table();
    let malformed_element =
        NestedField::optional(10, "not_element", Type::Primitive(PrimitiveType::String))
            .with_doc("not retained")
            .with_initial_default(Literal::string("not retained"));
    let malformed_key =
        NestedField::optional(11, "not_key", Type::Primitive(PrimitiveType::String))
            .with_doc("not retained")
            .with_write_default(Literal::string("not retained"));
    let malformed_value =
        NestedField::required(12, "not_value", Type::Primitive(PrimitiveType::Long))
            .with_doc("not retained");
    let schema = apply_schema(
        &table,
        Transaction::new(&table)
            .update_schema()
            .add_column(AddColumn::optional(
                "items",
                Type::List(ListType {
                    element_field: malformed_element.into(),
                }),
            ))
            .add_column(AddColumn::optional(
                "properties",
                Type::Map(MapType {
                    key_field: malformed_key.into(),
                    value_field: malformed_value.into(),
                }),
            )),
    )
    .await;

    let element = schema.field_by_name("items.element").unwrap();
    assert_eq!(element.name, "element");
    assert_eq!(element.doc, None);
    assert_eq!(element.initial_default, None);
    assert_eq!(element.write_default, None);
    let key = schema.field_by_name("properties.key").unwrap();
    assert_eq!(key.name, "key");
    assert!(key.required);
    assert_eq!(key.doc, None);
    assert_eq!(key.write_default, None);
    let value = schema.field_by_name("properties.value").unwrap();
    assert_eq!(value.name, "value");
    assert!(value.required);
    assert_eq!(value.doc, None);

    let round_tripped: Schema =
        serde_json::from_str(&serde_json::to_string(&schema).unwrap()).unwrap();
    assert_eq!(round_tripped, schema);
}

#[tokio::test]
async fn test_add_column() {
    let table = make_v2_table();
    let tx = Transaction::new(&table);

    let action = tx.update_schema().add_column(AddColumn::optional(
        "new_col",
        Type::Primitive(PrimitiveType::Int),
    ));

    let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
    let updates = action_commit.take_updates();
    let requirements = action_commit.take_requirements();

    assert_eq!(updates.len(), 2);

    // Extract the new schema from the AddSchema update.
    let new_schema = match &updates[0] {
        TableUpdate::AddSchema { schema } => schema,
        other => panic!("expected AddSchema, got {other:?}"),
    };

    let expected_schema = table
        .metadata()
        .current_schema()
        .as_ref()
        .clone()
        .into_builder()
        .with_schema_id(DEFAULT_SCHEMA_ID)
        .with_fields([
            NestedField::optional(4, "new_col", Type::Primitive(PrimitiveType::Int)).into(),
        ])
        .build()
        .unwrap();
    assert_eq!(new_schema, &expected_schema);

    assert_eq!(updates[1], TableUpdate::SetCurrentSchema { schema_id: -1 });

    // Verify requirement.
    assert_eq!(requirements.len(), 3);
    assert_eq!(requirements[0], TableRequirement::UuidMatch {
        uuid: table.metadata().uuid()
    });
    assert_eq!(
        requirements[1],
        TableRequirement::LastAssignedFieldIdMatch {
            last_assigned_field_id: table.metadata().last_column_id()
        }
    );
    assert_eq!(requirements[2], TableRequirement::CurrentSchemaIdMatch {
        current_schema_id: table.metadata().current_schema().schema_id()
    });
}

#[tokio::test]
async fn test_commit_replays_operations_against_latest_metadata() {
    let table = make_v2_table();
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
    let refreshed_metadata = table
        .metadata()
        .clone()
        .into_builder(None)
        .add_schema(concurrent_schema)
        .unwrap()
        .set_current_schema(-1)
        .unwrap()
        .build()
        .unwrap()
        .metadata;
    let refreshed = table.with_metadata(Arc::new(refreshed_metadata));

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
async fn test_transaction_commits_multiple_schema_updates() {
    let catalog = new_memory_catalog().await;
    let table = make_v3_minimal_table_in_catalog(&catalog).await;
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

    let updated = tx.commit(&catalog).await.unwrap();

    let schema = updated.metadata().current_schema();
    assert!(schema.field_by_name("first").is_some());
    assert!(schema.field_by_name("second").is_some());
}

#[tokio::test]
async fn test_no_op_schema_update_skips_catalog_commit() {
    let table = make_v2_table();
    let refreshed = table.clone();
    let mut catalog = MockCatalog::new();
    catalog.expect_load_table().times(1).returning_st(move |_| {
        let refreshed = refreshed.clone();
        Box::pin(async move { Ok(refreshed) })
    });
    catalog.expect_update_table().times(0);

    let tx = Transaction::new(&table);
    let tx = tx
        .update_schema()
        .update_column_type("z", PrimitiveType::Long)
        .apply(tx)
        .unwrap();
    let result = tx.commit(&catalog).await.unwrap();

    assert_eq!(result.metadata(), table.metadata());
}

#[tokio::test]
async fn test_add_column_with_doc() {
    let table = make_v2_table();
    let tx = Transaction::new(&table);

    let action = tx.update_schema().add_column(
        AddColumn::builder()
            .name("documented_col")
            .field_type(Type::Primitive(PrimitiveType::String))
            .doc("A documented column")
            .build(),
    );

    let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
    let updates = action_commit.take_updates();

    let new_schema = match &updates[0] {
        TableUpdate::AddSchema { schema } => schema,
        other => panic!("expected AddSchema, got {other:?}"),
    };

    let field = new_schema
        .field_by_name("documented_col")
        .expect("documented_col should exist");
    assert_eq!(field.id, 4);
    assert!(!field.required);
    assert_eq!(field.doc.as_deref(), Some("A documented column"));
}

#[tokio::test]
async fn test_add_required_column_with_initial_default() {
    let table = make_v2_table();
    let tx = Transaction::new(&table);

    let action = tx.update_schema().add_column(AddColumn::required(
        "req_col",
        Type::Primitive(PrimitiveType::Int),
        Literal::int(0),
    ));

    let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
    let updates = action_commit.take_updates();

    let new_schema = match &updates[0] {
        TableUpdate::AddSchema { schema } => schema,
        other => panic!("expected AddSchema, got {other:?}"),
    };

    let field = new_schema
        .field_by_name("req_col")
        .expect("req_col should exist");
    assert_eq!(field.id, 4);
    assert!(field.required);
    assert_eq!(field.initial_default, Some(Literal::int(0)));
    assert_eq!(field.write_default, Some(Literal::int(0)));
}

#[tokio::test]
async fn test_add_column_name_conflict_fails() {
    let table = make_v2_table();
    let tx = Transaction::new(&table);

    // "x" already exists in the V2 test schema.
    let action = tx.update_schema().add_column(AddColumn::optional(
        "x",
        Type::Primitive(PrimitiveType::Int),
    ));

    let result = Arc::new(action).commit(&table).await;
    let err = match result {
        Err(e) => e,
        Ok(_) => panic!("should reject adding a column with an existing name"),
    };
    assert_eq!(err.kind(), ErrorKind::PreconditionFailed);
    assert!(
        err.message().contains("already exists"),
        "error should mention name conflict, got: {}",
        err.message()
    );
}

#[tokio::test]
async fn test_delete_column() {
    let table = make_v2_table();
    let tx = Transaction::new(&table);

    // z is not an identifier field, so we can delete it.
    let action = tx.update_schema().delete_column("z");

    let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
    let updates = action_commit.take_updates();

    let new_schema = match &updates[0] {
        TableUpdate::AddSchema { schema } => schema,
        other => panic!("expected AddSchema, got {other:?}"),
    };

    assert!(
        new_schema.field_by_name("z").is_none(),
        "z should be deleted"
    );
    assert!(new_schema.field_by_name("x").is_some());
    assert!(new_schema.field_by_name("y").is_some());
}

#[tokio::test]
async fn test_delete_missing_column_fails() {
    let table = make_v2_table();
    let tx = Transaction::new(&table);

    let action = tx.update_schema().delete_column("nonexistent");

    let result = Arc::new(action).commit(&table).await;
    let err = match result {
        Err(e) => e,
        Ok(_) => panic!("should reject deleting a non-existent column"),
    };
    assert_eq!(err.kind(), ErrorKind::PreconditionFailed);
    assert!(
        err.message().contains("nonexistent"),
        "error should mention the missing column, got: {}",
        err.message()
    );
}

#[tokio::test]
async fn test_add_and_delete_combined() {
    let table = make_v2_table();
    let tx = Transaction::new(&table);

    // Delete z, add a new column.
    let action = tx
        .update_schema()
        .delete_column("z")
        .add_column(AddColumn::optional(
            "w",
            Type::Primitive(PrimitiveType::Boolean),
        ));

    let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
    let updates = action_commit.take_updates();

    let new_schema = match &updates[0] {
        TableUpdate::AddSchema { schema } => schema,
        other => panic!("expected AddSchema, got {other:?}"),
    };

    assert!(
        new_schema.field_by_name("z").is_none(),
        "z should be deleted"
    );
    let w = new_schema.field_by_name("w").expect("w should exist");
    assert_eq!(w.id, 4);
    assert!(!w.required);
}

#[tokio::test]
async fn test_delete_and_readd_same_name() {
    let table = make_v2_table();
    let tx = Transaction::new(&table);

    // Delete z, then add a new column named z -- should succeed.
    let action = tx
        .update_schema()
        .delete_column("z")
        .add_column(AddColumn::optional(
            "z",
            Type::Primitive(PrimitiveType::Boolean),
        ));

    let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
    let updates = action_commit.take_updates();

    let new_schema = match &updates[0] {
        TableUpdate::AddSchema { schema } => schema,
        other => panic!("expected AddSchema, got {other:?}"),
    };

    let z = new_schema
        .field_by_name("z")
        .expect("z should exist with new type");
    assert_eq!(z.id, 4); // new ID, not the old 3
    assert_eq!(*z.field_type, Type::Primitive(PrimitiveType::Boolean));
}

#[tokio::test]
async fn test_java_recorded_no_op_updates_conflict_with_delete() {
    let table = make_v2_table();
    let rename_error = commit_error(
        &table,
        Transaction::new(&table)
            .update_schema()
            .rename_column("z", "z")
            .delete_column("z"),
    )
    .await;
    assert_eq!(rename_error.kind(), ErrorKind::PreconditionFailed);
    assert!(rename_error.message().contains("has updates"));

    let default_error = commit_error(
        &table,
        Transaction::new(&table)
            .update_schema()
            .update_column_default("z", None)
            .delete_column("z"),
    )
    .await;
    assert_eq!(default_error.kind(), ErrorKind::PreconditionFailed);
    assert!(default_error.message().contains("has updates"));
}

#[test]
fn test_apply() {
    let table = make_v2_table();
    let tx = Transaction::new(&table);

    let tx = tx
        .update_schema()
        .add_column(AddColumn::optional(
            "new_col",
            Type::Primitive(PrimitiveType::Int),
        ))
        .apply(tx)
        .unwrap();

    assert_eq!(tx.actions.len(), 1);
    (*tx.actions[0])
        .downcast_ref::<UpdateSchemaAction>()
        .expect("UpdateSchemaAction was not applied to Transaction!");
}

// -----------------------------------------------------------------------
// Nested add tests
// -----------------------------------------------------------------------

#[tokio::test]
async fn test_add_column_to_struct() {
    let table = make_v2_table_with_nested();
    let tx = Transaction::new(&table);

    // Add "email" to the "person" struct.
    let action = tx.update_schema().add_column(
        AddColumn::builder()
            .name("email")
            .field_type(Type::Primitive(PrimitiveType::String))
            .parent("person")
            .build(),
    );

    let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
    let updates = action_commit.take_updates();

    let new_schema = match &updates[0] {
        TableUpdate::AddSchema { schema } => schema,
        other => panic!("expected AddSchema, got {other:?}"),
    };

    // "email" should be nested under "person" with ID = last_column_id + 1 = 15.
    let email = new_schema
        .field_by_name("person.email")
        .expect("person.email should exist");
    assert_eq!(email.id, 15);
    assert!(!email.required);
    assert_eq!(*email.field_type, Type::Primitive(PrimitiveType::String));

    // Original nested fields should still be there.
    assert!(new_schema.field_by_name("person.name").is_some());
    assert!(new_schema.field_by_name("person.age").is_some());
}

#[tokio::test]
async fn test_add_column_to_struct_with_doc() {
    let table = make_v2_table_with_nested();
    let tx = Transaction::new(&table);

    let action = tx.update_schema().add_column(
        AddColumn::builder()
            .name("phone")
            .field_type(Type::Primitive(PrimitiveType::String))
            .parent("person")
            .doc("Phone number")
            .build(),
    );

    let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
    let updates = action_commit.take_updates();

    let new_schema = match &updates[0] {
        TableUpdate::AddSchema { schema } => schema,
        other => panic!("expected AddSchema, got {other:?}"),
    };

    let phone = new_schema
        .field_by_name("person.phone")
        .expect("person.phone should exist");
    assert_eq!(phone.id, 15);
    assert_eq!(phone.doc.as_deref(), Some("Phone number"));
}

#[tokio::test]
async fn test_add_column_to_list_element_struct() {
    let table = make_v2_table_with_nested();
    let tx = Transaction::new(&table);

    // "tags" is a list<struct{key, value}>. Adding to the list navigates to its
    // element struct automatically.
    let action = tx.update_schema().add_column(
        AddColumn::builder()
            .name("score")
            .field_type(Type::Primitive(PrimitiveType::Double))
            .parent("tags")
            .build(),
    );

    let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
    let updates = action_commit.take_updates();

    let new_schema = match &updates[0] {
        TableUpdate::AddSchema { schema } => schema,
        other => panic!("expected AddSchema, got {other:?}"),
    };

    // The list element struct should now contain "score".
    let score = new_schema
        .field_by_name("tags.element.score")
        .expect("tags.element.score should exist");
    assert_eq!(score.id, 15);
    assert!(!score.required);

    // Existing fields preserved.
    assert!(new_schema.field_by_name("tags.element.key").is_some());
    assert!(new_schema.field_by_name("tags.element.value").is_some());
}

#[tokio::test]
async fn test_add_column_to_map_value_struct() {
    let table = make_v2_table_with_nested();
    let tx = Transaction::new(&table);

    // "props" is a map<string, struct{data}>. Adding to the map navigates to its
    // value struct automatically.
    let action = tx.update_schema().add_column(
        AddColumn::builder()
            .name("version")
            .field_type(Type::Primitive(PrimitiveType::Int))
            .parent("props")
            .build(),
    );

    let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
    let updates = action_commit.take_updates();

    let new_schema = match &updates[0] {
        TableUpdate::AddSchema { schema } => schema,
        other => panic!("expected AddSchema, got {other:?}"),
    };

    let version = new_schema
        .field_by_name("props.value.version")
        .expect("props.value.version should exist");
    assert_eq!(version.id, 15);

    // Existing map value fields preserved.
    assert!(new_schema.field_by_name("props.value.data").is_some());
}

#[tokio::test]
async fn test_add_column_to_nonexistent_parent_fails() {
    let table = make_v2_table_with_nested();
    let tx = Transaction::new(&table);

    let action = tx.update_schema().add_column(
        AddColumn::builder()
            .name("col")
            .field_type(Type::Primitive(PrimitiveType::Int))
            .parent("nonexistent")
            .build(),
    );

    let err = match Arc::new(action).commit(&table).await {
        Err(e) => e,
        Ok(_) => panic!("should reject adding to a nonexistent parent"),
    };
    assert_eq!(err.kind(), ErrorKind::PreconditionFailed);
    assert!(
        err.message().contains("nonexistent"),
        "error should mention the missing parent, got: {}",
        err.message()
    );
}

#[tokio::test]
async fn test_add_column_to_primitive_parent_fails() {
    let table = make_v2_table_with_nested();
    let tx = Transaction::new(&table);

    // "x" is a primitive (long), not a struct.
    let action = tx.update_schema().add_column(
        AddColumn::builder()
            .name("col")
            .field_type(Type::Primitive(PrimitiveType::Int))
            .parent("x")
            .build(),
    );

    let err = match Arc::new(action).commit(&table).await {
        Err(e) => e,
        Ok(_) => panic!("should reject adding to a primitive parent"),
    };
    assert_eq!(err.kind(), ErrorKind::PreconditionFailed);
    assert!(
        err.message().contains("not a struct"),
        "error should mention type mismatch, got: {}",
        err.message()
    );
}

#[tokio::test]
async fn test_add_column_to_nested_name_conflict_fails() {
    let table = make_v2_table_with_nested();
    let tx = Transaction::new(&table);

    // "name" already exists in the "person" struct.
    let action = tx.update_schema().add_column(
        AddColumn::builder()
            .name("name")
            .field_type(Type::Primitive(PrimitiveType::String))
            .parent("person")
            .build(),
    );

    let err = match Arc::new(action).commit(&table).await {
        Err(e) => e,
        Ok(_) => panic!("should reject adding a column with conflicting name"),
    };
    assert_eq!(err.kind(), ErrorKind::PreconditionFailed);
    assert!(
        err.message().contains("already exists"),
        "error should mention name conflict, got: {}",
        err.message()
    );
}

#[tokio::test]
async fn test_root_and_nested_add_combined() {
    let table = make_v2_table_with_nested();
    let tx = Transaction::new(&table);

    // Add a root column and a nested column in the same action.
    let action = tx
        .update_schema()
        .add_column(AddColumn::optional(
            "root_col",
            Type::Primitive(PrimitiveType::Boolean),
        ))
        .add_column(
            AddColumn::builder()
                .name("email")
                .field_type(Type::Primitive(PrimitiveType::String))
                .parent("person")
                .build(),
        );

    let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
    let updates = action_commit.take_updates();

    let new_schema = match &updates[0] {
        TableUpdate::AddSchema { schema } => schema,
        other => panic!("expected AddSchema, got {other:?}"),
    };

    // Root column gets the first fresh ID.
    let root_col = new_schema
        .field_by_name("root_col")
        .expect("root_col should exist");
    assert_eq!(root_col.id, 15);

    // Nested column gets the next ID.
    let email = new_schema
        .field_by_name("person.email")
        .expect("person.email should exist");
    assert_eq!(email.id, 16);
}

#[tokio::test]
async fn test_add_nested_struct_type_with_fresh_ids() {
    // Adding a new column whose TYPE contains nested fields (e.g. a struct column). All sub-fields must receive
    // fresh IDs, not placeholder `DEFAULT_FIELD_ID`.
    let table = make_v2_table();
    let tx = Transaction::new(&table);

    let action = tx.update_schema().add_column(AddColumn::optional(
        "address",
        Type::Struct(StructType::new(vec![
            NestedField::optional(
                DEFAULT_FIELD_ID,
                "street",
                Type::Primitive(PrimitiveType::String),
            )
            .into(),
            NestedField::optional(
                DEFAULT_FIELD_ID,
                "city",
                Type::Primitive(PrimitiveType::String),
            )
            .into(),
        ])),
    ));

    let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
    let updates = action_commit.take_updates();

    let new_schema = match &updates[0] {
        TableUpdate::AddSchema { schema } => schema,
        other => panic!("expected AddSchema, got {other:?}"),
    };

    // "address" gets ID 4 (last_column_id=3, +1).
    let address = new_schema
        .field_by_name("address")
        .expect("address should exist");
    assert_eq!(address.id, 4);

    // Sub-fields get IDs 5 and 6.
    let street = new_schema
        .field_by_name("address.street")
        .expect("address.street should exist");
    assert_eq!(street.id, 5);

    let city = new_schema
        .field_by_name("address.city")
        .expect("address.city should exist");
    assert_eq!(city.id, 6);
}

#[tokio::test]
async fn test_rename_root_and_nested_columns_preserves_ids() {
    let table = make_v2_table_with_nested();
    let schema = apply_schema(
        &table,
        Transaction::new(&table)
            .update_schema()
            .rename_column("z", "payload")
            .rename_column("person.name", "full_name"),
    )
    .await;

    assert!(schema.field_by_name("z").is_none());
    assert_eq!(schema.field_by_name("payload").unwrap().id, 3);
    assert!(schema.field_by_name("person.name").is_none());
    assert_eq!(schema.field_by_name("person.full_name").unwrap().id, 5);
    assert_eq!(schema.field_by_name("person").unwrap().id, 4);
}

#[tokio::test]
async fn test_update_type_doc_and_default_preserves_metadata() {
    let table = make_v2_table_with_nested();
    let schema = apply_schema(
        &table,
        Transaction::new(&table)
            .update_schema()
            .update_column_type("person.age", PrimitiveType::Long)
            .update_column_doc("person.age", Some("age in years".to_string()))
            .update_column_default("person.age", Some(Literal::long(18))),
    )
    .await;

    let age = schema.field_by_name("person.age").unwrap();
    assert_eq!(age.id, 6);
    assert!(age.required);
    assert_eq!(
        age.field_type.as_ref(),
        &Type::Primitive(PrimitiveType::Long)
    );
    assert_eq!(age.doc.as_deref(), Some("age in years"));
    assert_eq!(age.write_default, Some(Literal::long(18)));
    assert_eq!(age.initial_default, None);
}

#[tokio::test]
async fn test_type_promotion_casts_existing_defaults() {
    let table = make_v2_table();
    let schema = apply_schema(
        &table,
        Transaction::new(&table)
            .update_schema()
            .add_column(
                AddColumn::builder()
                    .name("count")
                    .field_type(Type::Primitive(PrimitiveType::Int))
                    .initial_default(Literal::int(7))
                    .write_default(Literal::int(8))
                    .build(),
            )
            .update_column_type("count", PrimitiveType::Long),
    )
    .await;

    let count = schema.field_by_name("count").unwrap();
    assert_eq!(
        count.field_type.as_ref(),
        &Type::Primitive(PrimitiveType::Long)
    );
    assert_eq!(count.initial_default, Some(Literal::long(7)));
    assert_eq!(count.write_default, Some(Literal::long(8)));
}

#[tokio::test]
async fn test_rejects_invalid_type_promotion_and_default() {
    let table = make_v2_table_with_nested();
    let type_error = commit_error(
        &table,
        Transaction::new(&table)
            .update_schema()
            .update_column_type("person.age", PrimitiveType::String),
    )
    .await;
    assert_eq!(type_error.kind(), ErrorKind::PreconditionFailed);
    assert!(type_error.message().contains("Cannot change column type"));

    let default_error = commit_error(
        &table,
        Transaction::new(&table)
            .update_schema()
            .update_column_default("person.age", Some(Literal::string("unknown"))),
    )
    .await;
    assert_eq!(default_error.kind(), ErrorKind::PreconditionFailed);
    assert!(default_error.message().contains("Invalid default"));

    let nested_default_error = commit_error(
        &table,
        Transaction::new(&table).update_schema().add_column(
            AddColumn::builder()
                .name("details")
                .field_type(Type::Struct(StructType::new(vec![
                    NestedField::optional(100, "value", Type::Primitive(PrimitiveType::String))
                        .into(),
                ])))
                .initial_default(Literal::Struct(Struct::from_iter(vec![Some(
                    Literal::long(1),
                )])))
                .build(),
        ),
    )
    .await;
    assert_eq!(nested_default_error.kind(), ErrorKind::PreconditionFailed);
    assert!(nested_default_error.message().contains("Invalid default"));

    let invalid_nested_field_default = commit_error(
        &table,
        Transaction::new(&table)
            .update_schema()
            .add_column(AddColumn::optional(
                "details",
                Type::Struct(StructType::new(vec![
                    NestedField::optional(100, "value", Type::Primitive(PrimitiveType::Int))
                        .with_initial_default(Literal::string("not an int"))
                        .into(),
                ])),
            )),
    )
    .await;
    assert_eq!(
        invalid_nested_field_default.kind(),
        ErrorKind::PreconditionFailed
    );
    assert!(
        invalid_nested_field_default
            .message()
            .contains("details.value")
    );
}

#[tokio::test]
async fn test_nullability_changes_require_explicit_opt_in() {
    let table = make_v2_table_with_nested();
    let error = commit_error(
        &table,
        Transaction::new(&table)
            .update_schema()
            .require_column("person.name"),
    )
    .await;
    assert_eq!(error.kind(), ErrorKind::PreconditionFailed);
    assert!(error.message().contains("optional -> required"));

    let schema = apply_schema(
        &table,
        Transaction::new(&table)
            .update_schema()
            .allow_incompatible_changes()
            .require_column("person.name")
            .make_column_optional("person.age"),
    )
    .await;
    assert!(schema.field_by_name("person.name").unwrap().required);
    assert!(!schema.field_by_name("person.age").unwrap().required);
}

#[tokio::test]
async fn test_required_add_without_default_requires_opt_in() {
    fn required_column() -> AddColumn {
        AddColumn::builder()
            .name("required_col")
            .required(true)
            .field_type(Type::Primitive(PrimitiveType::String))
            .build()
    }

    let table = make_v2_table();
    let error = commit_error(
        &table,
        Transaction::new(&table)
            .update_schema()
            .add_column(required_column()),
    )
    .await;
    assert_eq!(error.kind(), ErrorKind::PreconditionFailed);

    let schema = apply_schema(
        &table,
        Transaction::new(&table)
            .update_schema()
            .allow_incompatible_changes()
            .add_column(required_column()),
    )
    .await;
    assert!(schema.field_by_name("required_col").unwrap().required);
}

#[tokio::test]
async fn test_incompatible_change_opt_in_only_applies_to_subsequent_operations() {
    let table = make_v2_table();
    let required_column = || {
        AddColumn::builder()
            .name("required_col")
            .required(true)
            .field_type(Type::Primitive(PrimitiveType::String))
            .build()
    };

    let error = commit_error(
        &table,
        Transaction::new(&table)
            .update_schema()
            .add_column(required_column())
            .allow_incompatible_changes(),
    )
    .await;
    assert_eq!(error.kind(), ErrorKind::PreconditionFailed);

    let schema = apply_schema(
        &table,
        Transaction::new(&table)
            .update_schema()
            .allow_incompatible_changes()
            .add_column(required_column()),
    )
    .await;
    assert!(schema.field_by_name("required_col").unwrap().required);
}

#[tokio::test]
async fn test_move_columns_at_root_and_nested_levels() {
    let table = make_v2_table_with_nested();
    let schema = apply_schema(
        &table,
        Transaction::new(&table)
            .update_schema()
            .move_column_first("z")
            .move_column_after("person.name", "person.age"),
    )
    .await;

    let root_ids: Vec<i32> = schema
        .as_struct()
        .fields()
        .iter()
        .map(|field| field.id)
        .collect();
    assert_eq!(root_ids, vec![3, 1, 2, 4, 7, 11]);
    let Type::Struct(person) = schema.field_by_name("person").unwrap().field_type.as_ref() else {
        panic!("person should remain a struct")
    };
    assert_eq!(person.fields()[0].id, 6);
    assert_eq!(person.fields()[1].id, 5);
}

#[tokio::test]
async fn test_move_added_replacement_column() {
    let table = make_v2_table();
    let schema = apply_schema(
        &table,
        Transaction::new(&table)
            .update_schema()
            .delete_column("z")
            .add_column(AddColumn::optional(
                "z",
                Type::Primitive(PrimitiveType::String),
            ))
            .move_column_first("z"),
    )
    .await;

    assert_eq!(schema.as_struct().fields()[0].name, "z");
    assert_eq!(schema.as_struct().fields()[0].id, 4);
}

#[tokio::test]
async fn test_move_validation() {
    let table = make_v2_table_with_nested();
    let self_move = commit_error(
        &table,
        Transaction::new(&table)
            .update_schema()
            .move_column_before("z", "z"),
    )
    .await;
    assert!(self_move.message().contains("itself"));

    let cross_struct = commit_error(
        &table,
        Transaction::new(&table)
            .update_schema()
            .move_column_after("person.name", "z"),
    )
    .await;
    assert!(cross_struct.message().contains("different struct"));

    let list_element = commit_error(
        &table,
        Transaction::new(&table)
            .update_schema()
            .move_column_first("tags.element"),
    )
    .await;
    assert!(list_element.message().contains("non-struct"));
}

#[tokio::test]
async fn test_case_insensitive_resolution() {
    let table = make_v2_table_with_nested();
    let schema = apply_schema(
        &table,
        Transaction::new(&table)
            .update_schema()
            .case_sensitive(false)
            .rename_column("PERSON.NAME", "full_name")
            .update_column_type("PERSON.AGE", PrimitiveType::Long),
    )
    .await;
    assert_eq!(schema.field_by_name("person.full_name").unwrap().id, 5);
    assert_eq!(
        schema
            .field_by_name("person.age")
            .unwrap()
            .field_type
            .as_ref(),
        &Type::Primitive(PrimitiveType::Long)
    );
}

#[tokio::test]
async fn test_case_sensitivity_changes_only_apply_to_subsequent_operations() {
    let table = make_v2_table();
    let error = commit_error(
        &table,
        Transaction::new(&table)
            .update_schema()
            .rename_column("Z", "renamed_z")
            .case_sensitive(false),
    )
    .await;
    assert_eq!(error.kind(), ErrorKind::PreconditionFailed);

    let schema = apply_schema(
        &table,
        Transaction::new(&table)
            .update_schema()
            .case_sensitive(false)
            .rename_column("Z", "renamed_z")
            .case_sensitive(true),
    )
    .await;
    assert!(schema.field_by_name("renamed_z").is_some());
}

#[tokio::test]
async fn test_added_names_keep_add_time_case_normalization() {
    let table = make_v2_table();
    let error = commit_error(
        &table,
        Transaction::new(&table)
            .update_schema()
            .add_column(AddColumn::optional(
                "Added",
                Type::Primitive(PrimitiveType::String),
            ))
            .case_sensitive(false)
            .update_column_doc("added", Some("doc".to_string())),
    )
    .await;
    assert_eq!(error.kind(), ErrorKind::PreconditionFailed);

    let error = commit_error(
        &table,
        Transaction::new(&table)
            .update_schema()
            .case_sensitive(false)
            .add_column(AddColumn::optional(
                "Added",
                Type::Primitive(PrimitiveType::String),
            ))
            .case_sensitive(true)
            .update_column_doc("Added", Some("doc".to_string())),
    )
    .await;
    assert_eq!(error.kind(), ErrorKind::PreconditionFailed);
}

#[tokio::test]
async fn test_identifier_field_replacement_controls_deletes() {
    let table = make_v2_table();
    let error = commit_error(
        &table,
        Transaction::new(&table).update_schema().delete_column("x"),
    )
    .await;
    assert!(error.message().contains("identifier field"));

    let schema = apply_schema(
        &table,
        Transaction::new(&table)
            .update_schema()
            .delete_column("x")
            .delete_column("y")
            .set_identifier_fields(["z"]),
    )
    .await;
    assert_eq!(schema.identifier_field_ids().collect::<Vec<_>>(), vec![3]);
    assert!(schema.field_by_name("x").is_none());
}

#[tokio::test]
async fn test_case_insensitive_identifier_names_match_java_lowercase_index() {
    let table = make_v2_table();
    let error = commit_error(
        &table,
        Transaction::new(&table)
            .update_schema()
            .case_sensitive(false)
            .set_identifier_fields(["X"]),
    )
    .await;
    assert_eq!(error.kind(), ErrorKind::PreconditionFailed);

    let schema = apply_schema(
        &table,
        Transaction::new(&table)
            .update_schema()
            .case_sensitive(false)
            .set_identifier_fields(["x"]),
    )
    .await;
    assert_eq!(schema.identifier_field_ids().collect::<Vec<_>>(), vec![1]);
}

#[tokio::test]
async fn test_identifier_field_validation_uses_updated_schema() {
    let table = make_v2_table();
    let error = commit_error(
        &table,
        Transaction::new(&table)
            .update_schema()
            .add_column(AddColumn::optional(
                "candidate",
                Type::Primitive(PrimitiveType::String),
            ))
            .set_identifier_fields(["candidate"]),
    )
    .await;
    assert_eq!(error.kind(), ErrorKind::PreconditionFailed);
    assert!(error.to_string().contains("optional field"));

    let schema = apply_schema(
        &table,
        Transaction::new(&table)
            .update_schema()
            .add_column(AddColumn::required(
                "candidate",
                Type::Primitive(PrimitiveType::String),
                Literal::string("unknown"),
            ))
            .set_identifier_fields(["candidate"]),
    )
    .await;
    assert_eq!(schema.identifier_field_ids().collect::<Vec<_>>(), vec![4]);

    let schema = apply_schema(
        &table,
        Transaction::new(&table)
            .update_schema()
            .set_identifier_fields(["future_candidate"])
            .add_column(AddColumn::required(
                "future_candidate",
                Type::Primitive(PrimitiveType::String),
                Literal::string("unknown"),
            )),
    )
    .await;
    assert_eq!(schema.identifier_field_ids().collect::<Vec<_>>(), vec![4]);
}

#[tokio::test]
async fn test_new_required_nested_fields_can_be_identifiers() {
    let table = make_v2_table();
    let action = Transaction::new(&table)
        .update_schema()
        .allow_incompatible_changes()
        .add_column(
            AddColumn::builder()
                .name("parent")
                .required(true)
                .field_type(Type::Struct(StructType::new(vec![
                    NestedField::required(10, "id", Type::Primitive(PrimitiveType::String)).into(),
                ])))
                .build(),
        )
        .add_column(
            AddColumn::builder()
                .name("multi")
                .required(true)
                .field_type(Type::Struct(StructType::new(vec![
                    NestedField::required(
                        20,
                        "inner",
                        Type::Struct(StructType::new(vec![
                            NestedField::required(21, "id", Type::Primitive(PrimitiveType::String))
                                .into(),
                        ])),
                    )
                    .into(),
                ])))
                .build(),
        )
        .set_identifier_fields(["parent.id", "multi.inner.id"]);

    let schema = apply_schema(&table, action).await;
    let identifiers = schema
        .identifier_field_ids()
        .collect::<std::collections::HashSet<_>>();
    assert_eq!(
        identifiers,
        std::collections::HashSet::from([
            schema.field_by_name("parent.id").unwrap().id,
            schema.field_by_name("multi.inner.id").unwrap().id,
        ])
    );
}

#[tokio::test]
async fn test_rejects_map_key_changes() {
    let table = make_v2_table_with_nested();
    let error = commit_error(
        &table,
        Transaction::new(&table)
            .update_schema()
            .update_column_doc("props.key", Some("key docs".to_string())),
    )
    .await;
    assert_eq!(error.kind(), ErrorKind::PreconditionFailed);
    assert!(error.message().contains("Cannot alter map keys"));
}

#[tokio::test]
async fn test_union_by_name_adds_and_evolves_nested_fields() {
    let table = make_v2_table_with_nested();
    let incoming = Schema::builder()
        .with_fields(vec![
            NestedField::optional(
                100,
                "person",
                Type::Struct(StructType::new(vec![
                    NestedField::optional(101, "age", Type::Primitive(PrimitiveType::Long))
                        .with_doc("age in years")
                        .into(),
                    NestedField::required(102, "country", Type::Primitive(PrimitiveType::String))
                        .into(),
                ])),
            )
            .into(),
        ])
        .build()
        .unwrap();
    let schema = apply_schema(
        &table,
        Transaction::new(&table)
            .update_schema()
            .union_by_name(incoming),
    )
    .await;

    let age = schema.field_by_name("person.age").unwrap();
    assert_eq!(age.id, 6);
    assert!(!age.required);
    assert_eq!(
        age.field_type.as_ref(),
        &Type::Primitive(PrimitiveType::Long)
    );
    assert_eq!(age.doc.as_deref(), Some("age in years"));
    let country = schema.field_by_name("person.country").unwrap();
    assert_eq!(country.id, 15);
    assert!(!country.required, "union additions must be optional");
}

#[tokio::test]
async fn test_union_by_name_promotes_type_before_applying_default() {
    let table = table_with_schema(
        Schema::builder()
            .with_fields([
                NestedField::optional(1, "count", Type::Primitive(PrimitiveType::Int)).into(),
            ])
            .build()
            .unwrap(),
    );
    let incoming = Schema::builder()
        .with_fields([
            NestedField::optional(50, "count", Type::Primitive(PrimitiveType::Long))
                .with_write_default(Literal::long(i64::MAX))
                .into(),
        ])
        .build()
        .unwrap();

    let schema = apply_schema(
        &table,
        Transaction::new(&table)
            .update_schema()
            .union_by_name(incoming),
    )
    .await;
    let count = schema.field_by_name("count").unwrap();
    assert_eq!(
        count.field_type.as_ref(),
        &Type::Primitive(PrimitiveType::Long)
    );
    assert_eq!(count.write_default, Some(Literal::long(i64::MAX)));
}

#[tokio::test]
async fn test_union_by_name_accepts_matching_variant_fields() {
    let catalog = new_memory_catalog().await;
    let table = make_v3_minimal_table_in_catalog(&catalog).await;
    let tx = Transaction::new(&table);
    let tx = tx
        .update_schema()
        .add_column(AddColumn::optional("data", Type::Variant(VariantType)))
        .add_column(AddColumn::optional(
            "struct_data",
            Type::Struct(StructType::new(vec![
                NestedField::optional(10, "value", Type::Primitive(PrimitiveType::String)).into(),
            ])),
        ))
        .apply(tx)
        .unwrap();
    let table = tx.commit(&catalog).await.unwrap();
    let incoming = Schema::builder()
        .with_fields([
            NestedField::optional(50, "data", Type::Variant(VariantType)).into(),
            NestedField::optional(51, "struct_data", Type::Variant(VariantType)).into(),
        ])
        .build()
        .unwrap();

    let schema = apply_schema(
        &table,
        Transaction::new(&table)
            .update_schema()
            .union_by_name(incoming),
    )
    .await;
    assert_eq!(
        schema.field_by_name("data").unwrap().field_type.as_ref(),
        &Type::Variant(VariantType)
    );
    assert!(matches!(
        schema
            .field_by_name("struct_data")
            .unwrap()
            .field_type
            .as_ref(),
        Type::Struct(_)
    ));
}

#[tokio::test]
async fn test_name_mapping_tracks_renames_and_additions() {
    let table = make_v2_table();
    let mapping = r#"[
        {"field-id":1,"names":["x"]},
        {"field-id":2,"names":["y"]},
        {"field-id":3,"names":["z"]}
    ]"#;
    let metadata = table
        .metadata()
        .clone()
        .into_builder(None)
        .set_properties(HashMap::from([(
            DEFAULT_SCHEMA_NAME_MAPPING.to_string(),
            mapping.to_string(),
        )]))
        .unwrap()
        .build()
        .unwrap()
        .metadata;
    let table = table.with_metadata(Arc::new(metadata));
    let action = Transaction::new(&table)
        .update_schema()
        .rename_column("z", "payload")
        .add_column(AddColumn::optional(
            "extra",
            Type::Primitive(PrimitiveType::String),
        ));
    let mut commit = Arc::new(action).commit(&table).await.unwrap();
    let property_update = commit
        .take_updates()
        .into_iter()
        .find_map(|update| match update {
            TableUpdate::SetProperties { updates } => {
                updates.get(DEFAULT_SCHEMA_NAME_MAPPING).cloned()
            }
            _ => None,
        })
        .expect("schema update should maintain the name mapping");
    let mapping: NameMapping = serde_json::from_str(&property_update).unwrap();
    let renamed = mapping
        .fields()
        .iter()
        .find(|field| field.field_id() == Some(3))
        .unwrap();
    assert_eq!(renamed.names(), &["z".to_string(), "payload".to_string()]);
    let added = mapping
        .fields()
        .iter()
        .find(|field| field.field_id() == Some(4))
        .unwrap();
    assert_eq!(added.names(), &["extra".to_string()]);
}

#[tokio::test]
async fn test_root_addition_does_not_nest_under_idless_mapping_field() {
    let table = make_v2_table();
    let metadata = table
        .metadata()
        .clone()
        .into_builder(None)
        .set_properties(HashMap::from([(
            DEFAULT_SCHEMA_NAME_MAPPING.to_string(),
            r#"[{"names":["legacy"]}]"#.to_string(),
        )]))
        .unwrap()
        .build()
        .unwrap()
        .metadata;
    let table = table.with_metadata(Arc::new(metadata));
    let action = Transaction::new(&table)
        .update_schema()
        .add_column(AddColumn::optional(
            "extra",
            Type::Primitive(PrimitiveType::String),
        ));
    let mut commit = Arc::new(action).commit(&table).await.unwrap();
    let property_update = commit
        .take_updates()
        .into_iter()
        .find_map(|update| match update {
            TableUpdate::SetProperties { updates } => {
                updates.get(DEFAULT_SCHEMA_NAME_MAPPING).cloned()
            }
            _ => None,
        })
        .unwrap();

    let mapping: NameMapping = serde_json::from_str(&property_update).unwrap();

    assert_eq!(mapping.fields().len(), 2);
    let legacy = mapping
        .fields()
        .iter()
        .find(|field| field.names() == ["legacy".to_string()])
        .unwrap();
    assert!(legacy.fields().is_empty());
    assert!(
        mapping
            .fields()
            .iter()
            .any(|field| { field.field_id() == Some(4) && field.names() == ["extra".to_string()] })
    );
}

#[tokio::test]
async fn test_invalid_name_mapping_does_not_block_update() {
    let table = make_v2_table();
    let metadata = table
        .metadata()
        .clone()
        .into_builder(None)
        .set_properties(HashMap::from([(
            DEFAULT_SCHEMA_NAME_MAPPING.to_string(),
            "{not valid json".to_string(),
        )]))
        .unwrap()
        .build()
        .unwrap()
        .metadata;
    let table = table.with_metadata(Arc::new(metadata));
    let action = Transaction::new(&table)
        .update_schema()
        .rename_column("z", "payload");
    let mut commit = Arc::new(action).commit(&table).await.unwrap();

    let updates = commit.take_updates();
    assert!(updates.iter().any(|update| matches!(
        update,
        TableUpdate::AddSchema { schema }
            if schema.field_by_name("payload").is_some()
    )));
    assert!(!updates.iter().any(|update| matches!(
        update,
        TableUpdate::SetProperties { updates }
            if updates.contains_key(DEFAULT_SCHEMA_NAME_MAPPING)
    )));
}

#[tokio::test]
async fn test_column_properties_follow_renames_and_deletes() {
    let table = make_v2_table_with_nested();
    let metrics_key = "write.metadata.metrics.column.z";
    let bloom_key = "write.parquet.bloom-filter-enabled.column.z";
    let deleted_key = "write.parquet.stats-enabled.column.person.name";
    let metadata = table
        .metadata()
        .clone()
        .into_builder(None)
        .set_properties(HashMap::from([
            (metrics_key.to_string(), "full".to_string()),
            (bloom_key.to_string(), "true".to_string()),
            (deleted_key.to_string(), "false".to_string()),
            (
                "write.metadata.metrics.column.x".to_string(),
                "counts".to_string(),
            ),
        ]))
        .unwrap()
        .build()
        .unwrap()
        .metadata;
    let table = table.with_metadata(Arc::new(metadata));
    let action = Transaction::new(&table)
        .update_schema()
        .rename_column("z", "payload")
        .delete_column("person.name");
    let mut commit = Arc::new(action).commit(&table).await.unwrap();

    let updates = commit.take_updates();
    let removals = updates
        .iter()
        .find_map(|update| match update {
            TableUpdate::RemoveProperties { removals } => Some(removals),
            _ => None,
        })
        .unwrap();
    assert_eq!(removals, &vec![
        metrics_key.to_string(),
        bloom_key.to_string(),
        deleted_key.to_string(),
    ]);
    let property_updates = updates
        .iter()
        .find_map(|update| match update {
            TableUpdate::SetProperties { updates } => Some(updates),
            _ => None,
        })
        .unwrap();
    assert_eq!(
        property_updates.get("write.metadata.metrics.column.payload"),
        Some(&"full".to_string())
    );
    assert_eq!(
        property_updates.get("write.parquet.bloom-filter-enabled.column.payload"),
        Some(&"true".to_string())
    );
    assert!(!property_updates.contains_key(deleted_key));
}

#[tokio::test]
async fn test_rejects_updates_inside_map_keys() {
    let schema = Schema::builder()
        .with_fields([NestedField::optional(
            1,
            "mapped",
            Type::Map(MapType {
                key_field: NestedField::map_key_element(
                    2,
                    Type::Struct(StructType::new(vec![
                        NestedField::required(
                            3,
                            "key_part",
                            Type::Primitive(PrimitiveType::String),
                        )
                        .into(),
                    ])),
                )
                .into(),
                value_field: NestedField::map_value_element(
                    4,
                    Type::Primitive(PrimitiveType::String),
                    false,
                )
                .into(),
            }),
        )
        .into()])
        .build()
        .unwrap();
    let table = table_with_schema(schema);
    let action = Transaction::new(&table)
        .update_schema()
        .rename_column("mapped.key.key_part", "renamed");

    let error = commit_error(&table, action).await;

    assert_eq!(error.kind(), ErrorKind::PreconditionFailed);
    assert!(error.message().contains("Cannot alter map keys"));
}

#[tokio::test]
async fn test_identifier_field_names_follow_renames() {
    let table = make_v2_table();
    let action = Transaction::new(&table)
        .update_schema()
        .set_identifier_fields(["x"])
        .rename_column("x", "renamed_x");

    let schema = apply_schema(&table, action).await;

    let renamed = schema.field_by_name("renamed_x").unwrap();
    assert_eq!(renamed.id, 1);
    assert_eq!(schema.identifier_field_ids().collect::<Vec<_>>(), vec![1]);
}

#[tokio::test]
async fn test_nested_identifier_rename_matches_java_leaf_name_behavior() {
    let table = table_with_nested_identifier_candidate();
    let action = Transaction::new(&table)
        .update_schema()
        .set_identifier_fields(["parent.id"])
        .rename_column("parent.id", "renamed");

    let error = commit_error(&table, action).await;

    assert_eq!(error.kind(), ErrorKind::PreconditionFailed);
    assert!(error.message().contains("not found in updated schema"));
}

#[tokio::test]
async fn test_identifier_rename_uses_exact_selector_spelling() {
    let table = table_with_nested_identifier_candidate();
    let action = Transaction::new(&table)
        .update_schema()
        .case_sensitive(false)
        .set_identifier_fields(["parent.id"])
        .rename_column("PARENT.ID", "renamed");

    let error = commit_error(&table, action).await;

    assert_eq!(error.kind(), ErrorKind::PreconditionFailed);
    assert!(error.message().contains("not found in updated schema"));
}

#[tokio::test]
async fn test_identifier_field_updates_preserve_call_order() {
    let table = make_v2_table();
    let error = commit_error(
        &table,
        Transaction::new(&table)
            .update_schema()
            .rename_column("x", "intermediate_x")
            .rename_column("x", "final_x"),
    )
    .await;
    assert_eq!(error.kind(), ErrorKind::PreconditionFailed);
    assert!(error.message().contains("not found in updated schema"));

    let action = Transaction::new(&table)
        .update_schema()
        .rename_column("x", "renamed_x")
        .set_identifier_fields(["x"]);

    let error = commit_error(&table, action).await;

    assert_eq!(error.kind(), ErrorKind::PreconditionFailed);
    assert!(error.message().contains("not found in updated schema"));

    let error = commit_error(
        &table,
        Transaction::new(&table)
            .update_schema()
            .rename_column("x", "intermediate_x")
            .set_identifier_fields(["intermediate_x"])
            .rename_column("x", "final_x"),
    )
    .await;
    assert_eq!(error.kind(), ErrorKind::PreconditionFailed);
    assert!(error.message().contains("not found in updated schema"));
}

#[tokio::test]
async fn test_identifier_fields_defer_intermediate_name_conflicts() {
    let table = make_v2_table();
    let action = Transaction::new(&table)
        .update_schema()
        .rename_column("x", "y")
        .set_identifier_fields(["y"])
        .rename_column("y", "renamed_y");

    let schema = apply_schema(&table, action).await;

    assert_eq!(schema.field_by_name("y").unwrap().id, 1);
    let identifier = schema.field_by_name("renamed_y").unwrap();
    assert_eq!(identifier.id, 2);
    assert_eq!(schema.identifier_field_ids().collect::<Vec<_>>(), vec![
        identifier.id
    ]);
}

#[tokio::test]
async fn test_collection_pseudo_fields_remain_canonical() {
    let schema = Schema::builder()
        .with_fields([
            NestedField::optional(
                1,
                "items",
                Type::List(ListType {
                    element_field: NestedField::list_element(
                        2,
                        Type::Primitive(PrimitiveType::String),
                        false,
                    )
                    .into(),
                }),
            )
            .into(),
            NestedField::optional(
                3,
                "properties",
                Type::Map(MapType {
                    key_field: NestedField::map_key_element(
                        4,
                        Type::Primitive(PrimitiveType::String),
                    )
                    .into(),
                    value_field: NestedField::map_value_element(
                        5,
                        Type::Primitive(PrimitiveType::String),
                        false,
                    )
                    .into(),
                }),
            )
            .into(),
        ])
        .build()
        .unwrap();
    let table = table_with_schema(schema);
    let action = Transaction::new(&table)
        .update_schema()
        .rename_column("items.element", "renamed_element")
        .update_column_doc("properties.value", Some("not serialized".to_string()));

    let schema = apply_schema(&table, action).await;

    let element = schema.field_by_name("items.element").unwrap();
    assert_eq!(element.name, "element");
    let value = schema.field_by_name("properties.value").unwrap();
    assert_eq!(value.name, "value");
    assert_eq!(value.doc, None);
    let round_tripped: Schema =
        serde_json::from_str(&serde_json::to_string(&schema).unwrap()).unwrap();
    assert_eq!(round_tripped, schema);
}

#[tokio::test]
async fn test_pseudo_field_mapping_update_does_not_stage_unchanged_schema() {
    let table = make_v2_table_with_nested();
    let mapping = r#"[
        {
            "field-id": 7,
            "names": ["tags"],
            "fields": [
                {"field-id": 8, "names": ["element"]}
            ]
        }
    ]"#;
    let metadata = table
        .metadata()
        .clone()
        .into_builder(None)
        .set_properties(HashMap::from([(
            DEFAULT_SCHEMA_NAME_MAPPING.to_string(),
            mapping.to_string(),
        )]))
        .unwrap()
        .build()
        .unwrap()
        .metadata;
    let table = table.with_metadata(Arc::new(metadata));
    let action = Transaction::new(&table)
        .update_schema()
        .rename_column("tags.element", "renamed_element");

    let mut commit = Arc::new(action).commit(&table).await.unwrap();
    let updates = commit.take_updates();
    let requirements = commit.take_requirements();

    assert_eq!(updates.len(), 1);
    let TableUpdate::SetProperties { updates } = &updates[0] else {
        panic!(
            "expected name-mapping property update, got {:?}",
            updates[0]
        );
    };
    let updated_mapping: NameMapping =
        serde_json::from_str(updates.get(DEFAULT_SCHEMA_NAME_MAPPING).unwrap()).unwrap();
    let element = &updated_mapping.fields()[0].fields()[0];
    assert_eq!(element.names(), &["element", "renamed_element"]);
    assert_eq!(requirements, vec![TableRequirement::UuidMatch {
        uuid: table.metadata().uuid(),
    }]);
}

#[tokio::test]
async fn test_adds_literal_dotted_root_name() {
    let table = make_v2_table();
    let action = Transaction::new(&table)
        .update_schema()
        .add_column(AddColumn::optional(
            "dot.field",
            Type::Primitive(PrimitiveType::Int),
        ));

    let schema = apply_schema(&table, action).await;

    assert!(
        schema
            .as_struct()
            .fields()
            .iter()
            .any(|field| field.name == "dot.field")
    );
}

#[tokio::test]
async fn test_case_insensitive_updates_reject_name_collisions() {
    let schema = Schema::builder()
        .with_fields([
            NestedField::optional(1, "Foo", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::optional(2, "foo", Type::Primitive(PrimitiveType::String)).into(),
        ])
        .build()
        .unwrap();
    assert!(schema.field_by_name_case_insensitive("FOO").is_none());
    let table = table_with_schema(schema);
    let action = Transaction::new(&table)
        .update_schema()
        .case_sensitive(false)
        .rename_column("FOO", "renamed");

    let error = commit_error(&table, action).await;

    assert_eq!(error.kind(), ErrorKind::PreconditionFailed);
    assert!(error.message().contains("multiple fields match"));

    let error = commit_error(
        &table,
        Transaction::new(&table)
            .update_schema()
            .delete_column("Foo")
            .case_sensitive(false)
            .rename_column("foo", "renamed")
            .case_sensitive(true),
    )
    .await;
    assert_eq!(error.kind(), ErrorKind::PreconditionFailed);
    assert!(error.message().contains("multiple fields match"));
}

#[tokio::test]
async fn test_case_insensitive_updates_cannot_create_name_collisions() {
    let schema = Schema::builder()
        .with_fields([
            NestedField::optional(1, "Foo", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::optional(2, "bar", Type::Primitive(PrimitiveType::String)).into(),
        ])
        .build()
        .unwrap();
    let table = table_with_schema(schema);
    let action = Transaction::new(&table)
        .update_schema()
        .case_sensitive(false)
        .rename_column("bar", "foo");

    let error = commit_error(&table, action).await;

    assert_eq!(error.kind(), ErrorKind::PreconditionFailed);
    assert!(error.message().contains("multiple fields match"));
}

#[tokio::test]
async fn test_identifier_fields_are_resolved_using_final_case_sensitivity() {
    let schema = Schema::builder()
        .with_fields([
            NestedField::required(1, "Foo", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::required(2, "foo", Type::Primitive(PrimitiveType::String)).into(),
        ])
        .build()
        .unwrap();
    let table = table_with_schema(schema);
    let schema = apply_schema(
        &table,
        Transaction::new(&table)
            .update_schema()
            .case_sensitive(false)
            .set_identifier_fields(["Foo"])
            .case_sensitive(true),
    )
    .await;

    assert_eq!(schema.identifier_field_ids().collect::<Vec<_>>(), vec![
        schema.field_by_name("Foo").unwrap().id
    ]);
}

#[tokio::test]
async fn test_default_literals_are_coerced_to_column_types() {
    let table = make_v2_table();
    let decimal_type = PrimitiveType::Decimal {
        precision: 9,
        scale: 2,
    };
    let action = Transaction::new(&table)
        .update_schema()
        .add_column(
            AddColumn::builder()
                .name("float_default")
                .field_type(Type::Primitive(PrimitiveType::Float))
                .initial_default(Literal::int(1))
                .write_default(Literal::int(2))
                .build(),
        )
        .add_column(
            AddColumn::builder()
                .name("double_default")
                .field_type(Type::Primitive(PrimitiveType::Double))
                .initial_default(Literal::long(3))
                .write_default(Literal::long(4))
                .build(),
        )
        .add_column(
            AddColumn::builder()
                .name("decimal_default")
                .field_type(Type::Primitive(decimal_type.clone()))
                .initial_default(Literal::int(5))
                .write_default(Literal::double(6.255))
                .build(),
        )
        .add_column(
            AddColumn::builder()
                .name("decimal_string_default")
                .field_type(Type::Primitive(decimal_type))
                .initial_default(Literal::string("1.23"))
                .build(),
        )
        .add_column(
            AddColumn::builder()
                .name("date_default")
                .field_type(Type::Primitive(PrimitiveType::Date))
                .initial_default(Literal::string("2024-01-02"))
                .write_default(Literal::string("2024-01-03"))
                .build(),
        )
        .add_column(
            AddColumn::builder()
                .name("timestamptz_default")
                .field_type(Type::Primitive(PrimitiveType::Timestamptz))
                .initial_default(Literal::string("2024-01-02T03:04:05-05:00"))
                .write_default(Literal::string("2024-01-03T03:04:05+02:00"))
                .build(),
        )
        .add_column(
            AddColumn::builder()
                .name("pre_epoch_timestamptz_default")
                .field_type(Type::Primitive(PrimitiveType::Timestamptz))
                .initial_default(Literal::string("1969-12-31T23:59:59.999999Z"))
                .build(),
        )
        .add_column(
            AddColumn::builder()
                .name("submicro_timestamp_default")
                .field_type(Type::Primitive(PrimitiveType::Timestamp))
                .initial_default(Literal::string("1969-12-31T23:59:59.999999999"))
                .build(),
        )
        .add_column(
            AddColumn::builder()
                .name("submicro_timestamptz_default")
                .field_type(Type::Primitive(PrimitiveType::Timestamptz))
                .initial_default(Literal::string("1969-12-31T23:59:59.999999999Z"))
                .build(),
        )
        .add_column(
            AddColumn::builder()
                .name("pre_epoch_timestamptz_ns_default")
                .field_type(Type::Primitive(PrimitiveType::TimestamptzNs))
                .initial_default(Literal::string("1969-12-31T23:59:59.999999999Z"))
                .build(),
        )
        .add_column(
            AddColumn::builder()
                .name("uuid_default")
                .field_type(Type::Primitive(PrimitiveType::Uuid))
                .initial_default(Literal::string("a1a2a3a4-b1b2-c1c2-d1d2-d3d4d5d6d7d8"))
                .write_default(Literal::string("d8d7d6d5-d4d3-d2d1-c2c1-b2b1a4a3a2a1"))
                .build(),
        );

    let schema = apply_schema(&table, action).await;

    let float_default = schema.field_by_name("float_default").unwrap();
    assert_eq!(float_default.initial_default, Some(Literal::float(1.0)));
    assert_eq!(float_default.write_default, Some(Literal::float(2.0)));
    let double_default = schema.field_by_name("double_default").unwrap();
    assert_eq!(double_default.initial_default, Some(Literal::double(3.0)));
    assert_eq!(double_default.write_default, Some(Literal::double(4.0)));
    let decimal_default = schema.field_by_name("decimal_default").unwrap();
    assert_eq!(decimal_default.initial_default, Some(Literal::decimal(500)));
    assert_eq!(decimal_default.write_default, Some(Literal::decimal(626)));
    let decimal_string_default = schema.field_by_name("decimal_string_default").unwrap();
    assert_eq!(
        decimal_string_default.initial_default,
        Some(Literal::decimal(123))
    );
    let date_default = schema.field_by_name("date_default").unwrap();
    assert_eq!(
        date_default.initial_default,
        Some(Literal::date_from_str("2024-01-02").unwrap())
    );
    assert_eq!(
        date_default.write_default,
        Some(Literal::date_from_str("2024-01-03").unwrap())
    );
    let timestamptz_default = schema.field_by_name("timestamptz_default").unwrap();
    assert_eq!(
        timestamptz_default.initial_default,
        Some(Literal::timestamptz(
            chrono::DateTime::parse_from_rfc3339("2024-01-02T03:04:05-05:00")
                .unwrap()
                .timestamp_micros()
        ))
    );
    assert_eq!(
        timestamptz_default.write_default,
        Some(Literal::timestamptz(
            chrono::DateTime::parse_from_rfc3339("2024-01-03T03:04:05+02:00")
                .unwrap()
                .timestamp_micros()
        ))
    );
    let pre_epoch_timestamptz_default = schema
        .field_by_name("pre_epoch_timestamptz_default")
        .unwrap();
    assert_eq!(
        pre_epoch_timestamptz_default.initial_default,
        Some(Literal::timestamptz(-1))
    );
    let submicro_timestamp_default = schema.field_by_name("submicro_timestamp_default").unwrap();
    assert_eq!(
        submicro_timestamp_default.initial_default,
        Some(Literal::timestamp(0))
    );
    let submicro_timestamptz_default = schema
        .field_by_name("submicro_timestamptz_default")
        .unwrap();
    assert_eq!(
        submicro_timestamptz_default.initial_default,
        Some(Literal::timestamptz(0))
    );
    let pre_epoch_timestamptz_ns_default = schema
        .field_by_name("pre_epoch_timestamptz_ns_default")
        .unwrap();
    assert_eq!(
        pre_epoch_timestamptz_ns_default.initial_default,
        Some(Literal::timestamptz_nano(-1))
    );
    let uuid_default = schema.field_by_name("uuid_default").unwrap();
    assert_eq!(
        uuid_default.initial_default,
        Some(Literal::uuid_from_str("a1a2a3a4-b1b2-c1c2-d1d2-d3d4d5d6d7d8").unwrap())
    );
    assert_eq!(
        uuid_default.write_default,
        Some(Literal::uuid_from_str("d8d7d6d5-d4d3-d2d1-c2c1-b2b1a4a3a2a1").unwrap())
    );
}

#[tokio::test]
async fn test_java_temporal_and_uuid_string_defaults() {
    let table = make_v2_table();
    let action = Transaction::new(&table)
        .update_schema()
        .add_column(
            AddColumn::builder()
                .name("time_default")
                .field_type(Type::Primitive(PrimitiveType::Time))
                .initial_default(Literal::string("12:30"))
                .build(),
        )
        .add_column(
            AddColumn::builder()
                .name("timestamp_default")
                .field_type(Type::Primitive(PrimitiveType::Timestamp))
                .initial_default(Literal::string("2024-01-02t12:30"))
                .build(),
        )
        .add_column(
            AddColumn::builder()
                .name("timestamptz_default")
                .field_type(Type::Primitive(PrimitiveType::Timestamptz))
                .initial_default(Literal::string("2024-01-02T12:30+01:02:03"))
                .build(),
        )
        .add_column(
            AddColumn::builder()
                .name("timestamp_ns_default")
                .field_type(Type::Primitive(PrimitiveType::TimestampNs))
                .initial_default(Literal::string("2024-01-02T12:30"))
                .build(),
        )
        .add_column(
            AddColumn::builder()
                .name("timestamptz_ns_default")
                .field_type(Type::Primitive(PrimitiveType::TimestamptzNs))
                .initial_default(Literal::string("2024-01-02t12:30z"))
                .build(),
        )
        .add_column(
            AddColumn::builder()
                .name("uuid_default")
                .field_type(Type::Primitive(PrimitiveType::Uuid))
                .initial_default(Literal::string("1-1-1-1-1"))
                .build(),
        );

    let schema = apply_schema(&table, action).await;
    assert_eq!(
        schema
            .field_by_name("time_default")
            .unwrap()
            .initial_default,
        Some(Literal::time(45_000_000_000))
    );
    let local = chrono::NaiveDate::from_ymd_opt(2024, 1, 2)
        .unwrap()
        .and_hms_opt(12, 30, 0)
        .unwrap()
        .and_utc();
    assert_eq!(
        schema
            .field_by_name("timestamp_default")
            .unwrap()
            .initial_default,
        Some(Literal::timestamp(local.timestamp_micros()))
    );
    assert_eq!(
        schema
            .field_by_name("timestamptz_default")
            .unwrap()
            .initial_default,
        Some(Literal::timestamptz(
            local.timestamp_micros() - 3_723_000_000
        ))
    );
    assert_eq!(
        schema
            .field_by_name("timestamp_ns_default")
            .unwrap()
            .initial_default,
        Some(Literal::timestamp_nano(
            local.timestamp_nanos_opt().unwrap()
        ))
    );
    assert_eq!(
        schema
            .field_by_name("timestamptz_ns_default")
            .unwrap()
            .initial_default,
        Some(Literal::timestamptz_nano(
            local.timestamp_nanos_opt().unwrap()
        ))
    );
    assert_eq!(
        schema
            .field_by_name("uuid_default")
            .unwrap()
            .initial_default,
        Some(Literal::uuid_from_str("00000001-0001-0001-0001-000000000001").unwrap())
    );
}

#[tokio::test]
async fn test_signed_zero_default_updates_are_committed() {
    let catalog = new_memory_catalog().await;
    let table = make_v3_minimal_table_in_catalog(&catalog).await;
    let tx = Transaction::new(&table);
    let tx = tx
        .update_schema()
        .add_column(
            AddColumn::builder()
                .name("float_zero")
                .field_type(Type::Primitive(PrimitiveType::Float))
                .write_default(Literal::float(-0.0))
                .build(),
        )
        .add_column(
            AddColumn::builder()
                .name("double_zero")
                .field_type(Type::Primitive(PrimitiveType::Double))
                .write_default(Literal::double(-0.0))
                .build(),
        )
        .apply(tx)
        .unwrap();
    let table = tx.commit(&catalog).await.unwrap();

    let tx = Transaction::new(&table);
    let tx = tx
        .update_schema()
        .update_column_default("float_zero", Some(Literal::float(0.0)))
        .update_column_default("double_zero", Some(Literal::double(0.0)))
        .apply(tx)
        .unwrap();
    let table = tx.commit(&catalog).await.unwrap();

    let Some(Literal::Primitive(PrimitiveLiteral::Float(float))) = &table
        .metadata()
        .current_schema()
        .field_by_name("float_zero")
        .unwrap()
        .write_default
    else {
        panic!("expected float default");
    };
    assert_eq!(float.0.to_bits(), 0.0_f32.to_bits());
    let Some(Literal::Primitive(PrimitiveLiteral::Double(double))) = &table
        .metadata()
        .current_schema()
        .field_by_name("double_zero")
        .unwrap()
        .write_default
    else {
        panic!("expected double default");
    };
    assert_eq!(double.0.to_bits(), 0.0_f64.to_bits());
}

#[tokio::test]
async fn test_invalid_default_coercions_are_rejected() {
    let table = make_v2_table();
    let nan_error = commit_error(
        &table,
        Transaction::new(&table).update_schema().add_column(
            AddColumn::builder()
                .name("nan_default")
                .field_type(Type::Primitive(PrimitiveType::Float))
                .initial_default(Literal::float(f32::NAN))
                .build(),
        ),
    )
    .await;
    assert_eq!(nan_error.kind(), ErrorKind::PreconditionFailed);
    assert!(nan_error.message().contains("Invalid default"));

    let precision_error = commit_error(
        &table,
        Transaction::new(&table).update_schema().add_column(
            AddColumn::builder()
                .name("decimal_default")
                .field_type(Type::Primitive(PrimitiveType::Decimal {
                    precision: 2,
                    scale: 0,
                }))
                .initial_default(Literal::int(123))
                .build(),
        ),
    )
    .await;
    assert_eq!(precision_error.kind(), ErrorKind::PreconditionFailed);
    assert!(precision_error.message().contains("Invalid default"));

    for value in ["1.2", "1.234"] {
        let error = commit_error(
            &table,
            Transaction::new(&table).update_schema().add_column(
                AddColumn::builder()
                    .name("decimal_string_default")
                    .field_type(Type::Primitive(PrimitiveType::Decimal {
                        precision: 9,
                        scale: 2,
                    }))
                    .initial_default(Literal::string(value))
                    .build(),
            ),
        )
        .await;
        assert_eq!(error.kind(), ErrorKind::PreconditionFailed, "{value}");
        assert!(error.message().contains("Invalid default"), "{value}");
    }

    let exponent_error = commit_error(
        &table,
        Transaction::new(&table).update_schema().add_column(
            AddColumn::builder()
                .name("decimal_exponent_default")
                .field_type(Type::Primitive(PrimitiveType::Decimal {
                    precision: 3,
                    scale: 0,
                }))
                .initial_default(Literal::string("1E+2"))
                .build(),
        ),
    )
    .await;
    assert_eq!(exponent_error.kind(), ErrorKind::PreconditionFailed);

    let compact_uuid_error = commit_error(
        &table,
        Transaction::new(&table).update_schema().add_column(
            AddColumn::builder()
                .name("uuid_default")
                .field_type(Type::Primitive(PrimitiveType::Uuid))
                .initial_default(Literal::string("a1a2a3a4b1b2c1c2d1d2d3d4d5d6d7d8"))
                .build(),
        ),
    )
    .await;
    assert_eq!(compact_uuid_error.kind(), ErrorKind::PreconditionFailed);

    for (field_type, value) in [
        (PrimitiveType::Time, "23:59:60"),
        (PrimitiveType::Timestamp, "2024-01-02T23:59:60"),
        (PrimitiveType::Timestamptz, "2024-01-02T23:59:60Z"),
        (PrimitiveType::TimestampNs, "2024-01-02T23:59:60"),
        (PrimitiveType::TimestamptzNs, "2024-01-02T23:59:60Z"),
        (PrimitiveType::Time, "12:30:00.1234567890"),
        (PrimitiveType::Timestamp, "2024-01-02T12:30:00.1234567890"),
        (
            PrimitiveType::Timestamptz,
            "2024-01-02T12:30:00.1234567890Z",
        ),
        (PrimitiveType::Timestamptz, "2024-01-02 12:30:00Z"),
        (PrimitiveType::Timestamptz, "2024-01-02T12:30:00+18:00:01"),
    ] {
        let error = commit_error(
            &table,
            Transaction::new(&table).update_schema().add_column(
                AddColumn::builder()
                    .name("temporal_default")
                    .field_type(Type::Primitive(field_type))
                    .initial_default(Literal::string(value))
                    .build(),
            ),
        )
        .await;
        assert_eq!(error.kind(), ErrorKind::PreconditionFailed, "{value}");
    }

    let time_error = commit_error(
        &table,
        Transaction::new(&table).update_schema().add_column(
            AddColumn::builder()
                .name("time_default")
                .field_type(Type::Primitive(PrimitiveType::Time))
                .initial_default(Literal::time(i64::MAX))
                .build(),
        ),
    )
    .await;
    assert_eq!(time_error.kind(), ErrorKind::PreconditionFailed);
    assert!(time_error.message().contains("Invalid default"));
}

#[tokio::test]
async fn test_full_range_temporal_defaults_round_trip() {
    let table = make_v2_table();
    let action = Transaction::new(&table)
        .update_schema()
        .add_column(
            AddColumn::builder()
                .name("date_max")
                .field_type(Type::Primitive(PrimitiveType::Date))
                .initial_default(Literal::date(i32::MAX))
                .build(),
        )
        .add_column(
            AddColumn::builder()
                .name("date_min")
                .field_type(Type::Primitive(PrimitiveType::Date))
                .initial_default(Literal::string("-5877641-06-23"))
                .build(),
        )
        .add_column(
            AddColumn::builder()
                .name("timestamp_max")
                .field_type(Type::Primitive(PrimitiveType::Timestamp))
                .initial_default(Literal::timestamp(i64::MAX))
                .build(),
        )
        .add_column(
            AddColumn::builder()
                .name("timestamp_min")
                .field_type(Type::Primitive(PrimitiveType::Timestamp))
                .initial_default(Literal::string("-290308-12-21T19:59:05.224192"))
                .build(),
        )
        .add_column(
            AddColumn::builder()
                .name("timestamptz_max")
                .field_type(Type::Primitive(PrimitiveType::Timestamptz))
                .initial_default(Literal::timestamptz(i64::MAX))
                .build(),
        )
        .add_column(
            AddColumn::builder()
                .name("timestamptz_min")
                .field_type(Type::Primitive(PrimitiveType::Timestamptz))
                .initial_default(Literal::string("-290308-12-21T19:59:05.224192+00:00"))
                .build(),
        );

    let schema = apply_schema(&table, action).await;
    for (name, expected) in [
        ("date_max", Literal::date(i32::MAX)),
        ("date_min", Literal::date(i32::MIN)),
        ("timestamp_max", Literal::timestamp(i64::MAX)),
        ("timestamp_min", Literal::timestamp(i64::MIN)),
        ("timestamptz_max", Literal::timestamptz(i64::MAX)),
        ("timestamptz_min", Literal::timestamptz(i64::MIN)),
    ] {
        assert_eq!(
            schema.field_by_name(name).unwrap().initial_default,
            Some(expected)
        );
    }

    let round_tripped: Schema =
        serde_json::from_str(&serde_json::to_string(&schema).unwrap()).unwrap();
    assert_eq!(round_tripped, schema);
}
