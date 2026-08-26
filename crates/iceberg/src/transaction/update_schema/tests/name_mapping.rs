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
use std::sync::Arc;

use super::tests::make_v2_table_with_nested;
use super::{AddColumn, UpdateSchemaAction};
use crate::spec::{
    DEFAULT_SCHEMA_NAME_MAPPING, ListType, MapType, MappedField, NameMapping, NestedField,
    PrimitiveType, StructType, Type,
};
use crate::table::Table;
use crate::transaction::Transaction;
use crate::transaction::action::TransactionAction;
use crate::{TableRequirement, TableUpdate};

fn with_properties(table: Table, properties: HashMap<String, String>) -> Table {
    let metadata = table
        .metadata()
        .clone()
        .into_builder(None)
        .set_properties(properties)
        .unwrap()
        .build()
        .unwrap()
        .metadata;
    table.with_metadata(Arc::new(metadata))
}

async fn commit_parts(
    table: &Table,
    action: UpdateSchemaAction,
) -> (Vec<TableUpdate>, Vec<TableRequirement>) {
    let mut commit = Arc::new(action).commit(table).await.unwrap();
    (commit.take_updates(), commit.take_requirements())
}

fn apply_updates(table: &Table, updates: &[TableUpdate]) -> Table {
    let mut builder = table.metadata().clone().into_builder(None);
    for update in updates {
        builder = update.clone().apply(builder).unwrap();
    }
    table
        .clone()
        .with_metadata(Arc::new(builder.build().unwrap().metadata))
}

fn mapping_json(updates: &[TableUpdate]) -> Option<&str> {
    updates.iter().find_map(|update| match update {
        TableUpdate::SetProperties { updates } => {
            updates.get(DEFAULT_SCHEMA_NAME_MAPPING).map(String::as_str)
        }
        _ => None,
    })
}

fn mapped_field_by_id(fields: &[MappedField], id: i32) -> Option<&MappedField> {
    for field in fields {
        if let Some(found) = mapped_field_or_descendant(field, id) {
            return Some(found);
        }
    }
    None
}

fn mapped_field_or_descendant(field: &MappedField, id: i32) -> Option<&MappedField> {
    if field.field_id() == Some(id) {
        return Some(field);
    }
    for child in field.fields() {
        if let Some(found) = mapped_field_or_descendant(child, id) {
            return Some(found);
        }
    }
    None
}

fn mapped_names(mapping: &NameMapping, id: i32) -> Vec<String> {
    mapped_field_by_id(mapping.fields(), id)
        .unwrap_or_else(|| panic!("missing mapped field {id}"))
        .names()
        .to_vec()
}

#[tokio::test]
async fn mapping_tracks_root_and_nested_additions() {
    let raw_mapping = r#"[
        {"field-id":3,"names":["legacy_z","z"]},
        {"field-id":4,"names":["person"],"fields":[
            {"field-id":5,"names":["name"]}
        ]},
        {"field-id":7,"names":["tags"],"fields":[
            {"field-id":8,"names":["element"]}
        ]},
        {"field-id":11,"names":["props"],"fields":[
            {"field-id":12,"names":["key"]},
            {"field-id":13,"names":["value"]}
        ]}
    ]"#;
    let table = with_properties(
        make_v2_table_with_nested(),
        HashMap::from([(
            DEFAULT_SCHEMA_NAME_MAPPING.to_string(),
            raw_mapping.to_string(),
        )]),
    );
    let action = Transaction::new(&table)
        .update_schema()
        .add_column(AddColumn::optional(
            "extra",
            Type::Primitive(PrimitiveType::String),
        ))
        .add_column(
            AddColumn::builder()
                .parent("tags")
                .name("score")
                .field_type(Type::Primitive(PrimitiveType::Double))
                .build(),
        )
        .add_column(
            AddColumn::builder()
                .parent("props")
                .name("version")
                .field_type(Type::Primitive(PrimitiveType::Int))
                .build(),
        );
    let (updates, _) = commit_parts(&table, action).await;
    let mapping: NameMapping = serde_json::from_str(mapping_json(&updates).unwrap()).unwrap();

    assert_eq!(mapped_names(&mapping, 3), ["legacy_z", "z"]);
    assert_eq!(mapped_names(&mapping, 15), ["extra"]);
    assert_eq!(mapped_names(&mapping, 16), ["score"]);
    assert_eq!(mapped_names(&mapping, 17), ["version"]);

    assert!(
        mapping
            .fields()
            .iter()
            .any(|field| field.field_id() == Some(15))
    );
}

#[tokio::test]
async fn replacement_addition_reassigns_the_deleted_fields_alias() {
    let metrics_key = "write.metadata.metrics.column.z";
    let table = with_properties(
        make_v2_table_with_nested(),
        HashMap::from([
            (
                DEFAULT_SCHEMA_NAME_MAPPING.to_string(),
                r#"[{"field-id":3,"names":["z","legacy_z"]}]"#.to_string(),
            ),
            (metrics_key.to_string(), "full".to_string()),
        ]),
    );
    let action = Transaction::new(&table)
        .update_schema()
        .delete_column("z")
        .add_column(AddColumn::optional(
            "z",
            Type::Primitive(PrimitiveType::String),
        ));
    let (updates, _) = commit_parts(&table, action).await;
    let mapping: NameMapping = serde_json::from_str(mapping_json(&updates).unwrap()).unwrap();

    assert_eq!(mapped_names(&mapping, 3), ["legacy_z"]);
    assert_eq!(mapped_names(&mapping, 15), ["z"]);
    let remove_index = updates
        .iter()
        .position(|update| matches!(update, TableUpdate::RemoveProperties { .. }))
        .unwrap();
    let set_index = updates
        .iter()
        .position(|update| matches!(update, TableUpdate::SetProperties { .. }))
        .unwrap();
    assert!(remove_index < set_index);

    let committed = apply_updates(&table, &updates);
    assert!(!committed.metadata().properties().contains_key(metrics_key));
}

#[tokio::test]
async fn mapping_adds_all_nested_collection_fields() {
    let table = with_properties(
        crate::transaction::tests::make_v2_table(),
        HashMap::from([(DEFAULT_SCHEMA_NAME_MAPPING.to_string(), "[]".to_string())]),
    );
    let complex_type = Type::Map(MapType {
        key_field: NestedField::map_key_element(100, Type::Primitive(PrimitiveType::String)).into(),
        value_field: NestedField::map_value_element(
            101,
            Type::List(ListType {
                element_field: NestedField::list_element(
                    102,
                    Type::Struct(StructType::new(vec![
                        NestedField::optional(103, "leaf", Type::Primitive(PrimitiveType::Long))
                            .into(),
                    ])),
                    false,
                )
                .into(),
            }),
            false,
        )
        .into(),
    });
    let action = Transaction::new(&table)
        .update_schema()
        .add_column(AddColumn::optional("complex", complex_type));
    let (updates, _) = commit_parts(&table, action).await;
    let mapping: NameMapping = serde_json::from_str(mapping_json(&updates).unwrap()).unwrap();

    for (id, name) in [
        (4, "complex"),
        (5, "key"),
        (6, "value"),
        (7, "element"),
        (8, "leaf"),
    ] {
        assert_eq!(mapped_names(&mapping, id), [name]);
    }
}

#[tokio::test]
async fn invalid_mappings_are_left_raw_without_blocking_other_changes() {
    let invalid_mappings = [
        "{not valid json",
        r#"[
            {"field-id":3,"names":["z"]},
            {"field-id":20,"names":["outer"],"fields":[
                {"field-id":3,"names":["inner"]}
            ]}
        ]"#,
        r#"[
            {"field-id":20,"names":["alias"]},
            {"field-id":21,"names":["alias"]}
        ]"#,
        r#"[
            {"field-id":20,"names":["a.b"]},
            {"field-id":21,"names":["a"],"fields":[
                {"field-id":22,"names":["b"]}
            ]}
        ]"#,
        r#"[
            {"names":["first"]},
            {"names":["second"]}
        ]"#,
        r#"[{"names":["idless"]}]"#,
        r#"[{"field-id":3,"names":["z"],"fields":null}]"#,
    ];

    for raw_mapping in invalid_mappings {
        let metrics_key = "write.metadata.metrics.column.z";
        let table = with_properties(
            make_v2_table_with_nested(),
            HashMap::from([
                (
                    DEFAULT_SCHEMA_NAME_MAPPING.to_string(),
                    raw_mapping.to_string(),
                ),
                (metrics_key.to_string(), "full".to_string()),
            ]),
        );
        let action = Transaction::new(&table)
            .update_schema()
            .delete_column("z")
            .add_column(AddColumn::optional(
                "extra",
                Type::Primitive(PrimitiveType::String),
            ));
        let (updates, _) = commit_parts(&table, action).await;

        assert!(mapping_json(&updates).is_none());
        let committed = apply_updates(&table, &updates);
        assert_eq!(
            committed
                .metadata()
                .properties()
                .get(DEFAULT_SCHEMA_NAME_MAPPING)
                .map(String::as_str),
            Some(raw_mapping)
        );
        assert!(!committed.metadata().properties().contains_key(metrics_key));
        assert!(
            committed
                .metadata()
                .current_schema()
                .field_by_name("extra")
                .is_some()
        );
    }
}

#[tokio::test]
async fn output_conflicts_leave_the_mapping_unchanged() {
    let cases = [
        (r#"[{"field-id":15,"names":["future"]}]"#, None, "extra"),
        (
            r#"[
                {"field-id":4,"names":["person"]},
                {"field-id":20,"names":["person.new"]}
            ]"#,
            Some("person"),
            "new",
        ),
    ];

    for (raw_mapping, parent, name) in cases {
        let table = with_properties(
            make_v2_table_with_nested(),
            HashMap::from([(
                DEFAULT_SCHEMA_NAME_MAPPING.to_string(),
                raw_mapping.to_string(),
            )]),
        );
        let add = AddColumn::builder()
            .name(name)
            .field_type(Type::Primitive(PrimitiveType::String));
        let add = match parent {
            Some(parent) => add.parent(parent).build(),
            None => add.build(),
        };
        let action = Transaction::new(&table).update_schema().add_column(add);
        let (updates, _) = commit_parts(&table, action).await;

        assert!(mapping_json(&updates).is_none());
        assert_eq!(
            apply_updates(&table, &updates)
                .metadata()
                .properties()
                .get(DEFAULT_SCHEMA_NAME_MAPPING)
                .map(String::as_str),
            Some(raw_mapping)
        );
    }
}

#[tokio::test]
async fn multiple_alias_reassignments_leave_the_mapping_unchanged() {
    let raw_mapping = r#"[{"field-id":3,"names":["first","second"]}]"#;
    let table = with_properties(
        make_v2_table_with_nested(),
        HashMap::from([(
            DEFAULT_SCHEMA_NAME_MAPPING.to_string(),
            raw_mapping.to_string(),
        )]),
    );
    let action = Transaction::new(&table)
        .update_schema()
        .add_column(AddColumn::optional(
            "first",
            Type::Primitive(PrimitiveType::String),
        ))
        .add_column(AddColumn::optional(
            "second",
            Type::Primitive(PrimitiveType::String),
        ));
    let (updates, _) = commit_parts(&table, action).await;

    assert!(mapping_json(&updates).is_none());
    let committed = apply_updates(&table, &updates);
    assert_eq!(
        committed
            .metadata()
            .properties()
            .get(DEFAULT_SCHEMA_NAME_MAPPING)
            .map(String::as_str),
        Some(raw_mapping)
    );
    assert!(
        committed
            .metadata()
            .current_schema()
            .field_by_name("first")
            .is_some()
    );
    assert!(
        committed
            .metadata()
            .current_schema()
            .field_by_name("second")
            .is_some()
    );
}

#[tokio::test]
async fn deletions_leave_name_mappings_unchanged() {
    let raw_mapping = r#"[{"field-id":3,"names":["z","legacy_z"]}]"#;
    let table = with_properties(
        make_v2_table_with_nested(),
        HashMap::from([(
            DEFAULT_SCHEMA_NAME_MAPPING.to_string(),
            raw_mapping.to_string(),
        )]),
    );
    let action = Transaction::new(&table).update_schema().delete_column("z");
    let (updates, _) = commit_parts(&table, action).await;

    assert!(mapping_json(&updates).is_none());
    assert_eq!(
        apply_updates(&table, &updates)
            .metadata()
            .properties()
            .get(DEFAULT_SCHEMA_NAME_MAPPING)
            .map(String::as_str),
        Some(raw_mapping)
    );
}

#[tokio::test]
async fn valid_omitted_names_are_preserved_when_extending_a_mapping() {
    let table = with_properties(
        make_v2_table_with_nested(),
        HashMap::from([(
            DEFAULT_SCHEMA_NAME_MAPPING.to_string(),
            r#"[{"field-id":3}]"#.to_string(),
        )]),
    );
    let action = Transaction::new(&table)
        .update_schema()
        .add_column(AddColumn::optional(
            "extra",
            Type::Primitive(PrimitiveType::String),
        ));
    let (updates, _) = commit_parts(&table, action).await;
    let mapping: NameMapping = serde_json::from_str(mapping_json(&updates).unwrap()).unwrap();

    assert!(mapped_names(&mapping, 3).is_empty());
    assert_eq!(mapped_names(&mapping, 15), ["extra"]);
}

#[tokio::test]
async fn nested_additions_are_ignored_when_the_parent_is_not_mapped() {
    let table = with_properties(
        make_v2_table_with_nested(),
        HashMap::from([(DEFAULT_SCHEMA_NAME_MAPPING.to_string(), "[]".to_string())]),
    );
    let action = Transaction::new(&table).update_schema().add_column(
        AddColumn::builder()
            .parent("person")
            .name("new")
            .field_type(Type::Primitive(PrimitiveType::String))
            .build(),
    );
    let (updates, _) = commit_parts(&table, action).await;

    assert!(mapping_json(&updates).is_none());
}
