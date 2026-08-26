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

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::spec::{ListType, MapType, NestedFieldRef, StructType, Type};
use crate::{Error, ErrorKind, Result};

pub(super) fn index_parent_ids(
    fields: &[NestedFieldRef],
    parent_id: Option<i32>,
    result: &mut HashMap<i32, i32>,
) {
    for field in fields {
        if let Some(parent_id) = parent_id {
            result.insert(field.id, parent_id);
        }
        index_type_parent_ids(&field.field_type, field.id, result);
    }
}

fn index_type_parent_ids(field_type: &Type, parent_id: i32, result: &mut HashMap<i32, i32>) {
    match field_type {
        Type::Struct(struct_type) => {
            index_parent_ids(struct_type.fields(), Some(parent_id), result)
        }
        Type::List(list_type) => {
            result.insert(list_type.element_field.id, parent_id);
            index_type_parent_ids(
                &list_type.element_field.field_type,
                list_type.element_field.id,
                result,
            );
        }
        Type::Map(map_type) => {
            result.insert(map_type.key_field.id, parent_id);
            result.insert(map_type.value_field.id, parent_id);
            index_type_parent_ids(
                &map_type.key_field.field_type,
                map_type.key_field.id,
                result,
            );
            index_type_parent_ids(
                &map_type.value_field.field_type,
                map_type.value_field.id,
                result,
            );
        }
        Type::Primitive(_) | Type::Variant(_) => {}
    }
}

pub(super) fn rebuild_fields(
    fields: &[NestedFieldRef],
    updates: &HashMap<i32, NestedFieldRef>,
    additions: &HashMap<Option<i32>, Vec<i32>>,
    deletes: &HashSet<i32>,
    parent_id: Option<i32>,
) -> Result<Vec<NestedFieldRef>> {
    let mut rebuilt =
        Vec::with_capacity(fields.len() + additions.get(&parent_id).map_or(0, Vec::len));
    for field in fields {
        let rebuilt_field = rebuild_field(field, updates, additions, deletes)?;
        if !deletes.contains(&field.id) {
            rebuilt.push(rebuilt_field);
        }
    }
    if let Some(added_ids) = additions.get(&parent_id) {
        for id in added_ids {
            rebuilt.push(updates.get(id).cloned().ok_or_else(|| {
                Error::new(ErrorKind::Unexpected, "Added column is missing its field")
            })?);
        }
    }
    Ok(rebuilt)
}

fn rebuild_field(
    field: &NestedFieldRef,
    updates: &HashMap<i32, NestedFieldRef>,
    additions: &HashMap<Option<i32>, Vec<i32>>,
    deletes: &HashSet<i32>,
) -> Result<NestedFieldRef> {
    let pending = updates.get(&field.id).unwrap_or(field);
    match field.field_type.as_ref() {
        Type::Primitive(_) | Type::Variant(_) => Ok(pending.clone()),
        Type::Struct(struct_type) => {
            let fields = rebuild_fields(
                struct_type.fields(),
                updates,
                additions,
                deletes,
                Some(field.id),
            )?;
            Ok(Arc::new(crate::spec::NestedField {
                id: pending.id,
                name: pending.name.clone(),
                required: pending.required,
                field_type: Box::new(Type::Struct(StructType::new(fields))),
                doc: pending.doc.clone(),
                initial_default: pending.initial_default.clone(),
                write_default: pending.write_default.clone(),
            }))
        }
        Type::List(list_type) => {
            if deletes.contains(&list_type.element_field.id) {
                return Err(precondition(format!(
                    "Cannot delete element type from list: {}",
                    field.name
                )));
            }
            let element = rebuild_field(&list_type.element_field, updates, additions, deletes)?;
            let element = crate::spec::NestedField::list_element(
                element.id,
                element.field_type.as_ref().clone(),
                element.required,
            )
            .into();
            Ok(Arc::new(crate::spec::NestedField {
                id: pending.id,
                name: pending.name.clone(),
                required: pending.required,
                field_type: Box::new(Type::List(ListType {
                    element_field: element,
                })),
                doc: pending.doc.clone(),
                initial_default: pending.initial_default.clone(),
                write_default: pending.write_default.clone(),
            }))
        }
        Type::Map(map_type) => {
            let key_id = map_type.key_field.id;
            if deletes.contains(&key_id) {
                return Err(precondition(format!(
                    "Cannot delete map keys: {}",
                    field.name
                )));
            }
            if updates.contains_key(&key_id) {
                return Err(precondition(format!(
                    "Cannot update map keys: {}",
                    field.name
                )));
            }
            if additions.contains_key(&Some(key_id)) {
                return Err(precondition(format!(
                    "Cannot add fields to map keys: {}",
                    field.name
                )));
            }
            let key = rebuild_field(&map_type.key_field, updates, additions, deletes)?;
            if key.as_ref() != map_type.key_field.as_ref() {
                return Err(precondition(format!(
                    "Cannot alter map keys: {}",
                    field.name
                )));
            }
            if deletes.contains(&map_type.value_field.id) {
                return Err(precondition(format!(
                    "Cannot delete value type from map: {}",
                    field.name
                )));
            }
            let value = rebuild_field(&map_type.value_field, updates, additions, deletes)?;
            let value = crate::spec::NestedField::map_value_element(
                value.id,
                value.field_type.as_ref().clone(),
                value.required,
            )
            .into();
            Ok(Arc::new(crate::spec::NestedField {
                id: pending.id,
                name: pending.name.clone(),
                required: pending.required,
                field_type: Box::new(Type::Map(MapType {
                    key_field: map_type.key_field.clone(),
                    value_field: value,
                })),
                doc: pending.doc.clone(),
                initial_default: pending.initial_default.clone(),
                write_default: pending.write_default.clone(),
            }))
        }
    }
}

fn precondition(message: impl Into<String>) -> Error {
    Error::new(ErrorKind::PreconditionFailed, message)
}
