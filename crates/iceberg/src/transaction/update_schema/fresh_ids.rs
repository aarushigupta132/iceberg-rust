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

use crate::spec::{ListType, MapType, NestedField, NestedFieldRef, StructType, Type};
use crate::{Error, ErrorKind, Result};

#[cfg(test)]
mod tests;

/// Assign fresh field IDs in the same order as Java's `AssignFreshIds` visitor.
pub(super) fn assign_fresh_ids(field: &NestedField, last_id: &mut i32) -> Result<NestedFieldRef> {
    let id = take_next_id(last_id)?;
    rebuild_field(field, id, last_id)
}

fn assign_fresh_ids_to_type(field_type: &Type, last_id: &mut i32) -> Result<Type> {
    match field_type {
        Type::Primitive(_) => Ok(field_type.clone()),
        Type::Variant(variant) => Ok(Type::Variant(*variant)),
        Type::Struct(struct_type) => {
            // Java reserves IDs for every sibling before descending into child types.
            let ids = struct_type
                .fields()
                .iter()
                .map(|_| take_next_id(last_id))
                .collect::<Result<Vec<_>>>()?;
            let fields = struct_type
                .fields()
                .iter()
                .zip(ids)
                .map(|(field, id)| rebuild_field(field, id, last_id))
                .collect::<Result<Vec<_>>>()?;
            Ok(Type::Struct(StructType::new(fields)))
        }
        Type::List(list_type) => {
            let element_id = take_next_id(last_id)?;
            let element_type =
                assign_fresh_ids_to_type(&list_type.element_field.field_type, last_id)?;
            Ok(Type::List(ListType {
                element_field: NestedField::list_element(
                    element_id,
                    element_type,
                    list_type.element_field.required,
                )
                .into(),
            }))
        }
        Type::Map(map_type) => {
            // Map key and value are siblings, so both IDs are reserved first.
            let key_id = take_next_id(last_id)?;
            let value_id = take_next_id(last_id)?;
            let key_type = assign_fresh_ids_to_type(&map_type.key_field.field_type, last_id)?;
            let value_type = assign_fresh_ids_to_type(&map_type.value_field.field_type, last_id)?;
            Ok(Type::Map(MapType {
                key_field: NestedField::map_key_element(key_id, key_type).into(),
                value_field: NestedField::map_value_element(
                    value_id,
                    value_type,
                    map_type.value_field.required,
                )
                .into(),
            }))
        }
    }
}

fn rebuild_field(field: &NestedField, id: i32, last_id: &mut i32) -> Result<NestedFieldRef> {
    let field_type = assign_fresh_ids_to_type(&field.field_type, last_id)?;
    let mut rebuilt = NestedField::new(id, &field.name, field_type, field.required);
    rebuilt.doc = field.doc.clone();
    rebuilt.initial_default = field.initial_default.clone();
    rebuilt.write_default = field.write_default.clone();
    Ok(Arc::new(rebuilt))
}

fn take_next_id(last_id: &mut i32) -> Result<i32> {
    let id = last_id.checked_add(1).ok_or_else(|| {
        Error::new(
            ErrorKind::DataInvalid,
            "Field ID overflowed, cannot add more fields",
        )
    })?;
    *last_id = id;
    Ok(id)
}
