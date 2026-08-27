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

use super::AddColumn;
use super::apply::PendingSchemaUpdate;
use super::defaults::defaults_equal;
use super::type_promotion::is_promotion_allowed;
use crate::spec::{NestedFieldRef, Schema, StructType, Type};
use crate::{Error, ErrorKind, Result};

pub(super) fn apply_union_by_name(
    pending: &mut PendingSchemaUpdate<'_>,
    new_schema: &Schema,
) -> Result<()> {
    if !pending.is_case_sensitive() {
        for schema in [pending.base_schema(), new_schema] {
            if let Some(name) = schema.case_insensitive_name_collision() {
                return Err(precondition(format!(
                    "Cannot use case-insensitive schema updates because multiple fields match: {name}"
                )));
            }
        }
    }
    let existing = pending.base_schema().as_struct().clone();
    union_struct(pending, None, &existing, new_schema.as_struct())
}

fn union_struct(
    pending: &mut PendingSchemaUpdate<'_>,
    parent_id: Option<i32>,
    existing: &StructType,
    new: &StructType,
) -> Result<()> {
    let case_sensitive = pending.is_case_sensitive();
    let matching_fields = matching_fields(existing, new, case_sensitive);
    for (new_field, existing_field) in new.fields().iter().zip(&matching_fields) {
        if let Some(existing_field) = existing_field {
            recurse_field(pending, existing_field, new_field)?;
        }
    }

    for (new_field, existing_field) in new.fields().iter().zip(matching_fields) {
        if let Some(existing_field) = existing_field {
            apply_field_update(pending, existing_field, new_field)?;
        } else {
            add_field(pending, parent_id, new_field)?;
        }
    }
    Ok(())
}

fn add_field(
    pending: &mut PendingSchemaUpdate<'_>,
    parent_id: Option<i32>,
    new_field: &NestedFieldRef,
) -> Result<()> {
    let parent = parent_id
        .and_then(|id| pending.base_schema().name_by_field_id(id))
        .map(str::to_string);
    pending.add_union_column(&AddColumn {
        parent,
        name: new_field.name.clone(),
        required: false,
        field_type: new_field.field_type.as_ref().clone(),
        doc: new_field.doc.clone(),
        initial_default: new_field.initial_default.clone(),
        write_default: new_field.write_default.clone(),
    })
}

fn recurse_field(
    pending: &mut PendingSchemaUpdate<'_>,
    existing_field: &NestedFieldRef,
    new_field: &NestedFieldRef,
) -> Result<()> {
    match (
        existing_field.field_type.as_ref(),
        new_field.field_type.as_ref(),
    ) {
        (Type::Struct(existing), Type::Struct(new)) => {
            union_struct(pending, Some(existing_field.id), existing, new)?
        }
        (Type::List(existing), Type::List(new)) => {
            recurse_field(pending, &existing.element_field, &new.element_field)?;
            apply_field_update(pending, &existing.element_field, &new.element_field)?;
        }
        (Type::Map(existing), Type::Map(new)) => {
            recurse_field(pending, &existing.key_field, &new.key_field)?;
            recurse_field(pending, &existing.value_field, &new.value_field)?;
            apply_field_update(pending, &existing.key_field, &new.key_field)?;
            apply_field_update(pending, &existing.value_field, &new.value_field)?;
        }
        _ => {}
    }
    Ok(())
}

fn apply_field_update(
    pending: &mut PendingSchemaUpdate<'_>,
    existing_field: &NestedFieldRef,
    new_field: &NestedFieldRef,
) -> Result<()> {
    let name = pending
        .base_schema()
        .name_by_field_id(existing_field.id)
        .ok_or_else(|| {
            Error::new(
                ErrorKind::Unexpected,
                format!(
                    "Field {} is missing from the schema name index",
                    existing_field.id
                ),
            )
        })?
        .to_string();

    if !new_field.required && existing_field.required {
        pending.set_required(&name, false)?;
    }

    match (
        existing_field.field_type.as_ref(),
        new_field.field_type.as_ref(),
    ) {
        (Type::Primitive(existing), Type::Primitive(new)) => {
            if existing != new && !is_promotion_allowed(&Type::Primitive(new.clone()), existing) {
                pending.update_column_type(&name, new)?;
            }
        }
        (Type::Struct(_), Type::Struct(_))
        | (Type::List(_), Type::List(_))
        | (Type::Map(_), Type::Map(_)) => {}
        (Type::Variant(_), Type::Variant(_)) => {}
        (Type::Struct(_) | Type::List(_) | Type::Map(_), Type::Variant(_)) => {}
        (existing, new) => {
            return Err(precondition(format!(
                "Cannot merge column {name}: incompatible types {existing} and {new}"
            )));
        }
    }

    if new_field.doc.is_some() && new_field.doc != existing_field.doc {
        pending.update_column_doc(&name, &new_field.doc)?;
    }
    if new_field.write_default.is_some()
        && !defaults_equal(&new_field.write_default, &existing_field.write_default)
    {
        pending.update_column_default(&name, new_field.write_default.as_ref())?;
    }
    Ok(())
}

fn matching_fields<'a>(
    existing: &'a StructType,
    new: &StructType,
    case_sensitive: bool,
) -> Vec<Option<&'a NestedFieldRef>> {
    if case_sensitive {
        new.fields()
            .iter()
            .map(|field| existing.field_by_name(&field.name))
            .collect()
    } else {
        let existing_by_name = existing
            .fields()
            .iter()
            .map(|field| (field.name.to_lowercase(), field))
            .collect::<HashMap<_, _>>();
        new.fields()
            .iter()
            .map(|field| existing_by_name.get(&field.name.to_lowercase()).copied())
            .collect()
    }
}

fn precondition(message: impl Into<String>) -> Error {
    Error::new(ErrorKind::PreconditionFailed, message)
}
