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

use crate::spec::{MappedField, NameMapping, NestedFieldRef, Type};

#[derive(Clone, Copy)]
enum MappingParent {
    Root,
    Field(i32),
}

pub(super) fn update_name_mapping_json(
    raw_mapping: &str,
    updates: &HashMap<i32, NestedFieldRef>,
    additions: &HashMap<Option<i32>, Vec<i32>>,
) -> Result<Option<String>, String> {
    let value = serde_json::from_str(raw_mapping).map_err(|error| error.to_string())?;
    validate_mapping_json_shape(&value)?;
    let mapping =
        serde_json::from_value::<NameMapping>(value).map_err(|error| error.to_string())?;
    let updated = update_name_mapping(&mapping, updates, additions)?;
    if updated == mapping {
        Ok(None)
    } else {
        serde_json::to_string(&updated)
            .map(Some)
            .map_err(|error| error.to_string())
    }
}

fn validate_mapping_json_shape(value: &serde_json::Value) -> Result<(), String> {
    let fields = value
        .as_array()
        .ok_or_else(|| format!("Cannot parse non-array mapping fields: {value}"))?;
    for field in fields {
        let object = field
            .as_object()
            .ok_or_else(|| format!("Cannot parse non-object mapping field: {field}"))?;
        if let Some(children) = object.get("fields") {
            validate_mapping_json_shape(children)?;
        }
    }
    Ok(())
}

fn update_name_mapping(
    mapping: &NameMapping,
    updates: &HashMap<i32, NestedFieldRef>,
    additions: &HashMap<Option<i32>, Vec<i32>>,
) -> Result<NameMapping, String> {
    validate_name_mapping(mapping)?;
    let fields = update_mapped_fields(mapping.fields(), MappingParent::Root, updates, additions)?;
    let updated = NameMapping::new(fields);
    validate_name_mapping(&updated)?;
    Ok(updated)
}

fn update_mapped_fields(
    fields: &[MappedField],
    parent: MappingParent,
    updates: &HashMap<i32, NestedFieldRef>,
    additions: &HashMap<Option<i32>, Vec<i32>>,
) -> Result<Vec<MappedField>, String> {
    let mut updated = fields
        .iter()
        .map(|field| update_mapped_field(field, updates, additions))
        .collect::<Result<Vec<_>, _>>()?;

    let added_ids = match parent {
        MappingParent::Root => additions.get(&None),
        MappingParent::Field(parent_id) => additions.get(&Some(parent_id)),
    };
    if let Some(added_ids) = added_ids {
        for id in added_ids {
            let field = updates
                .get(id)
                .ok_or_else(|| format!("Added field {id} is missing its pending update"))?;
            updated.push(mapped_field_from_nested(field));
        }
    }

    let mut assignments = HashMap::new();
    for field in &updated {
        let Some(id) = field.field_id() else {
            continue;
        };
        let Some(update) = updates.get(&id) else {
            continue;
        };
        if let Some(previous_id) = assignments.insert(update.name.clone(), id)
            && previous_id != id
        {
            return Err(format!(
                "Multiple mapped fields assign alias {}",
                update.name
            ));
        }
    }

    let mut result = Vec::with_capacity(updated.len());
    for field in updated {
        let reassigned_aliases = field
            .names()
            .iter()
            .filter(|name| {
                assignments
                    .get(*name)
                    .is_some_and(|assigned_id| Some(*assigned_id) != field.field_id())
            })
            .count();
        // Java leaves the mapping unchanged when multiple aliases are reassigned from one field.
        if reassigned_aliases > 1 {
            return Err(format!(
                "Cannot reassign multiple aliases from mapped field {:?}",
                field.field_id()
            ));
        }

        let names = field
            .names()
            .iter()
            .filter(|name| {
                assignments
                    .get(*name)
                    .is_none_or(|assigned_id| Some(*assigned_id) == field.field_id())
            })
            .cloned()
            .collect();
        result.push(MappedField::new(
            field.field_id(),
            names,
            field
                .fields()
                .iter()
                .map(|child| (**child).clone())
                .collect(),
        ));
    }
    Ok(result)
}

fn update_mapped_field(
    field: &MappedField,
    updates: &HashMap<i32, NestedFieldRef>,
    additions: &HashMap<Option<i32>, Vec<i32>>,
) -> Result<MappedField, String> {
    let mut names = deduplicate_names(field.names());
    if let Some(id) = field.field_id()
        && let Some(update) = updates.get(&id)
        && !names.contains(&update.name)
    {
        names.push(update.name.clone());
    }

    let children = field
        .fields()
        .iter()
        .map(|child| (**child).clone())
        .collect::<Vec<_>>();
    let parent_id = field
        .field_id()
        .ok_or_else(|| "Mapped field ID is missing".to_string())?;
    let children = update_mapped_fields(
        &children,
        MappingParent::Field(parent_id),
        updates,
        additions,
    )?;
    Ok(MappedField::new(field.field_id(), names, children))
}

fn mapped_field_from_nested(field: &NestedFieldRef) -> MappedField {
    MappedField::new(
        Some(field.id),
        vec![field.name.clone()],
        mapped_fields_from_type(&field.field_type),
    )
}

fn mapped_fields_from_type(field_type: &Type) -> Vec<MappedField> {
    match field_type {
        Type::Struct(struct_type) => struct_type
            .fields()
            .iter()
            .map(mapped_field_from_nested)
            .collect(),
        Type::List(list_type) => vec![mapped_field_from_nested(&list_type.element_field)],
        Type::Map(map_type) => vec![
            mapped_field_from_nested(&map_type.key_field),
            mapped_field_from_nested(&map_type.value_field),
        ],
        Type::Primitive(_) | Type::Variant(_) => Vec::new(),
    }
}

fn validate_name_mapping(mapping: &NameMapping) -> Result<(), String> {
    let mut field_ids = HashSet::new();
    let mut full_names = HashSet::new();
    validate_mapped_fields(mapping.fields(), true, &[], &mut field_ids, &mut full_names)
}

fn validate_mapped_fields(
    fields: &[MappedField],
    at_root: bool,
    parent_names: &[String],
    field_ids: &mut HashSet<i32>,
    full_names: &mut HashSet<String>,
) -> Result<(), String> {
    let mut sibling_names = HashSet::new();
    for field in fields {
        let names = deduplicate_names(field.names());
        for name in &names {
            if !sibling_names.insert(name.clone()) {
                return Err(format!(
                    "Mapped alias {name} is assigned to multiple siblings"
                ));
            }
        }

        let field_id = field
            .field_id()
            .ok_or_else(|| "Mapped field ID is missing".to_string())?;
        if !field_ids.insert(field_id) {
            return Err(format!("Mapped field ID {field_id} is not unique"));
        }

        let current_names = if at_root {
            names
        } else if parent_names.is_empty() {
            Vec::new()
        } else {
            parent_names
                .iter()
                .flat_map(|parent| names.iter().map(move |name| format!("{parent}.{name}")))
                .collect()
        };
        for name in &current_names {
            if !full_names.insert(name.clone()) {
                return Err(format!("Mapped full name {name} is not unique"));
            }
        }
        let children = field
            .fields()
            .iter()
            .map(|child| (**child).clone())
            .collect::<Vec<_>>();
        validate_mapped_fields(&children, false, &current_names, field_ids, full_names)?;
    }
    Ok(())
}

fn deduplicate_names(names: &[String]) -> Vec<String> {
    let mut seen = HashSet::new();
    names
        .iter()
        .filter(|name| seen.insert((*name).clone()))
        .cloned()
        .collect()
}
