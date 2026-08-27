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

use super::defaults::{coerce_default, coerce_field_defaults, defaults_equal};
use super::fresh_ids::assign_fresh_ids;
use super::moves::{MovePosition, PendingMove};
use super::tree::{index_parent_ids, rebuild_fields};
use super::type_promotion::{is_promotion_allowed, promote_default, validated_primitive_type};
use super::{AddColumn, SchemaOperation};
use crate::spec::{
    Literal, NestedField, NestedFieldRef, PrimitiveType, SCHEMA_NAME_DELIMITER, Schema, Type,
};
use crate::{Error, ErrorKind, Result};

// A new field receives its real ID when operations are replayed at commit time.
pub(super) const DEFAULT_FIELD_ID: i32 = 0;

impl AddColumn {
    fn to_nested_field(&self) -> NestedFieldRef {
        let mut field = NestedField::new(
            DEFAULT_FIELD_ID,
            self.name.clone(),
            self.field_type.clone(),
            self.required,
        );
        field.doc = self.doc.clone();
        field.initial_default = self.initial_default.clone();
        field.write_default = self.write_default.clone();
        Arc::new(field)
    }
}

struct PendingIdentifierName {
    name: String,
    case_sensitive: bool,
}

#[derive(Default)]
struct IdentifierFieldSelection {
    resolved_ids: HashSet<i32>,
    unresolved_names: Vec<PendingIdentifierName>,
}

pub(super) struct PendingSchemaUpdate<'a> {
    schema: &'a Schema,
    updates: HashMap<i32, NestedFieldRef>,
    deletes: HashSet<i32>,
    additions: HashMap<Option<i32>, Vec<i32>>,
    moves: HashMap<Option<i32>, Vec<PendingMove>>,
    added_name_to_id: HashMap<String, i32>,
    added_fields_by_name: Vec<(String, i32)>,
    id_to_parent: HashMap<i32, i32>,
    identifier_field_ids: HashSet<i32>,
    identifier_field_replacement: Option<IdentifierFieldSelection>,
    last_column_id: i32,
    case_sensitive: bool,
    allow_incompatible_changes: bool,
}

impl<'a> PendingSchemaUpdate<'a> {
    pub(super) fn new(schema: &'a Schema, last_column_id: i32) -> Self {
        let mut id_to_parent = HashMap::new();
        index_parent_ids(schema.as_struct().fields(), None, &mut id_to_parent);

        Self {
            schema,
            updates: HashMap::new(),
            deletes: HashSet::new(),
            additions: HashMap::new(),
            moves: HashMap::new(),
            added_name_to_id: HashMap::new(),
            added_fields_by_name: Vec::new(),
            id_to_parent,
            identifier_field_ids: schema.identifier_field_ids().collect(),
            identifier_field_replacement: None,
            last_column_id,
            case_sensitive: true,
            allow_incompatible_changes: false,
        }
    }

    pub(super) fn apply_operations(&mut self, operations: &[SchemaOperation]) -> Result<()> {
        for operation in operations {
            match operation {
                SchemaOperation::Add(add) => self.add_column(add)?,
                SchemaOperation::Delete(name) => self.delete_column(name)?,
                SchemaOperation::Rename { name, new_name } => self.rename_column(name, new_name)?,
                SchemaOperation::SetRequired { name, required } => {
                    self.set_required(name, *required)?
                }
                SchemaOperation::UpdateType { name, new_type } => {
                    self.update_column_type(name, new_type)?
                }
                SchemaOperation::UpdateDoc { name, doc } => self.update_column_doc(name, doc)?,
                SchemaOperation::UpdateDefault { name, default } => {
                    self.update_column_default(name, default.as_ref())?
                }
                SchemaOperation::Move { name, position } => self.move_column(name, position)?,
                SchemaOperation::SetIdentifierFields(names) => self.set_identifier_fields(names)?,
                SchemaOperation::SetCaseSensitive(case_sensitive) => {
                    self.case_sensitive = *case_sensitive;
                }
                SchemaOperation::AllowIncompatibleChanges => {
                    self.allow_incompatible_changes = true;
                }
            }
        }
        Ok(())
    }

    pub(super) fn apply(&self) -> Result<Schema> {
        self.validate_identifier_deletions()?;

        let fields = rebuild_fields(
            self.schema.as_struct().fields(),
            &self.updates,
            &self.additions,
            &self.deletes,
            &self.moves,
            None,
        )?;
        let schema_without_identifiers = Schema::builder()
            .with_fields(fields)
            .build()
            .map_err(|error| precondition("Cannot apply schema update").with_source(error))?;
        let identifier_field_ids = match &self.identifier_field_replacement {
            Some(selection) => {
                self.resolve_identifier_fields(&schema_without_identifiers, selection)?
            }
            None => self.identifier_field_ids.clone(),
        };
        let schema = schema_without_identifiers
            .into_builder()
            .with_identifier_field_ids(identifier_field_ids)
            .build()
            .map_err(|error| precondition("Cannot apply schema update").with_source(error))?;

        if !self.case_sensitive {
            validate_case_insensitive_names(&schema)?;
        }

        Ok(schema)
    }

    fn set_identifier_fields(&mut self, names: &HashSet<String>) -> Result<()> {
        if !self.case_sensitive {
            validate_case_insensitive_names(self.schema)?;
        }

        let mut selection = IdentifierFieldSelection::default();
        for name in names {
            if let Some(field_id) = self.field_id_for_identifier(name)? {
                selection.resolved_ids.insert(field_id);
            } else {
                selection.unresolved_names.push(PendingIdentifierName {
                    name: name.clone(),
                    case_sensitive: self.case_sensitive,
                });
            }
        }
        self.identifier_field_replacement = Some(selection);
        Ok(())
    }

    fn resolve_identifier_fields(
        &self,
        schema: &Schema,
        selection: &IdentifierFieldSelection,
    ) -> Result<HashSet<i32>> {
        let mut field_ids = selection.resolved_ids.clone();
        for pending in &selection.unresolved_names {
            if !pending.case_sensitive {
                validate_case_insensitive_names(schema)?;
            }
            let field = if pending.case_sensitive {
                schema.field_by_name(&pending.name)
            } else {
                schema.field_by_name_case_insensitive(&pending.name)
            }
            .ok_or_else(|| {
                precondition(format!(
                    "Cannot add field {} as an identifier field: not found in current schema or added columns",
                    pending.name
                ))
            })?;
            field_ids.insert(field.id);
        }
        Ok(field_ids)
    }

    fn field_id_for_identifier(&self, name: &str) -> Result<Option<i32>> {
        let base_field = self.resolve_field(name)?;
        let mut live_added_match = None;
        let mut stale_added_match = None;
        let mut multiple_live_matches = false;
        for (_, field_id) in self.added_fields_by_name.iter().filter(|(added_name, _)| {
            if self.case_sensitive {
                added_name == name
            } else {
                added_name.to_lowercase() == name.to_lowercase()
            }
        }) {
            if self.will_be_deleted(*field_id) {
                stale_added_match.get_or_insert(*field_id);
            } else if live_added_match.replace(*field_id).is_some() {
                multiple_live_matches = true;
            }
        }
        let live_base_field = base_field.filter(|field| !self.will_be_deleted(field.id));
        if !self.case_sensitive
            && (multiple_live_matches || live_added_match.is_some() && live_base_field.is_some())
        {
            return Err(precondition(format!(
                "Cannot use case-insensitive schema updates because multiple fields match: {}",
                name.to_lowercase()
            )));
        }
        if let Some(field_id) = live_added_match {
            return Ok(Some(field_id));
        }
        if let Some(field) = live_base_field {
            return Ok(Some(field.id));
        }
        if let Some(field_id) = stale_added_match {
            return Ok(Some(field_id));
        }
        Ok(base_field.map(|field| field.id))
    }

    fn will_be_deleted(&self, mut field_id: i32) -> bool {
        loop {
            if self.deletes.contains(&field_id) {
                return true;
            }
            let Some(parent_id) = self.id_to_parent.get(&field_id).copied() else {
                return false;
            };
            field_id = parent_id;
        }
    }

    fn add_column(&mut self, add: &AddColumn) -> Result<()> {
        if add.parent.is_none() && add.name.is_empty() {
            return Err(precondition("Invalid column name: (empty)"));
        }
        if add.parent.is_none() && add.name.contains(SCHEMA_NAME_DELIMITER) {
            return Err(precondition(format!(
                "Cannot add column with ambiguous name: {}, set a parent to add a nested column",
                add.name
            )));
        }

        let (parent_id, conflict_name, full_name) = if let Some(parent) = &add.parent {
            let parent_id = self.resolve_parent(parent)?;
            if self.deletes.contains(&parent_id) {
                return Err(precondition(format!(
                    "Cannot add to a column that will be deleted: {parent}"
                )));
            }
            let canonical_parent = self
                .schema
                .name_by_field_id(parent_id)
                .unwrap_or(parent.as_str());
            (
                Some(parent_id),
                format!("{parent}.{}", add.name),
                format!("{canonical_parent}.{}", add.name),
            )
        } else {
            (None, add.name.clone(), add.name.clone())
        };

        if let Some(existing) = self.resolve_field(&conflict_name)?
            && !self.deletes.contains(&existing.id)
        {
            return Err(precondition(format!(
                "Cannot add column, name already exists: {conflict_name}"
            )));
        }

        if add.required && add.initial_default.is_none() && !self.allow_incompatible_changes {
            return Err(precondition(format!(
                "Incompatible change: cannot add required column without an initial default: {full_name}"
            )));
        }

        let field = assign_fresh_ids(&add.to_nested_field(), &mut self.last_column_id)?;
        let field = coerce_field_defaults(&field, &full_name)?;
        index_field_names(&field, full_name.clone(), &mut self.added_fields_by_name);
        index_parent_ids(
            std::slice::from_ref(&field),
            parent_id,
            &mut self.id_to_parent,
        );
        self.added_name_to_id
            .insert(self.case_sensitivity_aware_name(&full_name), field.id);
        self.additions.entry(parent_id).or_default().push(field.id);
        self.updates.insert(field.id, field);
        Ok(())
    }

    fn delete_column(&mut self, name: &str) -> Result<()> {
        let field = self
            .resolve_field(name)?
            .ok_or_else(|| precondition(format!("Cannot delete missing column: {name}")))?;

        if self.additions.contains_key(&Some(field.id)) {
            return Err(precondition(format!(
                "Cannot delete a column that has additions: {name}"
            )));
        }
        if self.updates.contains_key(&field.id) {
            return Err(precondition(format!(
                "Cannot delete a column that has updates: {name}"
            )));
        }

        self.deletes.insert(field.id);
        Ok(())
    }

    fn rename_column(&mut self, name: &str, new_name: &str) -> Result<()> {
        if name.is_empty() {
            return Err(precondition("Invalid column name: (empty)"));
        }
        let field = self
            .resolve_field(name)?
            .ok_or_else(|| precondition(format!("Cannot rename missing column: {name}")))?;
        if self.deletes.contains(&field.id) {
            return Err(precondition(format!(
                "Cannot rename a column that will be deleted: {}",
                field.name
            )));
        }

        let current = self
            .updates
            .get(&field.id)
            .cloned()
            .unwrap_or_else(|| field.clone());
        let mut updated = (*current).clone();
        updated.name = new_name.to_string();
        self.updates.insert(updated.id, Arc::new(updated));
        Ok(())
    }

    fn move_column(&mut self, name: &str, position: &MovePosition) -> Result<()> {
        let field_id = self
            .field_id_for_move(name)?
            .ok_or_else(|| precondition(format!("Cannot move missing column: {name}")))?;
        let parent_id = self.id_to_parent.get(&field_id).copied();
        let (pending_move, reference_id) = match position {
            MovePosition::First => (PendingMove::first(field_id), None),
            MovePosition::Before(reference) => {
                let reference_id = self.field_id_for_move(reference)?.ok_or_else(|| {
                    precondition(format!(
                        "Cannot move {name} before missing column: {reference}"
                    ))
                })?;
                if field_id == reference_id {
                    return Err(precondition(format!("Cannot move {name} before itself")));
                }
                (
                    PendingMove::before(field_id, reference_id),
                    Some(reference_id),
                )
            }
            MovePosition::After(reference) => {
                let reference_id = self.field_id_for_move(reference)?.ok_or_else(|| {
                    precondition(format!(
                        "Cannot move {name} after missing column: {reference}"
                    ))
                })?;
                if field_id == reference_id {
                    return Err(precondition(format!("Cannot move {name} after itself")));
                }
                (
                    PendingMove::after(field_id, reference_id),
                    Some(reference_id),
                )
            }
        };

        if let Some(parent_id) = parent_id {
            let parent = self.field_by_id(parent_id).ok_or_else(|| {
                Error::new(
                    ErrorKind::Unexpected,
                    format!("Parent field {parent_id} is missing for column {name}"),
                )
            })?;
            if !parent.field_type.is_struct() {
                return Err(precondition(format!(
                    "Cannot move fields in non-struct type: {}",
                    parent.field_type
                )));
            }
        }
        if let Some(reference_id) = reference_id {
            self.validate_move_parent(name, parent_id, reference_id)?;
        }

        self.moves.entry(parent_id).or_default().push(pending_move);
        Ok(())
    }

    fn validate_move_parent(
        &self,
        name: &str,
        parent_id: Option<i32>,
        reference_id: i32,
    ) -> Result<()> {
        if self.id_to_parent.get(&reference_id).copied() != parent_id {
            return Err(precondition(format!(
                "Cannot move field {name} to a different struct"
            )));
        }
        Ok(())
    }

    fn set_required(&mut self, name: &str, required: bool) -> Result<()> {
        let field = self
            .find_for_update(name)?
            .ok_or_else(|| precondition(format!("Cannot update missing column: {name}")))?;
        if field.required == required {
            return Ok(());
        }

        let is_defaulted_add = self
            .added_name_to_id
            .values()
            .any(|field_id| *field_id == field.id)
            && field.initial_default.is_some();
        if required && !is_defaulted_add && !self.allow_incompatible_changes {
            return Err(precondition(format!(
                "Cannot change column nullability: {name}: optional -> required"
            )));
        }
        if self.deletes.contains(&field.id) {
            return Err(precondition(format!(
                "Cannot update a column that will be deleted: {}",
                field.name
            )));
        }

        let mut updated = (*field).clone();
        updated.required = required;
        self.updates.insert(updated.id, Arc::new(updated));
        Ok(())
    }

    fn update_column_type(&mut self, name: &str, new_type: &PrimitiveType) -> Result<()> {
        let field = self
            .find_for_update(name)?
            .ok_or_else(|| precondition(format!("Cannot update missing column: {name}")))?;
        if self.deletes.contains(&field.id) {
            return Err(precondition(format!(
                "Cannot update a column that will be deleted: {}",
                field.name
            )));
        }

        let old_type = field.field_type.as_ref();
        let promoted_type = validated_primitive_type(new_type).map_err(|error| {
            precondition(format!(
                "Cannot change column type: {name}: {old_type} -> {new_type}"
            ))
            .with_source(error)
        })?;
        if old_type == &promoted_type {
            return Ok(());
        }
        if !is_promotion_allowed(old_type, new_type) {
            return Err(precondition(format!(
                "Cannot change column type: {name}: {old_type} -> {new_type}"
            )));
        }

        let old_primitive = old_type
            .as_primitive_type()
            .expect("promotion source must be primitive");
        let mut updated = (*field).clone();
        updated.field_type = Box::new(promoted_type.clone());
        updated.initial_default = promote_default(
            updated.initial_default.as_ref(),
            old_primitive,
            &promoted_type,
        )?;
        updated.write_default = promote_default(
            updated.write_default.as_ref(),
            old_primitive,
            &promoted_type,
        )?;
        self.updates.insert(updated.id, Arc::new(updated));
        Ok(())
    }

    fn update_column_doc(&mut self, name: &str, doc: &Option<String>) -> Result<()> {
        let field = self
            .find_for_update(name)?
            .ok_or_else(|| precondition(format!("Cannot update missing column: {name}")))?;
        if self.deletes.contains(&field.id) {
            return Err(precondition(format!(
                "Cannot update a column that will be deleted: {}",
                field.name
            )));
        }
        if field.doc.as_ref() == doc.as_ref() {
            return Ok(());
        }

        let mut updated = (*field).clone();
        updated.doc = doc.clone();
        self.updates.insert(updated.id, Arc::new(updated));
        Ok(())
    }

    fn update_column_default(&mut self, name: &str, default: Option<&Literal>) -> Result<()> {
        let field = self
            .find_for_update(name)?
            .ok_or_else(|| precondition(format!("Cannot update missing column: {name}")))?;
        if self.deletes.contains(&field.id) {
            return Err(precondition(format!(
                "Cannot update a column that will be deleted: {}",
                field.name
            )));
        }

        let default = coerce_default(&field.field_type, default, name)?;
        if default.is_some() && defaults_equal(&field.write_default, &default) {
            return Ok(());
        }

        let mut updated = (*field).clone();
        updated.write_default = default;
        self.updates.insert(updated.id, Arc::new(updated));
        Ok(())
    }

    fn resolve_parent(&self, parent: &str) -> Result<i32> {
        let parent_field = self
            .resolve_field(parent)?
            .ok_or_else(|| precondition(format!("Cannot find parent struct: {parent}")))?;

        let target = match parent_field.field_type.as_ref() {
            Type::Map(map_type) => &map_type.value_field,
            Type::List(list_type) => &list_type.element_field,
            Type::Struct(_) => parent_field,
            _ => {
                return Err(precondition(format!(
                    "Cannot add to non-struct column: {parent}: {}",
                    parent_field.field_type
                )));
            }
        };

        if !target.field_type.is_struct() {
            return Err(precondition(format!(
                "Cannot add to non-struct column: {parent}: {}",
                target.field_type
            )));
        }
        Ok(target.id)
    }

    fn resolve_field(&self, name: &str) -> Result<Option<&NestedFieldRef>> {
        if name.is_empty() {
            return Err(precondition("Invalid column name: (empty)"));
        }

        if self.case_sensitive {
            Ok(self.schema.field_by_name(name))
        } else {
            validate_case_insensitive_names(self.schema)?;
            Ok(self.schema.field_by_name_case_insensitive(name))
        }
    }

    fn find_for_update(&self, name: &str) -> Result<Option<NestedFieldRef>> {
        if let Some(existing) = self.resolve_field(name)? {
            return Ok(Some(
                self.updates
                    .get(&existing.id)
                    .cloned()
                    .unwrap_or_else(|| existing.clone()),
            ));
        }

        Ok(self
            .added_name_to_id
            .get(&self.case_sensitivity_aware_name(name))
            .and_then(|id| self.updates.get(id))
            .cloned())
    }

    fn field_id_for_move(&self, name: &str) -> Result<Option<i32>> {
        if let Some(field_id) = self
            .added_name_to_id
            .get(&self.case_sensitivity_aware_name(name))
        {
            return Ok(Some(*field_id));
        }
        Ok(self.resolve_field(name)?.map(|field| field.id))
    }

    fn field_by_id(&self, field_id: i32) -> Option<&NestedFieldRef> {
        self.updates
            .get(&field_id)
            .or_else(|| self.schema.field_by_id(field_id))
    }

    fn case_sensitivity_aware_name(&self, name: &str) -> String {
        if self.case_sensitive {
            name.to_string()
        } else {
            name.to_lowercase()
        }
    }

    fn validate_identifier_deletions(&self) -> Result<()> {
        let identifier_field_ids = self
            .identifier_field_replacement
            .as_ref()
            .map(|selection| &selection.resolved_ids)
            .unwrap_or(&self.identifier_field_ids);
        for identifier_id in identifier_field_ids {
            let Some(identifier) = self.schema.field_by_id(*identifier_id) else {
                if self
                    .added_fields_by_name
                    .iter()
                    .any(|(_, added_id)| added_id == identifier_id)
                {
                    continue;
                }
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    format!("Identifier field {identifier_id} is missing from the schema"),
                ));
            };

            if self.deletes.contains(identifier_id) {
                return Err(precondition(format!(
                    "Cannot delete identifier field {}",
                    identifier.name
                )));
            }

            let mut parent_id = self.id_to_parent.get(identifier_id).copied();
            while let Some(id) = parent_id {
                if self.deletes.contains(&id) {
                    let parent = self.schema.field_by_id(id).ok_or_else(|| {
                        Error::new(
                            ErrorKind::Unexpected,
                            format!("Identifier field parent {id} is missing from the schema"),
                        )
                    })?;
                    return Err(precondition(format!(
                        "Cannot delete field {} as it will delete nested identifier field {}",
                        parent.name, identifier.name
                    )));
                }
                parent_id = self.id_to_parent.get(&id).copied();
            }
        }
        Ok(())
    }

    pub(super) fn deletes(&self) -> &HashSet<i32> {
        &self.deletes
    }

    pub(super) fn updates(&self) -> &HashMap<i32, NestedFieldRef> {
        &self.updates
    }

    pub(super) fn additions(&self) -> &HashMap<Option<i32>, Vec<i32>> {
        &self.additions
    }
}

fn index_field_names(field: &NestedFieldRef, full_name: String, result: &mut Vec<(String, i32)>) {
    result.push((full_name.clone(), field.id));
    match field.field_type.as_ref() {
        Type::Struct(struct_type) => {
            for child in struct_type.fields() {
                index_field_names(child, format!("{full_name}.{}", child.name), result);
            }
        }
        Type::List(list_type) => {
            let element = &list_type.element_field;
            index_field_names(element, format!("{full_name}.{}", element.name), result);
        }
        Type::Map(map_type) => {
            for child in [&map_type.key_field, &map_type.value_field] {
                index_field_names(child, format!("{full_name}.{}", child.name), result);
            }
        }
        Type::Primitive(_) | Type::Variant(_) => {}
    }
}

fn precondition(message: impl Into<String>) -> Error {
    Error::new(ErrorKind::PreconditionFailed, message)
}

fn validate_case_insensitive_names(schema: &Schema) -> Result<()> {
    if let Some(name) = schema.case_insensitive_name_collision() {
        return Err(precondition(format!(
            "Cannot use case-insensitive schema updates because multiple fields match: {name}"
        )));
    }
    Ok(())
}
