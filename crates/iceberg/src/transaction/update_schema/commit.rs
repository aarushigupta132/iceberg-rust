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

use async_trait::async_trait;

use super::UpdateSchemaAction;
use super::apply::PendingSchemaUpdate;
use super::column_properties::column_property_changes;
use super::name_mapping::update_name_mapping_json;
use crate::spec::DEFAULT_SCHEMA_NAME_MAPPING;
use crate::table::Table;
use crate::transaction::action::{ActionCommit, TransactionAction};
use crate::{Result, TableRequirement, TableUpdate};

#[async_trait]
impl TransactionAction for UpdateSchemaAction {
    async fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
        let base_schema = table.metadata().current_schema();
        let last_column_id = table.metadata().last_column_id();
        let mut pending = PendingSchemaUpdate::new(base_schema, last_column_id);
        pending.apply_operations(&self.operations)?;
        let schema = pending.apply()?;
        let schema_changed = !schema.is_same_schema(base_schema);

        let mut property_changes = column_property_changes(
            table.metadata().properties(),
            base_schema,
            &schema,
            pending.updates(),
            pending.deletes(),
            pending.additions(),
        );
        if let Some(raw_mapping) = table
            .metadata()
            .properties()
            .get(DEFAULT_SCHEMA_NAME_MAPPING)
        {
            match update_name_mapping_json(raw_mapping, pending.updates(), pending.additions()) {
                Ok(Some(updated_mapping)) => {
                    property_changes
                        .updates
                        .insert(DEFAULT_SCHEMA_NAME_MAPPING.to_string(), updated_mapping);
                }
                Ok(None) => {}
                Err(error) => {
                    tracing::warn!(
                        error = %error,
                        "Failed to update external schema name mapping"
                    );
                }
            }
        }

        if !schema_changed && property_changes.is_empty() {
            return Ok(ActionCommit::new(Vec::new(), Vec::new()));
        }

        let mut updates = Vec::new();
        if schema_changed {
            updates.extend([
                TableUpdate::AddSchema { schema },
                TableUpdate::SetCurrentSchema { schema_id: -1 },
            ]);
        }
        if !property_changes.removals.is_empty() {
            updates.push(TableUpdate::RemoveProperties {
                removals: property_changes.removals,
            });
        }
        if !property_changes.updates.is_empty() {
            updates.push(TableUpdate::SetProperties {
                updates: property_changes.updates,
            });
        }

        let mut requirements = vec![TableRequirement::UuidMatch {
            uuid: table.metadata().uuid(),
        }];
        if schema_changed {
            requirements.push(TableRequirement::LastAssignedFieldIdMatch {
                last_assigned_field_id: last_column_id,
            });
        }
        requirements.push(TableRequirement::CurrentSchemaIdMatch {
            current_schema_id: base_schema.schema_id(),
        });

        Ok(ActionCommit::new(updates, requirements))
    }
}
