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
use super::column_properties::deleted_column_property_keys;
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

        if schema.is_same_schema(base_schema) {
            return Ok(ActionCommit::new(Vec::new(), Vec::new()));
        }

        let mut updates = vec![
            TableUpdate::AddSchema { schema },
            TableUpdate::SetCurrentSchema { schema_id: -1 },
        ];
        let property_removals = deleted_column_property_keys(
            table.metadata().properties(),
            base_schema,
            pending.deletes(),
        );
        if !property_removals.is_empty() {
            updates.push(TableUpdate::RemoveProperties {
                removals: property_removals,
            });
        }

        Ok(ActionCommit::new(updates, vec![
            TableRequirement::UuidMatch {
                uuid: table.metadata().uuid(),
            },
            TableRequirement::LastAssignedFieldIdMatch {
                last_assigned_field_id: last_column_id,
            },
            TableRequirement::CurrentSchemaIdMatch {
                current_schema_id: base_schema.schema_id(),
            },
        ]))
    }
}
