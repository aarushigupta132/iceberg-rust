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

use crate::spec::Schema;

const COLUMN_PROPERTY_PREFIXES: [&str; 3] = [
    "write.metadata.metrics.column.",
    "write.parquet.bloom-filter-enabled.column.",
    "write.parquet.stats-enabled.column.",
];

#[derive(Debug, Default, PartialEq, Eq)]
pub(super) struct ColumnPropertyChanges {
    pub(super) removals: Vec<String>,
    pub(super) updates: HashMap<String, String>,
}

impl ColumnPropertyChanges {
    pub(super) fn is_empty(&self) -> bool {
        self.removals.is_empty() && self.updates.is_empty()
    }
}

pub(super) fn column_property_changes(
    properties: &HashMap<String, String>,
    base_schema: &Schema,
    updated_schema: &Schema,
    updates: &HashMap<i32, crate::spec::NestedFieldRef>,
    deletes: &HashSet<i32>,
    additions: &HashMap<Option<i32>, Vec<i32>>,
) -> ColumnPropertyChanges {
    let deleted_columns: HashSet<&str> = deletes
        .iter()
        .filter_map(|id| base_schema.name_by_field_id(*id))
        .collect();
    let added_ids: HashSet<i32> = additions.values().flatten().copied().collect();
    let renamed_columns: HashMap<&str, &str> = updates
        .keys()
        .filter(|id| !added_ids.contains(id))
        .filter_map(|id| {
            let old_name = base_schema.name_by_field_id(*id)?;
            let new_name = updated_schema.name_by_field_id(*id)?;
            (old_name != new_name).then_some((old_name, new_name))
        })
        .collect();

    let mut changes = ColumnPropertyChanges::default();
    for (key, value) in properties {
        let Some(prefix) = COLUMN_PROPERTY_PREFIXES
            .iter()
            .find(|prefix| key.starts_with(**prefix))
        else {
            continue;
        };
        let column_name = &key[prefix.len()..];
        if let Some(new_name) = renamed_columns.get(column_name) {
            changes.removals.push(key.clone());
            changes
                .updates
                .insert(format!("{prefix}{new_name}"), value.clone());
        } else if deleted_columns.contains(column_name) {
            changes.removals.push(key.clone());
        }
    }
    changes.removals.sort();
    changes
}
