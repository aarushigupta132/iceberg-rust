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

pub(super) fn deleted_column_property_keys(
    properties: &HashMap<String, String>,
    schema: &Schema,
    deleted_field_ids: &HashSet<i32>,
) -> Vec<String> {
    let deleted_columns: HashSet<&str> = deleted_field_ids
        .iter()
        .filter_map(|id| schema.name_by_field_id(*id))
        .collect();

    let mut removals = properties
        .keys()
        .filter(|key| {
            COLUMN_PROPERTY_PREFIXES.iter().any(|prefix| {
                key.strip_prefix(prefix)
                    .is_some_and(|column| deleted_columns.contains(column))
            })
        })
        .cloned()
        .collect::<Vec<_>>();
    removals.sort();
    removals
}
