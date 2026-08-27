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

use crate::spec::NestedFieldRef;
use crate::{Error, ErrorKind, Result};

pub(super) enum MovePosition {
    First,
    Before(String),
    After(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MoveKind {
    First,
    Before,
    After,
}

pub(super) struct PendingMove {
    field_id: i32,
    reference_field_id: Option<i32>,
    kind: MoveKind,
}

impl PendingMove {
    pub(super) fn first(field_id: i32) -> Self {
        Self {
            field_id,
            reference_field_id: None,
            kind: MoveKind::First,
        }
    }

    pub(super) fn before(field_id: i32, reference_field_id: i32) -> Self {
        Self {
            field_id,
            reference_field_id: Some(reference_field_id),
            kind: MoveKind::Before,
        }
    }

    pub(super) fn after(field_id: i32, reference_field_id: i32) -> Self {
        Self {
            field_id,
            reference_field_id: Some(reference_field_id),
            kind: MoveKind::After,
        }
    }
}

pub(super) fn apply_moves(fields: &mut Vec<NestedFieldRef>, moves: &[PendingMove]) -> Result<()> {
    for pending_move in moves {
        let from = fields
            .iter()
            .position(|field| field.id == pending_move.field_id)
            .ok_or_else(|| {
                precondition("Cannot move a column that is not in the updated struct")
            })?;
        let field = fields.remove(from);
        let index = match pending_move.kind {
            MoveKind::First => 0,
            MoveKind::Before | MoveKind::After => {
                let reference_id = pending_move.reference_field_id.ok_or_else(|| {
                    Error::new(
                        ErrorKind::Unexpected,
                        "Relative move is missing its reference field",
                    )
                })?;
                let reference = fields
                    .iter()
                    .position(|field| field.id == reference_id)
                    .ok_or_else(|| {
                        precondition(
                            "Cannot move relative to a column that is not in the updated struct",
                        )
                    })?;
                if pending_move.kind == MoveKind::After {
                    reference + 1
                } else {
                    reference
                }
            }
        };
        fields.insert(index, field);
    }
    Ok(())
}

fn precondition(message: impl Into<String>) -> Error {
    Error::new(ErrorKind::PreconditionFailed, message)
}
