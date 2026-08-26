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

use crate::spec::{Datum, Literal, PrimitiveType, Type};
use crate::{Error, ErrorKind, Result};

pub(super) fn validated_primitive_type(primitive: &PrimitiveType) -> Result<Type> {
    match primitive {
        PrimitiveType::Decimal { precision, scale } => Type::decimal(*precision, *scale),
        _ => Ok(Type::Primitive(primitive.clone())),
    }
}

pub(super) fn is_promotion_allowed(old_type: &Type, new_type: &PrimitiveType) -> bool {
    match (old_type, new_type) {
        (Type::Primitive(PrimitiveType::Int), PrimitiveType::Long)
        | (Type::Primitive(PrimitiveType::Float), PrimitiveType::Double) => true,
        (
            Type::Primitive(PrimitiveType::Decimal {
                precision: old_precision,
                scale: old_scale,
            }),
            PrimitiveType::Decimal {
                precision: new_precision,
                scale: new_scale,
            },
        ) => new_precision > old_precision && new_scale == old_scale,
        _ => false,
    }
}

pub(super) fn promote_default(
    default: Option<&Literal>,
    old_type: &PrimitiveType,
    new_type: &Type,
) -> Result<Option<Literal>> {
    default
        .map(|literal| {
            let primitive = literal.as_primitive_literal().ok_or_else(|| {
                precondition(format!(
                    "Cannot promote non-primitive default for type {old_type}"
                ))
            })?;
            Datum::new(old_type.clone(), primitive)
                .to(new_type)
                .map(Literal::from)
                .map_err(|error| {
                    precondition(format!(
                        "Cannot promote default from {old_type} to {new_type}"
                    ))
                    .with_source(error)
                })
        })
        .transpose()
}

fn precondition(message: impl Into<String>) -> Error {
    Error::new(ErrorKind::PreconditionFailed, message)
}
