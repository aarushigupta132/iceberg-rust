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

use fastnum::decimal::{Context, RoundingMode};

use crate::spec::temporal::{time, timestamp, timestamptz};
use crate::spec::{
    ListType, Literal, MapType, NestedField, NestedFieldRef, PrimitiveLiteral, PrimitiveType,
    StructType, Type, decimal_utils,
};
use crate::{Error, ErrorKind, Result};

/// Coerce defaults on an added field and every real field nested in its type.
///
/// The field has already received its final IDs. This matters for complex types because default
/// serialization is defined against those IDs. List element and map key/value pseudo-fields are
/// rebuilt without defaults, matching Java's `AssignFreshIds` behavior.
pub(super) fn coerce_field_defaults(
    field: &NestedField,
    full_name: &str,
) -> Result<NestedFieldRef> {
    let initial_default =
        coerce_default(&field.field_type, field.initial_default.as_ref(), full_name)?;
    let write_default = coerce_default(&field.field_type, field.write_default.as_ref(), full_name)?;
    let field_type = coerce_nested_defaults(&field.field_type, full_name)?;

    let mut coerced = field.clone();
    coerced.field_type = Box::new(field_type);
    coerced.initial_default = initial_default;
    coerced.write_default = write_default;
    Ok(Arc::new(coerced))
}

fn coerce_nested_defaults(field_type: &Type, full_name: &str) -> Result<Type> {
    match field_type {
        Type::Primitive(_) | Type::Variant(_) => Ok(field_type.clone()),
        Type::Struct(struct_type) => {
            let fields = struct_type
                .fields()
                .iter()
                .map(|field| coerce_field_defaults(field, &format!("{full_name}.{}", field.name)))
                .collect::<Result<Vec<_>>>()?;
            Ok(Type::Struct(StructType::new(fields)))
        }
        Type::List(list_type) => {
            let element = &list_type.element_field;
            let element_type = coerce_nested_defaults(
                &element.field_type,
                &format!("{full_name}.{}", element.name),
            )?;
            Ok(Type::List(ListType {
                element_field: rebuild_collection_field(element, element_type),
            }))
        }
        Type::Map(map_type) => {
            let key = &map_type.key_field;
            let value = &map_type.value_field;
            let key_type =
                coerce_nested_defaults(&key.field_type, &format!("{full_name}.{}", key.name))?;
            let value_type =
                coerce_nested_defaults(&value.field_type, &format!("{full_name}.{}", value.name))?;
            Ok(Type::Map(MapType {
                key_field: rebuild_collection_field(key, key_type),
                value_field: rebuild_collection_field(value, value_type),
            }))
        }
    }
}

fn rebuild_collection_field(field: &NestedField, field_type: Type) -> NestedFieldRef {
    let mut rebuilt = field.clone();
    rebuilt.field_type = Box::new(field_type);
    rebuilt.initial_default = None;
    rebuilt.write_default = None;
    Arc::new(rebuilt)
}

fn coerce_default(
    field_type: &Type,
    default: Option<&Literal>,
    full_name: &str,
) -> Result<Option<Literal>> {
    let Some(default) = default else {
        return Ok(None);
    };

    // `Literal` intentionally erases logical source types: `Long`, for example, represents long,
    // time, and both timestamp precisions. Compatible variants are therefore already in the
    // target type's physical units. In particular, a Long targeting timestamp_ns is nanoseconds;
    // scaling it as Java does for a source-typed Long would corrupt an actual timestamp_ns value.
    let converted = match (field_type, default) {
        (
            Type::Primitive(PrimitiveType::Decimal { precision, .. }),
            Literal::Primitive(PrimitiveLiteral::Int128(value)),
        ) if decimal_utils::decimal_precision(*value) > *precision => Err(Error::new(
            ErrorKind::DataInvalid,
            format!("Decimal default exceeds precision {precision}"),
        )),
        (Type::Primitive(field_type), Literal::Primitive(value))
            if field_type.compatible(value) =>
        {
            Ok(default.clone())
        }
        (
            Type::Primitive(PrimitiveType::Long),
            Literal::Primitive(PrimitiveLiteral::Int(value)),
        ) => Ok(Literal::long(*value)),
        (
            Type::Primitive(PrimitiveType::Float),
            Literal::Primitive(PrimitiveLiteral::Int(value)),
        ) => Ok(Literal::float(*value as f32)),
        (
            Type::Primitive(PrimitiveType::Double),
            Literal::Primitive(PrimitiveLiteral::Int(value)),
        ) => Ok(Literal::double(*value as f64)),
        (
            Type::Primitive(PrimitiveType::Int),
            Literal::Primitive(PrimitiveLiteral::Long(value)),
        ) => i32::try_from(*value).map(Literal::int).map_err(|error| {
            Error::new(ErrorKind::DataInvalid, "Integer default is out of range").with_source(error)
        }),
        (
            Type::Primitive(PrimitiveType::Date),
            Literal::Primitive(PrimitiveLiteral::Long(value)),
        ) => i32::try_from(*value).map(Literal::date).map_err(|error| {
            Error::new(ErrorKind::DataInvalid, "Date default is out of range").with_source(error)
        }),
        (
            Type::Primitive(PrimitiveType::Float),
            Literal::Primitive(PrimitiveLiteral::Long(value)),
        ) => Ok(Literal::float(*value as f32)),
        (
            Type::Primitive(PrimitiveType::Double),
            Literal::Primitive(PrimitiveLiteral::Long(value)),
        ) => Ok(Literal::double(*value as f64)),
        (
            Type::Primitive(PrimitiveType::Double),
            Literal::Primitive(PrimitiveLiteral::Float(value)),
        ) => Ok(Literal::double(value.0 as f64)),
        (
            Type::Primitive(PrimitiveType::Float),
            Literal::Primitive(PrimitiveLiteral::Double(value)),
        ) if value.0.is_finite() && (-f32::MAX as f64..=f32::MAX as f64).contains(&value.0) => {
            Ok(Literal::float(value.0 as f32))
        }
        (
            Type::Primitive(PrimitiveType::Decimal { precision, scale }),
            Literal::Primitive(PrimitiveLiteral::Int(value)),
        ) => decimal_default(&value.to_string(), *precision, *scale),
        (
            Type::Primitive(PrimitiveType::Decimal { precision, scale }),
            Literal::Primitive(PrimitiveLiteral::Long(value)),
        ) => decimal_default(&value.to_string(), *precision, *scale),
        (
            Type::Primitive(PrimitiveType::Decimal { precision, scale }),
            Literal::Primitive(PrimitiveLiteral::Float(value)),
        ) => decimal_default(&(value.0 as f64).to_string(), *precision, *scale),
        (
            Type::Primitive(PrimitiveType::Decimal { precision, scale }),
            Literal::Primitive(PrimitiveLiteral::Double(value)),
        ) => decimal_default(&value.0.to_string(), *precision, *scale),
        (Type::Primitive(_), Literal::Primitive(PrimitiveLiteral::String(value))) => {
            string_default(value, field_type)
        }
        _ => Err(Error::new(
            ErrorKind::DataInvalid,
            "Default cannot be converted to the column type",
        )),
    }
    .map_err(|error| invalid_default(full_name, field_type).with_source(error))?;

    if let (
        Type::Primitive(PrimitiveType::Decimal { precision, .. }),
        Literal::Primitive(PrimitiveLiteral::Int128(value)),
    ) = (field_type, &converted)
        && decimal_utils::decimal_precision(*value) > *precision
    {
        return Err(precondition(format!(
            "Invalid default for column {full_name}: decimal default exceeds precision {precision}"
        )));
    }

    validate_temporal_default(field_type, &converted)
        .map_err(|error| invalid_default(full_name, field_type).with_source(error))?;

    let json = converted
        .clone()
        .try_into_json(field_type)
        .map_err(|error| invalid_default(full_name, field_type).with_source(error))?;
    if json.is_null() {
        return Err(
            invalid_default(full_name, field_type).with_source(Error::new(
                ErrorKind::DataInvalid,
                "Non-null default serialized as null",
            )),
        );
    }

    Ok(Some(converted))
}

fn invalid_default(full_name: &str, field_type: &Type) -> Error {
    precondition(format!(
        "Invalid default for column {full_name}: default is incompatible with {field_type}"
    ))
}

fn validate_temporal_default(field_type: &Type, default: &Literal) -> Result<()> {
    let valid = !matches!(
        (field_type, default),
        (
            Type::Primitive(PrimitiveType::Time),
            Literal::Primitive(PrimitiveLiteral::Long(micros)),
        ) if !(0..86_400_000_000).contains(micros)
    );
    if valid {
        Ok(())
    } else {
        Err(Error::new(
            ErrorKind::DataInvalid,
            "Temporal default is out of range",
        ))
    }
}

fn decimal_default(value: &str, precision: u32, scale: u32) -> Result<Literal> {
    let context = Context::default()
        .without_traps()
        .with_rounding_mode(RoundingMode::HalfUp);
    let decimal = decimal_utils::decimal_from_str_exact(value)?
        .with_ctx(context)
        .rescale(scale as i16);
    if !decimal.is_finite() {
        return Err(Error::new(
            ErrorKind::DataInvalid,
            "Decimal default is not finite",
        ));
    }
    if decimal.fractional_digits_count() != scale as i16 {
        return Err(Error::new(
            ErrorKind::DataInvalid,
            format!("Decimal default cannot be represented at scale {scale}"),
        ));
    }
    if decimal.digits_count() > precision as usize {
        return Err(Error::new(
            ErrorKind::DataInvalid,
            format!("Decimal default exceeds precision {precision}"),
        ));
    }
    let mantissa = decimal_utils::decimal_mantissa(&decimal);
    Ok(Literal::decimal(mantissa))
}

fn decimal_string_default(value: &str, precision: u32, scale: u32) -> Result<Literal> {
    let decimal = decimal_utils::decimal_from_str_exact(value)?;
    if !decimal.is_finite() || decimal.fractional_digits_count() != scale as i16 {
        return Err(Error::new(
            ErrorKind::DataInvalid,
            format!("Decimal string default must have scale {scale}"),
        ));
    }
    if decimal.digits_count() > precision as usize {
        return Err(Error::new(
            ErrorKind::DataInvalid,
            format!("Decimal default exceeds precision {precision}"),
        ));
    }
    let mantissa = decimal_utils::decimal_mantissa(&decimal);
    Ok(Literal::decimal(mantissa))
}

fn string_default(value: &str, field_type: &Type) -> Result<Literal> {
    match field_type {
        Type::Primitive(PrimitiveType::Time) => {
            time::iso_time_to_microseconds(value).map(Literal::time)
        }
        Type::Primitive(PrimitiveType::Timestamp) => {
            timestamp::iso_datetime_to_microseconds(value).map(Literal::timestamp)
        }
        Type::Primitive(PrimitiveType::Timestamptz) => {
            timestamptz::iso_offset_datetime_to_microseconds(value).map(Literal::timestamptz)
        }
        Type::Primitive(PrimitiveType::TimestampNs) => {
            timestamp::iso_datetime_to_nanoseconds(value).map(Literal::timestamp_nano)
        }
        Type::Primitive(PrimitiveType::TimestamptzNs) => {
            timestamptz::iso_offset_datetime_to_nanoseconds(value).map(Literal::timestamptz_nano)
        }
        Type::Primitive(PrimitiveType::Decimal { precision, scale }) => {
            decimal_string_default(value, *precision, *scale)
        }
        Type::Primitive(PrimitiveType::Uuid) => uuid_string_default(value),
        Type::Primitive(_) => {
            Literal::try_from_json(serde_json::Value::String(value.to_string()), field_type)
                .and_then(|literal| {
                    literal.ok_or_else(|| {
                        Error::new(ErrorKind::DataInvalid, "Default conversion returned null")
                    })
                })
        }
        _ => Err(Error::new(
            ErrorKind::DataInvalid,
            "Nested defaults must be null",
        )),
    }
}

fn uuid_string_default(value: &str) -> Result<Literal> {
    let groups = value.split('-').collect::<Vec<_>>();
    let widths = [8, 4, 4, 4, 12];
    if groups.len() != widths.len()
        || groups.iter().zip(widths).any(|(group, width)| {
            group.is_empty()
                || group.len() > width
                || !group.bytes().all(|byte| byte.is_ascii_hexdigit())
        })
    {
        return Err(Error::new(ErrorKind::DataInvalid, "Invalid UUID default"));
    }

    let canonical = groups
        .iter()
        .zip(widths)
        .map(|(group, width)| format!("{group:0>width$}"))
        .collect::<Vec<_>>()
        .join("-");
    Literal::uuid_from_str(canonical)
}

fn precondition(message: impl Into<String>) -> Error {
    Error::new(ErrorKind::PreconditionFailed, message)
}
