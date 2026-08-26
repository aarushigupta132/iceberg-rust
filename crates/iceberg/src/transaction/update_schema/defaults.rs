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

use chrono::{DateTime, FixedOffset, NaiveDateTime, NaiveTime, TimeZone, Timelike};
use fastnum::decimal::{Context, RoundingMode};

use crate::spec::temporal::{timestamp, timestamptz};
use crate::spec::{Literal, PrimitiveLiteral, PrimitiveType, Type, decimal_utils};
use crate::{Error, ErrorKind, Result};

fn precondition(message: impl Into<String>) -> Error {
    Error::new(ErrorKind::PreconditionFailed, message)
}

pub(super) fn defaults_equal(left: &Option<Literal>, right: &Option<Literal>) -> bool {
    match (left, right) {
        (Some(Literal::Primitive(left)), Some(Literal::Primitive(right))) => left.same_value(right),
        _ => left == right,
    }
}

pub(super) fn convert_default(
    field_type: &Type,
    default: Option<&Literal>,
    name: &str,
) -> Result<Option<Literal>> {
    let Some(default) = default else {
        return Ok(None);
    };
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
        ) => i32::try_from(*value).map(Literal::int).map_err(|err| {
            Error::new(ErrorKind::DataInvalid, "Integer default is out of range").with_source(err)
        }),
        (
            Type::Primitive(PrimitiveType::Date),
            Literal::Primitive(PrimitiveLiteral::Long(value)),
        ) => i32::try_from(*value).map(Literal::date).map_err(|err| {
            Error::new(ErrorKind::DataInvalid, "Date default is out of range").with_source(err)
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
    .map_err(|err| {
        precondition(format!(
            "Invalid default for column {name}: default is incompatible with {field_type}"
        ))
        .with_source(err)
    })?;
    if let (
        Type::Primitive(PrimitiveType::Decimal { precision, .. }),
        Literal::Primitive(PrimitiveLiteral::Int128(value)),
    ) = (field_type, &converted)
        && decimal_utils::decimal_precision(*value) > *precision
    {
        return Err(precondition(format!(
            "Invalid default for column {name}: decimal default exceeds precision {precision}"
        )));
    }
    validate_temporal_default(field_type, &converted).map_err(|err| {
        precondition(format!(
            "Invalid default for column {name}: default is incompatible with {field_type}"
        ))
        .with_source(err)
    })?;
    converted
        .clone()
        .try_into_json(field_type)
        .and_then(|value| {
            if value.is_null() {
                Err(Error::new(
                    ErrorKind::DataInvalid,
                    "Non-null default serialized as null",
                ))
            } else {
                Ok(Some(converted))
            }
        })
        .map_err(|err| {
            precondition(format!(
                "Invalid default for column {name}: default is incompatible with {field_type}"
            ))
            .with_source(err)
        })
}

fn validate_temporal_default(field_type: &Type, default: &Literal) -> Result<()> {
    let is_valid = match (field_type, default) {
        (
            Type::Primitive(PrimitiveType::Time),
            Literal::Primitive(PrimitiveLiteral::Long(micros)),
        ) => (0..86_400_000_000).contains(micros),
        _ => true,
    };

    if is_valid {
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
    let mantissa = decimal_utils::decimal_mantissa(&decimal);
    if decimal_utils::decimal_precision(mantissa) > precision {
        return Err(Error::new(
            ErrorKind::DataInvalid,
            format!("Decimal default exceeds precision {precision}"),
        ));
    }
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
    let mantissa = decimal_utils::decimal_mantissa(&decimal);
    if decimal_utils::decimal_precision(mantissa) > precision {
        return Err(Error::new(
            ErrorKind::DataInvalid,
            format!("Decimal default exceeds precision {precision}"),
        ));
    }
    Ok(Literal::decimal(mantissa))
}

fn string_default(value: &str, field_type: &Type) -> Result<Literal> {
    match field_type {
        Type::Primitive(PrimitiveType::Time) => {
            let time = parse_local_time(value)?;
            Ok(Literal::time(
                i64::from(time.num_seconds_from_midnight()) * 1_000_000
                    + i64::from(time.nanosecond() / 1_000),
            ))
        }
        Type::Primitive(PrimitiveType::Timestamp) => {
            timestamp::iso_datetime_to_microseconds(value).map(Literal::timestamp)
        }
        Type::Primitive(PrimitiveType::Timestamptz) => {
            timestamptz::iso_offset_datetime_to_microseconds(value).map(Literal::timestamptz)
        }
        Type::Primitive(PrimitiveType::TimestampNs) => {
            let timestamp = parse_local_datetime(value)?.and_utc();
            timestamp
                .timestamp_nanos_opt()
                .map(Literal::timestamp_nano)
                .ok_or_else(|| {
                    Error::new(ErrorKind::DataInvalid, "Timestamp default is out of range")
                })
        }
        Type::Primitive(PrimitiveType::TimestamptzNs) => {
            let timestamp = parse_offset_datetime(value)?;
            timestamp
                .timestamp_nanos_opt()
                .map(Literal::timestamptz_nano)
                .ok_or_else(|| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        "Timestamptz default is out of range",
                    )
                })
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

fn parse_local_time(value: &str) -> Result<NaiveTime> {
    let case_normalized = normalize_iso_case(value);
    validate_fractional_precision(&case_normalized)?;
    let normalized = with_default_seconds(&case_normalized);
    let time = NaiveTime::parse_from_str(&normalized, "%H:%M:%S%.f").map_err(|err| {
        Error::new(ErrorKind::DataInvalid, "Invalid time default").with_source(err)
    })?;
    reject_leap_second(time.nanosecond(), "time")?;
    Ok(time)
}

fn parse_local_datetime(value: &str) -> Result<NaiveDateTime> {
    let case_normalized = normalize_iso_case(value);
    validate_fractional_precision(&case_normalized)?;
    let normalized = with_default_seconds(&case_normalized);
    let timestamp =
        NaiveDateTime::parse_from_str(&normalized, "%Y-%m-%dT%H:%M:%S%.f").map_err(|err| {
            Error::new(ErrorKind::DataInvalid, "Invalid timestamp default").with_source(err)
        })?;
    reject_leap_second(timestamp.nanosecond(), "timestamp")?;
    Ok(timestamp)
}

fn parse_offset_datetime(value: &str) -> Result<DateTime<FixedOffset>> {
    let value = normalize_iso_case(value);
    let separator = value.find('T').ok_or_else(|| {
        Error::new(
            ErrorKind::DataInvalid,
            "Invalid timestamptz default: expected a 'T' separator",
        )
    })?;
    let time_and_offset = &value[separator + 1..];
    let offset_start = time_and_offset
        .char_indices()
        .find_map(|(index, character)| matches!(character, 'Z' | '+' | '-').then_some(index))
        .ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                "Invalid timestamptz default: missing UTC offset",
            )
        })?;
    let local_end = separator + 1 + offset_start;
    let local = parse_local_datetime(&value[..local_end])?;
    let offset = parse_utc_offset(&value[local_end..])?;
    offset.from_local_datetime(&local).single().ok_or_else(|| {
        Error::new(
            ErrorKind::DataInvalid,
            "Timestamptz default is out of range",
        )
    })
}

fn parse_utc_offset(value: &str) -> Result<FixedOffset> {
    if value == "Z" {
        return FixedOffset::east_opt(0)
            .ok_or_else(|| Error::new(ErrorKind::Unexpected, "Cannot construct UTC offset"));
    }

    let (sign, digits) = match value.as_bytes().first() {
        Some(b'+') => (1, &value[1..]),
        Some(b'-') => (-1, &value[1..]),
        _ => {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Invalid timestamptz UTC offset",
            ));
        }
    };
    let components = digits.split(':').collect::<Vec<_>>();
    if !matches!(components.as_slice(), [_, _] | [_, _, _])
        || components.iter().any(|component| {
            component.len() != 2 || !component.bytes().all(|byte| byte.is_ascii_digit())
        })
    {
        return Err(Error::new(
            ErrorKind::DataInvalid,
            "Invalid timestamptz UTC offset",
        ));
    }
    let hours = components[0].parse::<i32>().map_err(|err| {
        Error::new(ErrorKind::DataInvalid, "Invalid timestamptz UTC offset").with_source(err)
    })?;
    let minutes = components[1].parse::<i32>().map_err(|err| {
        Error::new(ErrorKind::DataInvalid, "Invalid timestamptz UTC offset").with_source(err)
    })?;
    let seconds = components
        .get(2)
        .map_or(Ok(0), |value| value.parse::<i32>())
        .map_err(|err| {
            Error::new(ErrorKind::DataInvalid, "Invalid timestamptz UTC offset").with_source(err)
        })?;
    if hours > 18 || minutes > 59 || seconds > 59 || (hours == 18 && (minutes != 0 || seconds != 0))
    {
        return Err(Error::new(
            ErrorKind::DataInvalid,
            "Timestamptz UTC offset is out of range",
        ));
    }
    FixedOffset::east_opt(sign * (hours * 3_600 + minutes * 60 + seconds)).ok_or_else(|| {
        Error::new(
            ErrorKind::DataInvalid,
            "Timestamptz UTC offset is out of range",
        )
    })
}

fn reject_leap_second(nanosecond: u32, kind: &str) -> Result<()> {
    if nanosecond < 1_000_000_000 {
        Ok(())
    } else {
        Err(Error::new(
            ErrorKind::DataInvalid,
            format!("Invalid {kind} default: leap seconds are not supported"),
        ))
    }
}

fn validate_fractional_precision(value: &str) -> Result<()> {
    let time_start = value.find('T').map_or(0, |index| index + 1);
    let time = &value[time_start..];
    if let Some(decimal_point) = time.find('.')
        && time[decimal_point + 1..]
            .bytes()
            .take_while(u8::is_ascii_digit)
            .count()
            > 9
    {
        return Err(Error::new(
            ErrorKind::DataInvalid,
            "Temporal defaults support at most 9 fractional-second digits",
        ));
    }
    Ok(())
}

fn normalize_iso_case(value: &str) -> std::borrow::Cow<'_, str> {
    if value.bytes().any(|byte| matches!(byte, b't' | b'z')) {
        std::borrow::Cow::Owned(
            value
                .chars()
                .map(|character| match character {
                    't' => 'T',
                    'z' => 'Z',
                    _ => character,
                })
                .collect(),
        )
    } else {
        std::borrow::Cow::Borrowed(value)
    }
}

fn with_default_seconds(value: &str) -> std::borrow::Cow<'_, str> {
    let time_start = value.find('T').map_or(0, |index| index + 1);
    let time = &value[time_start..];
    let zone_start = time
        .char_indices()
        .find_map(|(index, character)| matches!(character, 'Z' | '+' | '-').then_some(index))
        .unwrap_or(time.len());
    if time[..zone_start]
        .bytes()
        .filter(|byte| *byte == b':')
        .count()
        == 1
    {
        let insert_at = time_start + zone_start;
        let mut normalized = String::with_capacity(value.len() + 3);
        normalized.push_str(&value[..insert_at]);
        normalized.push_str(":00");
        normalized.push_str(&value[insert_at..]);
        std::borrow::Cow::Owned(normalized)
    } else {
        std::borrow::Cow::Borrowed(value)
    }
}
