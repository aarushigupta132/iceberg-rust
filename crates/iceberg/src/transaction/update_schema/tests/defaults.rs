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

use super::tests::make_v2_table_with_nested;
use super::{AddColumn, UpdateSchemaAction};
use crate::spec::{
    ListType, Literal, MapType, NestedField, PrimitiveLiteral, PrimitiveType, Schema, Struct,
    StructType, Type,
};
use crate::table::Table;
use crate::transaction::Transaction;
use crate::transaction::action::{ActionCommit, TransactionAction};
use crate::{Error, ErrorKind, TableUpdate};

fn optional_with_defaults(
    name: &str,
    field_type: Type,
    initial_default: Option<Literal>,
    write_default: Option<Literal>,
) -> AddColumn {
    let mut column = AddColumn::optional(name, field_type);
    column.initial_default = initial_default;
    column.write_default = write_default;
    column
}

async fn updated_schema(table: &Table, action: UpdateSchemaAction) -> Schema {
    schema_from_commit(Arc::new(action).commit(table).await.unwrap())
}

fn schema_from_commit(mut commit: ActionCommit) -> Schema {
    match commit.take_updates().remove(0) {
        TableUpdate::AddSchema { schema } => schema,
        update => panic!("expected AddSchema, got {update:?}"),
    }
}

async fn commit_error(table: &Table, action: UpdateSchemaAction) -> Error {
    match Arc::new(action).commit(table).await {
        Err(error) => error,
        Ok(_) => panic!("schema update should fail"),
    }
}

fn defaults(schema: &Schema, name: &str) -> (Option<Literal>, Option<Literal>) {
    let field = schema
        .field_by_name(name)
        .unwrap_or_else(|| panic!("missing field {name}"));
    (field.initial_default.clone(), field.write_default.clone())
}

#[tokio::test]
async fn coerces_numeric_defaults_to_the_target_physical_type() {
    let table = crate::transaction::tests::make_v2_table();
    let decimal = Type::Primitive(PrimitiveType::Decimal {
        precision: 9,
        scale: 2,
    });
    let action = Transaction::new(&table)
        .update_schema()
        .add_column(optional_with_defaults(
            "int_value",
            Type::Primitive(PrimitiveType::Int),
            Some(Literal::long(i32::MAX as i64)),
            Some(Literal::long(i32::MIN as i64)),
        ))
        .add_column(optional_with_defaults(
            "long_value",
            Type::Primitive(PrimitiveType::Long),
            Some(Literal::int(34)),
            Some(Literal::int(-35)),
        ))
        .add_column(optional_with_defaults(
            "float_value",
            Type::Primitive(PrimitiveType::Float),
            Some(Literal::long(36)),
            Some(Literal::double(-37.5)),
        ))
        .add_column(optional_with_defaults(
            "double_value",
            Type::Primitive(PrimitiveType::Double),
            Some(Literal::float(38.5)),
            Some(Literal::int(-39)),
        ))
        .add_column(optional_with_defaults(
            "decimal_value",
            decimal,
            Some(Literal::double(6.255)),
            Some(Literal::double(-6.255)),
        ))
        .add_column(optional_with_defaults(
            "timestamp_ns_value",
            Type::Primitive(PrimitiveType::TimestampNs),
            Some(Literal::long(-1)),
            Some(Literal::long(1_234)),
        ))
        .add_column(optional_with_defaults(
            "fixed_physical",
            Type::Primitive(PrimitiveType::Fixed(3)),
            Some(Literal::binary([1, 2, 3])),
            Some(Literal::fixed([4, 5, 6])),
        ));

    let schema = updated_schema(&table, action).await;
    assert_eq!(
        defaults(&schema, "int_value"),
        (Some(Literal::int(i32::MAX)), Some(Literal::int(i32::MIN)))
    );
    assert_eq!(
        defaults(&schema, "long_value"),
        (Some(Literal::long(34)), Some(Literal::long(-35)))
    );
    assert_eq!(
        defaults(&schema, "float_value"),
        (Some(Literal::float(36.0)), Some(Literal::float(-37.5)))
    );
    assert_eq!(
        defaults(&schema, "double_value"),
        (Some(Literal::double(38.5)), Some(Literal::double(-39.0)))
    );
    assert_eq!(
        defaults(&schema, "decimal_value"),
        (Some(Literal::decimal(626)), Some(Literal::decimal(-626)))
    );
    assert_eq!(
        defaults(&schema, "timestamp_ns_value"),
        (
            Some(Literal::timestamp_nano(-1)),
            Some(Literal::timestamp_nano(1_234))
        )
    );
    assert_eq!(
        defaults(&schema, "fixed_physical"),
        (
            Some(Literal::fixed([1, 2, 3])),
            Some(Literal::fixed([4, 5, 6]))
        )
    );
}

#[tokio::test]
async fn coerces_java_compatible_string_defaults() {
    let table = crate::transaction::tests::make_v2_table();
    let action = Transaction::new(&table)
        .update_schema()
        .add_column(optional_with_defaults(
            "date_min",
            Type::Primitive(PrimitiveType::Date),
            Some(Literal::string("-5877641-06-23")),
            None,
        ))
        .add_column(optional_with_defaults(
            "time_value",
            Type::Primitive(PrimitiveType::Time),
            Some(Literal::string("12:30")),
            Some(Literal::string("23:59:59.999999999")),
        ))
        .add_column(optional_with_defaults(
            "timestamp_min",
            Type::Primitive(PrimitiveType::Timestamp),
            Some(Literal::string("-290308-12-21T19:59:05.224192")),
            None,
        ))
        .add_column(optional_with_defaults(
            "offset_timestamp",
            Type::Primitive(PrimitiveType::Timestamptz),
            Some(Literal::string("2024-01-02T03:04:05-05:00")),
            None,
        ))
        .add_column(optional_with_defaults(
            "timestamp_ns_value",
            Type::Primitive(PrimitiveType::TimestampNs),
            Some(Literal::string("1969-12-31t23:59:59.999999999")),
            None,
        ))
        .add_column(optional_with_defaults(
            "timestamptz_ns_value",
            Type::Primitive(PrimitiveType::TimestamptzNs),
            Some(Literal::string("1969-12-31t23:59:59.999999999z")),
            None,
        ))
        .add_column(optional_with_defaults(
            "decimal_value",
            Type::Primitive(PrimitiveType::Decimal {
                precision: 9,
                scale: 2,
            }),
            Some(Literal::string("1.23")),
            None,
        ))
        .add_column(optional_with_defaults(
            "decimal_zero",
            Type::Primitive(PrimitiveType::Decimal {
                precision: 9,
                scale: 2,
            }),
            Some(Literal::string("0.00")),
            None,
        ))
        .add_column(optional_with_defaults(
            "uuid_value",
            Type::Primitive(PrimitiveType::Uuid),
            Some(Literal::string("1-1-1-1-1")),
            None,
        ))
        .add_column(optional_with_defaults(
            "fixed_value",
            Type::Primitive(PrimitiveType::Fixed(3)),
            Some(Literal::string("00AaFf")),
            None,
        ))
        .add_column(optional_with_defaults(
            "binary_value",
            Type::Primitive(PrimitiveType::Binary),
            Some(Literal::string("00010FFF")),
            None,
        ));

    let schema = updated_schema(&table, action).await;
    assert_eq!(
        defaults(&schema, "date_min").0,
        Some(Literal::date(i32::MIN))
    );
    assert_eq!(
        defaults(&schema, "time_value"),
        (
            Some(Literal::time(45_000_000_000)),
            Some(Literal::time(86_399_999_999))
        )
    );
    assert_eq!(
        defaults(&schema, "timestamp_min").0,
        Some(Literal::timestamp(i64::MIN))
    );
    assert_eq!(
        defaults(&schema, "offset_timestamp").0,
        Some(Literal::timestamptz(
            chrono::DateTime::parse_from_rfc3339("2024-01-02T03:04:05-05:00")
                .unwrap()
                .timestamp_micros()
        ))
    );
    assert_eq!(
        defaults(&schema, "timestamp_ns_value").0,
        Some(Literal::timestamp_nano(-1))
    );
    assert_eq!(
        defaults(&schema, "timestamptz_ns_value").0,
        Some(Literal::timestamptz_nano(-1))
    );
    assert_eq!(
        defaults(&schema, "decimal_value").0,
        Some(Literal::decimal(123))
    );
    assert_eq!(
        defaults(&schema, "decimal_zero").0,
        Some(Literal::decimal(0))
    );
    assert_eq!(
        defaults(&schema, "uuid_value").0,
        Some(Literal::uuid_from_str("00000001-0001-0001-0001-000000000001").unwrap())
    );
    assert_eq!(
        defaults(&schema, "fixed_value").0,
        Some(Literal::Primitive(PrimitiveLiteral::Binary(vec![
            0, 170, 255
        ])))
    );
    assert_eq!(
        defaults(&schema, "binary_value").0,
        Some(Literal::Primitive(PrimitiveLiteral::Binary(vec![
            0, 1, 15, 255
        ])))
    );

    let round_tripped: Schema =
        serde_json::from_str(&serde_json::to_string(&schema).unwrap()).unwrap();
    assert_eq!(round_tripped, schema);
}

#[tokio::test]
async fn rejects_incompatible_or_unrepresentable_defaults() {
    let table = crate::transaction::tests::make_v2_table();
    let invalid = [
        (
            "int_overflow",
            PrimitiveType::Int,
            Literal::long(i64::from(i32::MAX) + 1),
        ),
        (
            "date_overflow",
            PrimitiveType::Date,
            Literal::long(i64::MAX),
        ),
        (
            "float_overflow",
            PrimitiveType::Float,
            Literal::double(f64::MAX),
        ),
        ("float_nan", PrimitiveType::Float, Literal::float(f32::NAN)),
        (
            "double_infinity",
            PrimitiveType::Double,
            Literal::double(f64::INFINITY),
        ),
        (
            "decimal_precision",
            PrimitiveType::Decimal {
                precision: 2,
                scale: 0,
            },
            Literal::int(123),
        ),
        (
            "decimal_physical_precision",
            PrimitiveType::Decimal {
                precision: 2,
                scale: 2,
            },
            Literal::decimal(123),
        ),
        (
            "decimal_scale_clamp",
            PrimitiveType::Decimal {
                precision: 38,
                scale: 38,
            },
            Literal::int(1),
        ),
        (
            "decimal_rounding_overflow",
            PrimitiveType::Decimal {
                precision: 2,
                scale: 1,
            },
            Literal::double(9.95),
        ),
        (
            "negative_decimal_rounding_overflow",
            PrimitiveType::Decimal {
                precision: 2,
                scale: 1,
            },
            Literal::double(-9.95),
        ),
        (
            "decimal_string_scale",
            PrimitiveType::Decimal {
                precision: 9,
                scale: 2,
            },
            Literal::string("1.2"),
        ),
        (
            "decimal_string_exponent",
            PrimitiveType::Decimal {
                precision: 9,
                scale: 2,
            },
            Literal::string("1E+2"),
        ),
        (
            "fixed_length",
            PrimitiveType::Fixed(2),
            Literal::string("ff"),
        ),
        (
            "fixed_physical_length",
            PrimitiveType::Fixed(2),
            Literal::binary([255]),
        ),
        ("binary_hex", PrimitiveType::Binary, Literal::string("fg")),
        (
            "uuid_format",
            PrimitiveType::Uuid,
            Literal::string("a1a2a3a4b1b2c1c2d1d2d3d4d5d6d7d8"),
        ),
        ("time_range", PrimitiveType::Time, Literal::time(i64::MAX)),
        (
            "time_leap_second",
            PrimitiveType::Time,
            Literal::string("23:59:60"),
        ),
        (
            "timestamp_ns_range",
            PrimitiveType::TimestampNs,
            Literal::string("2262-04-11T23:47:16.854775808"),
        ),
    ];

    for (name, primitive, default) in invalid {
        let action = Transaction::new(&table)
            .update_schema()
            .add_column(optional_with_defaults(
                name,
                Type::Primitive(primitive),
                Some(default),
                None,
            ));
        let error = commit_error(&table, action).await;
        assert_eq!(error.kind(), ErrorKind::PreconditionFailed, "{name}");
        assert!(
            error
                .message()
                .starts_with(&format!("Invalid default for column {name}:")),
            "{name}: {error}"
        );
    }
}

#[tokio::test]
async fn coerces_defaults_on_real_nested_fields_and_discards_pseudo_field_defaults() {
    let table = crate::transaction::tests::make_v2_table();
    let list_element = NestedField::list_element(
        501,
        Type::Struct(StructType::new(vec![
            NestedField::optional(502, "when", Type::Primitive(PrimitiveType::TimestampNs))
                .with_initial_default(Literal::string("1969-12-31T23:59:59.999999999"))
                .into(),
        ])),
        true,
    )
    .with_initial_default(Literal::string("discarded element default"))
    .with_write_default(Literal::string("discarded element default"))
    .into();
    let map_key = NestedField::map_key_element(503, Type::Primitive(PrimitiveType::String))
        .with_initial_default(Literal::int(1))
        .with_write_default(Literal::int(2))
        .into();
    let map_value = NestedField::map_value_element(
        504,
        Type::Struct(StructType::new(vec![
            NestedField::optional(
                505,
                "amount",
                Type::Primitive(PrimitiveType::Decimal {
                    precision: 9,
                    scale: 2,
                }),
            )
            .with_initial_default(Literal::double(1.235))
            .with_write_default(Literal::double(-1.235))
            .into(),
        ])),
        false,
    )
    .with_initial_default(Literal::string("discarded value default"))
    .with_write_default(Literal::string("discarded value default"))
    .into();
    let nested_type = Type::Struct(StructType::new(vec![
        NestedField::optional(500, "count", Type::Primitive(PrimitiveType::Long))
            .with_initial_default(Literal::int(7))
            .with_write_default(Literal::int(8))
            .into(),
        NestedField::optional(501, "entries", Type::List(ListType::new(list_element))).into(),
        NestedField::optional(503, "mapping", Type::Map(MapType::new(map_key, map_value))).into(),
    ]));

    let action = Transaction::new(&table)
        .update_schema()
        .add_column(AddColumn::optional("payload", nested_type));
    let schema = updated_schema(&table, action).await;

    let count = schema.field_by_name("payload.count").unwrap();
    assert_eq!(count.id, 5);
    assert_eq!(
        (count.initial_default.clone(), count.write_default.clone()),
        (Some(Literal::long(7)), Some(Literal::long(8)))
    );
    let when = schema
        .field_by_name("payload.entries.element.when")
        .unwrap();
    assert_eq!(when.id, 9);
    assert_eq!(when.initial_default, Some(Literal::timestamp_nano(-1)));
    let amount = schema
        .field_by_name("payload.mapping.value.amount")
        .unwrap();
    assert_eq!(amount.id, 12);
    assert_eq!(
        (amount.initial_default.clone(), amount.write_default.clone()),
        (Some(Literal::decimal(124)), Some(Literal::decimal(-124)))
    );

    for name in [
        "payload.entries.element",
        "payload.mapping.key",
        "payload.mapping.value",
    ] {
        assert_eq!(defaults(&schema, name), (None, None), "{name}");
    }

    let round_tripped: Schema =
        serde_json::from_str(&serde_json::to_string(&schema).unwrap()).unwrap();
    assert_eq!(round_tripped, schema);
}

#[tokio::test]
async fn nested_default_errors_include_the_canonical_full_path() {
    let table = make_v2_table_with_nested();
    let bad_type = Type::Struct(StructType::new(vec![
        NestedField::optional(
            100,
            "values",
            Type::List(ListType::new(
                NestedField::list_element(
                    101,
                    Type::Struct(StructType::new(vec![
                        NestedField::optional(102, "bad", Type::Primitive(PrimitiveType::Int))
                            .with_initial_default(Literal::string("not an int"))
                            .into(),
                    ])),
                    true,
                )
                .into(),
            )),
        )
        .into(),
    ]));
    let action = Transaction::new(&table)
        .update_schema()
        .case_sensitive(false)
        .add_column(
            AddColumn::builder()
                .parent("PERSON")
                .name("details")
                .field_type(bad_type)
                .build(),
        );

    let error = commit_error(&table, action).await;
    assert_eq!(error.kind(), ErrorKind::PreconditionFailed);
    assert_eq!(
        error.message(),
        "Invalid default for column person.details.values.element.bad: default is incompatible with int"
    );
}

#[tokio::test]
async fn rejects_non_null_defaults_for_nested_types() {
    let table = crate::transaction::tests::make_v2_table();
    let nested_type = Type::Struct(StructType::new(vec![
        NestedField::optional(100, "value", Type::Primitive(PrimitiveType::Int)).into(),
    ]));
    let action = Transaction::new(&table)
        .update_schema()
        .add_column(optional_with_defaults(
            "details",
            nested_type,
            Some(Literal::Struct(Struct::from_iter(vec![Some(
                Literal::int(1),
            )]))),
            None,
        ));

    let error = commit_error(&table, action).await;
    assert_eq!(error.kind(), ErrorKind::PreconditionFailed);
    assert_eq!(
        error.message(),
        "Invalid default for column details: default is incompatible with struct<int>"
    );
}

#[tokio::test]
async fn add_then_promote_uses_the_coerced_defaults() {
    let table = crate::transaction::tests::make_v2_table();
    let action = Transaction::new(&table)
        .update_schema()
        .add_column(optional_with_defaults(
            "count",
            Type::Primitive(PrimitiveType::Int),
            Some(Literal::long(7)),
            Some(Literal::long(8)),
        ))
        .update_column_type("count", PrimitiveType::Long)
        .add_column(optional_with_defaults(
            "ratio",
            Type::Primitive(PrimitiveType::Float),
            Some(Literal::int(9)),
            Some(Literal::long(10)),
        ))
        .update_column_type("ratio", PrimitiveType::Double);

    let schema = updated_schema(&table, action).await;
    assert_eq!(
        defaults(&schema, "count"),
        (Some(Literal::long(7)), Some(Literal::long(8)))
    );
    assert_eq!(
        defaults(&schema, "ratio"),
        (Some(Literal::double(9.0)), Some(Literal::double(10.0)))
    );
}

#[tokio::test]
async fn replay_against_refreshed_metadata_reassigns_ids_and_recoerces_defaults() {
    let table = crate::transaction::tests::make_v2_table();
    let concurrent_schema = table
        .metadata()
        .current_schema()
        .as_ref()
        .clone()
        .into_builder()
        .with_fields([NestedField::optional(
            20,
            "concurrent",
            Type::Primitive(PrimitiveType::String),
        )
        .into()])
        .build()
        .unwrap();
    let refreshed_metadata = table
        .metadata()
        .clone()
        .into_builder(None)
        .add_current_schema(concurrent_schema)
        .unwrap()
        .build()
        .unwrap()
        .metadata;
    let refreshed = table.clone().with_metadata(Arc::new(refreshed_metadata));
    let action = Arc::new(Transaction::new(&table).update_schema().add_column(
        optional_with_defaults(
            "value",
            Type::Primitive(PrimitiveType::Long),
            Some(Literal::int(7)),
            Some(Literal::int(8)),
        ),
    ));

    let original = schema_from_commit(action.clone().commit(&table).await.unwrap());
    let replayed = schema_from_commit(action.commit(&refreshed).await.unwrap());
    assert_eq!(original.field_by_name("value").unwrap().id, 4);
    assert_eq!(replayed.field_by_name("value").unwrap().id, 21);
    assert_eq!(
        defaults(&original, "value"),
        (Some(Literal::long(7)), Some(Literal::long(8)))
    );
    assert_eq!(defaults(&replayed, "value"), defaults(&original, "value"));
}
