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

use super::assign_fresh_ids;
use crate::ErrorKind;
use crate::spec::{
    ListType, Literal, MapType, NestedField, PrimitiveType, Schema, StructType, Type, VariantType,
};

#[test]
fn assigns_variant_field_id() {
    let mut last_id = 10;
    let field = NestedField::optional(1, "data", Type::Variant(VariantType));

    let assigned = assign_fresh_ids(&field, &mut last_id).unwrap();

    assert_eq!(assigned.id, 11);
    assert_eq!(*assigned.field_type, Type::Variant(VariantType));
    assert_eq!(last_id, 11);
}

#[test]
fn assigns_struct_siblings_before_descendants() {
    let field = NestedField::optional(
        20,
        "outer",
        Type::Struct(StructType::new(vec![
            NestedField::optional(
                21,
                "a",
                Type::Struct(StructType::new(vec![
                    NestedField::optional(22, "c", Type::Primitive(PrimitiveType::Int)).into(),
                ])),
            )
            .into(),
            NestedField::optional(23, "b", Type::Primitive(PrimitiveType::Int)).into(),
        ])),
    );
    let mut last_id = 3;

    let assigned = assign_fresh_ids(&field, &mut last_id).unwrap();
    let schema = Schema::builder().with_fields([assigned]).build().unwrap();

    assert_eq!(schema.field_by_name("outer").unwrap().id, 4);
    assert_eq!(schema.field_by_name("outer.a").unwrap().id, 5);
    assert_eq!(schema.field_by_name("outer.b").unwrap().id, 6);
    assert_eq!(schema.field_by_name("outer.a.c").unwrap().id, 7);
    assert_eq!(last_id, 7);
}

#[test]
fn assigns_map_key_and_value_before_their_descendants() {
    let field = NestedField::optional(
        20,
        "mapped",
        Type::Map(MapType {
            key_field: NestedField::map_key_element(
                21,
                Type::Struct(StructType::new(vec![
                    NestedField::required(22, "key_part", Type::Primitive(PrimitiveType::Int))
                        .into(),
                ])),
            )
            .into(),
            value_field: NestedField::map_value_element(
                23,
                Type::Struct(StructType::new(vec![
                    NestedField::optional(24, "value_part", Type::Primitive(PrimitiveType::String))
                        .into(),
                ])),
                false,
            )
            .into(),
        }),
    );
    let mut last_id = 0;

    let assigned = assign_fresh_ids(&field, &mut last_id).unwrap();
    let Type::Map(map) = assigned.field_type.as_ref() else {
        panic!("expected map field");
    };
    let Type::Struct(key) = map.key_field.field_type.as_ref() else {
        panic!("expected struct key");
    };
    let Type::Struct(value) = map.value_field.field_type.as_ref() else {
        panic!("expected struct value");
    };

    assert_eq!(assigned.id, 1);
    assert_eq!(map.key_field.id, 2);
    assert_eq!(map.value_field.id, 3);
    assert_eq!(key.fields()[0].id, 4);
    assert_eq!(value.fields()[0].id, 5);
    assert_eq!(last_id, 5);
}

#[test]
fn rebuilds_collection_pseudo_fields_canonically() {
    let element = NestedField::optional(2, "items", Type::Primitive(PrimitiveType::String))
        .with_doc("discarded")
        .with_initial_default(Literal::string("discarded"));
    let list_field = NestedField::optional(
        1,
        "listed",
        Type::List(ListType {
            element_field: element.into(),
        }),
    );
    let mut last_id = 0;
    let assigned_list = assign_fresh_ids(&list_field, &mut last_id).unwrap();
    let Type::List(list) = assigned_list.field_type.as_ref() else {
        panic!("expected list field");
    };

    assert_eq!(list.element_field.name, "element");
    assert!(!list.element_field.required);
    assert_eq!(list.element_field.doc, None);
    assert_eq!(list.element_field.initial_default, None);
    assert_eq!(list.element_field.write_default, None);

    let key = NestedField::optional(3, "map_key", Type::Primitive(PrimitiveType::Int))
        .with_doc("discarded");
    let value = NestedField::required(4, "map_value", Type::Primitive(PrimitiveType::String))
        .with_write_default(Literal::string("discarded"));
    let map_field = NestedField::optional(
        2,
        "mapped",
        Type::Map(MapType {
            key_field: key.into(),
            value_field: value.into(),
        }),
    );
    let assigned_map = assign_fresh_ids(&map_field, &mut last_id).unwrap();
    let Type::Map(map) = assigned_map.field_type.as_ref() else {
        panic!("expected map field");
    };

    assert_eq!(map.key_field.name, "key");
    assert!(map.key_field.required);
    assert_eq!(map.key_field.doc, None);
    assert_eq!(map.key_field.initial_default, None);
    assert_eq!(map.key_field.write_default, None);
    assert_eq!(map.value_field.name, "value");
    assert!(map.value_field.required);
    assert_eq!(map.value_field.doc, None);
    assert_eq!(map.value_field.initial_default, None);
    assert_eq!(map.value_field.write_default, None);
}

#[test]
fn rejects_field_id_overflow() {
    let field = NestedField::optional(1, "new", Type::Primitive(PrimitiveType::Int));
    let mut last_id = i32::MAX;

    let error = assign_fresh_ids(&field, &mut last_id).unwrap_err();

    assert_eq!(error.kind(), ErrorKind::DataInvalid);
    assert!(error.message().contains("overflowed"));
    assert_eq!(last_id, i32::MAX);
}
