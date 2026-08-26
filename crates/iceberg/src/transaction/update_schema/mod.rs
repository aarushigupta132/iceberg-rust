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

mod apply;
mod column_properties;
mod commit;
mod fresh_ids;
mod name_mapping;
mod tree;
mod type_promotion;

#[cfg(test)]
#[path = "tests/case_sensitive.rs"]
mod case_sensitive_tests;
#[cfg(test)]
#[path = "tests/column_properties.rs"]
mod column_properties_tests;
#[cfg(test)]
#[path = "tests/name_mapping.rs"]
mod name_mapping_tests;
#[cfg(test)]
#[path = "tests/ordered.rs"]
mod ordered_tests;
#[cfg(test)]
#[path = "tests/rename.rs"]
mod rename_tests;
#[cfg(test)]
mod tests;
#[cfg(test)]
#[path = "tests/type_promotion.rs"]
mod type_promotion_tests;

use typed_builder::TypedBuilder;

use crate::spec::{Literal, PrimitiveType, Type};

/// Declarative specification for adding a column in an [`UpdateSchemaAction`].
///
/// Use helper constructors such as [`AddColumn::optional`] and [`AddColumn::required`],
/// optionally combined with the builder's `parent` and `doc` setters via
/// [`AddColumn::builder`], then pass the value to [`UpdateSchemaAction::add_column`].
#[derive(TypedBuilder)]
pub struct AddColumn {
    #[builder(default = None, setter(strip_option, into))]
    parent: Option<String>,
    #[builder(setter(into))]
    name: String,
    #[builder(default = false)]
    required: bool,
    field_type: Type,
    #[builder(default = None, setter(strip_option, into))]
    doc: Option<String>,
    #[builder(default = None, setter(strip_option))]
    initial_default: Option<Literal>,
    #[builder(default = None, setter(strip_option))]
    write_default: Option<Literal>,
}

impl AddColumn {
    /// Create a root-level optional column specification.
    ///
    /// Empty root-level names are invalid, and names containing `.` are rejected as ambiguous.
    /// Use the builder's `parent` setter to add a nested field, including a nested field whose
    /// leaf name contains `.`.
    pub fn optional(name: impl ToString, field_type: Type) -> Self {
        Self::builder()
            .name(name.to_string())
            .field_type(field_type)
            .required(false)
            .build()
    }

    /// Create a root-level required column specification.
    ///
    /// Empty root-level names are invalid, and names containing `.` are rejected as ambiguous.
    /// Use the builder's `parent` setter to add a nested field, including a nested field whose
    /// leaf name contains `.`.
    pub fn required(name: impl ToString, field_type: Type, initial_default: Literal) -> Self {
        Self::builder()
            .name(name.to_string())
            .field_type(field_type)
            .required(true)
            .initial_default(initial_default.clone())
            .write_default(initial_default)
            .build()
    }
}

/// Schema evolution API modeled after Apache Iceberg Java's `SchemaUpdate` implementation.
///
/// Operations are replayed against the latest table metadata on every transaction commit attempt.
/// This keeps validation and field-ID assignment correct after a concurrent commit.
///
/// # Example
///
/// ```ignore
/// let tx = Transaction::new(&table);
/// let action = tx.update_schema()
///     .add_column(AddColumn::optional("new_col", Type::Primitive(PrimitiveType::Int)))
///     .add_column(
///         AddColumn::builder()
///             .parent("person")
///             .name("email")
///             .field_type(Type::Primitive(PrimitiveType::String))
///             .build()
///     )
///     .delete_column("old_col");
/// let tx = action.apply(tx).unwrap();
/// let table = tx.commit(&catalog).await.unwrap();
/// ```
pub struct UpdateSchemaAction {
    operations: Vec<SchemaOperation>,
}

enum SchemaOperation {
    Add(Box<AddColumn>),
    Delete(String),
    Rename {
        name: String,
        new_name: String,
    },
    UpdateType {
        name: String,
        new_type: PrimitiveType,
    },
    SetCaseSensitive(bool),
}

impl UpdateSchemaAction {
    /// Creates a new empty `UpdateSchemaAction`.
    pub(crate) fn new() -> Self {
        Self {
            operations: Vec::new(),
        }
    }

    // --- Root-level additions ---

    /// Add a column to the table schema.
    ///
    /// To add a root-level column, leave `AddColumn::parent` as `None`.
    /// For nested additions, set a parent path.
    /// If the parent resolves to a map/list, the column is added to map value/list element.
    pub fn add_column(mut self, add_column: AddColumn) -> Self {
        self.operations
            .push(SchemaOperation::Add(Box::new(add_column)));
        self
    }

    // --- Other builder methods ---

    /// Record a column deletion by name.
    ///
    /// At commit time, the column must exist in the current schema.
    pub fn delete_column(mut self, name: impl ToString) -> Self {
        self.operations
            .push(SchemaOperation::Delete(name.to_string()));
        self
    }

    /// Rename a column while preserving its field ID and other metadata.
    ///
    /// Operations in the same action continue to resolve the column by its original schema name.
    pub fn rename_column(mut self, name: impl ToString, new_name: impl ToString) -> Self {
        self.operations.push(SchemaOperation::Rename {
            name: name.to_string(),
            new_name: new_name.to_string(),
        });
        self
    }

    /// Promote a primitive column to a compatible type.
    ///
    /// Supported promotions are `int` to `long`, `float` to `double`, and increasing decimal
    /// precision without changing scale. The column's field ID and other metadata are preserved.
    pub fn update_column_type(mut self, name: impl ToString, new_type: PrimitiveType) -> Self {
        self.operations.push(SchemaOperation::UpdateType {
            name: name.to_string(),
            new_type,
        });
        self
    }

    /// Configure whether subsequent operations resolve column names case-sensitively.
    ///
    /// Column-name resolution is case-sensitive by default.
    pub fn case_sensitive(mut self, case_sensitive: bool) -> Self {
        self.operations
            .push(SchemaOperation::SetCaseSensitive(case_sensitive));
        self
    }
}
