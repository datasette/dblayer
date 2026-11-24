//! DuckDB error types, and the names the Python client gives them.
//!
//! The Python worker reports a driver error as `cause.type`
//! `_duckdb.<Class>` and maps it with `DuckDBDialect.map_exception`, whose
//! `code` is the class name without `Exception`. Results carry DuckDB's
//! error type; the C API gives prepare and open errors as text only, so the
//! type is recovered from the message's `"<Type> Error: "` prefix.

use libduckdb_sys as ffi;

/// `(error type, ExceptionTypeToString, Python class)`.
const TYPES: &[(u32, &str, &str)] = &[
    (ffi::duckdb_error_type_DUCKDB_ERROR_OUT_OF_RANGE, "Out of Range", "OutOfRangeException"),
    (ffi::duckdb_error_type_DUCKDB_ERROR_CONVERSION, "Conversion", "ConversionException"),
    (ffi::duckdb_error_type_DUCKDB_ERROR_UNKNOWN_TYPE, "Unknown Type", "Error"),
    (ffi::duckdb_error_type_DUCKDB_ERROR_DECIMAL, "Decimal", "Error"),
    (ffi::duckdb_error_type_DUCKDB_ERROR_MISMATCH_TYPE, "Mismatch Type", "TypeMismatchException"),
    (ffi::duckdb_error_type_DUCKDB_ERROR_DIVIDE_BY_ZERO, "Divide by Zero", "Error"),
    (ffi::duckdb_error_type_DUCKDB_ERROR_OBJECT_SIZE, "Object Size", "Error"),
    (ffi::duckdb_error_type_DUCKDB_ERROR_INVALID_TYPE, "Invalid type", "InvalidTypeException"),
    (ffi::duckdb_error_type_DUCKDB_ERROR_SERIALIZATION, "Serialization", "SerializationException"),
    (ffi::duckdb_error_type_DUCKDB_ERROR_TRANSACTION, "TransactionContext", "TransactionException"),
    (ffi::duckdb_error_type_DUCKDB_ERROR_NOT_IMPLEMENTED, "Not implemented", "NotImplementedException"),
    (ffi::duckdb_error_type_DUCKDB_ERROR_EXPRESSION, "Expression", "Error"),
    (ffi::duckdb_error_type_DUCKDB_ERROR_CATALOG, "Catalog", "CatalogException"),
    (ffi::duckdb_error_type_DUCKDB_ERROR_PARSER, "Parser", "ParserException"),
    (ffi::duckdb_error_type_DUCKDB_ERROR_PLANNER, "Planner", "Error"),
    (ffi::duckdb_error_type_DUCKDB_ERROR_SCHEDULER, "Scheduler", "Error"),
    (ffi::duckdb_error_type_DUCKDB_ERROR_EXECUTOR, "Executor", "Error"),
    (ffi::duckdb_error_type_DUCKDB_ERROR_CONSTRAINT, "Constraint", "ConstraintException"),
    (ffi::duckdb_error_type_DUCKDB_ERROR_INDEX, "Index", "Error"),
    (ffi::duckdb_error_type_DUCKDB_ERROR_STAT, "Stat", "Error"),
    (ffi::duckdb_error_type_DUCKDB_ERROR_CONNECTION, "Connection", "ConnectionException"),
    (ffi::duckdb_error_type_DUCKDB_ERROR_SYNTAX, "Syntax", "SyntaxException"),
    (ffi::duckdb_error_type_DUCKDB_ERROR_SETTINGS, "Settings", "Error"),
    (ffi::duckdb_error_type_DUCKDB_ERROR_BINDER, "Binder", "BinderException"),
    (ffi::duckdb_error_type_DUCKDB_ERROR_NETWORK, "Network", "Error"),
    (ffi::duckdb_error_type_DUCKDB_ERROR_OPTIMIZER, "Optimizer", "Error"),
    (ffi::duckdb_error_type_DUCKDB_ERROR_NULL_POINTER, "NullPointer", "Error"),
    (ffi::duckdb_error_type_DUCKDB_ERROR_IO, "IO", "IOException"),
    (ffi::duckdb_error_type_DUCKDB_ERROR_INTERRUPT, "INTERRUPT", "InterruptException"),
    (ffi::duckdb_error_type_DUCKDB_ERROR_FATAL, "FATAL", "FatalException"),
    (ffi::duckdb_error_type_DUCKDB_ERROR_INTERNAL, "INTERNAL", "InternalException"),
    (ffi::duckdb_error_type_DUCKDB_ERROR_INVALID_INPUT, "Invalid Input", "InvalidInputException"),
    (ffi::duckdb_error_type_DUCKDB_ERROR_OUT_OF_MEMORY, "Out of Memory", "OutOfMemoryException"),
    (ffi::duckdb_error_type_DUCKDB_ERROR_PERMISSION, "Permission", "PermissionException"),
    (ffi::duckdb_error_type_DUCKDB_ERROR_PARAMETER_NOT_RESOLVED, "Parameter Not Resolved", "Error"),
    (ffi::duckdb_error_type_DUCKDB_ERROR_PARAMETER_NOT_ALLOWED, "Parameter Not Allowed", "Error"),
    (ffi::duckdb_error_type_DUCKDB_ERROR_DEPENDENCY, "Dependency", "DependencyException"),
    (ffi::duckdb_error_type_DUCKDB_ERROR_HTTP, "HTTP", "HTTPException"),
    (ffi::duckdb_error_type_DUCKDB_ERROR_MISSING_EXTENSION, "Missing Extension", "Error"),
    (ffi::duckdb_error_type_DUCKDB_ERROR_AUTOLOAD, "Extension Autoloading", "Error"),
    (ffi::duckdb_error_type_DUCKDB_ERROR_SEQUENCE, "Sequence", "SequenceException"),
    (ffi::duckdb_error_type_DUCKDB_ERROR_RESOURCE_IN_USE, "Resource In Use", "ResourceInUseException"),
];

pub const INTERRUPT: u32 = ffi::duckdb_error_type_DUCKDB_ERROR_INTERRUPT;
pub const TRANSACTION: u32 = ffi::duckdb_error_type_DUCKDB_ERROR_TRANSACTION;
pub const CONSTRAINT: u32 = ffi::duckdb_error_type_DUCKDB_ERROR_CONSTRAINT;
pub const OUT_OF_MEMORY: u32 = ffi::duckdb_error_type_DUCKDB_ERROR_OUT_OF_MEMORY;
pub const CONNECTION: u32 = ffi::duckdb_error_type_DUCKDB_ERROR_CONNECTION;
pub const INVALID_INPUT: u32 = ffi::duckdb_error_type_DUCKDB_ERROR_INVALID_INPUT;
pub const BINDER: u32 = ffi::duckdb_error_type_DUCKDB_ERROR_BINDER;
pub const INVALID: u32 = ffi::duckdb_error_type_DUCKDB_ERROR_INVALID;

/// An error raised by DuckDB.
#[derive(Clone, Debug)]
pub struct DuckError {
    pub kind: u32,
    pub message: String,
}

impl DuckError {
    pub fn new(kind: u32, message: impl Into<String>) -> DuckError {
        DuckError { kind, message: message.into() }
    }

    /// An error known only by its text (prepare, open).
    pub fn from_message(message: impl Into<String>) -> DuckError {
        let message = message.into();
        let kind = TYPES
            .iter()
            .find(|(_, prefix, _)| {
                message.strip_prefix(prefix).is_some_and(|rest| rest.starts_with(" Error: ") || rest.starts_with(" Error:"))
            })
            .map(|(k, _, _)| *k)
            .unwrap_or(INVALID);
        DuckError { kind, message }
    }

    /// The Python client's exception class for this error.
    pub fn class(&self) -> &'static str {
        TYPES.iter().find(|(k, _, _)| *k == self.kind).map(|(_, _, c)| *c).unwrap_or("Error")
    }

    /// `DuckDBDialect.map_exception`'s code: the class without `Exception`.
    pub fn code(&self) -> &'static str {
        let c = self.class();
        c.strip_suffix("Exception").filter(|s| !s.is_empty()).unwrap_or(c)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classes_from_messages() {
        let e = DuckError::from_message("Catalog Error: Table with name nope does not exist!");
        assert_eq!((e.class(), e.code()), ("CatalogException", "Catalog"));
        let e = DuckError::from_message("TransactionContext Error: Current transaction is aborted");
        assert_eq!(e.class(), "TransactionException");
        assert_eq!(DuckError::from_message("weird").class(), "Error");
    }
}
