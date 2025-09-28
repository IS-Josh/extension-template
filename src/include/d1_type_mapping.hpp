//===----------------------------------------------------------------------===//
//                         DuckDB
//
// d1_type_mapping.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "duckdb/common/types.hpp"

namespace duckdb {

//! Utility class for mapping between D1/SQLite types and DuckDB LogicalTypes
class D1TypeMapping {
public:
    //! Map D1/SQLite type string to DuckDB LogicalType
    static LogicalType MapD1TypeToDuckDB(const string &d1_type);

    //! Map DuckDB LogicalType to D1/SQLite type string
    static string MapDuckDBTypeToD1(const LogicalType &duckdb_type);

    //! Check if a D1 type is numeric
    static bool IsNumericType(const string &d1_type);

    //! Check if a D1 type is text-based
    static bool IsTextType(const string &d1_type);

    //! Get the default value for a D1 type
    static Value GetDefaultValue(const LogicalType &type);

    //! Convert string value to DuckDB Value with proper type
    static Value ConvertStringToValue(const string &str_value, const LogicalType &target_type);

    //! Convert DuckDB Value to SQL string representation
    static string ConvertValueToSQL(const Value &value);

private:
    //! Normalize D1 type string (remove whitespace, convert to uppercase)
    static string NormalizeD1Type(const string &d1_type);
};

} // namespace duckdb
