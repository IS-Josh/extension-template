#pragma once

#include "duckdb.hpp"

namespace duckdb {

// Map SQLite type strings to DuckDB LogicalType based on SQLite type affinity rules
LogicalType MapSQLiteTypeToDuckDB(const string &sqlite_type);

// Convert string value to appropriate DuckDB Value based on type
Value ConvertStringToValue(const string &str_value, const LogicalType &type);

} // namespace duckdb
