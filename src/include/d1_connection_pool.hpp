//===----------------------------------------------------------------------===//
//                         DuckDB
//
// d1_connection_pool.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"

namespace duckdb {

// Forward declaration
class ExtensionLoader;

//! Register connection pool functions for D1
void RegisterD1ConnectionPool(ExtensionLoader &loader);

} // namespace duckdb
