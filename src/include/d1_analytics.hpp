//===----------------------------------------------------------------------===//
//                         DuckDB
//
// d1_analytics.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"

namespace duckdb {

// Forward declaration
class ExtensionLoader;

//! Register analytics functions for D1
void RegisterD1Analytics(ExtensionLoader &loader);

} // namespace duckdb
