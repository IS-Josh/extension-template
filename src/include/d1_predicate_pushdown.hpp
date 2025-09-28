//===----------------------------------------------------------------------===//
//                         DuckDB
//
// d1_predicate_pushdown.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"

namespace duckdb {

// Forward declaration
class ExtensionLoader;

//! Register predicate pushdown functions for D1
void RegisterD1PredicatePushdown(ExtensionLoader &loader);

} // namespace duckdb
