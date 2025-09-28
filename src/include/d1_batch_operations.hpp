//===----------------------------------------------------------------------===//
//                         DuckDB
//
// d1_batch_operations.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"

namespace duckdb {

// Forward declaration
class ExtensionLoader;

//! Register batch operations functions for D1
void RegisterD1BatchOperations(ExtensionLoader &loader);

} // namespace duckdb
