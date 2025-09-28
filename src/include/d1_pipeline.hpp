//===----------------------------------------------------------------------===//
//                         DuckDB
//
// d1_pipeline.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"

namespace duckdb {

// Forward declaration
class ExtensionLoader;

//! Register pipeline functions for D1
void RegisterD1Pipeline(ExtensionLoader &loader);

} // namespace duckdb
