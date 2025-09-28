//===----------------------------------------------------------------------===//
//                         DuckDB
//
// d1_streaming.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"

namespace duckdb {

// Forward declaration
class ExtensionLoader;

//! Register streaming functions for D1
void RegisterD1Streaming(ExtensionLoader &loader);

} // namespace duckdb
