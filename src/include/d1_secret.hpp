#pragma once

#include "duckdb.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {

// Secret type constant
constexpr const char* SECRET_TYPE_D1 = "d1";

void RegisterD1SecretFunctions(ExtensionLoader &loader);

} // namespace duckdb
