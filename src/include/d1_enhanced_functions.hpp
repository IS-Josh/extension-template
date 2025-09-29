//===----------------------------------------------------------------------===//
//                         DuckDB
//
// d1_enhanced_functions.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {

//! Enhanced D1 functions that automatically detect D1 tables and use their secrets
class D1EnhancedFunctions {
public:
    //! Register the enhanced D1 functions
    static void Register(ExtensionLoader &loader);

private:
    //! Enhanced d1_update function that auto-detects D1 tables
    static void D1SmartUpdateFunction(DataChunk &args, ExpressionState &state, Vector &result);

    //! Enhanced d1_delete function that auto-detects D1 tables
    static void D1SmartDeleteFunction(DataChunk &args, ExpressionState &state, Vector &result);

    //! Enhanced d1_insert function for consistency
    static void D1SmartInsertFunction(DataChunk &args, ExpressionState &state, Vector &result);

    //! Helper to execute D1 operation with auto-detected config
    static string ExecuteD1OperationSmart(ClientContext &context, const string &table_ref,
                                          const string &sql);
};

} // namespace duckdb
