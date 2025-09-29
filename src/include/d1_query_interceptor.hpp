//===----------------------------------------------------------------------===//
//                         DuckDB
//
// d1_query_interceptor.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "duckdb/function/pragma_function.hpp"
#include "duckdb/parser/parsed_data/pragma_info.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

//! Query interceptor that provides an alternative way to execute UPDATE/DELETE on D1 tables
class D1QueryInterceptor {
public:
    //! Register the query interceptor functions
    static void Register(ExtensionLoader &loader);

private:
    //! Pragma function to enable D1 query rewriting
    static void D1EnableRewritingPragma(ClientContext &context, const FunctionParameters &parameters);

    //! Function to execute D1 UPDATE statements
    static void D1UpdateFunction(DataChunk &args, ExpressionState &state, Vector &result);

    //! Function to execute D1 DELETE statements
    static void D1DeleteFunction(DataChunk &args, ExpressionState &state, Vector &result);

    //! Helper to parse and execute D1 operations
    static string ExecuteD1Operation(const string &sql, const string &account_id,
                                    const string &api_token, const string &database_id);
};

} // namespace duckdb
