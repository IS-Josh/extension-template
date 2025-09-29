//===----------------------------------------------------------------------===//
//                         DuckDB
//
// d1_query_interceptor.cpp
//
//
//===----------------------------------------------------------------------===//

#include "include/d1_query_interceptor.hpp"
#include "include/d1_client.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

void D1QueryInterceptor::Register(ExtensionLoader &loader) {
    fprintf(stderr, "D1QueryInterceptor: Registering D1 UPDATE/DELETE helper functions\n");

    // Register d1_update function: d1_update(table_name, set_clause, where_clause, account_id, api_token, database_id)
    ScalarFunctionSet d1_update_set("d1_update");
    d1_update_set.AddFunction(ScalarFunction(
        {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
        LogicalType::VARCHAR,
        [](DataChunk &args, ExpressionState &state, Vector &result) {
            D1UpdateFunction(args, state, result);
        }
    ));

    loader.RegisterFunction(d1_update_set);

    // Register d1_delete function: d1_delete(table_name, where_clause, account_id, api_token, database_id)
    ScalarFunctionSet d1_delete_set("d1_delete");
    d1_delete_set.AddFunction(ScalarFunction(
        {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
        LogicalType::VARCHAR,
        [](DataChunk &args, ExpressionState &state, Vector &result) {
            D1DeleteFunction(args, state, result);
        }
    ));

    loader.RegisterFunction(d1_delete_set);

    fprintf(stderr, "D1QueryInterceptor: Registered d1_update and d1_delete helper functions\n");
}

void D1QueryInterceptor::D1UpdateFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    auto count = args.size();

    // Get input vectors
    auto &table_name_vector = args.data[0];
    auto &set_clause_vector = args.data[1];
    auto &where_clause_vector = args.data[2];
    auto &account_id_vector = args.data[3];
    auto &api_token_vector = args.data[4];
    auto &database_id_vector = args.data[5];

    // Flatten all vectors
    table_name_vector.Flatten(count);
    set_clause_vector.Flatten(count);
    where_clause_vector.Flatten(count);
    account_id_vector.Flatten(count);
    api_token_vector.Flatten(count);
    database_id_vector.Flatten(count);

    auto table_name_data = FlatVector::GetData<string_t>(table_name_vector);
    auto set_clause_data = FlatVector::GetData<string_t>(set_clause_vector);
    auto where_clause_data = FlatVector::GetData<string_t>(where_clause_vector);
    auto account_id_data = FlatVector::GetData<string_t>(account_id_vector);
    auto api_token_data = FlatVector::GetData<string_t>(api_token_vector);
    auto database_id_data = FlatVector::GetData<string_t>(database_id_vector);

    // Process each row
    for (idx_t i = 0; i < count; i++) {
        auto table_name = table_name_data[i].GetString();
        auto set_clause = set_clause_data[i].GetString();
        auto where_clause = where_clause_data[i].GetString();
        auto account_id = account_id_data[i].GetString();
        auto api_token = api_token_data[i].GetString();
        auto database_id = database_id_data[i].GetString();

        // Build UPDATE SQL
        string sql = "UPDATE " + table_name + " SET " + set_clause;
        if (!where_clause.empty()) {
            sql += " WHERE " + where_clause;
        }

        fprintf(stderr, "D1QueryInterceptor: Executing UPDATE: %s\n", sql.c_str());

        try {
            string result_str = ExecuteD1Operation(sql, account_id, api_token, database_id);
            FlatVector::GetData<string_t>(result)[i] = StringVector::AddString(result, result_str);
        } catch (const std::exception &e) {
            throw InvalidInputException("D1 UPDATE failed: %s", e.what());
        }
    }
}

void D1QueryInterceptor::D1DeleteFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    auto count = args.size();

    // Get input vectors
    auto &table_name_vector = args.data[0];
    auto &where_clause_vector = args.data[1];
    auto &account_id_vector = args.data[2];
    auto &api_token_vector = args.data[3];
    auto &database_id_vector = args.data[4];

    // Flatten all vectors
    table_name_vector.Flatten(count);
    where_clause_vector.Flatten(count);
    account_id_vector.Flatten(count);
    api_token_vector.Flatten(count);
    database_id_vector.Flatten(count);

    auto table_name_data = FlatVector::GetData<string_t>(table_name_vector);
    auto where_clause_data = FlatVector::GetData<string_t>(where_clause_vector);
    auto account_id_data = FlatVector::GetData<string_t>(account_id_vector);
    auto api_token_data = FlatVector::GetData<string_t>(api_token_vector);
    auto database_id_data = FlatVector::GetData<string_t>(database_id_vector);

    // Process each row
    for (idx_t i = 0; i < count; i++) {
        auto table_name = table_name_data[i].GetString();
        auto where_clause = where_clause_data[i].GetString();
        auto account_id = account_id_data[i].GetString();
        auto api_token = api_token_data[i].GetString();
        auto database_id = database_id_data[i].GetString();

        // Build DELETE SQL
        string sql = "DELETE FROM " + table_name;
        if (!where_clause.empty()) {
            sql += " WHERE " + where_clause;
        }

        fprintf(stderr, "D1QueryInterceptor: Executing DELETE: %s\n", sql.c_str());

        try {
            string result_str = ExecuteD1Operation(sql, account_id, api_token, database_id);
            FlatVector::GetData<string_t>(result)[i] = StringVector::AddString(result, result_str);
        } catch (const std::exception &e) {
            throw InvalidInputException("D1 DELETE failed: %s", e.what());
        }
    }
}

string D1QueryInterceptor::ExecuteD1Operation(const string &sql, const string &account_id,
                                             const string &api_token, const string &database_id) {
    CloudflareD1Config config;
    config.account_id = account_id;
    config.api_token = api_token;
    config.database_id = database_id;

    CloudflareD1Client client(config);

    try {
        auto result = client.RawQuery(sql, {});
        if (result.success) {
            return "SUCCESS: Operation completed";
        } else {
            throw InvalidInputException("D1 operation failed: %s", result.error.c_str());
        }
    } catch (const std::exception &e) {
        throw InvalidInputException("D1 client error: %s", e.what());
    }
}

} // namespace duckdb
