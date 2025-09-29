//===----------------------------------------------------------------------===//
//                         DuckDB
//
// d1_enhanced_functions.cpp
//
//
//===----------------------------------------------------------------------===//

#include "include/d1_enhanced_functions.hpp"
#include "include/d1_secret_helper.hpp"
#include "include/d1_client.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

void D1EnhancedFunctions::Register(ExtensionLoader &loader) {
    fprintf(stderr, "D1EnhancedFunctions: Registering smart D1 helper functions\n");

    // Register d1_smart_update function: d1_smart_update(table_ref, set_clause, where_clause)
    ScalarFunctionSet d1_smart_update_set("d1_smart_update");
    d1_smart_update_set.AddFunction(ScalarFunction(
        {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
        LogicalType::VARCHAR,
        [](DataChunk &args, ExpressionState &state, Vector &result) {
            D1SmartUpdateFunction(args, state, result);
        }
    ));

    loader.RegisterFunction(d1_smart_update_set);

    // Register d1_smart_delete function: d1_smart_delete(table_ref, where_clause)
    ScalarFunctionSet d1_smart_delete_set("d1_smart_delete");
    d1_smart_delete_set.AddFunction(ScalarFunction(
        {LogicalType::VARCHAR, LogicalType::VARCHAR},
        LogicalType::VARCHAR,
        [](DataChunk &args, ExpressionState &state, Vector &result) {
            D1SmartDeleteFunction(args, state, result);
        }
    ));

    loader.RegisterFunction(d1_smart_delete_set);

    // Register d1_smart_insert function: d1_smart_insert(table_ref, columns, values)
    ScalarFunctionSet d1_smart_insert_set("d1_smart_insert");
    d1_smart_insert_set.AddFunction(ScalarFunction(
        {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
        LogicalType::VARCHAR,
        [](DataChunk &args, ExpressionState &state, Vector &result) {
            D1SmartInsertFunction(args, state, result);
        }
    ));

    loader.RegisterFunction(d1_smart_insert_set);

    fprintf(stderr, "D1EnhancedFunctions: Registered d1_smart_update, d1_smart_delete, and d1_smart_insert functions\n");
}

void D1EnhancedFunctions::D1SmartUpdateFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    auto count = args.size();
    auto &context = state.GetContext();

    // Get input vectors
    auto &table_ref_vector = args.data[0];
    auto &set_clause_vector = args.data[1];
    auto &where_clause_vector = args.data[2];

    // Flatten all vectors
    table_ref_vector.Flatten(count);
    set_clause_vector.Flatten(count);
    where_clause_vector.Flatten(count);

    auto table_ref_data = FlatVector::GetData<string_t>(table_ref_vector);
    auto set_clause_data = FlatVector::GetData<string_t>(set_clause_vector);
    auto where_clause_data = FlatVector::GetData<string_t>(where_clause_vector);

    // Process each row
    for (idx_t i = 0; i < count; i++) {
        auto table_ref = table_ref_data[i].GetString();
        auto set_clause = set_clause_data[i].GetString();
        auto where_clause = where_clause_data[i].GetString();

        // Parse table reference
        auto parsed = D1SecretHelper::ParseTableReference(table_ref);
        auto catalog_name = std::get<0>(parsed);
        auto schema_name = std::get<1>(parsed);
        auto table_name = std::get<2>(parsed);

        // Build UPDATE SQL
        string sql = "UPDATE " + table_name + " SET " + set_clause;
        if (!where_clause.empty()) {
            sql += " WHERE " + where_clause;
        }

        fprintf(stderr, "D1EnhancedFunctions: Smart UPDATE on %s.%s.%s: %s\n",
               catalog_name.c_str(), schema_name.c_str(), table_name.c_str(), sql.c_str());

        try {
            string result_str = ExecuteD1OperationSmart(context, table_ref, sql);
            FlatVector::GetData<string_t>(result)[i] = StringVector::AddString(result, result_str);
        } catch (const std::exception &e) {
            throw InvalidInputException("D1 smart UPDATE failed: %s", e.what());
        }
    }
}

void D1EnhancedFunctions::D1SmartDeleteFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    auto count = args.size();
    auto &context = state.GetContext();

    // Get input vectors
    auto &table_ref_vector = args.data[0];
    auto &where_clause_vector = args.data[1];

    // Flatten all vectors
    table_ref_vector.Flatten(count);
    where_clause_vector.Flatten(count);

    auto table_ref_data = FlatVector::GetData<string_t>(table_ref_vector);
    auto where_clause_data = FlatVector::GetData<string_t>(where_clause_vector);

    // Process each row
    for (idx_t i = 0; i < count; i++) {
        auto table_ref = table_ref_data[i].GetString();
        auto where_clause = where_clause_data[i].GetString();

        // Parse table reference
        auto parsed = D1SecretHelper::ParseTableReference(table_ref);
        auto catalog_name = std::get<0>(parsed);
        auto schema_name = std::get<1>(parsed);
        auto table_name = std::get<2>(parsed);

        // Build DELETE SQL
        string sql = "DELETE FROM " + table_name;
        if (!where_clause.empty()) {
            sql += " WHERE " + where_clause;
        }

        fprintf(stderr, "D1EnhancedFunctions: Smart DELETE on %s.%s.%s: %s\n",
               catalog_name.c_str(), schema_name.c_str(), table_name.c_str(), sql.c_str());

        try {
            string result_str = ExecuteD1OperationSmart(context, table_ref, sql);
            FlatVector::GetData<string_t>(result)[i] = StringVector::AddString(result, result_str);
        } catch (const std::exception &e) {
            throw InvalidInputException("D1 smart DELETE failed: %s", e.what());
        }
    }
}

void D1EnhancedFunctions::D1SmartInsertFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    auto count = args.size();
    auto &context = state.GetContext();

    // Get input vectors
    auto &table_ref_vector = args.data[0];
    auto &columns_vector = args.data[1];
    auto &values_vector = args.data[2];

    // Flatten all vectors
    table_ref_vector.Flatten(count);
    columns_vector.Flatten(count);
    values_vector.Flatten(count);

    auto table_ref_data = FlatVector::GetData<string_t>(table_ref_vector);
    auto columns_data = FlatVector::GetData<string_t>(columns_vector);
    auto values_data = FlatVector::GetData<string_t>(values_vector);

    // Process each row
    for (idx_t i = 0; i < count; i++) {
        auto table_ref = table_ref_data[i].GetString();
        auto columns = columns_data[i].GetString();
        auto values = values_data[i].GetString();

        // Parse table reference
        auto parsed = D1SecretHelper::ParseTableReference(table_ref);
        auto catalog_name = std::get<0>(parsed);
        auto schema_name = std::get<1>(parsed);
        auto table_name = std::get<2>(parsed);

        // Build INSERT SQL
        string sql = "INSERT INTO " + table_name;
        if (!columns.empty()) {
            sql += " (" + columns + ")";
        }
        sql += " VALUES (" + values + ")";

        fprintf(stderr, "D1EnhancedFunctions: Smart INSERT on %s.%s.%s: %s\n",
               catalog_name.c_str(), schema_name.c_str(), table_name.c_str(), sql.c_str());

        try {
            string result_str = ExecuteD1OperationSmart(context, table_ref, sql);
            FlatVector::GetData<string_t>(result)[i] = StringVector::AddString(result, result_str);
        } catch (const std::exception &e) {
            throw InvalidInputException("D1 smart INSERT failed: %s", e.what());
        }
    }
}

string D1EnhancedFunctions::ExecuteD1OperationSmart(ClientContext &context, const string &table_ref,
                                                   const string &sql) {
    // Parse table reference
    auto parsed = D1SecretHelper::ParseTableReference(table_ref);
    auto catalog_name = std::get<0>(parsed);
    auto schema_name = std::get<1>(parsed);
    auto table_name = std::get<2>(parsed);

    // Try to get config from D1 table
    auto config_ptr = D1SecretHelper::GetConfigFromTableRef(context, catalog_name, schema_name, table_name);

    if (!config_ptr) {
        throw InvalidInputException("Table '%s' is not a D1 table or does not exist", table_ref.c_str());
    }

    // Execute the operation using the table's config
    CloudflareD1Client client(*config_ptr);

    try {
        auto result = client.RawQuery(sql, {});
        if (result.success) {
            return "SUCCESS: Operation completed on D1 table " + table_ref;
        } else {
            throw InvalidInputException("D1 operation failed: %s", result.error.c_str());
        }
    } catch (const std::exception &e) {
        throw InvalidInputException("D1 client error: %s", e.what());
    }
}

} // namespace duckdb
