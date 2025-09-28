//===----------------------------------------------------------------------===//
//                         DuckDB
//
// d1_batch_operations.cpp
//
//
//===----------------------------------------------------------------------===//

#include "duckdb.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/table_function.hpp"
#include "include/d1_client.hpp"
#include "include/d1_secret.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {

//! Simple batch execute for multiple D1 operations
//! Takes account_id, api_token, database_id, and SQL array
struct D1BatchExecuteBindData : public TableFunctionData {
    CloudflareD1Config config;
    vector<string> sql_statements;
    bool use_transaction = true;

    explicit D1BatchExecuteBindData(CloudflareD1Config cfg, vector<string> statements, bool transaction = true)
        : config(std::move(cfg)), sql_statements(std::move(statements)), use_transaction(transaction) {}
};

struct D1BatchExecuteGlobalState : public GlobalTableFunctionState {
    CloudflareD1Client client;
    vector<CloudflareD1QueryResult> results;
    bool executed = false;
    idx_t current_row = 0;

    explicit D1BatchExecuteGlobalState(const CloudflareD1Config &cfg) : client(cfg) {}

    idx_t MaxThreads() const override { return 1; }
};

unique_ptr<FunctionData> D1BatchExecuteBind(ClientContext &context, TableFunctionBindInput &input,
                                           vector<LogicalType> &return_types, vector<string> &names) {
    if (input.inputs.size() < 4) {
        throw BinderException("d1_batch_execute requires: account_id, api_token, database_id, sql_statement");
    }

    // Get credentials directly
    CloudflareD1Config config;
    config.account_id = input.inputs[0].GetValue<string>();
    config.api_token = input.inputs[1].GetValue<string>();
    config.database_id = input.inputs[2].GetValue<string>();

    // Parse SQL statement (single statement for now)
    vector<string> sql_statements;
    string sql = input.inputs[3].GetValue<string>();
    sql_statements.push_back(sql);

    if (sql_statements.empty()) {
        throw BinderException("d1_batch_execute: No SQL statements provided");
    }

    // Set up return types
    return_types = {LogicalType::BIGINT, LogicalType::VARCHAR, LogicalType::BOOLEAN, LogicalType::VARCHAR};
    names = {"statement_index", "sql", "success", "error_message"};

    return make_uniq<D1BatchExecuteBindData>(std::move(config), std::move(sql_statements), true);
}

unique_ptr<GlobalTableFunctionState> D1BatchExecuteInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
    auto &bind_data = input.bind_data->Cast<D1BatchExecuteBindData>();
    return make_uniq<D1BatchExecuteGlobalState>(bind_data.config);
}

void D1BatchExecuteFunc(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    auto &state = data_p.global_state->Cast<D1BatchExecuteGlobalState>();
    auto &bind_data = data_p.bind_data->Cast<D1BatchExecuteBindData>();

    if (!state.executed) {
        fprintf(stderr, "D1BatchExecute: Executing %zu SQL statements\n", bind_data.sql_statements.size());

        // Execute all statements
        bool all_success = true;
        for (idx_t i = 0; i < bind_data.sql_statements.size(); i++) {
            const auto &sql = bind_data.sql_statements[i];
            fprintf(stderr, "D1BatchExecute: [%zu] %s\n", i, sql.c_str());

            CloudflareD1QueryResult result;
            try {
                result = state.client.ObjectQuery(sql, {});
                if (!result.success) {
                    all_success = false;
                    fprintf(stderr, "D1BatchExecute: [%zu] Failed: %s\n", i, result.error.c_str());
                }
            } catch (const std::exception &e) {
                result.success = false;
                result.error = string("Exception: ") + e.what();
                all_success = false;
                fprintf(stderr, "D1BatchExecute: [%zu] Exception: %s\n", i, e.what());
            }

            state.results.push_back(result);

            // If using transaction semantics and a statement fails, stop execution
            if (bind_data.use_transaction && !result.success) {
                fprintf(stderr, "D1BatchExecute: Transaction mode - stopping execution due to failure\n");
                break;
            }
        }

        if (bind_data.use_transaction && !all_success) {
            fprintf(stderr, "D1BatchExecute: Transaction failed - some operations may need manual rollback\n");
        }

        state.executed = true;
    }

    // Return results
    idx_t count = 0;
    while (count < STANDARD_VECTOR_SIZE && state.current_row < state.results.size()) {
        auto &result = state.results[state.current_row];

        output.SetValue(0, count, Value::BIGINT(static_cast<int64_t>(state.current_row))); // statement_index
        output.SetValue(1, count, Value(bind_data.sql_statements[state.current_row]));     // sql
        output.SetValue(2, count, Value::BOOLEAN(result.success));                        // success
        output.SetValue(3, count, Value(result.error.empty() ? "OK" : result.error));    // error_message

        state.current_row++;
        count++;
    }

    output.SetCardinality(count);
}

//! Register batch operations functions
void RegisterD1BatchOperations(ExtensionLoader &loader) {
    // d1_batch_execute table function
    // Parameters: account_id, api_token, database_id, sql_statement
    TableFunction batch_execute_tf("d1_batch_execute",
                                  {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
                                  D1BatchExecuteFunc, D1BatchExecuteBind, D1BatchExecuteInitGlobal);
    loader.RegisterFunction(batch_execute_tf);

    fprintf(stderr, "D1BatchOperations: Registered d1_batch_execute function\n");
}

} // namespace duckdb
