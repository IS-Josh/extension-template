//===----------------------------------------------------------------------===//
//                         DuckDB
//
// d1_streaming.cpp
//
//
//===----------------------------------------------------------------------===//

#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/common/string_util.hpp"
#include "include/d1_client.hpp"
#include "include/d1_secret.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include <chrono>

namespace duckdb {

//! Streaming configuration for large result sets
struct D1StreamingConfig {
    size_t batch_size = 1000;           // Rows per batch
    size_t max_memory_mb = 100;         // Maximum memory usage in MB
    bool enable_compression = true;      // Enable result compression
    std::chrono::seconds timeout{30};   // Query timeout

    static D1StreamingConfig Default() {
        return D1StreamingConfig{};
    }

    static D1StreamingConfig LargeDataset() {
        D1StreamingConfig config;
        config.batch_size = 5000;
        config.max_memory_mb = 500;
        config.enable_compression = true;
        config.timeout = std::chrono::seconds(300); // 5 minutes
        return config;
    }
};

//! Streaming table function bind data
struct D1StreamingBindData : public TableFunctionData {
    CloudflareD1Config d1_config;
    string sql_query;
    D1StreamingConfig streaming_config;
    vector<string> column_names;
    vector<LogicalType> column_types;

    D1StreamingBindData(CloudflareD1Config cfg, string sql, D1StreamingConfig stream_cfg)
        : d1_config(std::move(cfg)), sql_query(std::move(sql)), streaming_config(stream_cfg) {}
};

//! Streaming global state with batched processing
struct D1StreamingGlobalState : public GlobalTableFunctionState {
    CloudflareD1Client client;
    CloudflareD1QueryResult current_batch;
    idx_t current_row_in_batch = 0;
    idx_t total_rows_processed = 0;
    idx_t batch_number = 0;
    bool has_more_data = true;
    bool first_batch_executed = false;
    std::chrono::steady_clock::time_point start_time;

    explicit D1StreamingGlobalState(const CloudflareD1Config &cfg)
        : client(cfg), start_time(std::chrono::steady_clock::now()) {}

    idx_t MaxThreads() const override { return 1; }
};

unique_ptr<FunctionData> D1StreamingBind(ClientContext &context, TableFunctionBindInput &input,
                                         vector<LogicalType> &return_types, vector<string> &names) {
    if (input.inputs.size() < 2) {
        throw BinderException("d1_stream requires at least: secret_name, sql_query");
    }

    // Get secret name and resolve credentials
    string secret_name = input.inputs[0].GetValue<string>();
    auto &secret_manager = SecretManager::Get(context);
    auto transaction = CatalogTransaction::GetSystemCatalogTransaction(context);
    auto secret = secret_manager.GetSecretByName(transaction, secret_name);
    if (!secret) {
        throw BinderException("Secret '%s' not found", secret_name.c_str());
    }

    // For now, use direct credentials instead of secret resolution
    // This would be enhanced in a production implementation
    CloudflareD1Config config;
    config.account_id = "test";
    config.api_token = "test";
    config.database_id = "test";

    string sql_query = input.inputs[1].GetValue<string>();

    // Parse streaming configuration
    D1StreamingConfig streaming_config = D1StreamingConfig::Default();
    if (input.inputs.size() > 2) {
        string config_str = input.inputs[2].GetValue<string>();
        if (config_str == "large") {
            streaming_config = D1StreamingConfig::LargeDataset();
        }
    }

    // Set up return types (simplified - would be determined from actual query)
    return_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR};
    names = {"column1", "column2", "column3"};

    return make_uniq<D1StreamingBindData>(std::move(config), std::move(sql_query), streaming_config);
}

unique_ptr<GlobalTableFunctionState> D1StreamingInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
    auto &bind_data = input.bind_data->Cast<D1StreamingBindData>();
    return make_uniq<D1StreamingGlobalState>(bind_data.d1_config);
}

void D1StreamingFunc(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    auto &state = data_p.global_state->Cast<D1StreamingGlobalState>();
    auto &bind_data = data_p.bind_data->Cast<D1StreamingBindData>();

    // Execute first batch if not done
    if (!state.first_batch_executed) {
        fprintf(stderr, "D1Streaming: Starting streaming query execution\n");
        fprintf(stderr, "D1Streaming: Batch size: %zu, Max memory: %zu MB\n",
               bind_data.streaming_config.batch_size, bind_data.streaming_config.max_memory_mb);

        // For demonstration, we'll simulate batched processing
        // In a real implementation, this would use D1's pagination or LIMIT/OFFSET
        string batched_query = bind_data.sql_query;
        if (batched_query.find("LIMIT") == string::npos) {
            batched_query += " LIMIT " + std::to_string(bind_data.streaming_config.batch_size);
        }

        state.current_batch = state.client.RawQuery(batched_query, {});
        state.first_batch_executed = true;
        state.batch_number = 1;

        if (!state.current_batch.success) {
            throw InvalidInputException("D1 streaming query failed: %s", state.current_batch.error.c_str());
        }

        fprintf(stderr, "D1Streaming: Batch %llu returned %zu rows\n",
               (unsigned long long)state.batch_number, state.current_batch.rows.size());
    }

    // Process current batch
    idx_t count = 0;
    while (count < STANDARD_VECTOR_SIZE &&
           state.current_row_in_batch < state.current_batch.rows.size()) {

        auto &row = state.current_batch.rows[state.current_row_in_batch];

        // Fill output chunk
        for (idx_t col_idx = 0; col_idx < output.ColumnCount(); col_idx++) {
            string cell = col_idx < row.size() ? row[col_idx] : "";
            output.SetValue(col_idx, count, Value(cell));
        }

        state.current_row_in_batch++;
        state.total_rows_processed++;
        count++;
    }

    // Check if we need to fetch next batch
    if (state.current_row_in_batch >= state.current_batch.rows.size() &&
        state.current_batch.rows.size() == bind_data.streaming_config.batch_size) {

        // Simulate next batch (in real implementation, use OFFSET or pagination)
        state.current_row_in_batch = 0;
        state.batch_number++;

        // For demo, stop after 3 batches
        if (state.batch_number <= 3) {
            fprintf(stderr, "D1Streaming: Fetching batch %llu\n", (unsigned long long)state.batch_number);
            // In real implementation, modify query with OFFSET
            state.current_batch = state.client.RawQuery(bind_data.sql_query +
                " LIMIT " + std::to_string(bind_data.streaming_config.batch_size) +
                " OFFSET " + std::to_string((state.batch_number - 1) * bind_data.streaming_config.batch_size), {});

            if (state.current_batch.success) {
                fprintf(stderr, "D1Streaming: Batch %llu returned %zu rows\n",
                       (unsigned long long)state.batch_number, state.current_batch.rows.size());
            }
        } else {
            state.has_more_data = false;
        }
    }

    // Report progress periodically
    if (state.total_rows_processed % 1000 == 0 && state.total_rows_processed > 0) {
        auto elapsed = std::chrono::steady_clock::now() - state.start_time;
        auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(elapsed);
        fprintf(stderr, "D1Streaming: Processed %llu rows in %lld ms\n",
               (unsigned long long)state.total_rows_processed, (long long)elapsed_ms.count());
    }

    output.SetCardinality(count);
}

//! Advanced streaming with custom configuration
void D1StreamingConfigScalar(DataChunk &args, ExpressionState &state, Vector &result) {
    if (args.ColumnCount() < 4) {
        throw InvalidInputException("d1_streaming_config requires: batch_size, max_memory_mb, enable_compression, timeout_seconds");
    }

    int64_t batch_size = args.data[0].GetValue(0).GetValue<int64_t>();
    int64_t max_memory_mb = args.data[1].GetValue(0).GetValue<int64_t>();
    bool enable_compression = args.data[2].GetValue(0).GetValue<bool>();
    int64_t timeout_seconds = args.data[3].GetValue(0).GetValue<int64_t>();

    string config_json = StringUtil::Format(
        "{\"batch_size\":%lld,\"max_memory_mb\":%lld,\"enable_compression\":%s,\"timeout_seconds\":%lld}",
        (long long)batch_size, (long long)max_memory_mb,
        enable_compression ? "true" : "false", (long long)timeout_seconds
    );

    result.SetValue(0, Value(config_json));
}

//! Register streaming functions
void RegisterD1Streaming(ExtensionLoader &loader) {
    // d1_stream table function
    TableFunction stream_tf("d1_stream",
                           {LogicalType::VARCHAR, LogicalType::VARCHAR},
                           D1StreamingFunc, D1StreamingBind, D1StreamingInitGlobal);
    stream_tf.varargs = LogicalType::VARCHAR;
    loader.RegisterFunction(stream_tf);

    // d1_streaming_config scalar function
    ScalarFunction config_func("d1_streaming_config",
                              {LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BOOLEAN, LogicalType::BIGINT},
                              LogicalType::VARCHAR, D1StreamingConfigScalar);
    loader.RegisterFunction(config_func);

    fprintf(stderr, "D1Streaming: Registered streaming functions\n");
}

} // namespace duckdb
