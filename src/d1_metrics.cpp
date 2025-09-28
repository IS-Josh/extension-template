//===----------------------------------------------------------------------===//
//                         DuckDB
//
// d1_metrics.cpp
//
//
//===----------------------------------------------------------------------===//

#include "duckdb.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include <chrono>
#include <unordered_map>

namespace duckdb {

//! Performance metrics for D1 operations
struct D1QueryMetrics {
    string query_type;
    std::chrono::milliseconds total_duration{0};
    std::chrono::milliseconds avg_duration{0};
    size_t execution_count = 0;
    size_t success_count = 0;
    size_t error_count = 0;
    std::chrono::steady_clock::time_point last_execution;

    void RecordExecution(std::chrono::milliseconds duration, bool success) {
        execution_count++;
        if (success) {
            success_count++;
        } else {
            error_count++;
        }

        total_duration += duration;
        avg_duration = std::chrono::milliseconds(total_duration.count() / execution_count);
        last_execution = std::chrono::steady_clock::now();
    }

    double GetSuccessRate() const {
        return execution_count > 0 ? (double)success_count / execution_count * 100.0 : 0.0;
    }
};

//! D1 Performance Metrics Manager
class D1MetricsManager {
private:
    static D1MetricsManager instance;
    mutable mutex metrics_mutex;

    // Metrics by query type (SELECT, INSERT, UPDATE, DELETE, etc.)
    unordered_map<string, D1QueryMetrics> query_metrics;

    // Global metrics
    std::chrono::steady_clock::time_point start_time;
    size_t total_queries = 0;
    size_t total_errors = 0;
    std::chrono::milliseconds total_query_time{0};

    D1MetricsManager() : start_time(std::chrono::steady_clock::now()) {}

public:
    static D1MetricsManager& GetInstance() {
        return instance;
    }

    //! Record a query execution
    void RecordQuery(const string &sql, std::chrono::milliseconds duration, bool success) {
        lock_guard<mutex> lock(metrics_mutex);

        // Determine query type
        string query_type = ExtractQueryType(sql);

        // Update query-specific metrics
        query_metrics[query_type].RecordExecution(duration, success);

        // Update global metrics
        total_queries++;
        total_query_time += duration;
        if (!success) {
            total_errors++;
        }

        fprintf(stderr, "D1Metrics: Recorded %s query (duration: %lldms, success: %s)\n",
               query_type.c_str(), duration.count(), success ? "true" : "false");
    }

    //! Get metrics for a specific query type
    D1QueryMetrics GetQueryMetrics(const string &query_type) const {
        lock_guard<mutex> lock(metrics_mutex);

        auto it = query_metrics.find(query_type);
        return it != query_metrics.end() ? it->second : D1QueryMetrics{};
    }

    //! Get all metrics
    unordered_map<string, D1QueryMetrics> GetAllMetrics() const {
        lock_guard<mutex> lock(metrics_mutex);
        return query_metrics;
    }

    //! Get global statistics
    struct GlobalStats {
        size_t total_queries = 0;
        size_t total_errors = 0;
        double error_rate = 0.0;
        std::chrono::milliseconds avg_query_time{0};
        std::chrono::milliseconds uptime{0};
    };

    GlobalStats GetGlobalStats() const {
        lock_guard<mutex> lock(metrics_mutex);

        GlobalStats stats;
        stats.total_queries = total_queries;
        stats.total_errors = total_errors;
        stats.error_rate = total_queries > 0 ? (double)total_errors / total_queries * 100.0 : 0.0;
        stats.avg_query_time = total_queries > 0 ?
            std::chrono::milliseconds(total_query_time.count() / total_queries) :
            std::chrono::milliseconds(0);

        auto now = std::chrono::steady_clock::now();
        stats.uptime = std::chrono::duration_cast<std::chrono::milliseconds>(now - start_time);

        return stats;
    }

    //! Reset all metrics
    void Reset() {
        lock_guard<mutex> lock(metrics_mutex);

        query_metrics.clear();
        total_queries = 0;
        total_errors = 0;
        total_query_time = std::chrono::milliseconds(0);
        start_time = std::chrono::steady_clock::now();

        fprintf(stderr, "D1Metrics: Reset all metrics\n");
    }

private:
    string ExtractQueryType(const string &sql) const {
        string upper_sql = sql;
        std::transform(upper_sql.begin(), upper_sql.end(), upper_sql.begin(), ::toupper);

        if (upper_sql.find("SELECT") == 0) return "SELECT";
        if (upper_sql.find("INSERT") == 0) return "INSERT";
        if (upper_sql.find("UPDATE") == 0) return "UPDATE";
        if (upper_sql.find("DELETE") == 0) return "DELETE";
        if (upper_sql.find("CREATE") == 0) return "CREATE";
        if (upper_sql.find("DROP") == 0) return "DROP";
        if (upper_sql.find("ALTER") == 0) return "ALTER";
        if (upper_sql.find("PRAGMA") == 0) return "PRAGMA";

        return "OTHER";
    }
};

// Static instance
D1MetricsManager D1MetricsManager::instance;

//! Metrics table function bind data
struct D1MetricsBindData : public TableFunctionData {
    bool show_global_only;

    explicit D1MetricsBindData(bool global_only = false) : show_global_only(global_only) {}
};

struct D1MetricsGlobalState : public GlobalTableFunctionState {
    vector<pair<string, D1QueryMetrics>> metrics_data;
    D1MetricsManager::GlobalStats global_stats;
    idx_t current_row = 0;
    bool initialized = false;

    idx_t MaxThreads() const override { return 1; }
};

unique_ptr<FunctionData> D1MetricsBind(ClientContext &context, TableFunctionBindInput &input,
                                      vector<LogicalType> &return_types, vector<string> &names) {
    // Check if global-only flag is provided
    bool global_only = false;
    if (input.inputs.size() > 0) {
        global_only = input.inputs[0].GetValue<bool>();
    }

    if (global_only) {
        // Global metrics only
        return_types = {LogicalType::VARCHAR, LogicalType::BIGINT};
        names = {"metric_name", "value"};
    } else {
        // Per-query-type metrics
        return_types = {
            LogicalType::VARCHAR,    // query_type
            LogicalType::BIGINT,     // execution_count
            LogicalType::BIGINT,     // success_count
            LogicalType::BIGINT,     // error_count
            LogicalType::DOUBLE,     // success_rate
            LogicalType::BIGINT,     // avg_duration_ms
            LogicalType::BIGINT      // total_duration_ms
        };
        names = {"query_type", "execution_count", "success_count", "error_count",
                "success_rate", "avg_duration_ms", "total_duration_ms"};
    }

    return make_uniq<D1MetricsBindData>(global_only);
}

unique_ptr<GlobalTableFunctionState> D1MetricsInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
    return make_uniq<D1MetricsGlobalState>();
}

void D1MetricsFunc(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    auto &state = data_p.global_state->Cast<D1MetricsGlobalState>();
    auto &bind_data = data_p.bind_data->Cast<D1MetricsBindData>();

    if (!state.initialized) {
        if (bind_data.show_global_only) {
            state.global_stats = D1MetricsManager::GetInstance().GetGlobalStats();
        } else {
            auto all_metrics = D1MetricsManager::GetInstance().GetAllMetrics();
            for (const auto &pair : all_metrics) {
                state.metrics_data.emplace_back(pair.first, pair.second);
            }
        }
        state.initialized = true;
    }

    idx_t count = 0;

    if (bind_data.show_global_only) {
        // Return global statistics
        vector<pair<string, int64_t>> global_data = {
            {"total_queries", static_cast<int64_t>(state.global_stats.total_queries)},
            {"total_errors", static_cast<int64_t>(state.global_stats.total_errors)},
            {"avg_query_time_ms", static_cast<int64_t>(state.global_stats.avg_query_time.count())},
            {"uptime_ms", static_cast<int64_t>(state.global_stats.uptime.count())}
        };

        while (count < STANDARD_VECTOR_SIZE && state.current_row < global_data.size()) {
            auto &data = global_data[state.current_row];
            output.SetValue(0, count, Value(data.first));
            output.SetValue(1, count, Value::BIGINT(data.second));

            state.current_row++;
            count++;
        }
    } else {
        // Return per-query-type metrics
        while (count < STANDARD_VECTOR_SIZE && state.current_row < state.metrics_data.size()) {
            auto &data = state.metrics_data[state.current_row];

            output.SetValue(0, count, Value(data.first)); // query_type
            output.SetValue(1, count, Value::BIGINT(static_cast<int64_t>(data.second.execution_count)));
            output.SetValue(2, count, Value::BIGINT(static_cast<int64_t>(data.second.success_count)));
            output.SetValue(3, count, Value::BIGINT(static_cast<int64_t>(data.second.error_count)));
            output.SetValue(4, count, Value::DOUBLE(data.second.GetSuccessRate()));
            output.SetValue(5, count, Value::BIGINT(static_cast<int64_t>(data.second.avg_duration.count())));
            output.SetValue(6, count, Value::BIGINT(static_cast<int64_t>(data.second.total_duration.count())));

            state.current_row++;
            count++;
        }
    }

    output.SetCardinality(count);
}

//! Metrics reset function
void D1MetricsResetScalar(DataChunk &args, ExpressionState &state, Vector &result) {
    D1MetricsManager::GetInstance().Reset();
    result.SetValue(0, Value("Metrics reset successfully"));
}

//! Register metrics functions
void RegisterD1Metrics(ExtensionLoader &loader) {
    // d1_metrics table function
    TableFunction metrics_tf("d1_metrics", {}, D1MetricsFunc, D1MetricsBind, D1MetricsInitGlobal);
    metrics_tf.named_parameters["global_only"] = LogicalType::BOOLEAN;
    loader.RegisterFunction(metrics_tf);

    // d1_metrics_reset scalar function
    ScalarFunction reset_func("d1_metrics_reset", {}, LogicalType::VARCHAR, D1MetricsResetScalar);
    loader.RegisterFunction(reset_func);

    fprintf(stderr, "D1Metrics: Registered metrics functions\n");
}

//! Helper class to automatically record query metrics
class D1QueryTimer {
private:
    std::chrono::steady_clock::time_point start_time;
    string sql;

public:
    explicit D1QueryTimer(const string &query) : sql(query), start_time(std::chrono::steady_clock::now()) {}

    ~D1QueryTimer() {
        // This would be called when the timer goes out of scope
        // In practice, you'd call RecordResult explicitly
    }

    void RecordResult(bool success) {
        auto end_time = std::chrono::steady_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
        D1MetricsManager::GetInstance().RecordQuery(sql, duration, success);
    }
};

} // namespace duckdb
