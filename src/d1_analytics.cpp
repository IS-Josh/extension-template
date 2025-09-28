//===----------------------------------------------------------------------===//
//                         DuckDB
//
// d1_analytics.cpp
//
//
//===----------------------------------------------------------------------===//

#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/common/string_util.hpp"
#include "include/d1_client.hpp"
#include <chrono>
#include <unordered_map>

namespace duckdb {

//! Advanced analytics for D1 data
class D1Analytics {
public:
    //! Generate analytical SQL for common patterns
    static string GenerateAnalyticalSQL(const string &analysis_type, const string &table_name,
                                       const string &column_name, const string &options = "") {
        string sql;

        if (analysis_type == "distribution") {
            sql = StringUtil::Format(
                "SELECT \"%s\" as value, COUNT(*) as frequency, "
                "ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage "
                "FROM \"%s\" GROUP BY \"%s\" ORDER BY frequency DESC",
                column_name.c_str(), table_name.c_str(), column_name.c_str()
            );
        } else if (analysis_type == "summary_stats") {
            sql = StringUtil::Format(
                "SELECT "
                "COUNT(\"%s\") as count, "
                "COUNT(DISTINCT \"%s\") as distinct_count, "
                "MIN(\"%s\") as min_value, "
                "MAX(\"%s\") as max_value, "
                "AVG(CAST(\"%s\" AS REAL)) as avg_value, "
                "SUM(CASE WHEN \"%s\" IS NULL THEN 1 ELSE 0 END) as null_count "
                "FROM \"%s\"",
                column_name.c_str(), column_name.c_str(), column_name.c_str(),
                column_name.c_str(), column_name.c_str(), column_name.c_str(), table_name.c_str()
            );
        } else if (analysis_type == "time_series") {
            string time_unit = options.empty() ? "day" : options;
            sql = StringUtil::Format(
                "SELECT "
                "DATE(\"%s\") as date, "
                "COUNT(*) as daily_count, "
                "SUM(COUNT(*)) OVER (ORDER BY DATE(\"%s\")) as cumulative_count "
                "FROM \"%s\" "
                "WHERE \"%s\" IS NOT NULL "
                "GROUP BY DATE(\"%s\") "
                "ORDER BY date",
                column_name.c_str(), column_name.c_str(), table_name.c_str(),
                column_name.c_str(), column_name.c_str()
            );
        } else if (analysis_type == "correlation") {
            // Requires two columns in options (comma-separated)
            vector<string> columns;
            if (!options.empty()) {
                size_t pos = options.find(',');
                if (pos != string::npos) {
                    string col1 = options.substr(0, pos);
                    string col2 = options.substr(pos + 1);
                    StringUtil::Trim(col1);
                    StringUtil::Trim(col2);
                    columns.push_back(col1);
                    columns.push_back(col2);
                }
            }

            if (columns.size() == 2) {
                sql = StringUtil::Format(
                    "SELECT "
                    "CORR(CAST(\"%s\" AS REAL), CAST(\"%s\" AS REAL)) as correlation, "
                    "COUNT(*) as sample_size, "
                    "AVG(CAST(\"%s\" AS REAL)) as avg_%s, "
                    "AVG(CAST(\"%s\" AS REAL)) as avg_%s "
                    "FROM \"%s\" "
                    "WHERE \"%s\" IS NOT NULL AND \"%s\" IS NOT NULL",
                    columns[0].c_str(), columns[1].c_str(),
                    columns[0].c_str(), columns[0].c_str(),
                    columns[1].c_str(), columns[1].c_str(),
                    table_name.c_str(), columns[0].c_str(), columns[1].c_str()
                );
            } else {
                throw InvalidInputException("Correlation analysis requires two column names in options (comma-separated)");
            }
        } else if (analysis_type == "outliers") {
            sql = StringUtil::Format(
                "WITH stats AS ("
                "  SELECT "
                "    AVG(CAST(\"%s\" AS REAL)) as mean, "
                "    STDEV(CAST(\"%s\" AS REAL)) as stddev "
                "  FROM \"%s\" "
                "  WHERE \"%s\" IS NOT NULL"
                "), "
                "outliers AS ("
                "  SELECT *, "
                "    ABS(CAST(\"%s\" AS REAL) - stats.mean) / stats.stddev as z_score "
                "  FROM \"%s\", stats "
                "  WHERE \"%s\" IS NOT NULL"
                ") "
                "SELECT * FROM outliers WHERE z_score > 2 ORDER BY z_score DESC",
                column_name.c_str(), column_name.c_str(), table_name.c_str(), column_name.c_str(),
                column_name.c_str(), table_name.c_str(), column_name.c_str()
            );
        } else {
            throw InvalidInputException("Unsupported analysis type: %s. Supported: distribution, summary_stats, time_series, correlation, outliers", analysis_type.c_str());
        }

        return sql;
    }

    //! Generate data quality assessment SQL
    static string GenerateDataQualitySQL(const string &table_name, const vector<string> &columns) {
        if (columns.empty()) {
            throw InvalidInputException("Data quality analysis requires at least one column");
        }

        string sql = "SELECT 'Data Quality Report' as report_type, ";

        for (size_t i = 0; i < columns.size(); i++) {
            const auto &col = columns[i];
            sql += StringUtil::Format(
                "'%s' as column_%zu_name, "
                "COUNT(\"%s\") as column_%zu_total, "
                "COUNT(DISTINCT \"%s\") as column_%zu_distinct, "
                "SUM(CASE WHEN \"%s\" IS NULL THEN 1 ELSE 0 END) as column_%zu_nulls, "
                "ROUND(SUM(CASE WHEN \"%s\" IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as column_%zu_null_pct",
                col.c_str(), i, col.c_str(), i, col.c_str(), i,
                col.c_str(), i, col.c_str(), i
            );

            if (i < columns.size() - 1) {
                sql += ", ";
            }
        }

        sql += " FROM \"" + table_name + "\"";
        return sql;
    }
};

//! Analytics function for generating analytical queries
void D1AnalyticsScalar(DataChunk &args, ExpressionState &state, Vector &result) {
    if (args.ColumnCount() < 3) {
        throw InvalidInputException("d1_analytics requires: analysis_type, table_name, column_name (optional: options)");
    }

    string analysis_type = args.data[0].GetValue(0).ToString();
    string table_name = args.data[1].GetValue(0).ToString();
    string column_name = args.data[2].GetValue(0).ToString();
    string options = args.ColumnCount() > 3 ? args.data[3].GetValue(0).ToString() : "";

    try {
        string analytical_sql = D1Analytics::GenerateAnalyticalSQL(analysis_type, table_name, column_name, options);
        result.SetValue(0, Value(analytical_sql));
    } catch (const std::exception &e) {
        throw InvalidInputException("D1 analytics error: %s", e.what());
    }
}

//! Data quality assessment function
void D1DataQualityScalar(DataChunk &args, ExpressionState &state, Vector &result) {
    if (args.ColumnCount() < 2) {
        throw InvalidInputException("d1_data_quality requires: table_name, columns_csv");
    }

    string table_name = args.data[0].GetValue(0).ToString();
    string columns_csv = args.data[1].GetValue(0).ToString();

    // Parse columns
    vector<string> columns;
    size_t start = 0;
    size_t end = columns_csv.find(',');
    while (end != string::npos) {
        string col = columns_csv.substr(start, end - start);
        StringUtil::Trim(col);
        if (!col.empty()) {
            columns.push_back(col);
        }
        start = end + 1;
        end = columns_csv.find(',', start);
    }
    string last_col = columns_csv.substr(start);
    StringUtil::Trim(last_col);
    if (!last_col.empty()) {
        columns.push_back(last_col);
    }

    try {
        string quality_sql = D1Analytics::GenerateDataQualitySQL(table_name, columns);
        result.SetValue(0, Value(quality_sql));
    } catch (const std::exception &e) {
        throw InvalidInputException("D1 data quality error: %s", e.what());
    }
}

//! Performance benchmarking for D1 operations
struct D1BenchmarkResult {
    string operation_type;
    std::chrono::milliseconds duration;
    size_t rows_affected;
    bool success;
    string error_message;
};

class D1Benchmark {
private:
    static vector<D1BenchmarkResult> benchmark_results;

public:
    static void RecordBenchmark(const string &operation, std::chrono::milliseconds duration,
                               size_t rows, bool success, const string &error = "") {
        benchmark_results.push_back({operation, duration, rows, success, error});

        // Keep only last 100 results
        if (benchmark_results.size() > 100) {
            benchmark_results.erase(benchmark_results.begin());
        }
    }

    static vector<D1BenchmarkResult> GetResults() {
        return benchmark_results;
    }

    static void ClearResults() {
        benchmark_results.clear();
    }
};

// Static member definition
vector<D1BenchmarkResult> D1Benchmark::benchmark_results;

//! Benchmark table function
struct D1BenchmarkBindData : public TableFunctionData {
    bool show_summary_only;

    explicit D1BenchmarkBindData(bool summary = false) : show_summary_only(summary) {}
};

struct D1BenchmarkGlobalState : public GlobalTableFunctionState {
    vector<D1BenchmarkResult> results;
    idx_t current_row = 0;
    bool initialized = false;

    idx_t MaxThreads() const override { return 1; }
};

unique_ptr<FunctionData> D1BenchmarkBind(ClientContext &context, TableFunctionBindInput &input,
                                         vector<LogicalType> &return_types, vector<string> &names) {
    bool summary_only = false;
    if (input.inputs.size() > 0) {
        summary_only = input.inputs[0].GetValue<bool>();
    }

    if (summary_only) {
        return_types = {LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::DOUBLE, LogicalType::BIGINT};
        names = {"operation_type", "total_executions", "avg_duration_ms", "total_rows"};
    } else {
        return_types = {LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BOOLEAN, LogicalType::VARCHAR};
        names = {"operation_type", "duration_ms", "rows_affected", "success", "error_message"};
    }

    return make_uniq<D1BenchmarkBindData>(summary_only);
}

unique_ptr<GlobalTableFunctionState> D1BenchmarkInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
    return make_uniq<D1BenchmarkGlobalState>();
}

void D1BenchmarkFunc(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    auto &state = data_p.global_state->Cast<D1BenchmarkGlobalState>();
    auto &bind_data = data_p.bind_data->Cast<D1BenchmarkBindData>();

    if (!state.initialized) {
        state.results = D1Benchmark::GetResults();
        state.initialized = true;
    }

    idx_t count = 0;

    if (bind_data.show_summary_only) {
        // Aggregate results by operation type
        unordered_map<string, vector<D1BenchmarkResult>> grouped;
        for (const auto &result : state.results) {
            grouped[result.operation_type].push_back(result);
        }

        vector<pair<string, vector<D1BenchmarkResult>>> summary_data(grouped.begin(), grouped.end());

        while (count < STANDARD_VECTOR_SIZE && state.current_row < summary_data.size()) {
            auto &data = summary_data[state.current_row];

            double avg_duration = 0.0;
            size_t total_rows = 0;
            for (const auto &result : data.second) {
                avg_duration += result.duration.count();
                total_rows += result.rows_affected;
            }
            avg_duration /= data.second.size();

            output.SetValue(0, count, Value(data.first));
            output.SetValue(1, count, Value::BIGINT(static_cast<int64_t>(data.second.size())));
            output.SetValue(2, count, Value::DOUBLE(avg_duration));
            output.SetValue(3, count, Value::BIGINT(static_cast<int64_t>(total_rows)));

            state.current_row++;
            count++;
        }
    } else {
        // Return individual results
        while (count < STANDARD_VECTOR_SIZE && state.current_row < state.results.size()) {
            auto &result = state.results[state.current_row];

            output.SetValue(0, count, Value(result.operation_type));
            output.SetValue(1, count, Value::BIGINT(static_cast<int64_t>(result.duration.count())));
            output.SetValue(2, count, Value::BIGINT(static_cast<int64_t>(result.rows_affected)));
            output.SetValue(3, count, Value::BOOLEAN(result.success));
            output.SetValue(4, count, Value(result.error_message));

            state.current_row++;
            count++;
        }
    }

    output.SetCardinality(count);
}

//! Clear benchmark results
void D1BenchmarkClearScalar(DataChunk &args, ExpressionState &state, Vector &result) {
    D1Benchmark::ClearResults();
    result.SetValue(0, Value("Benchmark results cleared"));
}

//! Register analytics functions
void RegisterD1Analytics(ExtensionLoader &loader) {
    // d1_analytics scalar function
    ScalarFunction analytics_func("d1_analytics",
                                 {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
                                 LogicalType::VARCHAR, D1AnalyticsScalar);
    analytics_func.varargs = LogicalType::VARCHAR;
    loader.RegisterFunction(analytics_func);

    // d1_data_quality scalar function
    ScalarFunction quality_func("d1_data_quality",
                               {LogicalType::VARCHAR, LogicalType::VARCHAR},
                               LogicalType::VARCHAR, D1DataQualityScalar);
    loader.RegisterFunction(quality_func);

    // d1_benchmark table function
    TableFunction benchmark_tf("d1_benchmark", {}, D1BenchmarkFunc, D1BenchmarkBind, D1BenchmarkInitGlobal);
    benchmark_tf.named_parameters["summary_only"] = LogicalType::BOOLEAN;
    loader.RegisterFunction(benchmark_tf);

    // d1_benchmark_clear scalar function
    ScalarFunction clear_func("d1_benchmark_clear", {}, LogicalType::VARCHAR, D1BenchmarkClearScalar);
    loader.RegisterFunction(clear_func);

    fprintf(stderr, "D1Analytics: Registered analytics functions\n");
}

} // namespace duckdb
