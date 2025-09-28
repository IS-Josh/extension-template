//===----------------------------------------------------------------------===//
//                         DuckDB
//
// d1_pipeline.cpp
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
#include <queue>

namespace duckdb {

//! ETL Pipeline configuration
struct D1PipelineConfig {
    string source_table;
    string target_table;
    string transformation_sql;
    size_t batch_size = 1000;
    bool enable_validation = true;
    bool enable_logging = true;
    std::chrono::seconds retry_delay{5};
    size_t max_retries = 3;
};

//! Pipeline execution status
enum class PipelineStatus {
    PENDING,
    RUNNING,
    COMPLETED,
    FAILED,
    CANCELLED
};

//! Pipeline job
struct D1PipelineJob {
    string job_id;
    D1PipelineConfig config;
    PipelineStatus status;
    std::chrono::steady_clock::time_point start_time;
    std::chrono::steady_clock::time_point end_time;
    size_t rows_processed = 0;
    size_t rows_failed = 0;
    string error_message;
    vector<string> log_messages;

    D1PipelineJob(string id, D1PipelineConfig cfg)
        : job_id(std::move(id)), config(std::move(cfg)), status(PipelineStatus::PENDING) {}
};

//! Pipeline manager for ETL operations
class D1PipelineManager {
private:
    static D1PipelineManager instance;
    mutable mutex pipeline_mutex;

    unordered_map<string, unique_ptr<D1PipelineJob>> active_jobs;
    queue<string> job_queue;

    D1PipelineManager() = default;

public:
    static D1PipelineManager& GetInstance() {
        return instance;
    }

    //! Create a new pipeline job
    string CreateJob(const D1PipelineConfig &config) {
        lock_guard<mutex> lock(pipeline_mutex);

        // Generate unique job ID
        auto now = std::chrono::system_clock::now();
        auto timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();
        string job_id = "d1_pipeline_" + std::to_string(timestamp);

        auto job = make_uniq<D1PipelineJob>(job_id, config);
        job->log_messages.push_back("Pipeline job created: " + job_id);

        active_jobs[job_id] = std::move(job);
        job_queue.push(job_id);

        fprintf(stderr, "D1Pipeline: Created job %s\n", job_id.c_str());
        return job_id;
    }

    //! Execute a pipeline job
    bool ExecuteJob(const string &job_id, CloudflareD1Client &client) {
        lock_guard<mutex> lock(pipeline_mutex);

        auto it = active_jobs.find(job_id);
        if (it == active_jobs.end()) {
            return false;
        }

        auto &job = *it->second;
        job.status = PipelineStatus::RUNNING;
        job.start_time = std::chrono::steady_clock::now();
        job.log_messages.push_back("Starting pipeline execution");

        fprintf(stderr, "D1Pipeline: Executing job %s\n", job_id.c_str());

        try {
            // Step 1: Extract data from source
            string extract_sql = "SELECT * FROM \"" + job.config.source_table + "\"";
            if (job.config.batch_size > 0) {
                extract_sql += " LIMIT " + std::to_string(job.config.batch_size);
            }

            job.log_messages.push_back("Extracting data: " + extract_sql);
            auto extract_result = client.RawQuery(extract_sql, {});

            if (!extract_result.success) {
                job.status = PipelineStatus::FAILED;
                job.error_message = "Extract failed: " + extract_result.error;
                job.log_messages.push_back("Extract failed: " + extract_result.error);
                return false;
            }

            job.log_messages.push_back("Extracted " + std::to_string(extract_result.rows.size()) + " rows");

            // Step 2: Transform data (if transformation SQL provided)
            if (!job.config.transformation_sql.empty()) {
                job.log_messages.push_back("Applying transformation: " + job.config.transformation_sql);
                // In a real implementation, this would apply the transformation
                // For now, we'll just log it
            }

            // Step 3: Load data into target
            if (!job.config.target_table.empty()) {
                // Generate INSERT statements (simplified)
                string load_sql = "INSERT INTO \"" + job.config.target_table + "\" SELECT * FROM \"" + job.config.source_table + "\"";
                if (!job.config.transformation_sql.empty()) {
                    load_sql = "INSERT INTO \"" + job.config.target_table + "\" " + job.config.transformation_sql;
                }

                job.log_messages.push_back("Loading data: " + load_sql);
                auto load_result = client.ObjectQuery(load_sql, {});

                if (!load_result.success) {
                    job.status = PipelineStatus::FAILED;
                    job.error_message = "Load failed: " + load_result.error;
                    job.log_messages.push_back("Load failed: " + load_result.error);
                    return false;
                }

                job.rows_processed = load_result.changes;
                job.log_messages.push_back("Loaded " + std::to_string(job.rows_processed) + " rows");
            }

            // Step 4: Validation (if enabled)
            if (job.config.enable_validation) {
                string validation_sql = "SELECT COUNT(*) FROM \"" + job.config.target_table + "\"";
                auto validation_result = client.RawQuery(validation_sql, {});

                if (validation_result.success && !validation_result.rows.empty()) {
                    job.log_messages.push_back("Validation: " + validation_result.rows[0][0] + " rows in target table");
                }
            }

            job.status = PipelineStatus::COMPLETED;
            job.end_time = std::chrono::steady_clock::now();
            job.log_messages.push_back("Pipeline completed successfully");

            fprintf(stderr, "D1Pipeline: Job %s completed successfully\n", job_id.c_str());
            return true;

        } catch (const std::exception &e) {
            job.status = PipelineStatus::FAILED;
            job.error_message = string("Exception: ") + e.what();
            job.end_time = std::chrono::steady_clock::now();
            job.log_messages.push_back("Pipeline failed with exception: " + string(e.what()));

            fprintf(stderr, "D1Pipeline: Job %s failed: %s\n", job_id.c_str(), e.what());
            return false;
        }
    }

    //! Get job status
    PipelineStatus GetJobStatus(const string &job_id) const {
        lock_guard<mutex> lock(pipeline_mutex);

        auto it = active_jobs.find(job_id);
        return it != active_jobs.end() ? it->second->status : PipelineStatus::FAILED;
    }

    //! Get job details
    unique_ptr<D1PipelineJob> GetJobDetails(const string &job_id) const {
        lock_guard<mutex> lock(pipeline_mutex);

        auto it = active_jobs.find(job_id);
        if (it != active_jobs.end()) {
            // Return a copy
            auto copy = make_uniq<D1PipelineJob>(it->second->job_id, it->second->config);
            copy->status = it->second->status;
            copy->start_time = it->second->start_time;
            copy->end_time = it->second->end_time;
            copy->rows_processed = it->second->rows_processed;
            copy->rows_failed = it->second->rows_failed;
            copy->error_message = it->second->error_message;
            copy->log_messages = it->second->log_messages;
            return copy;
        }
        return nullptr;
    }

    //! Get all active jobs
    vector<string> GetActiveJobs() const {
        lock_guard<mutex> lock(pipeline_mutex);

        vector<string> jobs;
        for (const auto &pair : active_jobs) {
            jobs.push_back(pair.first);
        }
        return jobs;
    }

    //! Cancel a job
    bool CancelJob(const string &job_id) {
        lock_guard<mutex> lock(pipeline_mutex);

        auto it = active_jobs.find(job_id);
        if (it != active_jobs.end() && it->second->status == PipelineStatus::RUNNING) {
            it->second->status = PipelineStatus::CANCELLED;
            it->second->end_time = std::chrono::steady_clock::now();
            it->second->log_messages.push_back("Pipeline cancelled by user");
            return true;
        }
        return false;
    }

    //! Clean up completed jobs
    void CleanupJobs() {
        lock_guard<mutex> lock(pipeline_mutex);

        auto now = std::chrono::steady_clock::now();
        for (auto it = active_jobs.begin(); it != active_jobs.end();) {
            auto &job = *it->second;
            if ((job.status == PipelineStatus::COMPLETED || job.status == PipelineStatus::FAILED) &&
                (now - job.end_time) > std::chrono::hours(1)) {
                it = active_jobs.erase(it);
            } else {
                ++it;
            }
        }
    }
};

// Static instance
D1PipelineManager D1PipelineManager::instance;

//! Create pipeline job function
void D1PipelineCreateScalar(DataChunk &args, ExpressionState &state, Vector &result) {
    if (args.ColumnCount() < 2) {
        throw InvalidInputException("d1_pipeline_create requires: source_table, target_table (optional: transformation_sql, batch_size)");
    }

    D1PipelineConfig config;
    config.source_table = args.data[0].GetValue(0).ToString();
    config.target_table = args.data[1].GetValue(0).ToString();

    if (args.ColumnCount() > 2) {
        config.transformation_sql = args.data[2].GetValue(0).ToString();
    }
    if (args.ColumnCount() > 3) {
        config.batch_size = static_cast<size_t>(args.data[3].GetValue(0).GetValue<int64_t>());
    }

    string job_id = D1PipelineManager::GetInstance().CreateJob(config);
    result.SetValue(0, Value(job_id));
}

//! Execute pipeline job function
void D1PipelineExecuteScalar(DataChunk &args, ExpressionState &state, Vector &result) {
    if (args.ColumnCount() < 4) {
        throw InvalidInputException("d1_pipeline_execute requires: job_id, account_id, api_token, database_id");
    }

    string job_id = args.data[0].GetValue(0).ToString();

    CloudflareD1Config config;
    config.account_id = args.data[1].GetValue(0).ToString();
    config.api_token = args.data[2].GetValue(0).ToString();
    config.database_id = args.data[3].GetValue(0).ToString();

    CloudflareD1Client client(config);
    bool success = D1PipelineManager::GetInstance().ExecuteJob(job_id, client);

    result.SetValue(0, Value(success ? "SUCCESS" : "FAILED"));
}

//! Pipeline status table function
struct D1PipelineStatusBindData : public TableFunctionData {
    string job_id_filter;

    explicit D1PipelineStatusBindData(string filter = "") : job_id_filter(std::move(filter)) {}
};

struct D1PipelineStatusGlobalState : public GlobalTableFunctionState {
    vector<string> job_ids;
    idx_t current_row = 0;
    bool initialized = false;

    idx_t MaxThreads() const override { return 1; }
};

unique_ptr<FunctionData> D1PipelineStatusBind(ClientContext &context, TableFunctionBindInput &input,
                                              vector<LogicalType> &return_types, vector<string> &names) {
    string job_filter = "";
    if (input.inputs.size() > 0) {
        job_filter = input.inputs[0].GetValue<string>();
    }

    return_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
                   LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::VARCHAR};
    names = {"job_id", "status", "source_table", "rows_processed", "rows_failed", "error_message"};

    return make_uniq<D1PipelineStatusBindData>(job_filter);
}

unique_ptr<GlobalTableFunctionState> D1PipelineStatusInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
    return make_uniq<D1PipelineStatusGlobalState>();
}

void D1PipelineStatusFunc(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    auto &state = data_p.global_state->Cast<D1PipelineStatusGlobalState>();
    auto &bind_data = data_p.bind_data->Cast<D1PipelineStatusBindData>();

    if (!state.initialized) {
        state.job_ids = D1PipelineManager::GetInstance().GetActiveJobs();
        state.initialized = true;
    }

    idx_t count = 0;
    while (count < STANDARD_VECTOR_SIZE && state.current_row < state.job_ids.size()) {
        string job_id = state.job_ids[state.current_row];

        // Apply filter if specified
        if (!bind_data.job_id_filter.empty() && job_id.find(bind_data.job_id_filter) == string::npos) {
            state.current_row++;
            continue;
        }

        auto job_details = D1PipelineManager::GetInstance().GetJobDetails(job_id);
        if (job_details) {
            string status_str;
            switch (job_details->status) {
                case PipelineStatus::PENDING: status_str = "PENDING"; break;
                case PipelineStatus::RUNNING: status_str = "RUNNING"; break;
                case PipelineStatus::COMPLETED: status_str = "COMPLETED"; break;
                case PipelineStatus::FAILED: status_str = "FAILED"; break;
                case PipelineStatus::CANCELLED: status_str = "CANCELLED"; break;
            }

            output.SetValue(0, count, Value(job_details->job_id));
            output.SetValue(1, count, Value(status_str));
            output.SetValue(2, count, Value(job_details->config.source_table));
            output.SetValue(3, count, Value::BIGINT(static_cast<int64_t>(job_details->rows_processed)));
            output.SetValue(4, count, Value::BIGINT(static_cast<int64_t>(job_details->rows_failed)));
            output.SetValue(5, count, Value(job_details->error_message));
        }

        state.current_row++;
        count++;
    }

    output.SetCardinality(count);
}

//! Register pipeline functions
void RegisterD1Pipeline(ExtensionLoader &loader) {
    // d1_pipeline_create scalar function
    ScalarFunction create_func("d1_pipeline_create",
                              {LogicalType::VARCHAR, LogicalType::VARCHAR},
                              LogicalType::VARCHAR, D1PipelineCreateScalar);
    create_func.varargs = LogicalType::ANY;
    loader.RegisterFunction(create_func);

    // d1_pipeline_execute scalar function
    ScalarFunction execute_func("d1_pipeline_execute",
                               {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
                               LogicalType::VARCHAR, D1PipelineExecuteScalar);
    loader.RegisterFunction(execute_func);

    // d1_pipeline_status table function
    TableFunction status_tf("d1_pipeline_status", {}, D1PipelineStatusFunc,
                           D1PipelineStatusBind, D1PipelineStatusInitGlobal);
    status_tf.varargs = LogicalType::VARCHAR;
    loader.RegisterFunction(status_tf);

    fprintf(stderr, "D1Pipeline: Registered pipeline functions\n");
}

} // namespace duckdb
