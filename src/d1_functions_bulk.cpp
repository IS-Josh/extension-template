#include "include/d1_functions.hpp"
#include "include/d1_client.hpp"

#include "duckdb.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

// Helper to build JSON payload for JSON1-based bulk ops on D1
static int64_t D1BulkExecuteJSON(const string &sql, const string &json_payload, const CloudflareD1Config &cfg) {
    CloudflareD1Client client(cfg);
    vector<CloudflareD1QueryParam> params;
    // For /query, params is an object; we name it 'payload'
    params.push_back({"payload", json_payload});
    auto res = client.ObjectQuery(sql, params);
    if (!res.success) {
        throw InvalidInputException("D1 bulk operation failed: %s", res.error.c_str());
    }
    return res.changes;
}

// d1_bulk_insert(table, columns_csv, json_rows, account_id, api_token, database_id)
static void D1BulkInsertScalar(DataChunk &args, ExpressionState &state, Vector &result) {
    auto table = args.data[0].GetValue(0).ToString();
    auto columns_csv = args.data[1].GetValue(0).ToString();
    auto json_rows = args.data[2].GetValue(0).ToString();
    CloudflareD1Config cfg;
    cfg.account_id = args.data[3].GetValue(0).ToString();
    cfg.api_token = args.data[4].GetValue(0).ToString();
    cfg.database_id = args.data[5].GetValue(0).ToString();
    // Build portable CTE using json_extract on each column
    vector<string> cols = StringUtil::Split(columns_csv, ',');
    for (auto &c : cols) {
        StringUtil::Trim(c);
    }
    string select_cols; // e.g. json_extract(e.value,'$.name') AS name, ...
    for (idx_t i = 0; i < cols.size(); i++) {
        if (i) select_cols += ", ";
        select_cols += "json_extract(e.value, '$." + cols[i] + "') AS " + cols[i];
    }
    string sql = "WITH data AS (SELECT " + select_cols + " FROM json_each(?) e) "
                 "INSERT INTO \"" + table + "\"(" + columns_csv + ") "
                 "SELECT " + StringUtil::Join(cols, ", ") + " FROM data";
    auto changes = D1BulkExecuteJSON(sql, json_rows, cfg);
    result.SetValue(0, Value::BIGINT(changes));
}

// d1_bulk_update(table, key_column, columns_csv, json_rows, account_id, api_token, database_id)
static void D1BulkUpdateScalar(DataChunk &args, ExpressionState &state, Vector &result) {
    auto table = args.data[0].GetValue(0).ToString();
    auto key_col = args.data[1].GetValue(0).ToString();
    auto columns_csv = args.data[2].GetValue(0).ToString();
    auto json_rows = args.data[3].GetValue(0).ToString();
    CloudflareD1Config cfg;
    cfg.account_id = args.data[4].GetValue(0).ToString();
    cfg.api_token = args.data[5].GetValue(0).ToString();
    cfg.database_id = args.data[6].GetValue(0).ToString();

    vector<string> cols = StringUtil::Split(columns_csv, ',');
    for (auto &c : cols) {
        StringUtil::Trim(c);
    }
    // Build CTE extracting all columns from JSON rows
    string cte_select = "SELECT " + ("json_extract(e.value, '$." + key_col + "') AS " + key_col);
    for (auto &c : cols) {
        cte_select += ", json_extract(e.value, '$." + c + "') AS " + c;
    }
    string with_sql = "WITH data AS (" + cte_select + " FROM json_each(?) e) ";
    string set_clause;
    for (idx_t i = 0; i < cols.size(); i++) {
        if (i) set_clause += ", ";
        set_clause += "\"" + cols[i] + "\" = data." + cols[i];
    }
    string sql = with_sql + "UPDATE \"" + table + "\" SET " + set_clause + " FROM data WHERE \"" + table + "\".\"" + key_col + "\" = data." + key_col;
    auto changes = D1BulkExecuteJSON(sql, json_rows, cfg);
    result.SetValue(0, Value::BIGINT(changes));
}

// d1_bulk_delete(table, key_column, json_ids, account_id, api_token, database_id)
static void D1BulkDeleteScalar(DataChunk &args, ExpressionState &state, Vector &result) {
    auto table = args.data[0].GetValue(0).ToString();
    auto key_col = args.data[1].GetValue(0).ToString();
    auto json_ids = args.data[2].GetValue(0).ToString();
    CloudflareD1Config cfg;
    cfg.account_id = args.data[3].GetValue(0).ToString();
    cfg.api_token = args.data[4].GetValue(0).ToString();
    cfg.database_id = args.data[5].GetValue(0).ToString();
    string sql = "DELETE FROM \"" + table + "\" WHERE \"" + table + "\".\"" + key_col + "\" IN (SELECT e.value FROM json_each(?))";
    auto changes = D1BulkExecuteJSON(sql, json_ids, cfg);
    result.SetValue(0, Value::BIGINT(changes));
}

void RegisterD1BulkFunctions(ExtensionLoader &loader) {
    // All bulk ops return BIGINT (affected rows)
    loader.RegisterFunction(ScalarFunction("d1_bulk_insert",
        {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
        LogicalType::BIGINT, D1BulkInsertScalar));

    loader.RegisterFunction(ScalarFunction("d1_bulk_update",
        {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
        LogicalType::BIGINT, D1BulkUpdateScalar));

    loader.RegisterFunction(ScalarFunction("d1_bulk_delete",
        {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
        LogicalType::BIGINT, D1BulkDeleteScalar));
}

} // namespace duckdb


