//===----------------------------------------------------------------------===//
//                         DuckDB
//
// d1_predicate_pushdown.cpp
//
//
//===----------------------------------------------------------------------===//

#include "duckdb.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "include/d1_client.hpp"

namespace duckdb {

//! Simplified D1 Query Optimization
//! Provides basic query optimization capabilities for D1
class D1QueryOptimizer {
public:
    //! Generate optimized D1 query with column selection and basic WHERE clause
    static string GenerateOptimizedQuery(const string &table_name, const vector<string> &selected_columns,
                                        const string &where_clause = "", const string &limit_clause = "") {
        string optimized_query;

        if (selected_columns.empty()) {
            optimized_query = "SELECT * FROM \"" + table_name + "\"";
        } else {
            // Use specific columns for better performance
            string column_list = "\"" + StringUtil::Join(selected_columns, "\", \"") + "\"";
            optimized_query = "SELECT " + column_list + " FROM \"" + table_name + "\"";
        }

        if (!where_clause.empty()) {
            optimized_query += " WHERE " + where_clause;
        }

        if (!limit_clause.empty()) {
            optimized_query += " " + limit_clause;
        }

        return optimized_query;
    }

    //! Generate simple WHERE clause from basic conditions
    static string GenerateSimpleWhere(const string &column, const string &operator_str, const string &value) {
        return "\"" + column + "\" " + operator_str + " " + EscapeValue(value);
    }

    //! Combine multiple WHERE conditions
    static string CombineConditions(const vector<string> &conditions, const string &connector = "AND") {
        if (conditions.empty()) {
            return "";
        }
        return "(" + StringUtil::Join(conditions, " " + connector + " ") + ")";
    }

private:
    static string EscapeValue(const string &value) {
        // Simple value escaping for SQL
        if (value == "NULL" || value == "null") {
            return "NULL";
        }

        // Check if it's a number
        bool is_number = true;
        for (char c : value) {
            if (!std::isdigit(c) && c != '.' && c != '-') {
                is_number = false;
                break;
            }
        }

        if (is_number) {
            return value;
        } else {
            // Escape string value
            return "'" + StringUtil::Replace(value, "'", "''") + "'";
        }
    }
};

//! Simple query optimization function
void D1QueryOptimizerScalar(DataChunk &args, ExpressionState &state, Vector &result) {
    if (args.ColumnCount() < 2) {
        throw InvalidInputException("d1_optimize_query requires: table_name, columns (optional: where_clause, limit_clause)");
    }

    string table_name = args.data[0].GetValue(0).ToString();
    string columns_str = args.data[1].GetValue(0).ToString();

    // Parse columns
    vector<string> selected_columns;
    if (!columns_str.empty() && columns_str != "*") {
        // Simple comma-separated parsing
        size_t start = 0;
        size_t end = columns_str.find(',');
        while (end != string::npos) {
            string col = columns_str.substr(start, end - start);
            StringUtil::Trim(col);
            if (!col.empty()) {
                selected_columns.push_back(col);
            }
            start = end + 1;
            end = columns_str.find(',', start);
        }
        string last_col = columns_str.substr(start);
        StringUtil::Trim(last_col);
        if (!last_col.empty()) {
            selected_columns.push_back(last_col);
        }
    }

    string where_clause = "";
    string limit_clause = "";

    if (args.ColumnCount() > 2) {
        where_clause = args.data[2].GetValue(0).ToString();
    }
    if (args.ColumnCount() > 3) {
        limit_clause = args.data[3].GetValue(0).ToString();
    }

    string optimized_sql = D1QueryOptimizer::GenerateOptimizedQuery(table_name, selected_columns, where_clause, limit_clause);
    result.SetValue(0, Value(optimized_sql));
}

//! Register predicate pushdown functions
void RegisterD1PredicatePushdown(ExtensionLoader &loader) {
    // d1_optimize_query scalar function
    ScalarFunction optimize_query_func("d1_optimize_query",
                                      {LogicalType::VARCHAR, LogicalType::VARCHAR},
                                      LogicalType::VARCHAR, D1QueryOptimizerScalar);
    optimize_query_func.varargs = LogicalType::VARCHAR;
    loader.RegisterFunction(optimize_query_func);

    fprintf(stderr, "D1PredicatePushdown: Registered d1_optimize_query function\n");
}

} // namespace duckdb
