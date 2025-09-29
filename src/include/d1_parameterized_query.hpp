//===----------------------------------------------------------------------===//
//                         DuckDB
//
// d1_parameterized_query.hpp
//
// Secure parameterized query builder for D1 operations
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "d1_client.hpp"
#include <vector>
#include <string>

namespace duckdb {

//! Helper class for building secure parameterized D1 queries
class D1ParameterizedQuery {
private:
    string sql;
    vector<CloudflareD1QueryParam> parameters;
    idx_t param_counter = 0;

public:
    D1ParameterizedQuery() = default;

    //! Start building a query with base SQL
    D1ParameterizedQuery(const string &base_sql) : sql(base_sql) {}

    //! Add SQL fragment
    void AppendSQL(const string &fragment) {
        sql += fragment;
    }

    //! Add a parameter placeholder and register the value
    string AddParameter(const Value &value) {
        param_counter++;
        string param_name = "param" + std::to_string(param_counter);

        // Convert DuckDB Value to string for D1 API
        string param_value;
        if (value.IsNull()) {
            param_value = ""; // D1 will handle NULL appropriately
        } else {
            param_value = value.ToString();
        }

        parameters.push_back({param_name, param_value});

        // Return positional placeholder
        return "?";
    }

    //! Add a named parameter (alternative approach)
    string AddNamedParameter(const string &name, const Value &value) {
        string param_value;
        if (value.IsNull()) {
            param_value = "";
        } else {
            param_value = value.ToString();
        }

        parameters.push_back({name, param_value});
        return ":" + name;
    }

    //! Get the final SQL with placeholders
    const string& GetSQL() const {
        return sql;
    }

    //! Get the parameters
    const vector<CloudflareD1QueryParam>& GetParameters() const {
        return parameters;
    }

    //! Check if query has parameters
    bool HasParameters() const {
        return !parameters.empty();
    }

    //! Clear and reset for reuse
    void Clear() {
        sql.clear();
        parameters.clear();
        param_counter = 0;
    }

    //! Static helper: Create parameterized INSERT statement
    static D1ParameterizedQuery CreateInsert(const string &table_name,
                                              const vector<string> &column_names,
                                              const DataChunk &chunk, idx_t row);

    //! Static helper: Create parameterized UPDATE statement
    static D1ParameterizedQuery CreateUpdate(const string &table_name,
                                              const vector<string> &column_names,
                                              const vector<PhysicalIndex> &column_ids,
                                              const DataChunk &updates, idx_t row,
                                              const string &where_column, const Value &where_value);

    //! Static helper: Create parameterized DELETE statement
    static D1ParameterizedQuery CreateDelete(const string &table_name,
                                              const string &where_column, const Value &where_value);

    //! Static helper: Create parameterized UPSERT statement
    static D1ParameterizedQuery CreateUpsert(const string &table_name,
                                              const vector<string> &column_names,
                                              const vector<string> &primary_key_columns,
                                              const DataChunk &chunk, idx_t row);
};

} // namespace duckdb
