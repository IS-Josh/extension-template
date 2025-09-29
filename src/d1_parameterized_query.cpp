//===----------------------------------------------------------------------===//
//                         DuckDB
//
// d1_parameterized_query.cpp
//
// Secure parameterized query builder for D1 operations
//===----------------------------------------------------------------------===//

#include "include/d1_parameterized_query.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

D1ParameterizedQuery D1ParameterizedQuery::CreateInsert(const string &table_name,
                                                         const vector<string> &column_names,
                                                         const DataChunk &chunk, idx_t row) {
    D1ParameterizedQuery query;

    // Build: INSERT INTO "table" (col1, col2, ...) VALUES (?, ?, ...)
    query.AppendSQL("INSERT INTO \"" + table_name + "\" (");

    // Add column names (these are safe - they come from schema)
    for (idx_t i = 0; i < column_names.size(); i++) {
        if (i > 0) query.AppendSQL(", ");
        query.AppendSQL("\"" + column_names[i] + "\"");
    }

    query.AppendSQL(") VALUES (");

    // Add parameter placeholders for values
    for (idx_t i = 0; i < column_names.size() && i < chunk.ColumnCount(); i++) {
        if (i > 0) query.AppendSQL(", ");
        Value value = chunk.GetValue(i, row);
        query.AppendSQL(query.AddParameter(value));
    }

    query.AppendSQL(")");

    fprintf(stderr, "D1ParameterizedQuery: Created INSERT with %zu parameters\n", query.GetParameters().size());
    fprintf(stderr, "D1ParameterizedQuery: SQL = %s\n", query.GetSQL().c_str());

    return query;
}

D1ParameterizedQuery D1ParameterizedQuery::CreateUpdate(const string &table_name,
                                                         const vector<string> &column_names,
                                                         const vector<PhysicalIndex> &column_ids,
                                                         const DataChunk &updates, idx_t row,
                                                         const string &where_column, const Value &where_value) {
    D1ParameterizedQuery query;

    // Build: UPDATE "table" SET col1 = ?, col2 = ? WHERE where_col = ?
    query.AppendSQL("UPDATE \"" + table_name + "\" SET ");

    // Add SET clauses with parameters
    for (idx_t i = 0; i < column_ids.size(); i++) {
        if (i > 0) query.AppendSQL(", ");

        idx_t col_idx = column_ids[i].index;
        if (col_idx < column_names.size()) {
            string col_name = column_names[col_idx];
            Value update_value = updates.GetValue(i, row);

            query.AppendSQL("\"" + col_name + "\" = ");
            query.AppendSQL(query.AddParameter(update_value));
        }
    }

    // Add WHERE clause with parameter
    query.AppendSQL(" WHERE \"" + where_column + "\" = ");
    query.AppendSQL(query.AddParameter(where_value));

    fprintf(stderr, "D1ParameterizedQuery: Created UPDATE with %zu parameters\n", query.GetParameters().size());
    fprintf(stderr, "D1ParameterizedQuery: SQL = %s\n", query.GetSQL().c_str());

    return query;
}

D1ParameterizedQuery D1ParameterizedQuery::CreateDelete(const string &table_name,
                                                         const string &where_column, const Value &where_value) {
    D1ParameterizedQuery query;

    // Build: DELETE FROM "table" WHERE where_col = ?
    query.AppendSQL("DELETE FROM \"" + table_name + "\" WHERE \"" + where_column + "\" = ");
    query.AppendSQL(query.AddParameter(where_value));

    fprintf(stderr, "D1ParameterizedQuery: Created DELETE with %zu parameters\n", query.GetParameters().size());
    fprintf(stderr, "D1ParameterizedQuery: SQL = %s\n", query.GetSQL().c_str());

    return query;
}

D1ParameterizedQuery D1ParameterizedQuery::CreateUpsert(const string &table_name,
                                                         const vector<string> &column_names,
                                                         const vector<string> &primary_key_columns,
                                                         const DataChunk &chunk, idx_t row) {
    D1ParameterizedQuery query;

    // Build: INSERT INTO "table" (col1, col2, ...) VALUES (?, ?, ...)
    //        ON CONFLICT (pk1, pk2) DO UPDATE SET col1 = ?, col2 = ?

    // INSERT portion
    query.AppendSQL("INSERT INTO \"" + table_name + "\" (");

    // Add column names
    for (idx_t i = 0; i < column_names.size(); i++) {
        if (i > 0) query.AppendSQL(", ");
        query.AppendSQL("\"" + column_names[i] + "\"");
    }

    query.AppendSQL(") VALUES (");

    // Add parameter placeholders for INSERT values
    for (idx_t i = 0; i < column_names.size() && i < chunk.ColumnCount(); i++) {
        if (i > 0) query.AppendSQL(", ");
        Value value = chunk.GetValue(i, row);
        query.AppendSQL(query.AddParameter(value));
    }

    query.AppendSQL(")");

    // ON CONFLICT clause
    if (!primary_key_columns.empty()) {
        query.AppendSQL(" ON CONFLICT (");

        for (size_t i = 0; i < primary_key_columns.size(); i++) {
            if (i > 0) query.AppendSQL(", ");
            query.AppendSQL("\"" + primary_key_columns[i] + "\"");
        }

        query.AppendSQL(") DO UPDATE SET ");

        // Add UPDATE SET clauses for non-primary key columns
        bool first = true;
        for (idx_t i = 0; i < column_names.size() && i < chunk.ColumnCount(); i++) {
            const string &col_name = column_names[i];

            // Skip primary key columns in UPDATE SET
            bool is_pk = false;
            for (const auto &pk_col : primary_key_columns) {
                if (col_name == pk_col) {
                    is_pk = true;
                    break;
                }
            }

            if (!is_pk) {
                if (!first) query.AppendSQL(", ");
                Value value = chunk.GetValue(i, row);
                query.AppendSQL("\"" + col_name + "\" = ");
                query.AppendSQL(query.AddParameter(value));
                first = false;
            }
        }
    }

    fprintf(stderr, "D1ParameterizedQuery: Created UPSERT with %zu parameters\n", query.GetParameters().size());
    fprintf(stderr, "D1ParameterizedQuery: SQL = %s\n", query.GetSQL().c_str());

    return query;
}

} // namespace duckdb
