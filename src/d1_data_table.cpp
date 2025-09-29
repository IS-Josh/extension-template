//===----------------------------------------------------------------------===//
//                         DuckDB
//
// d1_data_table.cpp
//
//
//===----------------------------------------------------------------------===//

#include "include/d1_data_table.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/transaction/duck_transaction.hpp"
#include "include/d1_type_mapping.hpp"

namespace duckdb {

// Static instance
D1RowIdManager D1RowIdManager::instance;

//===--------------------------------------------------------------------===//
// D1TableSchema Implementation
//===--------------------------------------------------------------------===//

D1TableSchema D1TableSchema::DiscoverSchema(CloudflareD1Client &client, const string &table_name) {
    D1TableSchema schema;
    schema.table_name = table_name;

    // Query D1 for table schema using PRAGMA table_info
    string sql = "PRAGMA table_info(\"" + table_name + "\")";
    auto result = client.RawQuery(sql, {});

    if (!result.success) {
        throw InvalidInputException("Failed to discover schema for table '%s': %s",
                                   table_name.c_str(), result.error.c_str());
    }

    // Parse PRAGMA table_info results
    // Format: cid, name, type, notnull, dflt_value, pk
    for (const auto &row : result.rows) {
        if (row.size() >= 6) {
            string col_name = row[1];
            string col_type = row[2];
            bool is_primary_key = (row[5] == "1");

            // Strip quotes if present
            if (col_name.size() >= 2 && col_name.front() == '"' && col_name.back() == '"') {
                col_name = col_name.substr(1, col_name.size() - 2);
            }
            if (col_type.size() >= 2 && col_type.front() == '"' && col_type.back() == '"') {
                col_type = col_type.substr(1, col_type.size() - 2);
            }

            schema.column_names.push_back(col_name);
            schema.column_types.push_back(D1TypeMapping::MapD1TypeToDuckDB(col_type));
            schema.column_name_to_index[col_name] = schema.column_names.size() - 1;

            if (is_primary_key) {
                schema.primary_key_columns.push_back(col_name);
            }
        }
    }

    fprintf(stderr, "D1TableSchema: Discovered schema for '%s' with %zu columns, %zu primary keys\n",
           table_name.c_str(), schema.column_names.size(), schema.primary_key_columns.size());

    return schema;
}

string D1TableSchema::GetPrimaryKeyWhereClause(const DataChunk &chunk, idx_t row) const {
    if (primary_key_columns.empty()) {
        throw InvalidInputException("Table '%s' has no primary key for WHERE clause generation",
                                   table_name.c_str());
    }

    vector<string> conditions;
    for (const auto &pk_col : primary_key_columns) {
        auto it = column_name_to_index.find(pk_col);
        if (it != column_name_to_index.end()) {
            idx_t col_idx = it->second;
            if (col_idx < chunk.ColumnCount()) {
                Value val = chunk.GetValue(col_idx, row);
                string condition = "\"" + pk_col + "\" = " + D1TypeMapping::ConvertValueToSQL(val);
                conditions.push_back(condition);
            }
        }
    }

    return StringUtil::Join(conditions, " AND ");
}

//===--------------------------------------------------------------------===//
// D1RowIdManager Implementation
//===--------------------------------------------------------------------===//

row_t D1RowIdManager::GenerateRowId(const string &table_name) {
    lock_guard<mutex> lock(row_id_mutex);

    auto &counter = table_row_counters[table_name];
    return ++counter;
}

string D1RowIdManager::MapRowIdToPrimaryKey(row_t row_id, const string &table_name) {
    lock_guard<mutex> lock(row_id_mutex);

    auto table_it = row_id_to_pk_map.find(table_name);
    if (table_it != row_id_to_pk_map.end()) {
        auto row_it = table_it->second.find(row_id);
        if (row_it != table_it->second.end()) {
            return row_it->second;
        }
    }

    return "";
}

row_t D1RowIdManager::MapPrimaryKeyToRowId(const string &primary_key_values, const string &table_name) {
    lock_guard<mutex> lock(row_id_mutex);

    auto table_it = pk_to_row_id_map.find(table_name);
    if (table_it != pk_to_row_id_map.end()) {
        auto pk_it = table_it->second.find(primary_key_values);
        if (pk_it != table_it->second.end()) {
            return pk_it->second;
        }
    }

    return 0; // Invalid row ID
}

void D1RowIdManager::RegisterRow(const string &table_name, row_t row_id, const string &primary_key_values) {
    lock_guard<mutex> lock(row_id_mutex);

    row_id_to_pk_map[table_name][row_id] = primary_key_values;
    pk_to_row_id_map[table_name][primary_key_values] = row_id;

    fprintf(stderr, "D1RowIdManager: Registered row %llu -> '%s' for table '%s'\n",
           (unsigned long long)row_id, primary_key_values.c_str(), table_name.c_str());
}

void D1RowIdManager::UnregisterRow(const string &table_name, row_t row_id) {
    lock_guard<mutex> lock(row_id_mutex);

    auto table_it = row_id_to_pk_map.find(table_name);
    if (table_it != row_id_to_pk_map.end()) {
        auto row_it = table_it->second.find(row_id);
        if (row_it != table_it->second.end()) {
            string pk_values = row_it->second;
            table_it->second.erase(row_it);

            // Also remove from reverse mapping
            auto pk_table_it = pk_to_row_id_map.find(table_name);
            if (pk_table_it != pk_to_row_id_map.end()) {
                pk_table_it->second.erase(pk_values);
            }

            fprintf(stderr, "D1RowIdManager: Unregistered row %llu for table '%s'\n",
                   (unsigned long long)row_id, table_name.c_str());
        }
    }
}

void D1RowIdManager::ClearTable(const string &table_name) {
    lock_guard<mutex> lock(row_id_mutex);

    row_id_to_pk_map.erase(table_name);
    pk_to_row_id_map.erase(table_name);
    table_row_counters.erase(table_name);

    fprintf(stderr, "D1RowIdManager: Cleared all mappings for table '%s'\n", table_name.c_str());
}

//===--------------------------------------------------------------------===//
// D1DataTable Implementation
//===--------------------------------------------------------------------===//

D1DataTable::D1DataTable(const string &schema_name, const string &table_name, CloudflareD1Config cfg)
    : config(std::move(cfg)), table_name(table_name) {

    // Initialize D1 client
    client = make_uniq<CloudflareD1Client>(config);

    // Discover table schema
    try {
        schema = D1TableSchema::DiscoverSchema(*client, table_name);
    } catch (const std::exception &e) {
        fprintf(stderr, "D1DataTable: Failed to discover schema for '%s': %s\n",
               table_name.c_str(), e.what());
        // Use default schema for testing
        schema.table_name = table_name;
        schema.column_names = {"id", "name", "created_at"};
        schema.column_types = {LogicalType::BIGINT, LogicalType::VARCHAR, LogicalType::VARCHAR};
        schema.primary_key_columns = {"id"};
        schema.column_name_to_index["id"] = 0;
        schema.column_name_to_index["name"] = 1;
        schema.column_name_to_index["created_at"] = 2;
    }

    // Initialize state objects
    append_state = make_uniq<D1AppendState>();
    update_state = make_uniq<D1UpdateState>();
    delete_state = make_uniq<D1DeleteState>();

    fprintf(stderr, "D1DataTable: Initialized for table '%s' with %zu columns\n",
           table_name.c_str(), schema.column_names.size());
}

D1DataTable::~D1DataTable() {
    try {
        FlushPendingOperations();
    } catch (const std::exception &e) {
        fprintf(stderr, "D1DataTable: Error during destruction: %s\n", e.what());
    }
}

//===--------------------------------------------------------------------===//
// SQL Generation Helpers
//===--------------------------------------------------------------------===//

string D1DataTable::ConvertValueToSQL(const Value &value) {
    if (value.IsNull()) {
        return "NULL";
    }

    switch (value.type().id()) {
        case LogicalTypeId::BOOLEAN:
            return value.GetValue<bool>() ? "1" : "0";
        case LogicalTypeId::TINYINT:
        case LogicalTypeId::SMALLINT:
        case LogicalTypeId::INTEGER:
        case LogicalTypeId::BIGINT:
            return std::to_string(value.GetValue<int64_t>());
        case LogicalTypeId::FLOAT:
        case LogicalTypeId::DOUBLE:
            return std::to_string(value.GetValue<double>());
        case LogicalTypeId::VARCHAR:
        case LogicalTypeId::DATE:
        case LogicalTypeId::TIMESTAMP:
            return "'" + StringUtil::Replace(value.ToString(), "'", "''") + "'";
        default:
            return "'" + StringUtil::Replace(value.ToString(), "'", "''") + "'";
    }
}

string D1DataTable::GenerateInsertSQL(const DataChunk &chunk, idx_t row) {
    string sql = "INSERT INTO \"" + table_name + "\" (";

    // Add column names
    for (idx_t col = 0; col < schema.column_names.size() && col < chunk.ColumnCount(); col++) {
        if (col > 0) sql += ", ";
        sql += "\"" + schema.column_names[col] + "\"";
    }

    sql += ") VALUES (";

    // Add values
    for (idx_t col = 0; col < schema.column_names.size() && col < chunk.ColumnCount(); col++) {
        if (col > 0) sql += ", ";
        sql += ConvertValueToSQL(chunk.GetValue(col, row));
    }

    sql += ")";
    return sql;
}

string D1DataTable::GenerateUpsertSQL(const DataChunk &chunk, idx_t row) {
    // Start with INSERT portion
    string sql = "INSERT INTO \"" + table_name + "\" (";

    // Add column names
    for (idx_t col = 0; col < schema.column_names.size() && col < chunk.ColumnCount(); col++) {
        if (col > 0) sql += ", ";
        sql += "\"" + schema.column_names[col] + "\"";
    }

    sql += ") VALUES (";

    // Add values
    for (idx_t col = 0; col < schema.column_names.size() && col < chunk.ColumnCount(); col++) {
        if (col > 0) sql += ", ";
        sql += ConvertValueToSQL(chunk.GetValue(col, row));
    }

    sql += ")";

    // Add ON CONFLICT clause for primary key columns
    if (schema.HasPrimaryKey()) {
        sql += " ON CONFLICT (";

        // Add primary key column names
        for (size_t i = 0; i < schema.primary_key_columns.size(); i++) {
            if (i > 0) sql += ", ";
            sql += "\"" + schema.primary_key_columns[i] + "\"";
        }

        sql += ") DO UPDATE SET ";

        // Add SET clauses for all non-primary key columns
        bool first = true;
        for (idx_t col = 0; col < schema.column_names.size() && col < chunk.ColumnCount(); col++) {
            const string &col_name = schema.column_names[col];

            // Skip primary key columns in the UPDATE SET clause
            bool is_pk = false;
            for (const auto &pk_col : schema.primary_key_columns) {
                if (col_name == pk_col) {
                    is_pk = true;
                    break;
                }
            }

            if (!is_pk) {
                if (!first) sql += ", ";
                sql += "\"" + col_name + "\" = " + ConvertValueToSQL(chunk.GetValue(col, row));
                first = false;
            }
        }
    }

    return sql;
}

string D1DataTable::GenerateWhereClauseForRowId(row_t row_id) {
    string pk_values = D1RowIdManager::GetInstance().MapRowIdToPrimaryKey(row_id, table_name);
    if (pk_values.empty()) {
        throw InvalidInputException("No primary key mapping found for row ID %llu in table '%s'",
                                   (unsigned long long)row_id, table_name.c_str());
    }
    return pk_values;
}

string D1DataTable::GenerateUpdateSQL(row_t row_id, const vector<PhysicalIndex> &column_ids,
                                      const DataChunk &updates, idx_t row) {
    string sql = "UPDATE \"" + table_name + "\" SET ";

    // Add SET clauses
    for (idx_t i = 0; i < column_ids.size(); i++) {
        if (i > 0) sql += ", ";
        idx_t col_idx = column_ids[i].index;
        if (col_idx < schema.column_names.size()) {
            sql += "\"" + schema.column_names[col_idx] + "\" = " +
                   ConvertValueToSQL(updates.GetValue(i, row));
        }
    }

    // Add WHERE clause based on primary key
    sql += " WHERE " + GenerateWhereClauseForRowId(row_id);
    return sql;
}

string D1DataTable::GenerateUpdateSQLFromString(const string &row_id_str, const vector<PhysicalIndex> &column_ids,
                                                const DataChunk &updates, idx_t row) {
    string sql = "UPDATE \"" + table_name + "\" SET ";

    // Add SET clauses
    for (idx_t i = 0; i < column_ids.size(); i++) {
        if (i > 0) sql += ", ";
        idx_t col_idx = column_ids[i].index;
        if (col_idx < schema.column_names.size()) {
            sql += "\"" + schema.column_names[col_idx] + "\" = " +
                   ConvertValueToSQL(updates.GetValue(i, row));
        }
    }

    // Try to convert string row_id back to numeric row_t
    try {
        row_t numeric_row_id = std::stoull(row_id_str);
        fprintf(stderr, "D1DataTable: Converted string row_id '%s' to numeric %llu\n", row_id_str.c_str(), numeric_row_id);
        sql += " WHERE " + GenerateWhereClauseForRowId(numeric_row_id);
    } catch (const std::exception &e) {
        fprintf(stderr, "D1DataTable: Failed to convert row_id_str '%s' to numeric, treating as literal: %s\n", row_id_str.c_str(), e.what());
        // If conversion fails, assume the string contains the WHERE condition directly
        sql += " WHERE " + row_id_str;
    }

    return sql;
}

string D1DataTable::GenerateDeleteSQL(row_t row_id) {
    string sql = "DELETE FROM \"" + table_name + "\" WHERE " + GenerateWhereClauseForRowId(row_id);
    return sql;
}

//===--------------------------------------------------------------------===//
// Batch Execution
//===--------------------------------------------------------------------===//

void D1DataTable::ExecuteBatchOperations(const vector<string> &operations) {
    if (operations.empty()) {
        fprintf(stderr, "D1DataTable: ExecuteBatchOperations called with empty operations vector\n");
        return;
    }

    fprintf(stderr, "D1DataTable: Executing batch of %zu operations\n", operations.size());
    fprintf(stderr, "D1DataTable: Client config - account_id: '%s', database_id: '%s', api_token length: %zu\n",
           client->GetConfig().account_id.c_str(),
           client->GetConfig().database_id.c_str(),
           client->GetConfig().api_token.length());

    for (const auto &sql : operations) {
        fprintf(stderr, "D1DataTable: [API CALL] Executing: %s\n", sql.c_str());

        auto result = client->ObjectQuery(sql, {});
        fprintf(stderr, "D1DataTable: [API RESPONSE] Success: %s\n", result.success ? "true" : "false");

        if (!result.success) {
            fprintf(stderr, "D1DataTable: [API ERROR] %s\n", result.error.c_str());
            throw InvalidInputException("D1 operation failed: %s\nSQL: %s",
                                       result.error.c_str(), sql.c_str());
        } else {
            fprintf(stderr, "D1DataTable: [API SUCCESS] Changes: %lld\n", result.changes);
        }
    }

    fprintf(stderr, "D1DataTable: Successfully executed %zu operations\n", operations.size());
}

void D1DataTable::FlushPendingOperations() {
    vector<string> all_operations;

    // Collect all pending operations
    if (append_state && !append_state->pending_insert_statements.empty()) {
        all_operations.insert(all_operations.end(),
                             append_state->pending_insert_statements.begin(),
                             append_state->pending_insert_statements.end());
        append_state->Clear();
    }

    if (update_state && !update_state->pending_update_statements.empty()) {
        all_operations.insert(all_operations.end(),
                             update_state->pending_update_statements.begin(),
                             update_state->pending_update_statements.end());
        update_state->Clear();
    }

    if (delete_state && !delete_state->pending_delete_statements.empty()) {
        all_operations.insert(all_operations.end(),
                             delete_state->pending_delete_statements.begin(),
                             delete_state->pending_delete_statements.end());
        delete_state->Clear();
    }

    // Execute all operations
    if (!all_operations.empty()) {
        ExecuteBatchOperations(all_operations);
    }
}

void D1DataTable::Finalize() {
    fprintf(stderr, "D1DataTable: Finalize called - flushing any pending operations\n");
    FlushPendingOperations();
    fprintf(stderr, "D1DataTable: Finalize completed\n");
}


//===--------------------------------------------------------------------===//
// D1DataTable Storage Operations
//===--------------------------------------------------------------------===//

void D1DataTable::ExecuteInsert(const DataChunk &chunk) {
    fprintf(stderr, "D1DataTable: ExecuteInsert %zu rows to table '%s'\n", chunk.size(), table_name.c_str());
    fprintf(stderr, "D1DataTable: Batch mode: %s, pending statements: %zu\n",
           append_state->batch_mode ? "true" : "false",
           append_state->pending_insert_statements.size());

    if (chunk.size() == 0) {
        return;
    }

    // Check if table has primary keys to determine INSERT vs UPSERT strategy
    bool has_primary_key = schema.HasPrimaryKey();
    fprintf(stderr, "D1DataTable: Table '%s' has primary key: %s\n",
           table_name.c_str(), has_primary_key ? "true" : "false");

    if (has_primary_key) {
        fprintf(stderr, "D1DataTable: Primary key columns: ");
        for (size_t i = 0; i < schema.primary_key_columns.size(); i++) {
            if (i > 0) fprintf(stderr, ", ");
            fprintf(stderr, "%s", schema.primary_key_columns[i].c_str());
        }
        fprintf(stderr, "\n");
    }

    // Generate INSERT/UPSERT statements for each row
    for (idx_t row = 0; row < chunk.size(); row++) {
        string insert_sql;

        if (has_primary_key) {
            // Use UPSERT for tables with primary keys
            insert_sql = GenerateUpsertSQL(chunk, row);
            fprintf(stderr, "D1DataTable: Generated UPSERT SQL: %s\n", insert_sql.c_str());
        } else {
            // Use regular INSERT for tables without primary keys
            insert_sql = GenerateInsertSQL(chunk, row);
            fprintf(stderr, "D1DataTable: Generated INSERT SQL: %s\n", insert_sql.c_str());
        }

        append_state->AddInsertStatement(insert_sql);

        // Generate row ID and register primary key mapping
        row_t row_id = D1RowIdManager::GetInstance().GenerateRowId(table_name);
        if (has_primary_key) {
            string pk_where = schema.GetPrimaryKeyWhereClause(chunk, row);
            D1RowIdManager::GetInstance().RegisterRow(table_name, row_id, pk_where);
        }
    }

    fprintf(stderr, "D1DataTable: After adding statements, pending count: %zu\n",
           append_state->pending_insert_statements.size());

    // Execute immediately if not in batch mode, or if batch is getting large
    if (!append_state->batch_mode || append_state->pending_insert_statements.size() >= 100) {
        fprintf(stderr, "D1DataTable: Executing batch immediately (batch_mode: %s, count: %zu)\n",
               append_state->batch_mode ? "true" : "false",
               append_state->pending_insert_statements.size());
        ExecuteBatchOperations(append_state->pending_insert_statements);
        append_state->Clear();
    } else {
        fprintf(stderr, "D1DataTable: Deferring execution (batch mode enabled, count: %zu < 100)\n",
               append_state->pending_insert_statements.size());
    }
}

void D1DataTable::ExecuteUpdate(Vector &row_ids, const vector<PhysicalIndex> &column_ids, const DataChunk &updates) {
    fprintf(stderr, "D1DataTable: ExecuteUpdate %zu rows in table '%s'\n", updates.size(), table_name.c_str());

    if (updates.size() == 0) {
        return;
    }

    // Flatten the row_ids vector to access the data
    row_ids.Flatten(updates.size());

    fprintf(stderr, "D1DataTable: ExecuteUpdate row_ids vector type: %s\n", row_ids.GetType().ToString().c_str());

    // Handle different row identifier types
    if (row_ids.GetType().id() == LogicalTypeId::VARCHAR) {
        fprintf(stderr, "D1DataTable: Row IDs are VARCHAR - extracting string data\n");
        auto string_data = FlatVector::GetData<string_t>(row_ids);

        // For each row, extract the string row identifier
        for (idx_t row = 0; row < updates.size(); row++) {
            string row_id_str = string_data[row].GetString();
            fprintf(stderr, "D1DataTable: Row %llu has string row_id: '%s'\n", row, row_id_str.c_str());

            // Generate UPDATE SQL using string row identifier
            string update_sql = GenerateUpdateSQLFromString(row_id_str, column_ids, updates, row);
            update_state->AddUpdateStatement(update_sql);
        }
    } else if (row_ids.GetType().id() == LogicalTypeId::BIGINT) {
        fprintf(stderr, "D1DataTable: Row IDs are BIGINT - extracting numeric data\n");
        auto row_data = FlatVector::GetData<row_t>(row_ids);

        // Generate UPDATE statements for each row
        for (idx_t row = 0; row < updates.size(); row++) {
            string update_sql = GenerateUpdateSQL(row_data[row], column_ids, updates, row);
            update_state->AddUpdateStatement(update_sql);
        }
    } else {
        throw InternalException("D1DataTable: Unsupported row identifier type: " + row_ids.GetType().ToString());
    }

    // Execute immediately if not in batch mode, or if batch is getting large
    if (!update_state->batch_mode || update_state->pending_update_statements.size() >= 100) {
        ExecuteBatchOperations(update_state->pending_update_statements);
        update_state->Clear();
    }
}

void D1DataTable::ExecuteCustomUpdateSQL(const string &update_sql) {
    fprintf(stderr, "D1DataTable: ExecuteCustomUpdateSQL: %s\n", update_sql.c_str());

    if (!update_state) {
        update_state = make_uniq<D1UpdateState>();
    }

    // Add the custom SQL directly to the batch
    update_state->AddUpdateStatement(update_sql);

    // Execute immediately if not in batch mode, or if batch is getting large
    if (!update_state->batch_mode || update_state->pending_update_statements.size() >= 100) {
        ExecuteBatchOperations(update_state->pending_update_statements);
        update_state->Clear();
    }
}

void D1DataTable::ExecuteCustomDeleteSQL(const string &delete_sql) {
    fprintf(stderr, "D1DataTable: ExecuteCustomDeleteSQL: %s\n", delete_sql.c_str());

    if (!delete_state) {
        delete_state = make_uniq<D1DeleteState>();
    }

    // Add the custom SQL directly to the batch
    delete_state->AddDeleteStatement(delete_sql);

    // Execute immediately if not in batch mode, or if batch is getting large
    if (!delete_state->batch_mode || delete_state->pending_delete_statements.size() >= 100) {
        ExecuteBatchOperations(delete_state->pending_delete_statements);
        delete_state->Clear();
    }
}

idx_t D1DataTable::ExecuteDelete(Vector &row_ids, idx_t count) {
    fprintf(stderr, "D1DataTable: ExecuteDelete %zu rows from table '%s'\n", count, table_name.c_str());

    if (count == 0) {
        return 0;
    }

    // Flatten the row_ids vector to access the data
    row_ids.Flatten(count);
    auto row_data = FlatVector::GetData<row_t>(row_ids);

    // Generate DELETE statements for each row
    for (idx_t row = 0; row < count; row++) {
        string delete_sql = GenerateDeleteSQL(row_data[row]);
        delete_state->AddDeleteStatement(delete_sql);

        // Unregister the row from the row ID manager
        D1RowIdManager::GetInstance().UnregisterRow(table_name, row_data[row]);
    }

    // Execute immediately if not in batch mode, or if batch is getting large
    if (!delete_state->batch_mode || delete_state->pending_delete_statements.size() >= 100) {
        ExecuteBatchOperations(delete_state->pending_delete_statements);
        delete_state->Clear();
    }

    return count;
}

void D1DataTable::ExecuteBatch(const vector<string> &sql_statements) {
    ExecuteBatchOperations(sql_statements);
}

//===--------------------------------------------------------------------===//
// D1-specific Methods
//===--------------------------------------------------------------------===//

void D1DataTable::RefreshSchema() {
    try {
        schema = D1TableSchema::DiscoverSchema(*client, table_name);
        fprintf(stderr, "D1DataTable: Refreshed schema for table '%s'\n", table_name.c_str());
    } catch (const std::exception &e) {
        fprintf(stderr, "D1DataTable: Failed to refresh schema: %s\n", e.what());
    }
}

void D1DataTable::SetBatchMode(bool enabled) {
    append_state->batch_mode = enabled;
    fprintf(stderr, "D1DataTable: Set batch mode to %s for table '%s'\n",
           enabled ? "enabled" : "disabled", table_name.c_str());
}


} // namespace duckdb
