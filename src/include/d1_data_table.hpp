//===----------------------------------------------------------------------===//
//                         DuckDB
//
// d1_data_table.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/table/append_state.hpp"
#include "duckdb/storage/table/update_state.hpp"
#include "duckdb/storage/table/delete_state.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "d1_client.hpp"
#include <unordered_map>

namespace duckdb {

//! D1 Table Schema Information
struct D1TableSchema {
    string table_name;
    vector<string> column_names;
    vector<LogicalType> column_types;
    vector<string> primary_key_columns;
    unordered_map<string, idx_t> column_name_to_index;

    static D1TableSchema DiscoverSchema(CloudflareD1Client &client, const string &table_name);
    bool HasPrimaryKey() const { return !primary_key_columns.empty(); }
    string GetPrimaryKeyWhereClause(const DataChunk &chunk, idx_t row) const;
};

//! Row ID Management for D1 Tables
class D1RowIdManager {
private:
    static D1RowIdManager instance;
    mutable mutex row_id_mutex;

    // Map table_name -> next_row_id
    unordered_map<string, idx_t> table_row_counters;

    // Map table_name -> (row_id -> primary_key_values)
    unordered_map<string, unordered_map<row_t, string>> row_id_to_pk_map;

    // Map table_name -> (primary_key_values -> row_id)
    unordered_map<string, unordered_map<string, row_t>> pk_to_row_id_map;

    D1RowIdManager() = default;

public:
    static D1RowIdManager& GetInstance() { return instance; }

    //! Generate unique row ID for D1 table
    row_t GenerateRowId(const string &table_name);

    //! Map DuckDB row ID to D1 primary key values
    string MapRowIdToPrimaryKey(row_t row_id, const string &table_name);

    //! Map D1 primary key values to DuckDB row ID
    row_t MapPrimaryKeyToRowId(const string &primary_key_values, const string &table_name);

    //! Register a new row with its primary key
    void RegisterRow(const string &table_name, row_t row_id, const string &primary_key_values);

    //! Remove a row mapping
    void UnregisterRow(const string &table_name, row_t row_id);

    //! Clear all mappings for a table
    void ClearTable(const string &table_name);
};

//! D1 Append State for INSERT operations
struct D1AppendState {
    vector<string> pending_insert_statements;
    idx_t rows_appended = 0;
    bool batch_mode = true;

    void AddInsertStatement(const string &sql) {
        pending_insert_statements.push_back(sql);
        rows_appended++;
    }

    void Clear() {
        pending_insert_statements.clear();
        rows_appended = 0;
    }
};

//! D1 Update State for UPDATE operations
struct D1UpdateState {
    vector<string> pending_update_statements;
    idx_t rows_updated = 0;
    bool batch_mode = true;

    void AddUpdateStatement(const string &sql) {
        pending_update_statements.push_back(sql);
        rows_updated++;
    }

    void Clear() {
        pending_update_statements.clear();
        rows_updated = 0;
    }
};

//! D1 Delete State for DELETE operations
struct D1DeleteState {
    vector<string> pending_delete_statements;
    idx_t rows_deleted = 0;
    bool batch_mode = true;

    void AddDeleteStatement(const string &sql) {
        pending_delete_statements.push_back(sql);
        rows_deleted++;
    }

    void Clear() {
        pending_delete_statements.clear();
        rows_deleted = 0;
    }
};

//! Custom storage implementation for Cloudflare D1 (not inheriting from DataTable)
class D1DataTable {
private:
    CloudflareD1Config config;
    string table_name;
    unique_ptr<CloudflareD1Client> client;
    D1TableSchema schema;

    // State management
    unique_ptr<D1AppendState> append_state;
    unique_ptr<D1UpdateState> update_state;
    unique_ptr<D1DeleteState> delete_state;

    // SQL generation helpers
    string GenerateInsertSQL(const DataChunk &chunk, idx_t row);
    string GenerateUpdateSQL(row_t row_id, const vector<PhysicalIndex> &column_ids,
                           const DataChunk &updates, idx_t row);
    string GenerateDeleteSQL(row_t row_id);
    string ConvertValueToSQL(const Value &value);
    string GenerateWhereClauseForRowId(row_t row_id);

    // Batch execution
    void ExecuteBatchOperations(const vector<string> &operations);

public:
    D1DataTable(const string &schema_name, const string &table_name, CloudflareD1Config cfg);
    ~D1DataTable();

    // D1-specific storage operations (not overriding DataTable)
    void ExecuteInsert(const DataChunk &chunk);
    void ExecuteUpdate(Vector &row_ids, const vector<PhysicalIndex> &column_ids, const DataChunk &updates);
    idx_t ExecuteDelete(Vector &row_ids, idx_t count);

    // Batch operations
    void ExecuteBatch(const vector<string> &sql_statements);

    // D1-specific methods
    const D1TableSchema& GetSchema() const { return schema; }
    void RefreshSchema();
    void SetBatchMode(bool enabled);
    void FlushPendingOperations();
    void Finalize();
};

} // namespace duckdb
