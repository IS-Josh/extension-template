//===----------------------------------------------------------------------===//
//                         DuckDB
//
// d1_storage_data_table.cpp
//
//
//===----------------------------------------------------------------------===//

#include "include/d1_storage_data_table.hpp"
#include "duckdb/transaction/duck_transaction.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/storage/storage_manager.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// D1TableIOManager Implementation
//===--------------------------------------------------------------------===//

D1TableIOManager::D1TableIOManager(AttachedDatabase &db, const string &schema_name, const string &table_name,
                                   CloudflareD1Config cfg)
    : db(db), config(std::move(cfg)), table_name(table_name) {

    // Create the underlying D1DataTable
    d1_storage = make_uniq<D1DataTable>(schema_name, table_name, config);

    fprintf(stderr, "D1TableIOManager: Created for table '%s'\n", table_name.c_str());
}

D1TableIOManager::~D1TableIOManager() {
    fprintf(stderr, "D1TableIOManager: Destroying for table '%s'\n", table_name.c_str());
}

//===--------------------------------------------------------------------===//
// Pure Virtual Methods from TableIOManager
//===--------------------------------------------------------------------===//

BlockManager &D1TableIOManager::GetIndexBlockManager() {
    // D1 doesn't use local block managers - delegate to the attached database
    return db.GetStorageManager().GetBlockManager();
}

BlockManager &D1TableIOManager::GetBlockManagerForRowData() {
    // D1 doesn't use local block managers - delegate to the attached database
    return db.GetStorageManager().GetBlockManager();
}

MetadataManager &D1TableIOManager::GetMetadataManager() {
    // D1 doesn't use local metadata managers - delegate to the block manager
    return db.GetStorageManager().GetBlockManager().GetMetadataManager();
}

idx_t D1TableIOManager::GetRowGroupSize() const {
    // Use a reasonable default row group size for remote tables
    return 1024;
}

//===--------------------------------------------------------------------===//
// Append Operations (INSERT)
//===--------------------------------------------------------------------===//

void D1TableIOManager::InitializeAppend(DuckTransaction &transaction, TableAppendState &state) {
    fprintf(stderr, "D1TableIOManager: InitializeAppend for table '%s'\n", table_name.c_str());
    // D1DataTable manages its own state, so we don't need to do anything special here
}

void D1TableIOManager::Append(DataChunk &chunk, TableAppendState &state) {
    fprintf(stderr, "D1TableIOManager: Append %llu rows to table '%s'\n", chunk.size(), table_name.c_str());

    // Delegate to D1DataTable
    d1_storage->ExecuteInsert(chunk);
}

void D1TableIOManager::FinalizeAppend(DuckTransaction &transaction, TableAppendState &state) {
    fprintf(stderr, "D1TableIOManager: FinalizeAppend for table '%s'\n", table_name.c_str());

    // Flush any pending operations in D1DataTable
    d1_storage->FlushPendingOperations();
}

void D1TableIOManager::CommitAppend(transaction_t commit_id, idx_t row_start, idx_t count) {
    fprintf(stderr, "D1TableIOManager: CommitAppend %llu rows starting at %llu for table '%s'\n",
           count, row_start, table_name.c_str());
    // D1 operations are committed immediately, so nothing to do here
}

//===--------------------------------------------------------------------===//
// Update Operations
//===--------------------------------------------------------------------===//

unique_ptr<TableUpdateState> D1TableIOManager::InitializeUpdate(TableCatalogEntry &table, ClientContext &context,
                                                                const vector<unique_ptr<BoundConstraint>> &bound_constraints) {
    fprintf(stderr, "D1TableIOManager: InitializeUpdate for table '%s'\n", table_name.c_str());

    // For D1 tables, we don't want to use the standard update path
    // Instead, we'll throw an exception that suggests using d1_execute
    throw NotImplementedException("Direct UPDATE operations on D1 tables are not supported through the storage layer.\n"
                                 "D1 UPDATE operations should be rewritten to use d1_execute() at the query level.\n"
                                 "This indicates that the query rewriter did not intercept the UPDATE statement.\n"
                                 "Use: SELECT d1_execute('UPDATE table SET col = value WHERE condition', account, token, db);");
}

void D1TableIOManager::Update(TableUpdateState &state, ClientContext &context, Vector &row_ids,
                              const vector<PhysicalIndex> &column_ids, DataChunk &updates) {
    fprintf(stderr, "D1TableIOManager: Update operation for table '%s'\n", table_name.c_str());

    // This should never be called if the query rewriter is working properly
    throw NotImplementedException("D1TableIOManager::Update should not be called - UPDATE should be rewritten to d1_execute");
}

//===--------------------------------------------------------------------===//
// Delete Operations
//===--------------------------------------------------------------------===//

unique_ptr<TableDeleteState> D1TableIOManager::InitializeDelete(TableCatalogEntry &table, ClientContext &context) {
    fprintf(stderr, "D1TableIOManager: InitializeDelete for table '%s'\n", table_name.c_str());

    // For D1 tables, we don't want to use the standard delete path
    // Instead, we'll throw an exception that suggests using d1_execute
    throw NotImplementedException("Direct DELETE operations on D1 tables are not supported through the storage layer.\n"
                                 "D1 DELETE operations should be rewritten to use d1_execute() at the query level.\n"
                                 "This indicates that the query rewriter did not intercept the DELETE statement.\n"
                                 "Use: SELECT d1_execute('DELETE FROM table WHERE condition', account, token, db);");
}

idx_t D1TableIOManager::Delete(TableDeleteState &state, ClientContext &context, Vector &row_ids, idx_t count) {
    fprintf(stderr, "D1TableIOManager: Delete operation for table '%s'\n", table_name.c_str());

    // This should never be called if the query rewriter is working properly
    throw NotImplementedException("D1TableIOManager::Delete should not be called - DELETE should be rewritten to d1_execute");
}

//===--------------------------------------------------------------------===//
// Statistics and Metadata
//===--------------------------------------------------------------------===//

unique_ptr<BaseStatistics> D1TableIOManager::GetStatistics(ClientContext &context, column_t column_id) {
    // For remote tables, we don't have local statistics
    return nullptr;
}

TableStorageInfo D1TableIOManager::GetStorageInfo(ClientContext &context) {
    TableStorageInfo info;
    info.cardinality = 0; // Unknown cardinality for remote tables
    return info;
}

} // namespace duckdb
