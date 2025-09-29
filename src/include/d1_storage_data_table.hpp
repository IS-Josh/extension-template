//===----------------------------------------------------------------------===//
//                         DuckDB
//
// d1_storage_data_table.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/table_io_manager.hpp"
#include "duckdb/storage/block_manager.hpp"
#include "duckdb/storage/metadata/metadata_manager.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/storage/table/append_state.hpp"
#include "duckdb/storage/table/update_state.hpp"
#include "duckdb/storage/table/delete_state.hpp"
#include "d1_data_table.hpp"

namespace duckdb {

//! Custom TableIOManager for D1 tables that delegates operations to D1DataTable
class D1TableIOManager : public TableIOManager {
private:
    AttachedDatabase &db;
    unique_ptr<D1DataTable> d1_storage;
    CloudflareD1Config config;
    string table_name;

public:
    D1TableIOManager(AttachedDatabase &db, const string &schema_name, const string &table_name,
                     CloudflareD1Config cfg);
    ~D1TableIOManager();

    // Implement pure virtual methods from TableIOManager
    BlockManager &GetIndexBlockManager() override;
    BlockManager &GetBlockManagerForRowData() override;
    MetadataManager &GetMetadataManager() override;
    idx_t GetRowGroupSize() const override;

    // D1-specific methods that delegate to D1DataTable
    void InitializeAppend(DuckTransaction &transaction, TableAppendState &state);
    void Append(DataChunk &chunk, TableAppendState &state);
    void FinalizeAppend(DuckTransaction &transaction, TableAppendState &state);
    void CommitAppend(transaction_t commit_id, idx_t row_start, idx_t count);

    unique_ptr<TableUpdateState> InitializeUpdate(TableCatalogEntry &table, ClientContext &context,
                                                  const vector<unique_ptr<BoundConstraint>> &bound_constraints);
    void Update(TableUpdateState &state, ClientContext &context, Vector &row_ids,
                const vector<PhysicalIndex> &column_ids, DataChunk &updates);

    unique_ptr<TableDeleteState> InitializeDelete(TableCatalogEntry &table, ClientContext &context);
    idx_t Delete(TableDeleteState &state, ClientContext &context, Vector &row_ids, idx_t count);

    // Statistics and metadata
    unique_ptr<BaseStatistics> GetStatistics(ClientContext &context, column_t column_id);
    TableStorageInfo GetStorageInfo(ClientContext &context);

    // Access to underlying D1DataTable
    D1DataTable* GetD1Storage() { return d1_storage.get(); }
};

//! Custom state classes for D1 operations
struct D1TableAppendState : public TableAppendState {
    // No additional state needed - D1DataTable manages its own state
};

struct D1TableUpdateState : public TableUpdateState {
    // No additional state needed - D1DataTable manages its own state
};

struct D1TableDeleteState : public TableDeleteState {
    // No additional state needed - D1DataTable manages its own state
};

} // namespace duckdb
