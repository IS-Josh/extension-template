//===----------------------------------------------------------------------===//
//                         DuckDB
//
// d1_custom_data_table.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "duckdb/storage/data_table.hpp"
#include "d1_storage_data_table.hpp"

namespace duckdb {

//! Custom DataTable that bypasses LocalStorage and routes directly to D1
class D1CustomDataTable : public DataTable {
private:
    shared_ptr<D1TableIOManager> d1_io_manager;

public:
    D1CustomDataTable(AttachedDatabase &db, shared_ptr<D1TableIOManager> io_manager,
                      const string &schema, const string &table,
                      vector<ColumnDefinition> column_definitions);

    // Override LocalAppend methods to route directly to D1
    void InitializeLocalAppend(LocalAppendState &state, TableCatalogEntry &table,
                               ClientContext &context,
                               const vector<unique_ptr<BoundConstraint>> &bound_constraints);
    void LocalAppend(LocalAppendState &state, ClientContext &context, DataChunk &chunk, bool unsafe);
    void FinalizeLocalAppend(LocalAppendState &state);
    void LocalAppend(TableCatalogEntry &table, ClientContext &context, DataChunk &chunk,
                     const vector<unique_ptr<BoundConstraint>> &bound_constraints);
};

//! Custom LocalAppendState for D1 operations
struct D1LocalAppendState : public LocalAppendState {
    unique_ptr<TableAppendState> d1_append_state;
    DuckTransaction *transaction;
};

} // namespace duckdb
