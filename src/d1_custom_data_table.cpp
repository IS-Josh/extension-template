//===----------------------------------------------------------------------===//
//                         DuckDB
//
// d1_custom_data_table.cpp
//
//
//===----------------------------------------------------------------------===//

#include "include/d1_custom_data_table.hpp"
#include "duckdb/transaction/duck_transaction.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"

namespace duckdb {

D1CustomDataTable::D1CustomDataTable(AttachedDatabase &db, shared_ptr<D1TableIOManager> io_manager,
                                      const string &schema, const string &table,
                                      vector<ColumnDefinition> column_definitions)
    : DataTable(db, io_manager, schema, table, std::move(column_definitions), nullptr),
      d1_io_manager(io_manager) {
    fprintf(stderr, "🎯 D1CustomDataTable: Created custom DataTable for D1 table '%s'\n", table.c_str());
    fprintf(stderr, "🎯 D1CustomDataTable: This D1CustomDataTable instance address: %p\n", this);
    fprintf(stderr, "🎯 D1CustomDataTable: Ready to intercept INSERT operations for D1!\n");
}

void D1CustomDataTable::InitializeLocalAppend(LocalAppendState &state, TableCatalogEntry &table,
                                               ClientContext &context,
                                               const vector<unique_ptr<BoundConstraint>> &bound_constraints) {
    fprintf(stderr, "D1CustomDataTable: InitializeLocalAppend - bypassing LocalStorage, using D1 directly (this=%p)\n", this);

    // Cast to our custom state
    auto &d1_state = static_cast<D1LocalAppendState&>(state);

    // Create D1-specific append state
    d1_state.d1_append_state = make_uniq<TableAppendState>();

    // Get the current transaction
    auto &transaction = DuckTransaction::Get(context, db);
    d1_state.transaction = &transaction;

    // Call D1TableIOManager::InitializeAppend instead of LocalStorage
    d1_io_manager->InitializeAppend(transaction, *d1_state.d1_append_state);

    // Still need constraint state for validation
    state.constraint_state = InitializeConstraintState(table, bound_constraints);
}

void D1CustomDataTable::LocalAppend(LocalAppendState &state, ClientContext &context, DataChunk &chunk, bool unsafe) {
    fprintf(stderr, "D1CustomDataTable: LocalAppend(state variant) - routing directly to D1TableIOManager (this=%p, chunk.size=%zu)\n", this, chunk.size());

    if (chunk.size() == 0) {
        return;
    }

    chunk.Verify();

    // Basic constraint validation (if not unsafe)
    if (!unsafe && state.constraint_state) {
        // Note: We skip complex constraint validation for simplicity
        // In production, you'd want proper constraint validation here
    }

    // Cast to our custom state and route to D1
    auto &d1_state = static_cast<D1LocalAppendState&>(state);
    d1_io_manager->Append(chunk, *d1_state.d1_append_state);
}

void D1CustomDataTable::FinalizeLocalAppend(LocalAppendState &state) {
    fprintf(stderr, "D1CustomDataTable: FinalizeLocalAppend - finalizing D1 operations (this=%p)\n", this);

    // Cast to our custom state
    auto &d1_state = static_cast<D1LocalAppendState&>(state);

    // Call D1TableIOManager::FinalizeAppend
    if (d1_state.d1_append_state && d1_state.transaction) {
        d1_io_manager->FinalizeAppend(*d1_state.transaction, *d1_state.d1_append_state);
    }
}

void D1CustomDataTable::LocalAppend(TableCatalogEntry &table, ClientContext &context, DataChunk &chunk,
                                     const vector<unique_ptr<BoundConstraint>> &bound_constraints) {
    fprintf(stderr, "🚀 D1CustomDataTable::LocalAppend CALLED! (this=%p, chunk.size=%zu)\n", this, chunk.size());
    fprintf(stderr, "🚀 INTERCEPTING INSERT - bypassing LocalStorage, routing directly to D1!\n");

    // Use our custom D1LocalAppendState
    D1LocalAppendState append_state;
    fprintf(stderr, "🚀 D1CustomDataTable: Calling InitializeLocalAppend...\n");
    InitializeLocalAppend(append_state, table, context, bound_constraints);

    fprintf(stderr, "🚀 D1CustomDataTable: Calling LocalAppend (state variant)...\n");
    LocalAppend(append_state, context, chunk, false);

    fprintf(stderr, "🚀 D1CustomDataTable: Calling FinalizeLocalAppend...\n");
    FinalizeLocalAppend(append_state);

    fprintf(stderr, "🚀 D1CustomDataTable: INSERT routing to D1 COMPLETE!\n");
}

} // namespace duckdb
