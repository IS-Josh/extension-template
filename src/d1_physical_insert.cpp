//===----------------------------------------------------------------------===//
//                         DuckDB
//
// d1_physical_insert.cpp
//
//
//===----------------------------------------------------------------------===//

#include "include/d1_physical_insert.hpp"
#include "include/d1_catalog.hpp"
#include "include/d1_data_table.hpp"
#include "duckdb/execution/expression_executor.hpp"

namespace duckdb {

D1PhysicalInsert::D1PhysicalInsert(PhysicalPlan &physical_plan, vector<LogicalType> types, D1TableEntry &d1_table,
                                   vector<unique_ptr<BoundConstraint>> bound_constraints, idx_t estimated_cardinality)
    : PhysicalInsert(physical_plan, std::move(types), d1_table, std::move(bound_constraints),
                     vector<unique_ptr<Expression>>{}, vector<PhysicalIndex>{}, vector<LogicalType>{},
                     estimated_cardinality, false, false, OnConflictAction::THROW, nullptr, nullptr, {}, {}, false),
      d1_table(d1_table) {

    fprintf(stderr, "🔥 D1PhysicalInsert: Created custom INSERT operator for D1 table '%s'\n", d1_table.name.c_str());
    fprintf(stderr, "🔥 D1PhysicalInsert: This will bypass DataTable::LocalAppend and route directly to D1!\n");
}


SinkResultType D1PhysicalInsert::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
    fprintf(stderr, "🔥 D1PhysicalInsert::Sink: Processing INSERT chunk with %llu rows\n", chunk.size());
    fprintf(stderr, "🔥 D1PhysicalInsert::Sink: BYPASSING DataTable::LocalAppend, routing directly to D1!\n");

    if (chunk.size() == 0) {
        return SinkResultType::NEED_MORE_INPUT;
    }

    chunk.Flatten();

    // Get the D1DataTable directly and call ExecuteInsert
    auto *d1_storage = d1_table.GetD1Storage();
    if (!d1_storage) {
        throw InternalException("D1PhysicalInsert: Failed to get D1DataTable storage");
    }

    fprintf(stderr, "🔥 D1PhysicalInsert::Sink: Calling D1DataTable::ExecuteInsert directly\n");

    // This is the key - we bypass DataTable::LocalAppend completely and route directly to D1
    d1_storage->ExecuteInsert(chunk);

    fprintf(stderr, "🔥 D1PhysicalInsert::Sink: Successfully processed %llu rows\n", chunk.size());

    return SinkResultType::NEED_MORE_INPUT;
}

SinkFinalizeType D1PhysicalInsert::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                            OperatorSinkFinalizeInput &input) const {
    fprintf(stderr, "🔥 D1PhysicalInsert::Finalize: Forcing immediate flush of batched operations\n");

    // Get the D1DataTable and force flush any pending operations
    auto *d1_storage = d1_table.GetD1Storage();
    if (d1_storage) {
        fprintf(stderr, "🔥 D1PhysicalInsert::Finalize: Calling D1DataTable::FlushPendingOperations()\n");
        d1_storage->FlushPendingOperations();
        fprintf(stderr, "🔥 D1PhysicalInsert::Finalize: Flush completed - INSERTs now in D1!\n");
    }

    return SinkFinalizeType::READY;
}

} // namespace duckdb
