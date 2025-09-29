//===----------------------------------------------------------------------===//
//                         DuckDB
//
// d1_physical_update.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "duckdb/execution/physical_operator.hpp"

namespace duckdb {

// Forward declaration
class D1TableEntry;

//! Custom PhysicalUpdate that routes UPDATE operations directly to D1
class D1PhysicalUpdate : public PhysicalOperator {
public:
    D1PhysicalUpdate(PhysicalPlan &physical_plan, vector<LogicalType> types, D1TableEntry &d1_table,
                     vector<PhysicalIndex> columns, vector<unique_ptr<Expression>> expressions,
                     vector<unique_ptr<Expression>> bound_defaults,
                     vector<unique_ptr<BoundConstraint>> bound_constraints, idx_t estimated_cardinality);

    //! The D1 table to update
    D1TableEntry &d1_table;
    //! The columns being updated
    vector<PhysicalIndex> columns;

public:
    // Override sink methods for processing updates
    SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
    SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                              OperatorSinkFinalizeInput &input) const override;

    // Override source methods for returning results (like PhysicalUpdate)
    unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override;
    SourceResultType GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const override;

    // UPDATE operators are both sink AND source (like PhysicalUpdate)
    bool IsSink() const override { return true; }
    bool IsSource() const override { return true; }  // CRITICAL FIX!
};

} // namespace duckdb
