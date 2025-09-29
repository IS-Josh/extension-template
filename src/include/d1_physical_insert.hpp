//===----------------------------------------------------------------------===//
//                         DuckDB
//
// d1_physical_insert.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "duckdb/execution/operator/persistent/physical_insert.hpp"

namespace duckdb {

// Forward declaration
class D1TableEntry;

//! Custom PhysicalInsert that routes INSERT operations directly to D1
class D1PhysicalInsert : public PhysicalInsert {
public:
    D1PhysicalInsert(PhysicalPlan &physical_plan, vector<LogicalType> types, D1TableEntry &d1_table,
                     vector<unique_ptr<BoundConstraint>> bound_constraints, idx_t estimated_cardinality);

    //! The D1 table to insert into
    D1TableEntry &d1_table;

public:
    // Override Sink and Finalize to route to D1 and ensure immediate flushing
    SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
    SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                              OperatorSinkFinalizeInput &input) const override;
};


} // namespace duckdb
