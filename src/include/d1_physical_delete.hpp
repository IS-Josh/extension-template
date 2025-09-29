//===----------------------------------------------------------------------===//
//                         DuckDB
//
// d1_physical_delete.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "duckdb/execution/physical_operator.hpp"

namespace duckdb {

// Forward declaration
class D1TableEntry;

//! Custom PhysicalDelete that routes DELETE operations directly to D1
class D1PhysicalDelete : public PhysicalOperator {
public:
    D1PhysicalDelete(PhysicalPlan &physical_plan, vector<LogicalType> types, D1TableEntry &d1_table,
                     idx_t estimated_cardinality);

    //! The D1 table to delete from
    D1TableEntry &d1_table;

public:
    // Override sink methods for processing deletes
    SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
    SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                              OperatorSinkFinalizeInput &input) const override;

    // Override source methods for returning results (like PhysicalDelete)
    unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override;
    SourceResultType GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const override;

    // DELETE operators are both sink AND source (like PhysicalDelete)
    bool IsSink() const override { return true; }
    bool IsSource() const override { return true; }
};

} // namespace duckdb
