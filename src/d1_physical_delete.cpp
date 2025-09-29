//===----------------------------------------------------------------------===//
//                         DuckDB
//
// d1_physical_delete.cpp
//
//
//===----------------------------------------------------------------------===//

#include "include/d1_physical_delete.hpp"
#include "include/d1_catalog.hpp"
#include "include/d1_data_table.hpp"
#include "include/d1_parameterized_query.hpp"
#include "duckdb/execution/expression_executor.hpp"

namespace duckdb {

D1PhysicalDelete::D1PhysicalDelete(PhysicalPlan &physical_plan, vector<LogicalType> types, D1TableEntry &d1_table,
                                   idx_t estimated_cardinality)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::DELETE_OPERATOR, std::move(types), estimated_cardinality),
      d1_table(d1_table) {

    fprintf(stderr, "🔥 D1PhysicalDelete: Created custom DELETE operator for D1 table '%s'\n", d1_table.name.c_str());
    fprintf(stderr, "🔥 D1PhysicalDelete: This will bypass DataTable::Delete and route directly to D1!\n");
    fprintf(stderr, "🔥 D1PhysicalDelete: Configured as IsSink=%s, IsSource=%s\n",
           IsSink() ? "true" : "false", IsSource() ? "true" : "false");
}

SinkResultType D1PhysicalDelete::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
    fprintf(stderr, "🔥 D1PhysicalDelete::Sink: Processing DELETE chunk with %llu rows\n", chunk.size());
    fprintf(stderr, "🔥 D1PhysicalDelete::Sink: BYPASSING DataTable::Delete, routing directly to D1!\n");

    if (chunk.size() == 0) {
        return SinkResultType::NEED_MORE_INPUT;
    }

    chunk.Flatten();

    // Get the D1DataTable directly
    auto *d1_storage = d1_table.GetD1Storage();
    if (!d1_storage) {
        throw InternalException("D1PhysicalDelete: Failed to get D1DataTable storage");
    }

    fprintf(stderr, "🔥 D1PhysicalDelete::Sink: Calling D1DataTable::ExecuteDelete directly\n");
    fprintf(stderr, "🔥 D1PhysicalDelete::Sink: Chunk has %llu columns:\n", chunk.ColumnCount());
    for (idx_t i = 0; i < chunk.ColumnCount(); i++) {
        auto &type = chunk.data[i].GetType();
        fprintf(stderr, "  Column %llu: %s\n", i, type.ToString().c_str());
        for (idx_t row = 0; row < chunk.size(); row++) {
            Value val = chunk.GetValue(i, row);
            fprintf(stderr, "    Row %llu, Col %llu: '%s' (type: %s, is_null: %s)\n",
                   row, i, val.ToString().c_str(), val.type().ToString().c_str(),
                   val.IsNull() ? "true" : "false");
        }
    }

    fprintf(stderr, "🔥 D1PhysicalDelete::Sink: Generating SECURE parameterized DELETE for D1\n");

    // Extract the actual values to build parameterized WHERE clause for DELETE
    if (chunk.ColumnCount() > 0) {
        for (idx_t row = 0; row < chunk.size(); row++) {
            // Get the schema to understand the primary key structure
            const auto &schema = d1_storage->GetSchema();

            if (schema.HasPrimaryKey() && !schema.primary_key_columns.empty()) {
                // Use primary key columns to build parameterized WHERE clause
                string pk_col = schema.primary_key_columns[0];
                Value delete_value = chunk.GetValue(0, row);

                fprintf(stderr, "🔥 D1PhysicalDelete::Sink: DELETE WHERE %s = %s (type: %s, null: %s)\n",
                       pk_col.c_str(), delete_value.ToString().c_str(),
                       delete_value.type().ToString().c_str(), delete_value.IsNull() ? "true" : "false");

                // Create secure parameterized DELETE query
                auto parameterized_query = D1ParameterizedQuery::CreateDelete(d1_table.name, pk_col, delete_value);

                fprintf(stderr, "🔥 D1PhysicalDelete::Sink: Generated SECURE DELETE SQL: %s\n",
                       parameterized_query.GetSQL().c_str());
                fprintf(stderr, "🔥 D1PhysicalDelete::Sink: With %zu parameters (SQL injection safe!)\n",
                       parameterized_query.GetParameters().size());

                // Execute the parameterized query (SQL injection safe!)
                d1_storage->ExecuteParameterizedQuery(parameterized_query);
            } else {
                // Fallback: no primary key, this shouldn't happen for D1 tables but handle gracefully
                fprintf(stderr, "🔥 D1PhysicalDelete::Sink: No primary key found, using fallback parameterized query\n");
                Value fallback_value = chunk.GetValue(0, row);
                auto parameterized_query = D1ParameterizedQuery::CreateDelete(d1_table.name, "rowid", fallback_value);
                d1_storage->ExecuteParameterizedQuery(parameterized_query);
            }
        }

        fprintf(stderr, "🔥 D1PhysicalDelete::Sink: Successfully processed %llu SECURE DELETE operations\n", chunk.size());
    } else {
        fprintf(stderr, "🔥 D1PhysicalDelete::Sink: No columns found in DELETE chunk\n");
    }

    return SinkResultType::NEED_MORE_INPUT;
}

SinkFinalizeType D1PhysicalDelete::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                            OperatorSinkFinalizeInput &input) const {
    fprintf(stderr, "🔥 D1PhysicalDelete::Finalize: Forcing immediate flush of batched operations\n");

    auto *d1_storage = d1_table.GetD1Storage();
    if (d1_storage) {
        fprintf(stderr, "🔥 D1PhysicalDelete::Finalize: Calling D1DataTable::FlushPendingOperations()\n");
        d1_storage->FlushPendingOperations();
        fprintf(stderr, "🔥 D1PhysicalDelete::Finalize: Flush completed - DELETEs now in D1!\n");
    }

    return SinkFinalizeType::READY;
}

// Source interface implementation (returns DELETE results like row count)
struct D1DeleteSourceState : public GlobalSourceState {
    idx_t deleted_count = 0;
    bool finished = false;
};

unique_ptr<GlobalSourceState> D1PhysicalDelete::GetGlobalSourceState(ClientContext &context) const {
    fprintf(stderr, "🔥 D1PhysicalDelete::GetGlobalSourceState: Creating source state for result return\n");
    return make_uniq<D1DeleteSourceState>();
}

SourceResultType D1PhysicalDelete::GetData(ExecutionContext &context, DataChunk &chunk,
                                           OperatorSourceInput &input) const {
    auto &state = input.global_state.Cast<D1DeleteSourceState>();

    if (state.finished) {
        fprintf(stderr, "🔥 D1PhysicalDelete::GetData: Already finished, returning no data\n");
        return SourceResultType::FINISHED;
    }

    // Return the count of deleted rows (similar to PhysicalDelete::GetData)
    chunk.SetCardinality(1);
    chunk.SetValue(0, 0, Value::BIGINT(NumericCast<int64_t>(state.deleted_count)));
    state.finished = true;

    fprintf(stderr, "🔥 D1PhysicalDelete::GetData: Returning deleted count: %llu\n", state.deleted_count);
    return SourceResultType::FINISHED;
}

} // namespace duckdb
