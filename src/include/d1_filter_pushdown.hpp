#pragma once

#include "duckdb.hpp"
#include "d1_raw_bind_data.hpp"
// Ensure bound/planner expression classes are visible
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"

namespace duckdb {

// Return true if expression is simple column OP constant that D1 can execute
bool D1CanPush(Expression *expr, D1RawBindData &bind, string &out_sql);

// DuckDB callback signature used by pushdown_complex_filter
void D1PushdownComplexFilter(ClientContext &ctx, LogicalGet &get, FunctionData *bind_data, vector<unique_ptr<Expression>> &exprs);

} // namespace duckdb
