#include "include/d1_filter_pushdown.hpp"
#include "include/d1_type_mapping.hpp"

namespace duckdb {

static bool ExprIsSimple(const Expression &expr) {
    if (expr.type == ExpressionType::COMPARE_EQUAL ||
        expr.type == ExpressionType::COMPARE_NOTEQUAL ||
        expr.type == ExpressionType::COMPARE_LESSTHAN ||
        expr.type == ExpressionType::COMPARE_LESSTHANOREQUALTO ||
        expr.type == ExpressionType::COMPARE_GREATERTHAN ||
        expr.type == ExpressionType::COMPARE_GREATERTHANOREQUALTO) {
        return true;
    }
    if (expr.type == ExpressionType::BOUND_COLUMN_REF ||
        expr.type == ExpressionType::VALUE_CONSTANT) {
        return true;
    }
    return false;
}

bool D1CanPush(Expression *expr_p, D1RawBindData &bind, string &out_sql) {
    if (!expr_p) return false;
    auto &expr = *expr_p;
    // Support n-ary AND/OR
    if (expr.type == ExpressionType::CONJUNCTION_AND || expr.type == ExpressionType::CONJUNCTION_OR) {
        auto &cj_expr = expr.Cast<BoundConjunctionExpression>();
        string combined;
        string joiner = expr.type == ExpressionType::CONJUNCTION_AND ? " AND " : " OR ";
        for (idx_t i = 0; i < cj_expr.children.size(); i++) {
            string part;
            if (!D1CanPush(cj_expr.children[i].get(), bind, part)) return false;
            if (i) combined += joiner;
            combined += "(" + part + ")";
        }
        out_sql = combined;
        return true;
    }
    // Support NOT simple_predicate
    if (expr.type == ExpressionType::OPERATOR_NOT) {
        auto &not_expr = expr.Cast<BoundOperatorExpression>();
        if (not_expr.children.size() != 1) return false;
        string inner_sql;
        if (!D1CanPush(not_expr.children[0].get(), bind, inner_sql)) return false;
        out_sql = "NOT (" + inner_sql + ")";
        return true;
    }
    // Currently skip IS NULL / IS NOT NULL due to bound representation differences
    if (!ExprIsSimple(expr)) return false;
    auto &binary = expr.Cast<BoundComparisonExpression>();
    Expression *lhs = binary.left.get();
    Expression *rhs = binary.right.get();
    if (lhs->type != ExpressionType::BOUND_COLUMN_REF || rhs->type != ExpressionType::VALUE_CONSTANT) return false;
    auto &colref = lhs->Cast<BoundColumnRefExpression>();
    auto &constant = rhs->Cast<BoundConstantExpression>();
    string col_name = bind.names[colref.binding.column_index];
    string op;
    switch (expr.type) {
        case ExpressionType::COMPARE_EQUAL: op = "="; break;
        case ExpressionType::COMPARE_NOTEQUAL: op = "!="; break;
        case ExpressionType::COMPARE_LESSTHAN: op = "<"; break;
        case ExpressionType::COMPARE_LESSTHANOREQUALTO: op = "<="; break;
        case ExpressionType::COMPARE_GREATERTHAN: op = ">"; break;
        case ExpressionType::COMPARE_GREATERTHANOREQUALTO: op = ">="; break;
        default: return false;
    }
    out_sql = "\"" + col_name + "\" " + op + " ?";
    CloudflareD1QueryParam param;
    param.name = "filter_param_" + to_string(bind.filter_params.size()+1);
    param.value = constant.value.ToString();
    bind.filter_params.push_back(param);
    return true;
}

void D1PushdownComplexFilter(ClientContext &ctx, LogicalGet &get, FunctionData *bind_data, vector<unique_ptr<Expression>> &exprs) {
    auto &bind = bind_data->Cast<D1RawBindData>();
    vector<unique_ptr<Expression>> keep;
    string where_sql;
    for (auto &expr : exprs) {
        string sql_part;
        if (D1CanPush(expr.get(), bind, sql_part)) {
            if (!where_sql.empty()) where_sql += " AND ";
            where_sql += sql_part;
        } else {
            keep.push_back(std::move(expr));
        }
    }
    exprs = std::move(keep);
    if (!where_sql.empty()) {
        bind.where_clause = where_sql;
    }
}

} // namespace duckdb
