
#pragma once
#include "duckdb.hpp"
#include "d1_client.hpp"

namespace duckdb {

struct D1RawBindData : public TableFunctionData {
    CloudflareD1Config cfg;
    std::string sql;
    std::vector<CloudflareD1QueryParam> params;
    bool use_object = false;

    std::vector<std::string> names;
    std::vector<LogicalType> return_types;
    std::vector<idx_t> logical_to_physical;

    // Additional fields used by filter pushdown/table scans
    std::string table_name;                                    // target table
    std::vector<CloudflareD1QueryParam> filter_params;         // params from pushed filters
    std::string where_clause;                                  // parameterized WHERE

    unique_ptr<FunctionData> Copy() const override {
        return unique_ptr_cast<D1RawBindData, FunctionData>(make_uniq<D1RawBindData>(*this));
    }
    bool Equals(const FunctionData &) const override { return false; }
};

} // namespace duckdb
