#include "include/d1_catalog.hpp"
#include "include/d1_client.hpp"

#include "duckdb/catalog/duck_catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/catalog/catalog_transaction.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// D1Schema implementation (very thin)
//===--------------------------------------------------------------------===//
D1Schema::D1Schema(Catalog &catalog, CloudflareD1Config cfg, const string &name)
    : SchemaCatalogEntry(catalog, name), config(std::move(cfg)) {
}

//===--------------------------------------------------------------------===//
// D1TableEntry implementation
//===--------------------------------------------------------------------===//
class D1ScanData : public FunctionData {
public:
    D1ScanData(CloudflareD1Config cfg, string tbl) : config(std::move(cfg)), table(std::move(tbl)) {}
    CloudflareD1Config config;
    string table;

    unique_ptr<FunctionData> Copy() const override { return make_uniq<D1ScanData>(config, table); }
    bool Equals(const FunctionData &other) const override { return false; }
};

static unique_ptr<FunctionData> D1ScanBind(ClientContext &context, TableFunctionBindInput &input,
                                           vector<LogicalType> &return_types, vector<string> &names) {
    auto &data = input.extra_info->Cast<D1ScanData>();
    // fetch schema via PRAGMA table_info
    CloudflareD1Client client(data.config);
    auto pragma_res = client.RawQuery("PRAGMA table_info('" + data.table + "')", {});
    if (!pragma_res.success) {
        throw BinderException("D1 scan bind failed: %s", pragma_res.error.c_str());
    }
    for (auto &row : pragma_res.rows) {
        if (row.size() < 2) continue;
        names.push_back(row[1]); // name
        return_types.push_back(LogicalType::VARCHAR); // simple mapping
    }
    return make_uniq<D1ScanData>(data.config, data.table);
}

struct D1ScanState : public FunctionOperatorData {
    CloudflareD1QueryResult res;
    idx_t row_idx = 0;
};

static unique_ptr<FunctionOperatorData> D1ScanInit(ClientContext &context, const FunctionData *bind_data_p,
                                                   const vector<column_t> &columns, TableFilterSet *filters) {
    auto &bind = bind_data_p->Cast<D1ScanData>();
    CloudflareD1Client client(bind.config);
    auto res = client.RawQuery("SELECT * FROM '" + bind.table + "'", {});
    auto state = make_uniq<D1ScanState>();
    state->res = std::move(res);
    return std::move(state);
}

static void D1ScanFunc(ClientContext &context, const FunctionData *bind_data_p, FunctionOperatorData *state_p,
                       DataChunk &output) {
    auto &state = state_p->Cast<D1ScanState>();
    if (!state.res.success) {
        throw InvalidInputException("D1 scan failed: %s", state.res.error.c_str());
    }
    idx_t cols = output.ColumnCount();
    idx_t count = 0;
    while (count < STANDARD_VECTOR_SIZE && state.row_idx < state.res.rows.size()) {
        auto &row = state.res.rows[state.row_idx++];
        for (idx_t c = 0; c < cols; c++) {
            string cell = c < row.size() ? row[c] : string();
            output.SetValue(c, count, Value(cell));
        }
        count++;
    }
    output.SetCardinality(count);
}

D1TableEntry::D1TableEntry(Catalog &catalog, SchemaCatalogEntry &schema, const string &table_name,
                           CloudflareD1Config cfg)
    : TableCatalogEntry(catalog, schema, *CreateTableInfo::GetDummy()), config(std::move(cfg)) {
    name = table_name;
}

TableFunction D1TableEntry::GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) {
    auto extra = make_uniq<D1ScanData>(config, name);
    TableFunction tf("d1_table_scan", {}, D1ScanFunc, D1ScanBind, D1ScanInit);
    tf.extra_info = std::move(extra);
    bind_data = tf.extra_info->Copy();
    return tf;
}

//===--------------------------------------------------------------------===//
// D1Catalog implementation
//===--------------------------------------------------------------------===//
D1Catalog::D1Catalog(AttachedDatabase &db, CloudflareD1Config cfg, string db_name_p)
    : DuckCatalog(db), config(std::move(cfg)), db_name(std::move(db_name_p)) {}

void D1Catalog::Initialize(optional_ptr<ClientContext> context, bool load_builtin) {
    // create default schema
    auto &txn = CatalogTransaction::GetSystemCatalogTransaction(*this);
    CreateSchemaInfo sinfo; sinfo.schema = Catalog::GetDefaultSchema();
    sinfo.on_conflict = OnCreateConflict::IGNORE_ON_CONFLICT;
    auto schema_entry = make_uniq<D1Schema>(*this, config, sinfo.schema);
    this->schemas.CreateEntry(txn, sinfo.schema, std::move(schema_entry));

    // discover tables
    CloudflareD1Client client(config);
    auto res = client.RawQuery("SELECT name, type FROM sqlite_master WHERE type IN ('table','view') AND name NOT LIKE 'sqlite_%'", {});
    if (!res.success) {
        return; // ignore
    }
    auto schema = schemas.GetSchema(txn, sinfo.schema);
    for (auto &row : res.rows) {
        if (row.empty()) continue;
        string tbl = row[0];
        auto tbl_entry = make_uniq<D1TableEntry>(*this, *schema, tbl, config);
        schema->CreateTable(txn, *tbl_entry);
    }
}

} // namespace duckdb
