#include "include/d1_catalog.hpp"
#include "include/d1_client.hpp"
#include "include/d1_type_mapping.hpp"
#include "duckdb/catalog/default/default_schemas.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"

#include "duckdb/catalog/duck_catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/catalog/catalog_entry/duck_schema_entry.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/catalog/catalog_transaction.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/catalog/catalog_entry.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "include/d1_type_mapping.hpp"
#include <set>

namespace duckdb {


//===--------------------------------------------------------------------===//
//===--------------------------------------------------------------------===//
// D1TableSet implementation
//===--------------------------------------------------------------------===//
D1TableSet::D1TableSet(Catalog &catalog, CloudflareD1Config cfg)
    : CatalogSet(catalog), config(std::move(cfg)) {
}

void D1TableSet::LoadEntries(ClientContext &context) {
    // This method is not currently used
    // Table access is handled via the d1_scan function
    fprintf(stderr, "D1TableSet::LoadEntries: Not implemented - use d1_scan function instead\n");
}

//===--------------------------------------------------------------------===//
// D1Schema implementation (very thin)
//===--------------------------------------------------------------------===//
//===--------------------------------------------------------------------===//
D1Schema::D1Schema(Catalog &catalog, CloudflareD1Config cfg, const string &name)
    : SchemaCatalogEntry(catalog, *make_uniq<CreateSchemaInfo>()),
      config(std::move(cfg)),
      tables(catalog, config),
      indexes(catalog),
      table_functions(catalog),
      copy_functions(catalog),
      pragma_functions(catalog),
      functions(catalog),
      sequences(catalog),
      collations(catalog),
      types(catalog) {
    this->name = name;
}

// Implement required pure virtual methods
void D1Schema::Scan(ClientContext &context, CatalogType type, const std::function<void(CatalogEntry &)> &callback) {
    auto &set = GetCatalogSet(type);
    set.Scan(context, callback);
}

void D1Schema::Scan(CatalogType type, const std::function<void(CatalogEntry &)> &callback) {
    auto &set = GetCatalogSet(type);
    set.Scan(callback);
}

optional_ptr<CatalogEntry> D1Schema::CreateIndex(CatalogTransaction transaction, CreateIndexInfo &info, TableCatalogEntry &table) {
    throw NotImplementedException("D1 schema does not support creating indexes");
}

optional_ptr<CatalogEntry> D1Schema::CreateFunction(CatalogTransaction transaction, CreateFunctionInfo &info) {
    throw NotImplementedException("D1 schema does not support creating functions");
}

optional_ptr<CatalogEntry> D1Schema::CreateTable(CatalogTransaction transaction, BoundCreateTableInfo &info) {
    throw NotImplementedException("D1 schema does not support creating tables");
}

optional_ptr<CatalogEntry> D1Schema::CreateView(CatalogTransaction transaction, CreateViewInfo &info) {
    throw NotImplementedException("D1 schema does not support creating views");
}

optional_ptr<CatalogEntry> D1Schema::CreateSequence(CatalogTransaction transaction, CreateSequenceInfo &info) {
    throw NotImplementedException("D1 schema does not support creating sequences");
}

optional_ptr<CatalogEntry> D1Schema::CreateTableFunction(CatalogTransaction transaction, CreateTableFunctionInfo &info) {
    throw NotImplementedException("D1 schema does not support creating table functions");
}

optional_ptr<CatalogEntry> D1Schema::CreateCopyFunction(CatalogTransaction transaction, CreateCopyFunctionInfo &info) {
    throw NotImplementedException("D1 schema does not support creating copy functions");
}

optional_ptr<CatalogEntry> D1Schema::CreatePragmaFunction(CatalogTransaction transaction, CreatePragmaFunctionInfo &info) {
    throw NotImplementedException("D1 schema does not support creating pragma functions");
}

optional_ptr<CatalogEntry> D1Schema::CreateCollation(CatalogTransaction transaction, CreateCollationInfo &info) {
    throw NotImplementedException("D1 schema does not support creating collations");
}

optional_ptr<CatalogEntry> D1Schema::CreateType(CatalogTransaction transaction, CreateTypeInfo &info) {
    throw NotImplementedException("D1 schema does not support creating types");
}

optional_ptr<CatalogEntry> D1Schema::LookupEntry(CatalogTransaction transaction, const EntryLookupInfo &lookup_info) {
    return GetCatalogSet(lookup_info.GetCatalogType()).GetEntry(transaction, lookup_info.GetEntryName());
}

void D1Schema::DropEntry(ClientContext &context, DropInfo &info) {
    // For now, we'll implement a simplified version
    // In a full implementation, we would need to manage our own catalog sets
    // For Phase 4, we'll just mark the entry as deleted
    // This is a placeholder implementation
}

void D1Schema::Alter(CatalogTransaction transaction, AlterInfo &info) {
    throw NotImplementedException("D1 schema does not support altering entries");
}

CatalogSet &D1Schema::GetCatalogSet(CatalogType type) {
    switch (type) {
    case CatalogType::VIEW_ENTRY:
    case CatalogType::TABLE_ENTRY:
        return tables;
    case CatalogType::INDEX_ENTRY:
        return indexes;
    case CatalogType::TABLE_FUNCTION_ENTRY:
    case CatalogType::TABLE_MACRO_ENTRY:
        return table_functions;
    case CatalogType::COPY_FUNCTION_ENTRY:
        return copy_functions;
    case CatalogType::PRAGMA_FUNCTION_ENTRY:
        return pragma_functions;
    case CatalogType::AGGREGATE_FUNCTION_ENTRY:
    case CatalogType::SCALAR_FUNCTION_ENTRY:
    case CatalogType::MACRO_ENTRY:
        return functions;
    case CatalogType::SEQUENCE_ENTRY:
        return sequences;
    case CatalogType::COLLATION_ENTRY:
        return collations;
    case CatalogType::TYPE_ENTRY:
        return types;
    default:
        throw InternalException("Unsupported catalog type in D1Schema");
    }
}

optional_ptr<CatalogEntry> D1Schema::AddEntry(CatalogTransaction transaction, unique_ptr<StandardEntry> entry, OnCreateConflict on_conflict) {
    auto entry_name = entry->name;
    auto entry_type = entry->type;

    // Get the appropriate catalog set
    auto &set = GetCatalogSet(entry_type);

    if (on_conflict == OnCreateConflict::IGNORE_ON_CONFLICT) {
        auto old_entry = set.GetEntry(transaction, entry_name);
        if (old_entry) {
            return nullptr;
        }
    }

    if (on_conflict == OnCreateConflict::REPLACE_ON_CONFLICT) {
        // CREATE OR REPLACE: first try to drop the entry
        auto old_entry = set.GetEntry(transaction, entry_name);
        if (old_entry) {
            if (old_entry->type != entry_type) {
                throw CatalogException("Existing object %s is of type %s, trying to replace with type %s", entry_name,
                                       CatalogTypeToString(old_entry->type), CatalogTypeToString(entry_type));
            }
            (void)set.DropEntry(transaction, entry_name, false, entry->internal);
        }
    }

    // Create the entry in the catalog set
    LogicalDependencyList dependencies;
    if (!set.CreateEntry(transaction, entry_name, std::move(entry), dependencies)) {
        return nullptr;
    }

    // Return the created entry
    return set.GetEntry(transaction, entry_name);
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
    // Get the table name from the input
    if (input.inputs.empty()) {
        throw BinderException("D1 scan bind requires table name");
    }
    string table_name = input.inputs[0].GetValue<string>();

    // For now, create a simple schema - we'll get the real schema from the table entry
    names.push_back("value");
    return_types.push_back(LogicalType::VARCHAR);

    return make_uniq<D1ScanData>(CloudflareD1Config{}, table_name);
}

struct D1ScanState : public GlobalTableFunctionState {
    CloudflareD1QueryResult res;
    idx_t row_idx = 0;

    idx_t MaxThreads() const override { return 1; }
};

static unique_ptr<GlobalTableFunctionState> D1ScanInit(ClientContext &context, TableFunctionInitInput &input) {
    auto &bind = input.bind_data->Cast<D1ScanData>();
    CloudflareD1Client client(bind.config);
    auto res = client.RawQuery("SELECT * FROM '" + bind.table + "'", {});
    auto state = make_uniq<D1ScanState>();
    state->res = std::move(res);
    return std::move(state);
}

static void D1ScanFunc(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    auto &state = data_p.global_state->Cast<D1ScanState>();
    if (!state.res.success) {
        throw InvalidInputException("D1 scan failed: %s", state.res.error.c_str());
    }
    idx_t cols = output.ColumnCount();
    idx_t count = 0;
    while (count < STANDARD_VECTOR_SIZE && state.row_idx < state.res.rows.size()) {
        auto &row = state.res.rows[state.row_idx++];
        for (idx_t c = 0; c < cols; c++) {
            string cell = c < row.size() ? row[c] : string();
            // Use proper type conversion instead of treating everything as string
            Value converted_value = ConvertStringToValue(cell, output.data[c].GetType());
            output.SetValue(c, count, converted_value);
        }
        count++;
    }
    output.SetCardinality(count);
}

unique_ptr<CreateTableInfo> D1TableEntry::MakeCreateInfo(Catalog &catalog, SchemaCatalogEntry &schema,
                                                         const string &table_name, const CloudflareD1Config &cfg) {
    // Create a basic CreateTableInfo
    auto create_info = make_uniq<CreateTableInfo>(schema, table_name);

    // Fetch column information from D1
    CloudflareD1Client client(cfg);
    auto pragma_res = client.RawQuery("PRAGMA table_info('" + table_name + "')", {});

    if (!pragma_res.success) {
        // If we can't get table info, create a dummy table with a single VARCHAR column
        create_info->columns.AddColumn(ColumnDefinition("value", LogicalType::VARCHAR));
    } else {
        // Build columns from PRAGMA table_info result
        // PRAGMA table_info returns: cid, name, type, notnull, dflt_value, pk
        for (auto &row : pragma_res.rows) {
            if (row.size() < 2) continue;

            string col_name = row[1]; // name
            string col_type = row.size() > 2 ? row[2] : "TEXT"; // type
            bool not_null = row.size() > 3 ? (row[3] == "1") : false; // notnull
            string default_value = row.size() > 4 ? row[4] : ""; // dflt_value
            bool is_pk = row.size() > 5 ? (row[5] == "1") : false; // pk

            // Map SQLite types to DuckDB types using our comprehensive mapping function
            LogicalType duck_type = MapSQLiteTypeToDuckDB(col_type);

            // Create column definition
            ColumnDefinition col_def(col_name, duck_type);

            // Set default value if specified
            if (!default_value.empty()) {
                // For now, we'll skip default values as they require more complex parsing
                // This could be enhanced in the future to parse SQLite default expressions
            }

            create_info->columns.AddColumn(std::move(col_def));

            // Note: NOT NULL and PRIMARY KEY constraints are handled separately in DuckDB
            // For now, we'll skip constraint handling as it requires more complex integration
            // This could be enhanced in the future to add proper constraints
        }

        // Fetch foreign key information if available
        auto fk_res = client.RawQuery("PRAGMA foreign_key_list('" + table_name + "')", {});
        if (fk_res.success) {
            // PRAGMA foreign_key_list returns: id, seq, table, from, to, on_update, on_delete, match
            // For now, we'll skip foreign key constraints as they require more complex handling
            // This could be enhanced in the future to add foreign key constraints
        }
    }

    // Return CreateTableInfo
    return create_info;
}

D1TableEntry::D1TableEntry(Catalog &catalog, SchemaCatalogEntry &schema, const string &table_name,
                           CloudflareD1Config cfg)
    : TableCatalogEntry(catalog, schema, *MakeCreateInfo(catalog, schema, table_name, cfg)), config(std::move(cfg)) {
    name = table_name;
    fprintf(stderr, "D1TableEntry: Created table entry for '%s'\n", table_name.c_str());
}


TableFunction D1TableEntry::GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) {
    bind_data = make_uniq<D1ScanData>(config, name);
    TableFunction tf("d1_table_scan", {LogicalType::VARCHAR}, D1ScanFunc, D1ScanBind, D1ScanInit);
    return tf;
}

unique_ptr<BaseStatistics> D1TableEntry::GetStatistics(ClientContext &context, column_t column_id) {
    // For remote tables, we don't have local statistics
    // Return nullptr to indicate no statistics available
    return nullptr;
}

TableStorageInfo D1TableEntry::GetStorageInfo(ClientContext &context) {
    // For remote tables, we don't have local storage info
    TableStorageInfo info;
    info.cardinality = 0; // Unknown cardinality for remote tables
    return info;
}

//===--------------------------------------------------------------------===//
// D1Catalog implementation
//===--------------------------------------------------------------------===//
D1Catalog::D1Catalog(AttachedDatabase &db, CloudflareD1Config cfg, string db_name_p)
    : DuckCatalog(db), config(std::move(cfg)), db_name(std::move(db_name_p)) {}


void D1Catalog::Initialize(optional_ptr<ClientContext> context, bool load_builtin) {
    DuckCatalog::Initialize(load_builtin);

    if (!context) {
        return; // Cannot initialize without context
    }

    // create default schema
    auto txn = CatalogTransaction::GetSystemCatalogTransaction(*context);
    CreateSchemaInfo sinfo;
    sinfo.schema = Catalog::GetDefaultSchema();
    sinfo.on_conflict = OnCreateConflict::IGNORE_ON_CONFLICT;

    // Create the default schema (this will create a DuckSchemaEntry)
    CreateSchema(txn, sinfo);

    // Get the schema we just created
    auto schema = LookupSchema(txn, EntryLookupInfo(CatalogType::SCHEMA_ENTRY, sinfo.schema), OnEntryNotFound::RETURN_NULL);
    if (!schema) {
        fprintf(stderr, "D1Catalog::Initialize: Schema lookup failed\n");
        return; // Schema creation failed
    }
    fprintf(stderr, "D1Catalog::Initialize: Schema lookup successful\n");

    // For now, let's test the d1_scan function directly
    // This will allow users to query tables like: SELECT * FROM d1_scan('account_id', 'api_token', 'database_id', 'table_name')
    fprintf(stderr, "D1Catalog::Initialize: D1 tables can be accessed using d1_scan function\n");
    fprintf(stderr, "D1Catalog::Initialize: Example: SELECT * FROM d1_scan('%s', '%s', '%s', 'table_name')\n",
            config.account_id.c_str(), config.api_token.c_str(), config.database_id.c_str());
}

//===--------------------------------------------------------------------===//
// Phase 4: Catalog refresh implementation
//===--------------------------------------------------------------------===//

void D1Catalog::RefreshCatalog(ClientContext &context) {
    // Simplified implementation for Phase 4
    // In a full implementation, this would refresh the entire catalog
    // For now, we'll just re-initialize the catalog
    Initialize(&context, false);
}

void D1Catalog::RefreshTable(ClientContext &context, const string &table_name) {
    // Simplified implementation for Phase 4
    // In a full implementation, this would refresh a specific table
    // For now, we'll just refresh the entire catalog
    RefreshCatalog(context);
}

} // namespace duckdb
