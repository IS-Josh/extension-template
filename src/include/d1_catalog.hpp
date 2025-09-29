// D1 catalog declarations
#pragma once

#include "duckdb.hpp"
#include "d1_client.hpp"
#include "duckdb/catalog/duck_catalog.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/parser/parsed_data/create_index_info.hpp"
#include "duckdb/parser/parsed_data/create_function_info.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/parser/parsed_data/create_sequence_info.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/parser/parsed_data/create_copy_function_info.hpp"
#include "duckdb/parser/parsed_data/create_pragma_function_info.hpp"
#include "duckdb/parser/parsed_data/create_collation_info.hpp"
#include "duckdb/parser/parsed_data/create_type_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/parser/parsed_data/alter_info.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/catalog/catalog_set.hpp"

namespace duckdb {

// Forward declarations
class D1DataTable;
class D1TableIOManager;

class D1Schema;
class D1TableEntry;
class D1TableSet;

class D1Catalog : public DuckCatalog {
public:
    D1Catalog(AttachedDatabase &db, CloudflareD1Config cfg, string db_name);
    string GetCatalogType() override { return "cloudflare_d1"; }

    void Initialize(optional_ptr<ClientContext> context, bool load_builtin) override;

    CloudflareD1Config &GetConfig() { return config; }

    void CreateD1Views(ClientContext &context);


    // Phase 4: Catalog refresh methods
    void RefreshCatalog(ClientContext &context);
    void RefreshTable(ClientContext &context, const string &table_name);

    // Override PlanInsert to use custom D1PhysicalInsert
    PhysicalOperator &PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner, LogicalInsert &op,
                                 optional_ptr<PhysicalOperator> plan) override;

private:
    CloudflareD1Config config;
    string db_name;
};

// Base class for D1 catalog sets - similar to PostgresCatalogSet
class D1CatalogSet : public CatalogSet {
public:
    D1CatalogSet(Catalog &catalog, CloudflareD1Config cfg);

    // Try to load entries if not already loaded
    void TryLoadEntries(ClientContext &context);

    // Load all entries
    virtual void LoadEntries(ClientContext &context) = 0;

    // Clear all entries
    void ClearEntries();

    // Check if entries are loaded
    bool EntriesLoaded() const;

    // Use the base CatalogSet implementation for CreateEntry

protected:
    CloudflareD1Config config;
    std::atomic<bool> entries_loaded;
    std::mutex load_lock;
};

// D1TableSet - manages D1 table entries like PostgresTableSet
class D1TableSet : public D1CatalogSet {
public:
    D1TableSet(Catalog &catalog, CloudflareD1Config cfg);

    // Load table metadata from D1
    void LoadEntries(ClientContext &context) override;

    // Create a table entry from D1 metadata
    unique_ptr<TableCatalogEntry> CreateTableEntry(ClientContext &context, SchemaCatalogEntry &schema,
                                                  const string &table_name);
};

class D1Schema : public SchemaCatalogEntry {
public:
    D1Schema(Catalog &catalog, CloudflareD1Config cfg, const string &name);

    CloudflareD1Config &GetConfig() { return config; }

    // Get catalog set for a given type
    CatalogSet &GetCatalogSet(CatalogType type);

    // Get the D1TableSet for table management
    D1TableSet& GetTableSet() { return tables; }

    // Load all entries for this schema
    void LoadEntries(ClientContext &context);

    // Implement required pure virtual methods
    void Scan(ClientContext &context, CatalogType type, const std::function<void(CatalogEntry &)> &callback) override;
    void Scan(CatalogType type, const std::function<void(CatalogEntry &)> &callback) override;
    optional_ptr<CatalogEntry> CreateIndex(CatalogTransaction transaction, CreateIndexInfo &info, TableCatalogEntry &table) override;
    optional_ptr<CatalogEntry> CreateFunction(CatalogTransaction transaction, CreateFunctionInfo &info) override;
    optional_ptr<CatalogEntry> CreateTable(CatalogTransaction transaction, BoundCreateTableInfo &info) override;
    optional_ptr<CatalogEntry> CreateView(CatalogTransaction transaction, CreateViewInfo &info) override;
    optional_ptr<CatalogEntry> CreateSequence(CatalogTransaction transaction, CreateSequenceInfo &info) override;
    optional_ptr<CatalogEntry> CreateTableFunction(CatalogTransaction transaction, CreateTableFunctionInfo &info) override;
    optional_ptr<CatalogEntry> CreateCopyFunction(CatalogTransaction transaction, CreateCopyFunctionInfo &info) override;
    optional_ptr<CatalogEntry> CreatePragmaFunction(CatalogTransaction transaction, CreatePragmaFunctionInfo &info) override;
    optional_ptr<CatalogEntry> CreateCollation(CatalogTransaction transaction, CreateCollationInfo &info) override;
    optional_ptr<CatalogEntry> CreateType(CatalogTransaction transaction, CreateTypeInfo &info) override;
    optional_ptr<CatalogEntry> LookupEntry(CatalogTransaction transaction, const EntryLookupInfo &lookup_info) override;
    void DropEntry(ClientContext &context, DropInfo &info) override;
    void Alter(CatalogTransaction transaction, AlterInfo &info) override;
    optional_ptr<CatalogEntry> AddEntry(CatalogTransaction transaction, unique_ptr<StandardEntry> entry, OnCreateConflict on_conflict);

private:
    CloudflareD1Config config;

    // Catalog sets for managing entries
    D1TableSet tables;
    CatalogSet indexes;
    CatalogSet table_functions;
    CatalogSet copy_functions;
    CatalogSet pragma_functions;
    CatalogSet functions;
    CatalogSet sequences;
    CatalogSet collations;
    CatalogSet types;
};

// Forward declarations
struct D1RawBindData;

class D1TableEntry : public TableCatalogEntry {
public:
    D1TableEntry(Catalog &catalog, SchemaCatalogEntry &schema, const string &table_name, CloudflareD1Config cfg);

    TableFunction GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) override;

    // Implement pure virtual methods from TableCatalogEntry
    unique_ptr<BaseStatistics> GetStatistics(ClientContext &context, column_t column_id) override;
    TableStorageInfo GetStorageInfo(ClientContext &context) override;
    DataTable &GetStorage() override;

    static unique_ptr<CreateTableInfo> MakeCreateInfo(Catalog &catalog, SchemaCatalogEntry &schema,
                                                      const string &table_name, const CloudflareD1Config &cfg);

    // Get the column information for this table
    void GetColumnInfo(ClientContext &context);

    // Phase A: Get D1DataTable for internal operations
    D1DataTable* GetD1Storage();

    // Phase D: Get D1 config for helper functions
    const CloudflareD1Config& GetD1Config() const { return config; }

private:
    CloudflareD1Config config;

    // D1DataTable for direct storage operations (Phase A & B)
    mutable unique_ptr<D1DataTable> storage;

    // DataTable with custom D1TableIOManager (Phase C)
    mutable shared_ptr<DataTable> data_table;

    // Original D1/SQLite types for each column
    vector<string> d1_types;
};

} // namespace duckdb
