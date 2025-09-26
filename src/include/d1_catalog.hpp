// D1 catalog declarations
#pragma once

#include "duckdb.hpp"
#include "d1_client.hpp"

namespace duckdb {

class D1Schema;
class D1TableEntry;

class D1Catalog : public DuckCatalog {
public:
    D1Catalog(AttachedDatabase &db, CloudflareD1Config cfg, string db_name);
    string GetCatalogType() override { return "d1"; }

    void Initialize(optional_ptr<ClientContext> context, bool load_builtin) override;

    CloudflareD1Config &GetConfig() { return config; }

private:
    CloudflareD1Config config;
    string db_name;
};

class D1Schema : public SchemaCatalogEntry {
public:
    D1Schema(Catalog &catalog, CloudflareD1Config cfg, const string &name);

    CloudflareD1Config &GetConfig() { return config; }

private:
    CloudflareD1Config config;
};

class D1TableEntry : public TableCatalogEntry {
public:
    D1TableEntry(Catalog &catalog, SchemaCatalogEntry &schema, const string &table_name, CloudflareD1Config cfg);

    TableFunction GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) override;

private:
    CloudflareD1Config config;
};

} // namespace duckdb
