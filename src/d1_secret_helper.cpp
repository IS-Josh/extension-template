//===----------------------------------------------------------------------===//
//                         DuckDB
//
// d1_secret_helper.cpp
//
//
//===----------------------------------------------------------------------===//

#include "include/d1_secret_helper.hpp"
#include "include/d1_catalog.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

CloudflareD1Config D1SecretHelper::GetConfigFromSecret(ClientContext &context, const string &secret_name) {
    // For now, throw an error indicating this feature is not yet implemented
    // The secret integration requires more complex handling of DuckDB's secret system
    throw NotImplementedException("D1 secret integration in helper functions is not yet implemented.\n"
                                 "Use the manual credential functions: d1_update(table, set, where, account, token, db)\n"
                                 "Secret integration will be added in a future update.");
}

optional_ptr<CloudflareD1Config> D1SecretHelper::GetConfigFromTableRef(ClientContext &context,
                                                                       const string &catalog_name,
                                                                       const string &schema_name,
                                                                       const string &table_name) {
    try {
        auto &db_manager = DatabaseManager::Get(context);
        auto database = db_manager.GetDatabase(context, catalog_name);
        if (!database) {
            return nullptr;
        }

        auto &catalog = database->GetCatalog();
        auto schema_entry = catalog.GetSchema(context, schema_name, OnEntryNotFound::RETURN_NULL);
        if (!schema_entry) {
            return nullptr;
        }

        auto transaction = CatalogTransaction::GetSystemCatalogTransaction(context);
        auto table_entry = schema_entry->GetEntry(transaction, CatalogType::TABLE_ENTRY, table_name);
        if (!table_entry) {
            return nullptr;
        }

        // Check if this is a D1TableEntry
        auto d1_table = dynamic_cast<D1TableEntry*>(table_entry.get());
        if (!d1_table) {
            return nullptr;
        }

        // Return a pointer to the config (we need to be careful about lifetime)
        return const_cast<CloudflareD1Config*>(&d1_table->GetD1Config());

    } catch (const std::exception &e) {
        fprintf(stderr, "D1SecretHelper::GetConfigFromTableRef: Error: %s\n", e.what());
        return nullptr;
    }
}

bool D1SecretHelper::IsD1Table(ClientContext &context, const string &catalog_name,
                               const string &schema_name, const string &table_name) {
    return GetConfigFromTableRef(context, catalog_name, schema_name, table_name) != nullptr;
}

std::tuple<string, string, string> D1SecretHelper::ParseTableReference(const string &table_ref) {
    // Parse table reference in format: catalog.schema.table or schema.table or table
    vector<string> parts = StringUtil::Split(table_ref, '.');

    if (parts.size() == 3) {
        return std::make_tuple(parts[0], parts[1], parts[2]);
    } else if (parts.size() == 2) {
        return std::make_tuple("main", parts[0], parts[1]);
    } else if (parts.size() == 1) {
        return std::make_tuple("main", "main", parts[0]);
    } else {
        throw InvalidInputException("Invalid table reference format: '%s'. Expected 'table', 'schema.table', or 'catalog.schema.table'", table_ref.c_str());
    }
}

} // namespace duckdb
