//===----------------------------------------------------------------------===//
//                         DuckDB
//
// d1_secret_helper.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "duckdb/main/client_context.hpp"
#include "d1_client.hpp"

namespace duckdb {

//! Helper class for working with D1 secrets in helper functions
class D1SecretHelper {
public:
    //! Get D1 config from a secret name
    static CloudflareD1Config GetConfigFromSecret(ClientContext &context, const string &secret_name);

    //! Get D1 config from table reference (catalog.schema.table format)
    static optional_ptr<CloudflareD1Config> GetConfigFromTableRef(ClientContext &context,
                                                                  const string &catalog_name,
                                                                  const string &schema_name,
                                                                  const string &table_name);

    //! Check if a table reference points to a D1 table
    static bool IsD1Table(ClientContext &context, const string &catalog_name,
                         const string &schema_name, const string &table_name);

    //! Extract catalog, schema, table from a fully qualified table name
    static std::tuple<string, string, string> ParseTableReference(const string &table_ref);
};

} // namespace duckdb
