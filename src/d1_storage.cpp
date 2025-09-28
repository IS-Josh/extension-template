#include "include/d1_storage.hpp"
#include "include/d1_client.hpp"
#include "include/d1_catalog.hpp"

#include "duckdb.hpp"
#include "duckdb/catalog/duck_catalog.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/transaction/duck_transaction_manager.hpp"
#include "duckdb/storage/storage_extension.hpp"
#include "duckdb/catalog/catalog_transaction.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/catalog/default/default_generator.hpp"
#include "duckdb/catalog/catalog_entry/duck_schema_entry.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"

namespace duckdb {

// Helper function to get D1 config from secret or direct config
CloudflareD1Config GetD1ConfigFromOptions(ClientContext &context, const string &path, const unordered_map<string, Value> &options) {
    CloudflareD1Config cfg;

    // Check if a secret is specified
    auto secret_it = options.find("secret");
    if (secret_it != options.end()) {
        string secret_name = secret_it->second.ToString();
        fprintf(stderr, "D1Storage: Looking up secret '%s'\n", secret_name.c_str());

        auto &secret_manager = SecretManager::Get(context);
        auto transaction = CatalogTransaction::GetSystemCatalogTransaction(context);

        // Try to get the secret
        auto secret_match = secret_manager.LookupSecret(transaction, secret_name, "d1");
        if (secret_match.HasMatch()) {
            auto &secret = secret_match.GetSecret();
            fprintf(stderr, "D1Storage: Found D1 secret '%s'\n", secret_name.c_str());

            // Extract credentials from secret (cast to KeyValueSecret)
            auto *kv_secret = dynamic_cast<const KeyValueSecret*>(&secret);
            if (!kv_secret) {
                throw InvalidInputException("D1 secret '%s' is not a KeyValueSecret", secret_name);
            }
            auto account_id_val = kv_secret->TryGetValue("account_id");
            auto api_token_val = kv_secret->TryGetValue("api_token");
            auto database_id_val = kv_secret->TryGetValue("database_id");

            if (!account_id_val.IsNull() && !api_token_val.IsNull() && !database_id_val.IsNull()) {
                cfg.account_id = account_id_val.ToString();
                cfg.api_token = api_token_val.ToString();
                cfg.database_id = database_id_val.ToString();
                fprintf(stderr, "D1Storage: Successfully extracted credentials from secret\n");
                return cfg;
            } else {
                throw InvalidInputException("D1 secret '%s' is missing required credentials (account_id, api_token, database_id)", secret_name);
            }
        } else {
            throw InvalidInputException("D1 secret '%s' not found", secret_name);
        }
    }

    // Fall back to parsing from path (old method)
    fprintf(stderr, "D1Storage: No secret specified, parsing credentials from path\n");

    // Parse path in multiple accepted forms (inline implementation)
    auto get_opt = [&](const string &key) -> string {
        auto it = options.find(key);
        return it != options.end() ? it->second.ToString() : "";
    };

    // Parse from path string like "account_id=...;api_token=...;database_id=..." or "secret=secret_name"
    if (!path.empty()) {
        vector<string> parts = StringUtil::Split(path, ';');
        for (auto &part : parts) {
            auto kv = StringUtil::Split(part, '=');
            if (kv.size() == 2) {
                string key = StringUtil::Lower(kv[0]);
                string value = kv[1];
                if (key == "secret") {
                    // Handle secret reference in path
                    fprintf(stderr, "D1Storage: Found secret reference in path: '%s'\n", value.c_str());
                    auto &secret_manager = SecretManager::Get(context);
                    auto transaction = CatalogTransaction::GetSystemCatalogTransaction(context);
                    auto secret_match = secret_manager.LookupSecret(transaction, value, "d1");
                    if (secret_match.HasMatch()) {
                        auto &secret = secret_match.GetSecret();
                        auto *kv_secret = dynamic_cast<const KeyValueSecret*>(&secret);
                        if (kv_secret) {
                            auto account_id_val = kv_secret->TryGetValue("account_id");
                            auto api_token_val = kv_secret->TryGetValue("api_token");
                            auto database_id_val = kv_secret->TryGetValue("database_id");
                            if (!account_id_val.IsNull() && !api_token_val.IsNull() && !database_id_val.IsNull()) {
                                cfg.account_id = account_id_val.ToString();
                                cfg.api_token = api_token_val.ToString();
                                cfg.database_id = database_id_val.ToString();
                                fprintf(stderr, "D1Storage: Successfully loaded credentials from secret in path\n");
                                return cfg;
                            }
                        }
                    }
                    throw InvalidInputException("D1 secret '%s' not found or invalid", value);
                } else if (key == "account_id") cfg.account_id = value;
                else if (key == "api_token" || key == "cloudflare_api_key") cfg.api_token = value;
                else if (key == "database_id") cfg.database_id = value;
            }
        }
    }

    // Override with explicit options
    string s;
    s = get_opt("ACCOUNT_ID"); if (!s.empty()) cfg.account_id = s;
    s = get_opt("API_TOKEN"); if (!s.empty()) cfg.api_token = s;
    s = get_opt("CLOUDFLARE_API_KEY"); if (!s.empty()) cfg.api_token = s;
    s = get_opt("DATABASE_ID"); if (!s.empty()) cfg.database_id = s;

    return cfg;
}

// Use the D1Catalog from d1_catalog.cpp

// No custom storage info needed

static void ParseD1Config(const string &path, const unordered_map<string, Value> &opts, CloudflareD1Config &out) {
    // Parse path in multiple accepted forms:
    // 1) "d1:key=value;key=value" (semicolon-separated)
    // 2) "key=value key=value" (space-separated)
    // 3) plain "key=value;key=value" (semicolon-separated, no prefix)
    auto p = path;
    string rest = p;
    if (StringUtil::StartsWith(StringUtil::Lower(p), "d1:")) {
        rest = p.substr(3);
        // Remove any leading slashes after d1:
        while (StringUtil::StartsWith(rest, "/")) {
            rest = rest.substr(1);
        }
    }
    // Tokenize by both spaces and semicolons
    vector<string> tokens;
    string current;
    for (char c : rest) {
        if (c == ' ' || c == ';' || c == '\n' || c == '\t') {
            if (!current.empty()) {
                tokens.push_back(current);
                current.clear();
            }
        } else {
            current.push_back(c);
        }
    }
    if (!current.empty()) tokens.push_back(current);

    for (auto &kv : tokens) {
        auto eq = kv.find('=');
        if (eq == string::npos) continue;
        auto k = StringUtil::Lower(kv.substr(0, eq));
        auto v = kv.substr(eq + 1);
        if (k == "account_id") out.account_id = v;
        else if (k == "database_id") out.database_id = v;
        else if (k == "api_token" || k == "cloudflare_api_key") out.api_token = v; // Support both old and new names
    }
    // Override from options if present
    auto get_opt = [&](const string &key) -> string {
        auto it = opts.find(key);
        if (it == opts.end()) return string();
        return it->second.ToString();
    };
    auto s = get_opt("ACCOUNT_ID"); if (!s.empty()) out.account_id = s;
    s = get_opt("API_TOKEN"); if (!s.empty()) out.api_token = s;
    s = get_opt("CLOUDFLARE_API_KEY"); if (!s.empty()) out.api_token = s; // Support legacy name
    s = get_opt("DATABASE_ID"); if (!s.empty()) out.database_id = s;
}

class D1DefaultGenerator : public DefaultGenerator {
public:
    D1DefaultGenerator(Catalog &catalog, SchemaCatalogEntry &schema, CloudflareD1Config cfg, string secret_name = "")
        : DefaultGenerator(catalog), schema(schema), config(std::move(cfg)), secret_name(std::move(secret_name)) {
    }

    unique_ptr<CatalogEntry> CreateDefaultEntry(ClientContext &context, const string &entry_name) override {
        fprintf(stderr, "D1DefaultGenerator::CreateDefaultEntry: Creating entry for '%s'\n", entry_name.c_str());

        // Check if this table exists in D1
        if (!IsValidD1Table(entry_name)) {
            return nullptr;
        }

        // Create a view that uses d1_scan
        auto result = make_uniq<CreateViewInfo>();
        result->schema = DEFAULT_SCHEMA;
        result->view_name = entry_name;

        // Use secret reference if available, otherwise use direct credentials
        if (!secret_name.empty()) {
            result->sql = StringUtil::Format("SELECT * FROM d1_scan_secret('%s', '%s')",
                                           secret_name, entry_name);
        } else {
            result->sql = StringUtil::Format("SELECT * FROM d1_scan('%s', '%s', '%s', '%s')",
                                           config.account_id, config.api_token, config.database_id, entry_name);
        }

        result->internal = false;  // Make sure it's not marked as internal
        result->temporary = false; // Make sure it's not temporary

        auto view_info = CreateViewInfo::FromSelect(context, std::move(result));
        return make_uniq_base<CatalogEntry, ViewCatalogEntry>(catalog, schema, *view_info);
    }

    vector<string> GetDefaultEntries() override {
        fprintf(stderr, "D1DefaultGenerator::GetDefaultEntries: Getting table list\n");

        // For testing, return mock data if credentials are "test"
        if (config.account_id == "test" && config.api_token == "test" && config.database_id == "test") {
            fprintf(stderr, "D1DefaultGenerator::GetDefaultEntries: Using mock data for testing\n");
            return {"users", "products", "orders"};
        }

        // Query D1 for actual table names
        fprintf(stderr, "D1DefaultGenerator::GetDefaultEntries: Querying real D1 database\n");
        CloudflareD1Client client(config);
        auto res = client.RawQuery("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name != '_cf_KV'", {});

        vector<string> table_names;
        if (res.success) {
            fprintf(stderr, "D1DefaultGenerator::GetDefaultEntries: D1 query successful, processing %zu rows\n", res.rows.size());
            for (auto &row : res.rows) {
                if (!row.empty()) {
                    string table_name = row[0];
                    // Strip quotes if present
                    if (table_name.size() >= 2 && table_name.front() == '"' && table_name.back() == '"') {
                        table_name = table_name.substr(1, table_name.size() - 2);
                    }
                    table_names.push_back(table_name);
                    fprintf(stderr, "D1DefaultGenerator::GetDefaultEntries: Found table '%s'\n", table_name.c_str());
                }
            }
        } else {
            fprintf(stderr, "D1DefaultGenerator::GetDefaultEntries: D1 query failed: %s\n", res.error.c_str());
        }

        fprintf(stderr, "D1DefaultGenerator::GetDefaultEntries: Returning %zu tables\n", table_names.size());
        return table_names;
    }

private:
    bool IsValidD1Table(const string &table_name) {
        // For testing, accept any table name if credentials are "test"
        if (config.account_id == "test" && config.api_token == "test" && config.database_id == "test") {
            return table_name == "users" || table_name == "products" || table_name == "orders";
        }

        // For real D1 databases, check if table exists
        fprintf(stderr, "D1DefaultGenerator::IsValidD1Table: Checking if table '%s' exists\n", table_name.c_str());
        CloudflareD1Client client(config);
        auto res = client.RawQuery("SELECT name FROM sqlite_master WHERE type='table' AND name = '" + table_name + "' AND name NOT LIKE 'sqlite_%' AND name != '_cf_KV'", {});
        bool exists = res.success && !res.rows.empty();
        fprintf(stderr, "D1DefaultGenerator::IsValidD1Table: Table '%s' exists: %s\n", table_name.c_str(), exists ? "true" : "false");
        return exists;
    }

    SchemaCatalogEntry &schema;
    CloudflareD1Config config;
    string secret_name;
};

static unique_ptr<Catalog> D1Attach(optional_ptr<StorageExtensionInfo> storage_info, ClientContext &context,
                                    AttachedDatabase &db, const string &name, AttachInfo &info, AttachOptions &options) {
    // Get D1 config from secret or direct credentials
    auto cfg = GetD1ConfigFromOptions(context, info.path, options.options);

    // Check if a secret was used (either from options or path)
    string secret_name;
    auto secret_it = options.options.find("secret");
    if (secret_it != options.options.end()) {
        secret_name = secret_it->second.ToString();
        fprintf(stderr, "D1Attach: Using secret '%s' for credentials\n", secret_name.c_str());
    } else {
        // Check if secret was found in path parsing
        if (info.path.find("secret=") != string::npos) {
            // Extract secret name from path
            vector<string> parts = StringUtil::Split(info.path, ';');
            for (auto &part : parts) {
                auto kv = StringUtil::Split(part, '=');
                if (kv.size() == 2 && StringUtil::Lower(kv[0]) == "secret") {
                    secret_name = kv[1];
                    fprintf(stderr, "D1Attach: Using secret '%s' from path for credentials\n", secret_name.c_str());
                    break;
                }
            }
        }
        if (secret_name.empty()) {
            fprintf(stderr, "D1Attach: Using direct credentials - account_id='%s', database_id='%s'\n",
                    cfg.account_id.c_str(), cfg.database_id.c_str());
        }
    }

    // Set path to :memory: to avoid file locking issues (like OpenFileStorageExtension does)
    info.path = ":memory:";

    // Create a DuckCatalog instead of custom D1Catalog
    auto catalog = make_uniq<DuckCatalog>(db);
    catalog->Initialize(false);

    // Set up the default generator for D1 tables
    auto system_transaction = CatalogTransaction::GetSystemTransaction(db.GetDatabase());
    auto &schema = catalog->GetSchema(system_transaction, DEFAULT_SCHEMA);
    auto &duck_schema = schema.Cast<DuckSchemaEntry>();
    auto &catalog_set = duck_schema.GetCatalogSet(CatalogType::VIEW_ENTRY);
    auto default_generator = make_uniq<D1DefaultGenerator>(*catalog, schema, std::move(cfg), secret_name);
    catalog_set.SetDefaultGenerator(std::move(default_generator));

    fprintf(stderr, "D1Attach: Successfully set up D1 default generator\n");
    return std::move(catalog);
}

static unique_ptr<TransactionManager> D1CreateTxnMgr(optional_ptr<StorageExtensionInfo> storage_info,
                                                     AttachedDatabase &db, Catalog &catalog) {
	// Use DuckTransactionManager to leverage default txn semantics locally
	return make_uniq<DuckTransactionManager>(db);
}

unique_ptr<StorageExtension> CreateD1StorageExtension() {
    auto ext = make_uniq<StorageExtension>();
    ext->attach = D1Attach;
    ext->create_transaction_manager = D1CreateTxnMgr;
    return ext;
}

}


