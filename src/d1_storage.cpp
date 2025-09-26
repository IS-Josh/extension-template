#include "include/d1_storage.hpp"
#include "include/d1_client.hpp"

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

namespace duckdb {

// Minimal D1 Catalog: delegates reads to table function and writes to execute
class D1Catalog : public DuckCatalog {
public:
	explicit D1Catalog(AttachedDatabase &db, CloudflareD1Config cfg, string db_name)
	    : DuckCatalog(db), config(std::move(cfg)), name(std::move(db_name)) {}
	string GetCatalogType() override { return "d1"; }

public:
	void Initialize(optional_ptr<ClientContext> context, bool load_builtin) override {
		DuckCatalog::Initialize(load_builtin);
		// Note: Cannot create views during initialization due to DuckDB constraints
		// Remote tables will be accessible via manual view creation or utility functions
	}

private:
	CloudflareD1Config config;
	string name;
};

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

static unique_ptr<Catalog> D1Attach(optional_ptr<StorageExtensionInfo> storage_info, ClientContext &context,
                                    AttachedDatabase &db, const string &name, AttachInfo &info, AttachOptions &options) {
    CloudflareD1Config cfg;
    ParseD1Config(info.path, options.options, cfg);
    return make_uniq<D1Catalog>(db, cfg, name);
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


