#define DUCKDB_EXTENSION_MAIN

#include "cloudflare_d1_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/scalar_function.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
#include <duckdb/function/table_function.hpp>
#include <duckdb/common/vector_operations/unary_executor.hpp>
#include <duckdb/common/types/value.hpp>
#include <duckdb/parser/parsed_data/create_table_function_info.hpp>
#include <duckdb/common/types/uuid.hpp>
#include <duckdb/common/string_util.hpp>

// OpenSSL linked through vcpkg
#include <openssl/opensslv.h>
#include <curl/curl.h>
// no external json headers
#include "include/d1_client.hpp"
#include "include/d1_functions.hpp"
#include "include/d1_storage.hpp"
#include "duckdb/storage/storage_extension.hpp"

namespace duckdb {

static bool ShouldUseObjectQueryForSelect(const string &sql) {
    // Use object query only for specific meta queries that need object format
    auto lowered = StringUtil::Lower(sql);
    if (sql.find("/*object*/") != string::npos) return true;
    // PRAGMA statements often expect specific column metadata 
    if (StringUtil::StartsWith(lowered, "pragma")) return true;
    return false;
}

// --- Simple placeholder function
inline void CloudflareD1ScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &name_vector = args.data[0];
	UnaryExecutor::Execute<string_t, string_t>(name_vector, result, args.size(), [&](string_t name) {
		return StringVector::AddString(result, "CloudflareD1 " + name.GetString() + " 🐥");
	});
}

inline void CloudflareD1OpenSSLVersionScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &name_vector = args.data[0];
	UnaryExecutor::Execute<string_t, string_t>(name_vector, result, args.size(), [&](string_t name) {
		return StringVector::AddString(result, "CloudflareD1 " + name.GetString() + ", my linked OpenSSL version is " +
		                                           OPENSSL_VERSION_TEXT);
	});
}

// D1 client moved to d1_client.{hpp,cpp}

// --- Table function: d1_query(sql, named_params_json?, account_id, database_id, api_token, api_base_url?)
struct D1RawBindData : public FunctionData {
	std::string sql;
	CloudflareD1Config cfg;
	std::vector<LogicalType> return_types;
	std::vector<std::string> names;
	std::vector<CloudflareD1QueryParam> params;
	bool use_object = false;

	unique_ptr<FunctionData> Copy() const override {
		auto copy = make_uniq<D1RawBindData>();
		copy->sql = sql;
		copy->cfg = cfg;
		copy->return_types = return_types;
		copy->names = names;
		copy->params = params;
		copy->use_object = use_object;
		return copy;
	}
	bool Equals(const FunctionData &other_p) const override { return false; }
};

unique_ptr<FunctionData> D1RawBind(ClientContext &context, TableFunctionBindInput &input, vector<LogicalType> &return_types, vector<string> &names) {
	auto bind = make_uniq<D1RawBindData>();
	// args: sql, account_id, api_token, database_id
    if (input.inputs.size() < 4) {
        throw BinderException("d1_query requires 4 arguments: sql, account_id, api_token, database_id");
	}
	bind->sql = input.inputs[0].GetValue<string>();
    bind->cfg.account_id = input.inputs[1].GetValue<string>();
    bind->cfg.api_token = input.inputs[2].GetValue<string>();
    bind->cfg.database_id = input.inputs[3].GetValue<string>();
	bind->use_object = ShouldUseObjectQueryForSelect(bind->sql);
	// For bind, fetch one page to derive schema
	CloudflareD1Client client(bind->cfg);
	auto res = bind->use_object ? client.ObjectQuery(bind->sql, bind->params) : client.RawQuery(bind->sql, bind->params);
	if (!res.success) {
		throw BinderException("D1 query bind failed: %s", res.error.c_str());
	}
	if (res.columns.empty() && !res.rows.empty()) {
		// fabricate col names c0..cn
		size_t cols = res.rows[0].size();
		for (size_t i = 0; i < cols; i++) {
			bind->names.push_back("c" + to_string(i));
			bind->return_types.push_back(LogicalType::VARCHAR);
		}
	} else if (!res.columns.empty()) {
		for (auto &c : res.columns) {
			bind->names.push_back(c.name.empty() ? string("column") : c.name);
			// SQLite-like type mapping: detect common affinities
			auto type_lower = StringUtil::Lower(c.type);
			LogicalType lt = LogicalType::VARCHAR;
			if (type_lower.find("int") != string::npos) {
				lt = LogicalType::BIGINT;
			} else if (type_lower.find("char") != string::npos || type_lower.find("clob") != string::npos || type_lower.find("text") != string::npos) {
				lt = LogicalType::VARCHAR;
			} else if (type_lower.find("blob") != string::npos) {
				lt = LogicalType::BLOB;
			} else if (type_lower.find("real") != string::npos || type_lower.find("floa") != string::npos || type_lower.find("doub") != string::npos) {
				lt = LogicalType::DOUBLE;
			} else if (type_lower.find("bool") != string::npos) {
				lt = LogicalType::BOOLEAN;
			}
			bind->return_types.push_back(lt);
		}
	} else {
		// No metadata and no rows; return a single generic column
		bind->names.push_back("column");
		bind->return_types.push_back(LogicalType::VARCHAR);
	}
    return_types.clear();
    names.clear();
    return_types.reserve(bind->return_types.size());
    names.reserve(bind->names.size());
    for (auto &t : bind->return_types) return_types.push_back(t);
    for (auto &n : bind->names) names.push_back(n);
	return std::move(bind);
}

struct D1RawGlobalState : public GlobalTableFunctionState {
	CloudflareD1Client client;
	CloudflareD1QueryResult res;
	size_t row_idx = 0;
	bool use_object = false;
	explicit D1RawGlobalState(const CloudflareD1Config &cfg, const std::string &sql, const std::vector<CloudflareD1QueryParam> &params, bool use_object_p)
	    : client(cfg), use_object(use_object_p) {
		res = use_object ? client.ObjectQuery(sql, params) : client.RawQuery(sql, params);
	}
	idx_t MaxThreads() const override { return 1; }
};

unique_ptr<GlobalTableFunctionState> D1RawInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind = input.bind_data->Cast<D1RawBindData>();
	return make_uniq<D1RawGlobalState>(bind.cfg, bind.sql, bind.params, bind.use_object);
}

void D1RawFunc(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &state = data_p.global_state->Cast<D1RawGlobalState>();
	if (!state.res.success) {
		throw InvalidInputException("D1 raw query failed: %s", state.res.error.c_str());
	}
	idx_t out_cols = output.ColumnCount();
	idx_t count = 0;
	while (count < STANDARD_VECTOR_SIZE && state.row_idx < state.res.rows.size()) {
		auto &row = state.res.rows[state.row_idx++];
		for (idx_t c = 0; c < out_cols; c++) {
			const std::string cell = c < row.size() ? row[c] : std::string();
			output.SetValue(c, count, Value(cell));
		}
		count++;
	}
	output.SetCardinality(count);
}

// --- Catalog functions
// d1_tables(account_id, cloudflare_email, cloudflare_api_key, database_id) -> name VARCHAR, type VARCHAR
struct D1TablesBindData : public FunctionData {
	CloudflareD1Config cfg;
	unique_ptr<FunctionData> Copy() const override { return make_uniq<D1TablesBindData>(*this); }
	bool Equals(const FunctionData &other) const override { return false; }
};

unique_ptr<FunctionData> D1TablesBind(ClientContext &context, TableFunctionBindInput &input, vector<LogicalType> &return_types, vector<string> &names) {
    if (input.inputs.size() < 3) {
        throw BinderException("d1_tables requires: account_id, api_token, database_id");
	}
	auto bind = make_uniq<D1TablesBindData>();
    bind->cfg.account_id = input.inputs[0].GetValue<string>();
    bind->cfg.api_token = input.inputs[1].GetValue<string>();
    bind->cfg.database_id = input.inputs[2].GetValue<string>();
	return_types = {LogicalType::VARCHAR, LogicalType::VARCHAR};
	names = {"name", "type"};
	return std::move(bind);
}

void D1TablesFunc(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &bind = input.bind_data->Cast<D1TablesBindData>();
	CloudflareD1Client client(bind.cfg);
	// Query sqlite_master to list tables and views - use RawQuery for consistency
	auto res = client.RawQuery("SELECT name, type FROM sqlite_master WHERE type IN ('table','view') AND name NOT LIKE 'sqlite_%' ORDER BY 1", {});
	if (!res.success) {
		throw InvalidInputException("d1_tables failed: %s", res.error.c_str());
	}
	idx_t count = 0;
	for (auto &row : res.rows) {
		if (count == STANDARD_VECTOR_SIZE) break;
		std::string name = row.size() > 0 ? row[0] : std::string();
		std::string type = row.size() > 1 ? row[1] : std::string();
		output.SetValue(0, count, Value(name));
		output.SetValue(1, count, Value(type));
		count++;
	}
	output.SetCardinality(count);
}

// d1_columns(table_name, account_id, cloudflare_email, cloudflare_api_key, database_id) -> cid, name, type, notnull, dflt_value, pk
struct D1ColumnsBindData : public FunctionData {
	CloudflareD1Config cfg;
	std::string table;
	unique_ptr<FunctionData> Copy() const override { return make_uniq<D1ColumnsBindData>(*this); }
	bool Equals(const FunctionData &other) const override { return false; }
};

unique_ptr<FunctionData> D1ColumnsBind(ClientContext &context, TableFunctionBindInput &input, vector<LogicalType> &return_types, vector<string> &names) {
    if (input.inputs.size() < 4) {
        throw BinderException("d1_columns requires: table_name, account_id, api_token, database_id");
	}
	auto bind = make_uniq<D1ColumnsBindData>();
	bind->table = input.inputs[0].GetValue<string>();
    bind->cfg.account_id = input.inputs[1].GetValue<string>();
    bind->cfg.api_token = input.inputs[2].GetValue<string>();
    bind->cfg.database_id = input.inputs[3].GetValue<string>();
	return_types = {LogicalType::INTEGER, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::INTEGER, LogicalType::VARCHAR, LogicalType::INTEGER};
	names = {"cid", "name", "type", "notnull", "dflt_value", "pk"};
	return std::move(bind);
}

void D1ColumnsFunc(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &bind = input.bind_data->Cast<D1ColumnsBindData>();
	CloudflareD1Client client(bind.cfg);
	std::string sql = "PRAGMA table_info('" + bind.table + "')";
	auto res = client.RawQuery(sql, {});
	if (!res.success) {
		throw InvalidInputException("d1_columns failed: %s", res.error.c_str());
	}
	idx_t count = 0;
	for (auto &row : res.rows) {
		if (count == STANDARD_VECTOR_SIZE) break;
		// row is array; expected order: cid, name, type, notnull, dflt_value, pk in SQLite
		output.SetValue(0, count, Value::INTEGER(row.size() > 0 ? stoi(row[0]) : 0));
		output.SetValue(1, count, Value(row.size() > 1 ? row[1] : std::string()));
		output.SetValue(2, count, Value(row.size() > 2 ? row[2] : std::string()));
		output.SetValue(3, count, Value::INTEGER(row.size() > 3 ? stoi(row[3]) : 0));
		output.SetValue(4, count, Value(row.size() > 4 ? row[4] : std::string()));
		output.SetValue(5, count, Value::INTEGER(row.size() > 5 ? stoi(row[5]) : 0));
		count++;
	}
	output.SetCardinality(count);
}

// Emit CREATE VIEW statements to pseudo-attach a D1 schema under an alias
struct D1AttachBindData : public FunctionData {
	std::string alias;
	CloudflareD1Config cfg;
	unique_ptr<FunctionData> Copy() const override { return make_uniq<D1AttachBindData>(*this); }
	bool Equals(const FunctionData &other) const override { return false; }
};

unique_ptr<FunctionData> D1AttachBind(ClientContext &context, TableFunctionBindInput &input, vector<LogicalType> &return_types, vector<string> &names) {
    if (input.inputs.size() < 4) {
        throw BinderException("d1_attach_sql requires: alias, account_id, api_token, database_id");
	}
	auto bind = make_uniq<D1AttachBindData>();
	bind->alias = input.inputs[0].GetValue<string>();
    bind->cfg.account_id = input.inputs[1].GetValue<string>();
    bind->cfg.api_token = input.inputs[2].GetValue<string>();
    bind->cfg.database_id = input.inputs[3].GetValue<string>();
	return_types = {LogicalType::VARCHAR};
	names = {"sql"};
	return std::move(bind);
}

void D1AttachFunc(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &bind = input.bind_data->Cast<D1AttachBindData>();
	CloudflareD1Client client(bind.cfg);
	// Ensure schema exists
	std::string create_schema = "CREATE SCHEMA IF NOT EXISTS \"" + bind.alias + "\";";
	output.SetValue(0, 0, Value(create_schema));
	idx_t count = 1;
	// fetch tables - use RawQuery for consistency
	auto tables = client.RawQuery("SELECT name, type FROM sqlite_master WHERE type IN ('table','view') AND name NOT LIKE 'sqlite_%' ORDER BY 1", {});
	if (!tables.success) {
		throw InvalidInputException("d1_attach_sql failed: %s", tables.error.c_str());
	}
	for (auto &row : tables.rows) {
		if (count == STANDARD_VECTOR_SIZE) break;
		std::string name = row.size() > 0 ? row[0] : std::string();
		if (name.empty()) continue;
    // emit view selecting all columns from remote table using d1_query
    std::string sql = "CREATE OR REPLACE VIEW \"" + bind.alias + "\".\"" + name + "\" AS SELECT * FROM d1_query('SELECT * FROM \"" + name + "\"', '" + bind.cfg.account_id + "', '" + bind.cfg.api_token + "', '" + bind.cfg.database_id + "');";
		output.SetValue(0, count, Value(sql));
		count++;
	}
	output.SetCardinality(count);
}

// Execute function: returns number of changes
void D1ExecuteScalar(DataChunk &args, ExpressionState &state, Vector &result) {
    // args: sql, account_id, api_token, database_id
    if (args.ColumnCount() < 4) {
        throw InvalidInputException("d1_execute requires: sql, account_id, api_token, database_id");
    }
    string sql = args.data[0].GetValue(0).ToString();
    CloudflareD1Config cfg;
    cfg.account_id = args.data[1].GetValue(0).ToString();
    cfg.api_token = args.data[2].GetValue(0).ToString();
    cfg.database_id = args.data[3].GetValue(0).ToString();
	CloudflareD1Client client(cfg);
	auto res = client.ObjectQuery(sql, {});
	if (!res.success) {
		throw InvalidInputException("d1_execute failed: %s", res.error.c_str());
	}
	int64_t changes = res.changes;
    result.SetValue(0, Value::BIGINT(changes));
}

static void LoadInternal(ExtensionLoader &loader) {
	// Register a scalar function
	auto cloudflare_d1_scalar_function = ScalarFunction("cloudflare_d1", {LogicalType::VARCHAR}, LogicalType::VARCHAR, CloudflareD1ScalarFun);
	loader.RegisterFunction(cloudflare_d1_scalar_function);

	// Register another scalar function
	auto cloudflare_d1_openssl_version_scalar_function = ScalarFunction("cloudflare_d1_openssl_version", {LogicalType::VARCHAR},
	                                                            LogicalType::VARCHAR, CloudflareD1OpenSSLVersionScalarFun);
	loader.RegisterFunction(cloudflare_d1_openssl_version_scalar_function);

	RegisterD1QueryFunctions(loader);
	RegisterD1CatalogFunctions(loader);
	RegisterD1AttachFunctions(loader);
	RegisterD1BulkFunctions(loader);

    // Register storage extension for ATTACH ... (TYPE d1)
    auto &db = loader.GetDatabaseInstance();
    auto &config = DBConfig::GetConfig(db);
    config.storage_extensions["d1"] = CreateD1StorageExtension();
}

void CloudflareD1Extension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}
std::string CloudflareD1Extension::Name() {
	return "cloudflare_d1";
}

std::string CloudflareD1Extension::Version() const {
#ifdef EXT_VERSION_CLOUDFLARE_D1
	return EXT_VERSION_CLOUDFLARE_D1;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(cloudflare_d1, loader) {
	duckdb::LoadInternal(loader);
}
}
