#include "include/d1_secret.hpp"
#include "include/d1_client.hpp"

#include "duckdb.hpp"
#include "duckdb/main/secret/secret.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/common/exception.hpp"

namespace duckdb {

class D1Secret : public KeyValueSecret {
public:
    D1Secret(const string &account_id, const string &api_token, const string &database_id, const string &name)
        : KeyValueSecret({}, "d1", "config", name) {
        secret_map["account_id"] = Value(account_id);
        secret_map["api_token"] = Value(api_token);
        secret_map["database_id"] = Value(database_id);

        // Mark sensitive keys for redaction
        redact_keys.insert("api_token");
    }

    CloudflareD1Config GetConfig() const {
        CloudflareD1Config config;
        config.account_id = TryGetValue("account_id").ToString();
        config.api_token = TryGetValue("api_token").ToString();
        config.database_id = TryGetValue("database_id").ToString();
        return config;
    }
};

unique_ptr<BaseSecret> CreateD1Secret(ClientContext &context, CreateSecretInput &input) {
    // Extract required parameters
    string account_id, api_token, database_id;

    for (const auto &option : input.options) {
        if (option.first == "account_id") {
            account_id = option.second.ToString();
        } else if (option.first == "api_token") {
            api_token = option.second.ToString();
        } else if (option.first == "database_id") {
            database_id = option.second.ToString();
        }
    }

    if (account_id.empty()) {
        throw InvalidInputException("D1 secret requires 'account_id' parameter");
    }
    if (api_token.empty()) {
        throw InvalidInputException("D1 secret requires 'api_token' parameter");
    }
    if (database_id.empty()) {
        throw InvalidInputException("D1 secret requires 'database_id' parameter");
    }

    return make_uniq<D1Secret>(account_id, api_token, database_id, input.name);
}

void RegisterD1SecretFunctions(ExtensionLoader &loader) {
    // Register the D1 secret type
    SecretType secret_type;
    secret_type.name = "d1";
    secret_type.deserializer = nullptr; // We'll implement this if needed for persistence
    secret_type.default_provider = "config";
    loader.RegisterSecretType(secret_type);

    // Register the D1 secret function
    CreateSecretFunction secret_function;
    secret_function.secret_type = "d1";
    secret_function.provider = "config";
    secret_function.function = CreateD1Secret;

    // Define the named parameters for the secret
    secret_function.named_parameters["account_id"] = LogicalType::VARCHAR;
    secret_function.named_parameters["api_token"] = LogicalType::VARCHAR;
    secret_function.named_parameters["database_id"] = LogicalType::VARCHAR;

    loader.RegisterFunction(secret_function);
}

} // namespace duckdb
