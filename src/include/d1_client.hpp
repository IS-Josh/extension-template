#pragma once

#include <memory>
#include <string>
#include <vector>

namespace duckdb {

struct CloudflareD1Config {
	std::string account_id;
	std::string database_id;
	std::string api_token;  // Bearer token for D1 API
};

struct CloudflareD1QueryParam {
	std::string name;
	std::string value; // send as string for now; D1 will coerce
};

struct CloudflareD1QueryResultColumnMeta {
	std::string name;
	std::string type;
};

struct CloudflareD1QueryResult {
	bool success = false;
	std::string error;
	std::vector<CloudflareD1QueryResultColumnMeta> columns;
	std::vector<std::vector<std::string>> rows; // raw endpoint returns arrays; coerce to string
	int64_t changes = 0;
};

class CloudflareD1Client {
public:
	explicit CloudflareD1Client(CloudflareD1Config config);
	~CloudflareD1Client();

	CloudflareD1QueryResult RawQuery(const std::string &sql, const std::vector<CloudflareD1QueryParam> &params);
	CloudflareD1QueryResult ObjectQuery(const std::string &sql, const std::vector<CloudflareD1QueryParam> &params);
	const CloudflareD1Config& GetConfig() const;

private:
	struct Impl;
	std::unique_ptr<Impl> impl;
};

}


