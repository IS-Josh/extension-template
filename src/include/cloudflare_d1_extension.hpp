#pragma once

#include "duckdb.hpp"
#include <memory>
#include <string>
#include <vector>

namespace duckdb {

class CloudflareD1Extension : public Extension {
public:
	void Load(ExtensionLoader &db) override;
	std::string Name() override;
	std::string Version() const override;
};

// Forward declarations moved to dedicated headers
struct CloudflareD1Config;
struct CloudflareD1QueryParam;
struct CloudflareD1QueryResultColumnMeta;
struct CloudflareD1QueryResult;
class CloudflareD1Client;

} // namespace duckdb
