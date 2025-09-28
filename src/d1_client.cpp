#include "include/d1_client.hpp"

#include <curl/curl.h>
#include <nlohmann/json.hpp>

namespace duckdb {

struct CloudflareD1Client::Impl {
	CloudflareD1Config config;

	static size_t WriteCallback(void *contents, size_t size, size_t nmemb, void *userp) {
		size_t total = size * nmemb;
		auto *buffer = reinterpret_cast<std::string *>(userp);
		buffer->append(reinterpret_cast<const char *>(contents), total);
		return total;
	}

    CloudflareD1QueryResult PostJson(const std::string &path, const std::string &body) {
		CloudflareD1QueryResult res;
		CURL *curl = curl_easy_init();
		if (!curl) {
			res.success = false;
			res.error = "Failed to init CURL";
			return res;
		}
        std::string url = "https://api.cloudflare.com/client/v4";
		url += path;
		std::string response_body;
		struct curl_slist *headers = nullptr;
        headers = curl_slist_append(headers, "Content-Type: application/json");
        // Use Bearer token per Cloudflare API docs
        std::string auth = std::string("Authorization: Bearer ") + config.api_token;
        headers = curl_slist_append(headers, auth.c_str());
		curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
		curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
		curl_easy_setopt(curl, CURLOPT_POST, 1L);
		curl_easy_setopt(curl, CURLOPT_POSTFIELDS, body.c_str());
		curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteCallback);
		curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response_body);

		CURLcode rc = curl_easy_perform(curl);
		if (rc != CURLE_OK) {
			res.success = false;
			res.error = curl_easy_strerror(rc);
			curl_slist_free_all(headers);
			curl_easy_cleanup(curl);
			return res;
		}
		curl_slist_free_all(headers);
		curl_easy_cleanup(curl);

        try {
            auto json = nlohmann::json::parse(response_body);
            // Handle /raw envelope: { success, result: [ { success, meta, results } ] }
            const nlohmann::json *root = &json;
            nlohmann::json first_result;
            bool has_envelope = json.contains("result") && json["result"].is_array() && !json["result"].empty();
            if (has_envelope) {
                first_result = json["result"][0];
                root = &first_result;
            }

            if (root->contains("success") && (*root)["success"].is_boolean()) {
                res.success = (*root)["success"].get<bool>();
            } else if (json.contains("success") && json["success"].is_boolean()) {
                res.success = json["success"].get<bool>();
            }
            if (!res.success && json.contains("errors") && !json["errors"].empty()) {
                res.error = json["errors"].dump();
            }
            // meta: changes, etc.
            if (root->contains("meta")) {
                auto &meta = (*root)["meta"];
                if (meta.contains("changes") && meta["changes"].is_number_integer()) {
                    res.changes = meta["changes"].get<int64_t>();
                }
            }
            // Prefer raw results shape: results { columns: [..], rows: [[..]] }
            if (root->contains("results") && (*root)["results"].is_object()) {
                auto &results = (*root)["results"];
                if (results.contains("columns") && results["columns"].is_array()) {
                    for (auto &c : results["columns"]) {
                        CloudflareD1QueryResultColumnMeta m;
                        m.name = c.is_string() ? c.get<std::string>() : std::string();
                        m.type = "";
                        res.columns.push_back(std::move(m));
                    }
                }
                if (results.contains("rows") && results["rows"].is_array()) {
                    for (auto &row : results["rows"]) {
                        std::vector<std::string> out_row;
                        if (row.is_array()) {
                            for (auto &cell : row) {
                                out_row.push_back(cell.is_null() ? std::string() : cell.dump());
                            }
                        }
                        res.rows.push_back(std::move(out_row));
                    }
                }
            } else if (root->contains("results") && (*root)["results"].is_array()) {
                // Handle /query object-shaped rows: results = [ {col: val, ...}, ... ]
                const auto &arr = (*root)["results"];
                bool array_of_objects = !arr.empty() && arr.front().is_object();
                if (array_of_objects) {
                    // Derive columns from first row's keys in stable iteration order
                    std::vector<std::string> colnames;
                    for (auto it = arr.front().begin(); it != arr.front().end(); ++it) {
                        colnames.push_back(it.key());
                        CloudflareD1QueryResultColumnMeta m;
                        m.name = it.key();
                        m.type = "";
                        res.columns.push_back(std::move(m));
                    }
                    for (auto &row : arr) {
                        std::vector<std::string> out_row;
                        for (auto &col : colnames) {
                            auto it = row.find(col);
                            if (it == row.end() || it->is_null()) out_row.emplace_back(); else out_row.push_back(it->dump());
                        }
                        res.rows.push_back(std::move(out_row));
                    }
                } else {
                    // Fallback to array-of-arrays or empty
                    for (auto &row : arr) {
                        std::vector<std::string> out_row;
                        if (row.is_array()) {
                            for (auto &cell : row) {
                                out_row.push_back(cell.is_null() ? std::string() : cell.dump());
                            }
                        }
                        res.rows.push_back(std::move(out_row));
                    }
                }
            }
        } catch (std::exception &ex) {
            res.success = false;
            res.error = std::string("Failed to parse D1 response: ") + ex.what();
        }
		return res;
	}
};

CloudflareD1Client::CloudflareD1Client(CloudflareD1Config config) : impl(new Impl{std::move(config)}) {}
CloudflareD1Client::~CloudflareD1Client() = default;

CloudflareD1QueryResult CloudflareD1Client::RawQuery(const std::string &sql, const std::vector<CloudflareD1QueryParam> &params) {
	std::string path = "/accounts/" + impl->config.account_id + "/d1/database/" + impl->config.database_id + "/raw";
    // Build JSON payload
    nlohmann::json payload; payload["sql"] = sql; if (!params.empty()) { nlohmann::json arr = nlohmann::json::array(); for (auto &p : params) arr.push_back(p.value); payload["params"] = arr; }
    std::string body_str = payload.dump();
    return impl->PostJson(path, body_str);
}

CloudflareD1QueryResult CloudflareD1Client::ObjectQuery(const std::string &sql, const std::vector<CloudflareD1QueryParam> &params) {
	std::string path = "/accounts/" + impl->config.account_id + "/d1/database/" + impl->config.database_id + "/query";
    nlohmann::json payload; payload["sql"] = sql; if (!params.empty()) { nlohmann::json obj = nlohmann::json::object(); for (auto &p : params) obj[p.name] = p.value; payload["params"] = obj; }
    std::string body_str = payload.dump();
    return impl->PostJson(path, body_str);
}

const CloudflareD1Config& CloudflareD1Client::GetConfig() const {
	return impl->config;
}

}


