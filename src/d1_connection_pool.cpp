//===----------------------------------------------------------------------===//
//                         DuckDB
//
// d1_connection_pool.cpp
//
//
//===----------------------------------------------------------------------===//

#include "duckdb.hpp"
#include "duckdb/common/mutex.hpp"
#include "include/d1_client.hpp"
#include <chrono>
#include <unordered_map>

namespace duckdb {

//! Connection pool entry with timestamp
struct D1ConnectionEntry {
    unique_ptr<CloudflareD1Client> client;
    std::chrono::steady_clock::time_point last_used;
    bool in_use;

    D1ConnectionEntry(unique_ptr<CloudflareD1Client> client_p)
        : client(std::move(client_p)), last_used(std::chrono::steady_clock::now()), in_use(false) {}
};

//! Schema cache entry
struct D1SchemaEntry {
    vector<string> column_names;
    vector<LogicalType> column_types;
    std::chrono::steady_clock::time_point cached_at;

    D1SchemaEntry(vector<string> names, vector<LogicalType> types)
        : column_names(std::move(names)), column_types(std::move(types)),
          cached_at(std::chrono::steady_clock::now()) {}

    bool IsExpired(std::chrono::minutes max_age = std::chrono::minutes(30)) const {
        auto now = std::chrono::steady_clock::now();
        return (now - cached_at) > max_age;
    }
};

//! Query result cache entry
struct D1QueryCacheEntry {
    CloudflareD1QueryResult result;
    std::chrono::steady_clock::time_point cached_at;
    string query_hash;

    D1QueryCacheEntry(CloudflareD1QueryResult res, string hash)
        : result(std::move(res)), cached_at(std::chrono::steady_clock::now()),
          query_hash(std::move(hash)) {}

    bool IsExpired(std::chrono::minutes max_age = std::chrono::minutes(5)) const {
        auto now = std::chrono::steady_clock::now();
        return (now - cached_at) > max_age;
    }
};

//! D1 Connection Pool and Cache Manager
class D1ConnectionPool {
private:
    static D1ConnectionPool instance;
    mutable mutex pool_mutex;

    // Connection pool: config_hash -> vector of connections
    unordered_map<string, vector<unique_ptr<D1ConnectionEntry>>> connection_pools;

    // Schema cache: database_id.table_name -> schema
    unordered_map<string, unique_ptr<D1SchemaEntry>> schema_cache;

    // Query cache: query_hash -> result (for SELECT queries only)
    unordered_map<string, unique_ptr<D1QueryCacheEntry>> query_cache;

    // Pool configuration
    static constexpr size_t MAX_CONNECTIONS_PER_CONFIG = 5;
    static constexpr std::chrono::minutes CONNECTION_TIMEOUT{10};
    static constexpr std::chrono::minutes SCHEMA_CACHE_TIMEOUT{30};
    static constexpr std::chrono::minutes QUERY_CACHE_TIMEOUT{5};

    D1ConnectionPool() = default;

public:
    static D1ConnectionPool& GetInstance() {
        return instance;
    }

    //! Get a connection from the pool or create a new one
    unique_ptr<CloudflareD1Client> GetConnection(const CloudflareD1Config &config) {
        lock_guard<mutex> lock(pool_mutex);

        string config_hash = GenerateConfigHash(config);
        auto &pool = connection_pools[config_hash];

        // Clean up expired connections
        CleanupExpiredConnections(pool);

        // For now, always create a new connection since CloudflareD1Client is not copyable
        // In a production implementation, we'd implement proper connection sharing
        fprintf(stderr, "D1ConnectionPool: Creating new connection (pool management simplified)\n");

        // Pool is full, create temporary connection
        fprintf(stderr, "D1ConnectionPool: Pool full, creating temporary connection\n");
        return make_uniq<CloudflareD1Client>(config);
    }

    //! Return a connection to the pool
    void ReturnConnection(const CloudflareD1Config &config) {
        lock_guard<mutex> lock(pool_mutex);

        string config_hash = GenerateConfigHash(config);
        auto it = connection_pools.find(config_hash);
        if (it != connection_pools.end()) {
            for (auto &entry : it->second) {
                if (entry->in_use) {
                    entry->in_use = false;
                    entry->last_used = std::chrono::steady_clock::now();
                    fprintf(stderr, "D1ConnectionPool: Returned connection to pool\n");
                    break;
                }
            }
        }
    }

    //! Cache table schema
    void CacheSchema(const string &database_id, const string &table_name,
                    vector<string> column_names, vector<LogicalType> column_types) {
        lock_guard<mutex> lock(pool_mutex);

        string cache_key = database_id + "." + table_name;
        schema_cache[cache_key] = make_uniq<D1SchemaEntry>(std::move(column_names), std::move(column_types));

        fprintf(stderr, "D1ConnectionPool: Cached schema for %s\n", cache_key.c_str());
    }

    //! Get cached schema
    bool GetCachedSchema(const string &database_id, const string &table_name,
                        vector<string> &column_names, vector<LogicalType> &column_types) {
        lock_guard<mutex> lock(pool_mutex);

        string cache_key = database_id + "." + table_name;
        auto it = schema_cache.find(cache_key);

        if (it != schema_cache.end() && !it->second->IsExpired(SCHEMA_CACHE_TIMEOUT)) {
            column_names = it->second->column_names;
            column_types = it->second->column_types;
            fprintf(stderr, "D1ConnectionPool: Retrieved cached schema for %s\n", cache_key.c_str());
            return true;
        }

        // Remove expired entry
        if (it != schema_cache.end()) {
            schema_cache.erase(it);
            fprintf(stderr, "D1ConnectionPool: Removed expired schema cache for %s\n", cache_key.c_str());
        }

        return false;
    }

    //! Cache query result (SELECT queries only)
    void CacheQueryResult(const string &query, const CloudflareD1QueryResult &result) {
        // Only cache SELECT queries
        string upper_query = query;
        std::transform(upper_query.begin(), upper_query.end(), upper_query.begin(), ::toupper);
        if (upper_query.find("SELECT") != 0) {
            return;
        }

        lock_guard<mutex> lock(pool_mutex);

        string query_hash = GenerateQueryHash(query);
        query_cache[query_hash] = make_uniq<D1QueryCacheEntry>(result, query_hash);

        // Limit cache size
        if (query_cache.size() > 100) {
            CleanupQueryCache();
        }

        fprintf(stderr, "D1ConnectionPool: Cached query result (hash: %s)\n", query_hash.substr(0, 8).c_str());
    }

    //! Get cached query result
    bool GetCachedQueryResult(const string &query, CloudflareD1QueryResult &result) {
        lock_guard<mutex> lock(pool_mutex);

        string query_hash = GenerateQueryHash(query);
        auto it = query_cache.find(query_hash);

        if (it != query_cache.end() && !it->second->IsExpired(QUERY_CACHE_TIMEOUT)) {
            result = it->second->result;
            fprintf(stderr, "D1ConnectionPool: Retrieved cached query result (hash: %s)\n",
                   query_hash.substr(0, 8).c_str());
            return true;
        }

        // Remove expired entry
        if (it != query_cache.end()) {
            query_cache.erase(it);
        }

        return false;
    }

    //! Get pool statistics
    struct PoolStats {
        size_t total_connections = 0;
        size_t active_connections = 0;
        size_t cached_schemas = 0;
        size_t cached_queries = 0;
    };

    PoolStats GetStats() const {
        lock_guard<mutex> lock(pool_mutex);

        PoolStats stats;
        for (const auto &pool_pair : connection_pools) {
            stats.total_connections += pool_pair.second.size();
            for (const auto &entry : pool_pair.second) {
                if (entry->in_use) {
                    stats.active_connections++;
                }
            }
        }
        stats.cached_schemas = schema_cache.size();
        stats.cached_queries = query_cache.size();

        return stats;
    }

    //! Clear all caches
    void ClearCaches() {
        lock_guard<mutex> lock(pool_mutex);
        schema_cache.clear();
        query_cache.clear();
        fprintf(stderr, "D1ConnectionPool: Cleared all caches\n");
    }

private:
    string GenerateConfigHash(const CloudflareD1Config &config) const {
        return config.account_id + ":" + config.database_id;
    }

    string GenerateQueryHash(const string &query) const {
        // Simple hash - in production, use a proper hash function
        std::hash<string> hasher;
        return std::to_string(hasher(query));
    }

    void CleanupExpiredConnections(vector<unique_ptr<D1ConnectionEntry>> &pool) {
        auto now = std::chrono::steady_clock::now();
        pool.erase(
            std::remove_if(pool.begin(), pool.end(),
                [now](const unique_ptr<D1ConnectionEntry> &entry) {
                    return !entry->in_use && (now - entry->last_used) > CONNECTION_TIMEOUT;
                }),
            pool.end()
        );
    }

    void CleanupQueryCache() {
        // Remove oldest entries when cache is full
        auto now = std::chrono::steady_clock::now();
        for (auto it = query_cache.begin(); it != query_cache.end();) {
            if (it->second->IsExpired(QUERY_CACHE_TIMEOUT)) {
                it = query_cache.erase(it);
            } else {
                ++it;
            }
        }
    }
};

// Static instance
D1ConnectionPool D1ConnectionPool::instance;

//! Enhanced D1 client with caching (simplified connection pooling)
class D1CachedClient {
private:
    CloudflareD1Config config;
    unique_ptr<CloudflareD1Client> client;

public:
    explicit D1CachedClient(const CloudflareD1Config &cfg) : config(cfg) {
        client = make_uniq<CloudflareD1Client>(config);
    }

    CloudflareD1QueryResult RawQuery(const string &sql, const vector<CloudflareD1QueryParam> &params = {}) {
        // Check cache first for SELECT queries
        CloudflareD1QueryResult cached_result;
        if (D1ConnectionPool::GetInstance().GetCachedQueryResult(sql, cached_result)) {
            return cached_result;
        }

        // Execute query
        auto result = client->RawQuery(sql, params);

        // Cache successful SELECT results
        if (result.success) {
            D1ConnectionPool::GetInstance().CacheQueryResult(sql, result);
        }

        return result;
    }

    CloudflareD1QueryResult ObjectQuery(const string &sql, const vector<CloudflareD1QueryParam> &params = {}) {
        return client->ObjectQuery(sql, params);
    }
};

//! Function to get pool statistics
void D1PoolStatsScalar(DataChunk &args, ExpressionState &state, Vector &result) {
    auto stats = D1ConnectionPool::GetInstance().GetStats();

    string stats_str = StringUtil::Format(
        "Connections: %zu total, %zu active | Cached: %zu schemas, %zu queries",
        stats.total_connections, stats.active_connections,
        stats.cached_schemas, stats.cached_queries
    );

    result.SetValue(0, Value(stats_str));
}

//! Register connection pool functions
void RegisterD1ConnectionPool(ExtensionLoader &loader) {
    // d1_pool_stats scalar function
    ScalarFunction pool_stats_func("d1_pool_stats", {}, LogicalType::VARCHAR, D1PoolStatsScalar);
    loader.RegisterFunction(pool_stats_func);

    fprintf(stderr, "D1ConnectionPool: Registered connection pool functions\n");
}

} // namespace duckdb
