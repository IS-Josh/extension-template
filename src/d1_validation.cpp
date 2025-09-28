//===----------------------------------------------------------------------===//
//                         DuckDB
//
// d1_validation.cpp
//
//
//===----------------------------------------------------------------------===//

#include "duckdb.hpp"
#include "duckdb/common/string_util.hpp"
#include "include/d1_client.hpp"

namespace duckdb {

//! D1 SQL validation and constraint checking
class D1Validator {
public:
    //! Validate SQL statement for D1 compatibility
    static string ValidateSQL(const string &sql) {
        string trimmed_sql = sql;
        StringUtil::Trim(trimmed_sql);

        if (trimmed_sql.empty()) {
            return "SQL statement cannot be empty";
        }

        // Convert to uppercase for pattern matching
        string upper_sql = StringUtil::Upper(trimmed_sql);

        // Check for unsupported operations
        if (upper_sql.find("CREATE DATABASE") != string::npos ||
            upper_sql.find("DROP DATABASE") != string::npos) {
            return "Database creation/deletion operations are not supported in D1";
        }

        if (upper_sql.find("ATTACH") != string::npos ||
            upper_sql.find("DETACH") != string::npos) {
            return "ATTACH/DETACH operations are not supported in D1 SQL";
        }

        // Check for potentially dangerous operations
        if (upper_sql.find("PRAGMA") != string::npos) {
            // Allow some safe PRAGMAs
            if (upper_sql.find("PRAGMA TABLE_INFO") == string::npos &&
                upper_sql.find("PRAGMA SCHEMA_VERSION") == string::npos) {
                return "Most PRAGMA statements are not supported in D1. Use PRAGMA table_info() for schema information.";
            }
        }

        // Validate transaction statements
        if (upper_sql.find("BEGIN") != string::npos ||
            upper_sql.find("COMMIT") != string::npos ||
            upper_sql.find("ROLLBACK") != string::npos) {
            return "Explicit transaction control (BEGIN/COMMIT/ROLLBACK) is handled automatically by D1";
        }

        return ""; // No validation errors
    }

    //! Parse D1 error response and provide helpful suggestions
    static string ParseD1Error(const string &error_json) {
        if (error_json.empty()) {
            return "Unknown D1 error";
        }

        string result = "D1 Error: " + error_json;

        // Common error patterns and suggestions
        if (error_json.find("7003") != string::npos) {
            result += "\n💡 Suggestion: Verify your account_id and database_id are correct";
        } else if (error_json.find("7000") != string::npos) {
            result += "\n💡 Suggestion: Check your API token permissions for D1 access";
        } else if (error_json.find("syntax error") != string::npos) {
            result += "\n💡 Suggestion: Check your SQL syntax - D1 uses SQLite syntax";
        } else if (error_json.find("no such table") != string::npos) {
            result += "\n💡 Suggestion: Verify the table exists. Use 'SELECT name FROM sqlite_master WHERE type=\"table\"' to list tables";
        } else if (error_json.find("no such column") != string::npos) {
            result += "\n💡 Suggestion: Check column names. Use 'PRAGMA table_info(table_name)' to see table schema";
        } else if (error_json.find("constraint") != string::npos) {
            result += "\n💡 Suggestion: Check for constraint violations (PRIMARY KEY, UNIQUE, NOT NULL, FOREIGN KEY)";
        } else if (error_json.find("database is locked") != string::npos) {
            result += "\n💡 Suggestion: D1 database may be busy. Try again in a moment";
        }

        return result;
    }

    //! Validate table and column names for D1
    static string ValidateIdentifier(const string &identifier, const string &type = "identifier") {
        if (identifier.empty()) {
            return type + " cannot be empty";
        }

        // Check for reserved SQLite keywords that might cause issues
        string upper_id = StringUtil::Upper(identifier);
        vector<string> reserved_words = {
            "SELECT", "FROM", "WHERE", "INSERT", "UPDATE", "DELETE", "CREATE", "DROP",
            "ALTER", "TABLE", "INDEX", "VIEW", "TRIGGER", "DATABASE", "SCHEMA",
            "PRIMARY", "KEY", "FOREIGN", "REFERENCES", "UNIQUE", "NOT", "NULL",
            "DEFAULT", "CHECK", "CONSTRAINT", "AUTOINCREMENT", "INTEGER", "TEXT",
            "REAL", "BLOB", "NUMERIC", "BOOLEAN", "DATE", "TIME", "DATETIME"
        };

        for (const auto &word : reserved_words) {
            if (upper_id == word) {
                return type + " '" + identifier + "' is a reserved SQL keyword. Consider using quotes or a different name";
            }
        }

        // Check for valid identifier format
        if (!std::isalpha(identifier[0]) && identifier[0] != '_') {
            return type + " must start with a letter or underscore";
        }

        for (char c : identifier) {
            if (!std::isalnum(c) && c != '_') {
                return type + " can only contain letters, numbers, and underscores";
            }
        }

        return ""; // Valid identifier
    }

    //! Suggest performance optimizations for D1 queries
    static vector<string> GetPerformanceTips(const string &sql) {
        vector<string> tips;
        string upper_sql = StringUtil::Upper(sql);

        if (upper_sql.find("SELECT *") != string::npos) {
            tips.push_back("💡 Performance: Consider selecting specific columns instead of SELECT * for better performance");
        }

        if (upper_sql.find("WHERE") == string::npos &&
            (upper_sql.find("UPDATE") != string::npos || upper_sql.find("DELETE") != string::npos)) {
            tips.push_back("⚠️  Warning: UPDATE/DELETE without WHERE clause affects all rows");
        }

        if (upper_sql.find("ORDER BY") != string::npos && upper_sql.find("LIMIT") == string::npos) {
            tips.push_back("💡 Performance: Consider adding LIMIT when using ORDER BY for large datasets");
        }

        if (upper_sql.find("LIKE '%") != string::npos) {
            tips.push_back("💡 Performance: LIKE patterns starting with % cannot use indexes efficiently");
        }

        return tips;
    }
};

//! Enhanced D1 execution with validation
CloudflareD1QueryResult ExecuteValidatedD1Query(CloudflareD1Client &client, const string &sql,
                                               const vector<CloudflareD1QueryParam> &params = {}) {
    // Validate SQL before execution
    string validation_error = D1Validator::ValidateSQL(sql);
    if (!validation_error.empty()) {
        CloudflareD1QueryResult result;
        result.success = false;
        result.error = "SQL Validation Error: " + validation_error;
        return result;
    }

    // Get performance tips
    auto tips = D1Validator::GetPerformanceTips(sql);
    for (const auto &tip : tips) {
        fprintf(stderr, "D1Validation: %s\n", tip.c_str());
    }

    // Execute the query
    auto result = client.ObjectQuery(sql, params);

    // Enhanced error reporting
    if (!result.success && !result.error.empty()) {
        result.error = D1Validator::ParseD1Error(result.error);
    }

    return result;
}

} // namespace duckdb
