#include "include/d1_type_mapping.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/timestamp.hpp"

namespace duckdb {

// Map SQLite type strings to DuckDB LogicalType based on SQLite type affinity rules
LogicalType MapSQLiteTypeToDuckDB(const string &sqlite_type) {
    if (sqlite_type.empty()) {
        return LogicalType::VARCHAR; // Default to TEXT
    }

    auto type_lower = StringUtil::Lower(sqlite_type);

    // SQLite type affinity rules:
    // 1. If the declared type contains "INT", it has INTEGER affinity
    if (type_lower.find("int") != string::npos) {
        return LogicalType::BIGINT;
    }

    // 2. If the declared type contains "CHAR", "CLOB", or "TEXT", it has TEXT affinity
    if (type_lower.find("char") != string::npos ||
        type_lower.find("clob") != string::npos ||
        type_lower.find("text") != string::npos) {
        return LogicalType::VARCHAR;
    }

    // 3. If the declared type contains "BLOB", it has BLOB affinity
    if (type_lower.find("blob") != string::npos) {
        return LogicalType::BLOB;
    }

    // 4. If the declared type contains "REAL", "FLOA", or "DOUB", it has REAL affinity
    if (type_lower.find("real") != string::npos ||
        type_lower.find("floa") != string::npos ||
        type_lower.find("doub") != string::npos) {
        return LogicalType::DOUBLE;
    }

    // 5. If the declared type contains "BOOL", it has INTEGER affinity (SQLite stores booleans as integers)
    if (type_lower.find("bool") != string::npos) {
        return LogicalType::BOOLEAN;
    }

    // 5a. Additional boolean-like types
    if (type_lower.find("bit") != string::npos) {
        return LogicalType::BOOLEAN;
    }

    // 6. If the declared type contains "DATE", "TIME", or "TIMESTAMP", map to appropriate DuckDB types
    if (type_lower.find("date") != string::npos) {
        return LogicalType::DATE;
    }
    if (type_lower.find("time") != string::npos && type_lower.find("timestamp") == string::npos) {
        return LogicalType::TIME;
    }
    if (type_lower.find("timestamp") != string::npos) {
        return LogicalType::TIMESTAMP;
    }

    // 7. If the declared type contains "NUMERIC", it has NUMERIC affinity
    // NUMERIC can be either INTEGER or REAL depending on the value
    // For better precision, we'll use DECIMAL with high precision
    if (type_lower.find("numeric") != string::npos ||
        type_lower.find("decimal") != string::npos) {
        return LogicalType::DECIMAL(38, 10); // High precision decimal
    }

    // 8. If the declared type contains "JSON", map to JSON type
    if (type_lower.find("json") != string::npos) {
        return LogicalType::JSON();
    }

    // 9. If the declared type contains "UUID", map to UUID type
    if (type_lower.find("uuid") != string::npos) {
        return LogicalType(LogicalTypeId::UUID);
    }

    // 10. Additional numeric types
    if (type_lower.find("smallint") != string::npos) {
        return LogicalType::SMALLINT;
    }
    if (type_lower.find("tinyint") != string::npos) {
        return LogicalType::TINYINT;
    }
    if (type_lower.find("float") != string::npos) {
        return LogicalType::FLOAT;
    }

    // 11. Additional text types
    if (type_lower.find("varchar") != string::npos) {
        return LogicalType::VARCHAR;
    }
    if (type_lower.find("nvarchar") != string::npos) {
        return LogicalType::VARCHAR;
    }
    if (type_lower.find("nchar") != string::npos) {
        return LogicalType::VARCHAR;
    }

    // 12. Binary types
    if (type_lower.find("binary") != string::npos) {
        return LogicalType::BLOB;
    }
    if (type_lower.find("varbinary") != string::npos) {
        return LogicalType::BLOB;
    }

    // Default to TEXT affinity for any unrecognized types
    return LogicalType::VARCHAR;
}

// Convert string value to appropriate DuckDB Value based on type
Value ConvertStringToValue(const string &str_value, const LogicalType &type) {
    // Handle NULL values
    if (str_value.empty() || str_value == "NULL" || str_value == "null") {
        return Value(type);
    }

    try {
        switch (type.id()) {
            case LogicalTypeId::BOOLEAN: {
                auto lower = StringUtil::Lower(str_value);
                if (lower == "true" || lower == "1" || lower == "yes" || lower == "on") {
                    return Value::BOOLEAN(true);
                } else if (lower == "false" || lower == "0" || lower == "no" || lower == "off") {
                    return Value::BOOLEAN(false);
                } else {
                    // Try to parse as integer
                    int64_t int_val = std::stoll(str_value);
                    return Value::BOOLEAN(int_val != 0);
                }
            }
            case LogicalTypeId::TINYINT:
                return Value::TINYINT(std::stoi(str_value));
            case LogicalTypeId::SMALLINT:
                return Value::SMALLINT(std::stoi(str_value));
            case LogicalTypeId::INTEGER:
                return Value::INTEGER(std::stoi(str_value));
            case LogicalTypeId::BIGINT:
                return Value::BIGINT(std::stoll(str_value));
            case LogicalTypeId::FLOAT:
                return Value::FLOAT(std::stof(str_value));
            case LogicalTypeId::DOUBLE:
                return Value::DOUBLE(std::stod(str_value));
            case LogicalTypeId::DECIMAL: {
                // Parse decimal value as integer (multiply by scale factor)
                // For simplicity, we'll parse as int64_t and use default scale
                int64_t int_val = static_cast<int64_t>(std::stod(str_value) * 10000000000); // Scale by 10^10
                return Value::DECIMAL(int_val, 38, 10); // Use default precision/scale
            }
            case LogicalTypeId::DATE: {
                // Parse date string (assuming ISO format YYYY-MM-DD)
                return Value::DATE(Date::FromString(str_value));
            }
            case LogicalTypeId::TIME: {
                // Parse time string (assuming HH:MM:SS format)
                return Value::TIME(Time::FromString(str_value));
            }
            case LogicalTypeId::TIMESTAMP: {
                // Parse timestamp string
                return Value::TIMESTAMP(Timestamp::FromString(str_value, false));
            }
            case LogicalTypeId::BLOB: {
                // For BLOB, we'll treat the string as hex-encoded data
                // In a real implementation, you might want to handle this differently
                return Value::BLOB(str_value);
            }
            default:
                // Check if it's a JSON type (special handling)
                if (type.IsJSONType()) {
                    // For JSON, return as string for now
                    return Value(str_value);
                }
                // For VARCHAR and other string types, return as-is
                return Value(str_value);
            case LogicalTypeId::UUID: {
                // Parse UUID string
                return Value::UUID(str_value);
            }
        }
    } catch (const std::exception &e) {
        // If conversion fails, return as string
        return Value(str_value);
    }
}

} // namespace duckdb
