//===----------------------------------------------------------------------===//
//                         DuckDB
//
// d1_type_mapping.cpp
//
//
//===----------------------------------------------------------------------===//

#include "include/d1_type_mapping.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

string D1TypeMapping::NormalizeD1Type(const string &d1_type) {
    string normalized = StringUtil::Upper(StringUtil::Replace(d1_type, " ", ""));
    return normalized;
}

LogicalType D1TypeMapping::MapD1TypeToDuckDB(const string &d1_type) {
    string normalized = NormalizeD1Type(d1_type);

    // Handle common D1/SQLite types
    if (normalized == "INTEGER" || normalized == "INT") {
        return LogicalType::BIGINT;
    } else if (normalized == "REAL" || normalized == "FLOAT" || normalized == "DOUBLE") {
        return LogicalType::DOUBLE;
    } else if (normalized == "TEXT" || normalized == "VARCHAR" || normalized == "CHAR") {
        return LogicalType::VARCHAR;
    } else if (normalized == "BLOB") {
        return LogicalType::BLOB;
    } else if (normalized == "NUMERIC" || normalized == "DECIMAL") {
        return LogicalType::DOUBLE; // D1 doesn't have true decimal support
    } else if (normalized == "BOOLEAN" || normalized == "BOOL") {
        return LogicalType::BOOLEAN;
    } else if (normalized == "DATE") {
        return LogicalType::DATE;
    } else if (normalized == "DATETIME" || normalized == "TIMESTAMP") {
        return LogicalType::TIMESTAMP;
    } else if (normalized == "TIME") {
        return LogicalType::TIME;
    }

    // Handle parameterized types
    if (normalized.find("VARCHAR") == 0 || normalized.find("CHAR") == 0) {
        return LogicalType::VARCHAR;
    } else if (normalized.find("DECIMAL") == 0 || normalized.find("NUMERIC") == 0) {
        return LogicalType::DOUBLE;
    }

    // Default to VARCHAR for unknown types
    fprintf(stderr, "D1TypeMapping: Unknown D1 type '%s', defaulting to VARCHAR\n", d1_type.c_str());
    return LogicalType::VARCHAR;
}

string D1TypeMapping::MapDuckDBTypeToD1(const LogicalType &duckdb_type) {
    switch (duckdb_type.id()) {
        case LogicalTypeId::BOOLEAN:
            return "INTEGER"; // D1 stores booleans as integers
        case LogicalTypeId::TINYINT:
        case LogicalTypeId::SMALLINT:
        case LogicalTypeId::INTEGER:
        case LogicalTypeId::BIGINT:
            return "INTEGER";
        case LogicalTypeId::FLOAT:
        case LogicalTypeId::DOUBLE:
        case LogicalTypeId::DECIMAL:
            return "REAL";
        case LogicalTypeId::VARCHAR:
        case LogicalTypeId::CHAR:
            return "TEXT";
        case LogicalTypeId::DATE:
            return "TEXT"; // D1 stores dates as text
        case LogicalTypeId::TIME:
            return "TEXT"; // D1 stores time as text
        case LogicalTypeId::TIMESTAMP:
            return "TEXT"; // D1 stores timestamps as text
        case LogicalTypeId::BLOB:
            return "BLOB";
        default:
            return "TEXT"; // Default to TEXT for unknown types
    }
}

bool D1TypeMapping::IsNumericType(const string &d1_type) {
    string normalized = NormalizeD1Type(d1_type);
    return normalized == "INTEGER" || normalized == "INT" ||
           normalized == "REAL" || normalized == "FLOAT" ||
           normalized == "DOUBLE" || normalized == "NUMERIC" ||
           normalized == "DECIMAL";
}

bool D1TypeMapping::IsTextType(const string &d1_type) {
    string normalized = NormalizeD1Type(d1_type);
    return normalized == "TEXT" || normalized == "VARCHAR" ||
           normalized == "CHAR" || normalized.find("VARCHAR") == 0 ||
           normalized.find("CHAR") == 0;
}

Value D1TypeMapping::GetDefaultValue(const LogicalType &type) {
    switch (type.id()) {
        case LogicalTypeId::BOOLEAN:
            return Value::BOOLEAN(false);
        case LogicalTypeId::TINYINT:
            return Value::TINYINT(0);
        case LogicalTypeId::SMALLINT:
            return Value::SMALLINT(0);
        case LogicalTypeId::INTEGER:
            return Value::INTEGER(0);
        case LogicalTypeId::BIGINT:
            return Value::BIGINT(0);
        case LogicalTypeId::FLOAT:
            return Value::FLOAT(0.0f);
        case LogicalTypeId::DOUBLE:
            return Value::DOUBLE(0.0);
        case LogicalTypeId::VARCHAR:
        case LogicalTypeId::CHAR:
            return Value("");
        case LogicalTypeId::DATE:
            return Value::DATE(1970, 1, 1);
        case LogicalTypeId::TIME:
            return Value::TIME(0, 0, 0, 0);
        case LogicalTypeId::TIMESTAMP:
            return Value::TIMESTAMP(1970, 1, 1, 0, 0, 0, 0);
        case LogicalTypeId::BLOB:
            return Value::BLOB("");
        default:
            return Value("");
    }
}

Value D1TypeMapping::ConvertStringToValue(const string &str_value, const LogicalType &target_type) {
    if (str_value.empty()) {
        return Value(target_type);
    }

    try {
        switch (target_type.id()) {
            case LogicalTypeId::BOOLEAN:
                return Value::BOOLEAN(str_value == "1" || str_value == "true" || str_value == "TRUE");
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
            case LogicalTypeId::VARCHAR:
            case LogicalTypeId::CHAR:
                return Value(str_value);
            case LogicalTypeId::DATE:
                // Try to parse date from string
                return Value::DATE(Date::FromString(str_value));
            case LogicalTypeId::TIME:
                // Try to parse time from string - simplified for now
                return Value(str_value);
            case LogicalTypeId::TIMESTAMP:
                // Try to parse timestamp from string - simplified for now
                return Value(str_value);
            case LogicalTypeId::BLOB:
                return Value::BLOB(str_value);
            default:
                // Default to string representation
                return Value(str_value);
        }
    } catch (const std::exception &e) {
        // If conversion fails, return as string
        fprintf(stderr, "D1TypeMapping::ConvertStringToValue: Failed to convert '%s' to target type, using string: %s\n",
               str_value.c_str(), e.what());
        return Value(str_value);
    }
}

string D1TypeMapping::ConvertValueToSQL(const Value &value) {
    if (value.IsNull()) {
        return "NULL";
    }

    switch (value.type().id()) {
        case LogicalTypeId::BOOLEAN:
            return value.GetValue<bool>() ? "1" : "0";
        case LogicalTypeId::TINYINT:
        case LogicalTypeId::SMALLINT:
        case LogicalTypeId::INTEGER:
        case LogicalTypeId::BIGINT:
            return std::to_string(value.GetValue<int64_t>());
        case LogicalTypeId::FLOAT:
        case LogicalTypeId::DOUBLE:
            return std::to_string(value.GetValue<double>());
        case LogicalTypeId::VARCHAR:
        case LogicalTypeId::DATE:
        case LogicalTypeId::TIMESTAMP:
            return "'" + StringUtil::Replace(value.ToString(), "'", "''") + "'";
        default:
            return "'" + StringUtil::Replace(value.ToString(), "'", "''") + "'";
    }
}

} // namespace duckdb
