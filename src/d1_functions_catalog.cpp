#include "include/d1_functions.hpp"

namespace duckdb {

// Forward decls from cloudflare_d1_extension.cpp
extern unique_ptr<FunctionData> D1TablesBind(ClientContext &context, TableFunctionBindInput &input, vector<LogicalType> &return_types, vector<string> &names);
extern void D1TablesFunc(ClientContext &context, TableFunctionInput &input, DataChunk &output);
extern unique_ptr<FunctionData> D1ColumnsBind(ClientContext &context, TableFunctionBindInput &input, vector<LogicalType> &return_types, vector<string> &names);
extern void D1ColumnsFunc(ClientContext &context, TableFunctionInput &input, DataChunk &output);

void RegisterD1CatalogFunctions(ExtensionLoader &loader) {
	TableFunction d1_tables_tf("d1_tables", {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR}, D1TablesFunc, D1TablesBind);
    loader.RegisterFunction(d1_tables_tf);
	TableFunction d1_columns_tf("d1_columns", {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR}, D1ColumnsFunc, D1ColumnsBind);
    loader.RegisterFunction(d1_columns_tf);
}

}


