#include "include/d1_functions.hpp"
#include "include/d1_client.hpp"
#include "duckdb.hpp"

namespace duckdb {

// Forward decls from cloudflare_d1_extension.cpp
extern unique_ptr<FunctionData> D1RawBind(ClientContext &context, TableFunctionBindInput &input, vector<LogicalType> &return_types, vector<string> &names);
extern unique_ptr<FunctionData> D1ScanBind(ClientContext &context, TableFunctionBindInput &input, vector<LogicalType> &return_types, vector<string> &names);
extern unique_ptr<GlobalTableFunctionState> D1RawInitGlobal(ClientContext &context, TableFunctionInitInput &input);
extern void D1RawFunc(ClientContext &context, TableFunctionInput &data_p, DataChunk &output);
extern void D1ExecuteScalar(DataChunk &args, ExpressionState &state, Vector &result);

void RegisterD1QueryFunctions(ExtensionLoader &loader) {
	TableFunction tf("d1_query",
	                {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
	                D1RawFunc, D1RawBind, D1RawInitGlobal);
    loader.RegisterFunction(tf);

	auto exec_fun = ScalarFunction("d1_execute",
	                              {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
	                              LogicalType::BIGINT, D1ExecuteScalar);
    loader.RegisterFunction(exec_fun);

	// Register d1_scan function for individual table access
	// Parameters: account_id, api_token, database_id, table_name
	TableFunction d1_scan_tf("d1_scan",
	                         {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
	                         D1RawFunc, D1ScanBind, D1RawInitGlobal);
    loader.RegisterFunction(d1_scan_tf);
}

}


