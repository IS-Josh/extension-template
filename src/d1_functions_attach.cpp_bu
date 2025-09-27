#include "include/d1_functions.hpp"

namespace duckdb {

// Forward decls from cloudflare_d1_extension.cpp
extern unique_ptr<FunctionData> D1AttachBind(ClientContext &context, TableFunctionBindInput &input, vector<LogicalType> &return_types, vector<string> &names);
extern void D1AttachFunc(ClientContext &context, TableFunctionInput &input, DataChunk &output);

void RegisterD1AttachFunctions(ExtensionLoader &loader) {
	TableFunction d1_attach_tf("d1_attach_sql", {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR}, D1AttachFunc, D1AttachBind);
    loader.RegisterFunction(d1_attach_tf);
}

}


