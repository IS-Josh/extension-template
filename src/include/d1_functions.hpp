#pragma once

#include "duckdb.hpp"

namespace duckdb {

void RegisterD1QueryFunctions(ExtensionLoader &loader);
void RegisterD1CatalogFunctions(ExtensionLoader &loader);
void RegisterD1AttachFunctions(ExtensionLoader &loader);
void RegisterD1BulkFunctions(ExtensionLoader &loader);

}


