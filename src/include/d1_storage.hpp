#pragma once

#include "duckdb.hpp"

namespace duckdb {

// Factory to create and register the D1 storage extension (TYPE d1)
unique_ptr<StorageExtension> CreateD1StorageExtension();

}


