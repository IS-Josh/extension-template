cmake_minimum_required(VERSION 3.5)

# Set extension name here
set(TARGET_NAME cloudflare_d1)

# DuckDB's extension distribution supports vcpkg. As such, dependencies can be added in ./vcpkg.json and then
# used in cmake with find_package. Feel free to remove or replace with other dependencies.
# Note that it should also be removed from vcpkg.json to prevent needlessly installing it..
find_package(OpenSSL REQUIRED)
find_package(CURL REQUIRED)
find_package(nlohmann_json REQUIRED)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})
include_directories(src/include)

set(EXTENSION_SOURCES
  src/cloudflare_d1_extension.cpp
  src/d1_client.cpp
  src/d1_storage.cpp
  src/d1_catalog.cpp
  src/d1_data_table.cpp
  src/d1_storage_data_table.cpp
  src/d1_custom_data_table.cpp
  src/d1_physical_insert.cpp
  src/d1_physical_update.cpp
  src/d1_physical_delete.cpp
  src/d1_parameterized_query.cpp
  src/d1_type_mapping.cpp
  src/d1_query_interceptor.cpp
  src/d1_secret_helper.cpp
  src/d1_enhanced_functions.cpp
  src/d1_functions_query.cpp
  src/d1_functions_catalog.cpp
  src/d1_functions_attach.cpp
  src/d1_functions_bulk.cpp
  src/d1_secret.cpp
  src/d1_batch_operations.cpp
  src/d1_validation.cpp
  src/d1_predicate_pushdown.cpp
  src/d1_filter_pushdown.cpp
  src/d1_connection_pool.cpp
  src/d1_metrics.cpp
  src/d1_streaming.cpp
  src/d1_analytics.cpp
  src/d1_pipeline.cpp
)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

# Link OpenSSL in both the static library as the loadable extension
target_link_libraries(${EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto CURL::libcurl nlohmann_json::nlohmann_json)
target_link_libraries(${LOADABLE_EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto CURL::libcurl nlohmann_json::nlohmann_json)

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
