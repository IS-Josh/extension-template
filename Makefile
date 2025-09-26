PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
EXT_NAME=cloudflare_d1
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile

# Prefer arm64 Homebrew OpenSSL on macOS and avoid picking up /usr/local x86_64
UNAME_S := $(shell uname -s)
UNAME_M := $(shell uname -m)
ifeq ($(UNAME_S),Darwin)
ifeq ($(UNAME_M),arm64)
EXT_RELEASE_FLAGS += \
  -DOPENSSL_ROOT_DIR=/opt/homebrew/opt/openssl@3 \
  -DOPENSSL_INCLUDE_DIR=/opt/homebrew/opt/openssl@3/include \
  -DOPENSSL_CRYPTO_LIBRARY=/opt/homebrew/opt/openssl@3/lib/libcrypto.dylib \
  -DOPENSSL_SSL_LIBRARY=/opt/homebrew/opt/openssl@3/lib/libssl.dylib \
  -DCMAKE_PREFIX_PATH=/opt/homebrew \
  -DCMAKE_IGNORE_PREFIX_PATH=/usr/local
EXT_DEBUG_FLAGS += \
  -DOPENSSL_ROOT_DIR=/opt/homebrew/opt/openssl@3 \
  -DOPENSSL_INCLUDE_DIR=/opt/homebrew/opt/openssl@3/include \
  -DOPENSSL_CRYPTO_LIBRARY=/opt/homebrew/opt/openssl@3/lib/libcrypto.dylib \
  -DOPENSSL_SSL_LIBRARY=/opt/homebrew/opt/openssl@3/lib/libssl.dylib \
  -DCMAKE_PREFIX_PATH=/opt/homebrew \
  -DCMAKE_IGNORE_PREFIX_PATH=/usr/local
endif
endif
