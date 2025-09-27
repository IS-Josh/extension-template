# CloudflareD1 DuckDB Extension

A DuckDB extension that provides seamless integration with Cloudflare D1 databases, enabling you to query, insert, update, and delete data from D1 databases directly within DuckDB.

## Features

- **Read Operations**: Query D1 databases using SQL with automatic type mapping
- **Write Operations**: Insert, update, and delete data in D1 databases
- **Type Mapping**: Intelligent mapping between SQLite/D1 types and DuckDB types
- **Catalog Integration**: Automatic table discovery and schema management
- **Bulk Operations**: Efficient bulk insert operations for large datasets
- **Catalog Refresh**: Refresh table schemas to reflect DDL changes

## Functions

### Query Functions

#### `d1_query(sql, account_id, api_token, database_id)`
Execute a SELECT query against a D1 database and return results as a table.

```sql
SELECT * FROM d1_query('SELECT * FROM users WHERE age > 18', 'your_account_id', 'your_api_token', 'your_database_id');
```

#### `d1_execute(sql, account_id, api_token, database_id)`
Execute any SQL statement (INSERT, UPDATE, DELETE, CREATE, etc.) against a D1 database.

```sql
SELECT d1_execute('INSERT INTO users (name, age) VALUES (''John'', 25)', 'your_account_id', 'your_api_token', 'your_database_id');
```

### Write Functions

#### `d1_insert(table_name, account_id, api_token, database_id, col1, val1, col2, val2, ...)`
Insert a single row into a D1 table.

```sql
SELECT d1_insert('users', 'your_account_id', 'your_api_token', 'your_database_id', 'name', 'John', 'age', '25', 'email', 'john@example.com');
```

#### `d1_update(table_name, account_id, api_token, database_id, where_col, where_val, set_col1, set_val1, set_col2, set_val2, ...)`
Update rows in a D1 table.

```sql
SELECT d1_update('users', 'your_account_id', 'your_api_token', 'your_database_id', 'id', '1', 'name', 'John Updated', 'age', '26');
```

#### `d1_delete(table_name, account_id, api_token, database_id, where_col, where_val)`
Delete rows from a D1 table.

```sql
SELECT d1_delete('users', 'your_account_id', 'your_api_token', 'your_database_id', 'id', '1');
```

### Bulk Operations

#### `d1_bulk_insert(account_id, api_token, database_id, table_name, columns, values)`
Insert multiple rows efficiently using JSON format.

```sql
SELECT d1_bulk_insert('your_account_id', 'your_api_token', 'your_database_id', 'users', 'name,age,email', 'John,25,john@example.com;Jane,30,jane@example.com');
```

### Catalog Management

#### `d1_refresh(account_id, api_token, database_id, schema_or_table_name)`
Refresh the catalog to reflect schema changes in the D1 database.

```sql
SELECT d1_refresh('your_account_id', 'your_api_token', 'your_database_id', 'main');
```

### Storage Extension

#### `ATTACH 'd1://account_id=...;api_token=...;database_id=...' AS db_name`
Attach a D1 database as a DuckDB database for seamless integration.

```sql
ATTACH 'd1://account_id=your_account_id;api_token=your_api_token;database_id=your_database_id' AS d1_db;
SELECT * FROM d1_db.users;
```

## Type Mapping

The extension automatically maps SQLite/D1 types to appropriate DuckDB types:

- `INTEGER` → `BIGINT`
- `REAL` → `DOUBLE`
- `TEXT`/`VARCHAR` → `VARCHAR`
- `BLOB` → `BLOB`
- `BOOLEAN` → `BOOLEAN`
- `DATE` → `DATE`
- `TIME` → `TIME`
- `TIMESTAMP` → `TIMESTAMP`
- `NUMERIC`/`DECIMAL` → `DECIMAL(38,10)`
- `JSON` → `JSON`
- `UUID` → `UUID`

## Configuration

### Getting D1 Credentials

1. **Account ID**: Found in your Cloudflare dashboard URL or API
2. **API Token**: Create a custom token with D1 permissions in Cloudflare dashboard
3. **Database ID**: Found in your D1 database settings

### Example Configuration

```sql
-- Set up D1 connection parameters
SET d1_account_id = 'your_account_id';
SET d1_api_token = 'your_api_token';
SET d1_database_id = 'your_database_id';

-- Query D1 database
SELECT * FROM d1_query('SELECT * FROM users', current_setting('d1_account_id'), current_setting('d1_api_token'), current_setting('d1_database_id'));
```

## Examples

### Basic Query
```sql
-- Load the extension
LOAD 'cloudflare_d1';

-- Query users table
SELECT * FROM d1_query('SELECT id, name, email FROM users WHERE active = 1', 'account_id', 'api_token', 'database_id');
```

### Insert Data
```sql
-- Insert a new user
SELECT d1_insert('users', 'account_id', 'api_token', 'database_id', 'name', 'Alice', 'email', 'alice@example.com', 'age', '28');
```

### Update Data
```sql
-- Update user email
SELECT d1_update('users', 'account_id', 'api_token', 'database_id', 'id', '123', 'email', 'newemail@example.com');
```

### Bulk Operations
```sql
-- Insert multiple users
SELECT d1_bulk_insert('account_id', 'api_token', 'database_id', 'users', 'name,email,age', 'Bob,bob@example.com,30;Carol,carol@example.com,25');
```

### Attach as Database
```sql
-- Attach D1 database
ATTACH 'd1://account_id=your_account_id;api_token=your_api_token;database_id=your_database_id' AS my_d1_db;

-- Query directly
SELECT * FROM my_d1_db.users WHERE age > 25;

-- Join with local data
SELECT u.name, l.city 
FROM my_d1_db.users u 
JOIN local_cities l ON u.city_id = l.id;
```

## Error Handling

The extension provides detailed error messages for common issues:

- **Authentication errors**: Invalid API token or account ID
- **Database not found**: Invalid database ID
- **SQL syntax errors**: Malformed SQL statements
- **Type conversion errors**: Incompatible data types
- **Network errors**: Connection issues with Cloudflare API

## Performance Considerations

- **Bulk operations**: Use `d1_bulk_insert` for inserting large datasets
- **Query optimization**: Use appropriate WHERE clauses to limit data transfer
- **Connection reuse**: The extension reuses HTTP connections when possible
- **Type mapping**: Automatic type conversion may add overhead for large datasets

## Limitations

- **Read-only catalog**: Schema changes require manual refresh
- **No transactions**: Each operation is independent
- **Rate limits**: Subject to Cloudflare D1 API rate limits
- **Network dependency**: Requires internet connection to Cloudflare


## Building
### Managing dependencies
DuckDB extensions uses VCPKG for dependency management. Enabling VCPKG is very simple: follow the [installation instructions](https://vcpkg.io/en/getting-started) or just run the following:
```shell
git clone https://github.com/Microsoft/vcpkg.git
./vcpkg/bootstrap-vcpkg.sh
export VCPKG_TOOLCHAIN_PATH=`pwd`/vcpkg/scripts/buildsystems/vcpkg.cmake
```
Note: VCPKG is only required for extensions that want to rely on it for dependency management. If you want to develop an extension without dependencies, or want to do your own dependency management, just skip this step. Note that the example extension uses VCPKG to build with a dependency for instructive purposes, so when skipping this step the build may not work without removing the dependency.

### Build steps
Now to build the extension, run:
```sh
make
```
The main binaries that will be built are:
```sh
./build/release/duckdb
./build/release/test/unittest
./build/release/extension/cloudflare_d1/cloudflare_d1.duckdb_extension
```
- `duckdb` is the binary for the duckdb shell with the extension code automatically loaded.
- `unittest` is the test runner of duckdb. Again, the extension is already linked into the binary.
- `cloudflare_d1.duckdb_extension` is the loadable binary as it would be distributed.

## Running the extension
To run the extension code, simply start the shell with `./build/release/duckdb`.

Now we can use the features from the extension directly in DuckDB. The template contains a single scalar function `cloudflare_d1()` that takes a string arguments and returns a string:
```
D select cloudflare_d1('Jane') as result;
┌───────────────┐
│    result     │
│    varchar    │
├───────────────┤
│ CloudflareD1 Jane 🐥 │
└───────────────┘
```

## Running the tests
Different tests can be created for DuckDB extensions. The primary way of testing DuckDB extensions should be the SQL tests in `./test/sql`. These SQL tests can be run using:
```sh
make test
```

### Installing the deployed binaries
To install your extension binaries from S3, you will need to do two things. Firstly, DuckDB should be launched with the
`allow_unsigned_extensions` option set to true. How to set this will depend on the client you're using. Some examples:

CLI:
```shell
duckdb -unsigned
```

Python:
```python
con = duckdb.connect(':memory:', config={'allow_unsigned_extensions' : 'true'})
```

NodeJS:
```js
db = new duckdb.Database(':memory:', {"allow_unsigned_extensions": "true"});
```

Secondly, you will need to set the repository endpoint in DuckDB to the HTTP url of your bucket + version of the extension
you want to install. To do this run the following SQL query in DuckDB:
```sql
SET custom_extension_repository='bucket.s3.eu-west-1.amazonaws.com/<your_extension_name>/latest';
```
Note that the `/latest` path will allow you to install the latest extension version available for your current version of
DuckDB. To specify a specific version, you can pass the version instead.

After running these steps, you can install and load your extension using the regular INSTALL/LOAD commands in DuckDB:
```sql
INSTALL cloudflare_d1
LOAD cloudflare_d1
```
