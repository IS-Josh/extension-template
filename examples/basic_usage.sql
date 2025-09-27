-- CloudflareD1 Extension - Basic Usage Examples
-- This file demonstrates how to use the CloudflareD1 extension with DuckDB

-- Load the extension
LOAD 'cloudflare_d1';

-- Test basic functionality
SELECT cloudflare_d1('Hello World') as greeting;

-- Example 1: Query D1 database
-- Replace with your actual D1 credentials
SELECT * FROM d1_query(
    'SELECT id, name, email, created_at FROM users WHERE active = 1 ORDER BY created_at DESC LIMIT 10',
    'your_account_id',
    'your_api_token', 
    'your_database_id'
);

-- Example 2: Insert a new user
SELECT d1_insert(
    'users',
    'your_account_id',
    'your_api_token',
    'your_database_id',
    'name', 'John Doe',
    'email', 'john@example.com',
    'age', '30',
    'active', 'true'
) as inserted_rows;

-- Example 3: Update user information
SELECT d1_update(
    'users',
    'your_account_id', 
    'your_api_token',
    'your_database_id',
    'id', '123',  -- WHERE clause
    'email', 'newemail@example.com',  -- SET clause
    'age', '31'   -- Additional SET clause
) as updated_rows;

-- Example 4: Delete inactive users
SELECT d1_delete(
    'users',
    'your_account_id',
    'your_api_token', 
    'your_database_id',
    'active', 'false'  -- WHERE clause
) as deleted_rows;

-- Example 5: Bulk insert multiple users
SELECT d1_bulk_insert(
    'your_account_id',
    'your_api_token',
    'your_database_id',
    'users',
    'name,email,age,active',
    'Alice,alice@example.com,25,true;Bob,bob@example.com,30,true;Carol,carol@example.com,28,false'
) as bulk_inserted_rows;

-- Example 6: Execute DDL statements
SELECT d1_execute(
    'CREATE TABLE IF NOT EXISTS products (id INTEGER PRIMARY KEY, name TEXT, price REAL, in_stock BOOLEAN)',
    'your_account_id',
    'your_api_token',
    'your_database_id'
) as ddl_result;

-- Example 7: Refresh catalog after schema changes
SELECT d1_refresh(
    'your_account_id',
    'your_api_token',
    'your_database_id', 
    'main'
) as refresh_result;

-- Example 8: Attach D1 database as a DuckDB database
ATTACH 'd1://account_id=your_account_id;api_token=your_api_token;database_id=your_database_id' AS d1_db;

-- Now you can query D1 tables directly
SELECT * FROM d1_db.users WHERE age > 25;

-- Join D1 data with local data
CREATE TABLE local_cities (id INTEGER, name TEXT, country TEXT);
INSERT INTO local_cities VALUES (1, 'New York', 'USA'), (2, 'London', 'UK'), (3, 'Tokyo', 'Japan');

SELECT u.name, u.email, c.name as city, c.country
FROM d1_db.users u
JOIN local_cities c ON u.city_id = c.id
WHERE u.active = true;

-- Example 9: Complex query with aggregations
SELECT 
    c.country,
    COUNT(*) as user_count,
    AVG(u.age) as avg_age,
    MAX(u.created_at) as latest_signup
FROM d1_db.users u
JOIN local_cities c ON u.city_id = c.id
WHERE u.active = true
GROUP BY c.country
ORDER BY user_count DESC;

-- Example 10: Using with DuckDB's analytical functions
SELECT 
    name,
    email,
    age,
    RANK() OVER (ORDER BY age DESC) as age_rank,
    CASE 
        WHEN age < 25 THEN 'Young'
        WHEN age < 35 THEN 'Adult' 
        ELSE 'Senior'
    END as age_group
FROM d1_db.users
WHERE active = true
ORDER BY age_rank;
