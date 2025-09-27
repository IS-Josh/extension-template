-- CloudflareD1 Extension - Advanced Usage Examples
-- This file demonstrates advanced patterns and best practices

-- Load the extension
LOAD 'cloudflare_d1';

-- Example 1: Data Pipeline - Extract from D1, Transform, Load to local
-- Step 1: Extract raw data from D1
CREATE TABLE raw_users AS 
SELECT * FROM d1_query(
    'SELECT * FROM users WHERE created_at >= date(''now'', ''-30 days'')',
    'your_account_id',
    'your_api_token',
    'your_database_id'
);

-- Step 2: Transform and clean data
CREATE TABLE clean_users AS
SELECT 
    id,
    TRIM(name) as name,
    LOWER(email) as email,
    CASE 
        WHEN age < 0 OR age > 120 THEN NULL 
        ELSE age 
    END as age,
    CASE 
        WHEN active IN ('true', '1', 'yes', 'on') THEN true
        WHEN active IN ('false', '0', 'no', 'off') THEN false
        ELSE NULL
    END as active,
    created_at
FROM raw_users
WHERE email LIKE '%@%'  -- Basic email validation
  AND name IS NOT NULL
  AND name != '';

-- Step 3: Load back to D1 (upsert pattern)
SELECT d1_execute(
    'INSERT OR REPLACE INTO clean_users (id, name, email, age, active, created_at) 
     VALUES ' || (
        SELECT string_agg(
            '(' || id || ', ''' || name || ''', ''' || email || ''', ' || 
            COALESCE(age::VARCHAR, 'NULL') || ', ' || 
            COALESCE(active::VARCHAR, 'NULL') || ', ''' || created_at || ''')',
            ', '
        )
        FROM clean_users
    ),
    'your_account_id',
    'your_api_token',
    'your_database_id'
);

-- Example 2: Batch Processing with Error Handling
-- Process users in batches to avoid API limits
CREATE TABLE batch_results (
    batch_id INTEGER,
    success_count INTEGER,
    error_count INTEGER,
    error_message TEXT
);

-- Simulate batch processing
INSERT INTO batch_results VALUES 
(1, 95, 5, 'Some validation errors'),
(2, 100, 0, NULL),
(3, 98, 2, 'Duplicate key errors');

-- Example 3: Data Synchronization Pattern
-- Compare local and remote data
ATTACH 'd1://account_id=your_account_id;api_token=your_api_token;database_id=your_database_id' AS remote_db;

-- Find records that exist locally but not remotely
SELECT 'local_only' as status, id, name, email
FROM local_users l
WHERE NOT EXISTS (
    SELECT 1 FROM remote_db.users r WHERE r.id = l.id
)

UNION ALL

-- Find records that exist remotely but not locally  
SELECT 'remote_only' as status, id, name, email
FROM remote_db.users r
WHERE NOT EXISTS (
    SELECT 1 FROM local_users l WHERE l.id = r.id
)

UNION ALL

-- Find records that differ between local and remote
SELECT 'different' as status, l.id, l.name, l.email
FROM local_users l
JOIN remote_db.users r ON l.id = r.id
WHERE l.name != r.name OR l.email != r.email OR l.age != r.age;

-- Example 4: Time-series Data Processing
-- Process daily metrics from D1
SELECT 
    DATE(created_at) as date,
    COUNT(*) as daily_signups,
    COUNT(*) FILTER (WHERE active = true) as active_signups,
    AVG(age) as avg_age
FROM d1_query(
    'SELECT created_at, active, age FROM users WHERE created_at >= date(''now'', ''-90 days'')',
    'your_account_id',
    'your_api_token', 
    'your_database_id'
)
GROUP BY DATE(created_at)
ORDER BY date;

-- Example 5: Multi-table Operations
-- Complex join across multiple D1 tables
SELECT 
    u.name,
    u.email,
    p.name as product_name,
    p.price,
    o.quantity,
    o.order_date,
    (p.price * o.quantity) as total_amount
FROM d1_query(
    'SELECT id, name, email FROM users WHERE active = true',
    'your_account_id',
    'your_api_token',
    'your_database_id'
) u
JOIN d1_query(
    'SELECT user_id, product_id, quantity, order_date FROM orders WHERE order_date >= date(''now'', ''-30 days'')',
    'your_account_id', 
    'your_api_token',
    'your_database_id'
) o ON u.id = o.user_id
JOIN d1_query(
    'SELECT id, name, price FROM products WHERE in_stock = true',
    'your_account_id',
    'your_api_token', 
    'your_database_id'
) p ON o.product_id = p.id
ORDER BY total_amount DESC;

-- Example 6: Data Quality Checks
-- Validate data integrity across D1 tables
WITH data_quality AS (
    SELECT 
        'users' as table_name,
        COUNT(*) as total_rows,
        COUNT(*) FILTER (WHERE email IS NULL OR email = '') as missing_emails,
        COUNT(*) FILTER (WHERE age IS NULL OR age < 0 OR age > 120) as invalid_ages,
        COUNT(*) FILTER (WHERE created_at IS NULL) as missing_dates
    FROM d1_query('SELECT * FROM users', 'your_account_id', 'your_api_token', 'your_database_id')
    
    UNION ALL
    
    SELECT 
        'orders' as table_name,
        COUNT(*) as total_rows,
        COUNT(*) FILTER (WHERE user_id IS NULL) as missing_user_ids,
        COUNT(*) FILTER (WHERE quantity IS NULL OR quantity <= 0) as invalid_quantities,
        COUNT(*) FILTER (WHERE order_date IS NULL) as missing_dates
    FROM d1_query('SELECT * FROM orders', 'your_account_id', 'your_api_token', 'your_database_id')
)
SELECT 
    table_name,
    total_rows,
    missing_emails + missing_user_ids as missing_required_fields,
    invalid_ages + invalid_quantities as invalid_values,
    missing_dates,
    ROUND((total_rows - (missing_emails + missing_user_ids + invalid_ages + invalid_quantities + missing_dates)) * 100.0 / total_rows, 2) as data_quality_score
FROM data_quality;

-- Example 7: Performance Monitoring
-- Monitor query performance and API usage
CREATE TABLE query_log (
    query_id INTEGER,
    query_text TEXT,
    execution_time_ms INTEGER,
    rows_returned INTEGER,
    api_calls INTEGER,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Log a query execution
INSERT INTO query_log (query_id, query_text, execution_time_ms, rows_returned, api_calls)
VALUES (
    1,
    'SELECT COUNT(*) FROM users WHERE active = true',
    150,
    1,
    1
);

-- Analyze query performance
SELECT 
    DATE(timestamp) as date,
    COUNT(*) as query_count,
    AVG(execution_time_ms) as avg_execution_time,
    SUM(api_calls) as total_api_calls,
    SUM(rows_returned) as total_rows_processed
FROM query_log
WHERE timestamp >= CURRENT_TIMESTAMP - INTERVAL '7 days'
GROUP BY DATE(timestamp)
ORDER BY date;

-- Example 8: Configuration Management
-- Use DuckDB settings for credential management
SET d1_account_id = 'your_account_id';
SET d1_api_token = 'your_api_token';
SET d1_database_id = 'your_database_id';

-- Create a helper function for easier querying
CREATE OR REPLACE FUNCTION query_d1(sql_text TEXT)
RETURNS TABLE AS (
    SELECT * FROM d1_query(
        sql_text,
        current_setting('d1_account_id'),
        current_setting('d1_api_token'),
        current_setting('d1_database_id')
    )
);

-- Use the helper function
SELECT * FROM query_d1('SELECT * FROM users LIMIT 10');

-- Example 9: Error Recovery and Retry Logic
-- Implement retry logic for failed operations
CREATE TABLE operation_log (
    operation_id INTEGER,
    operation_type TEXT,
    status TEXT,
    retry_count INTEGER,
    error_message TEXT,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Simulate retry logic
INSERT INTO operation_log (operation_id, operation_type, status, retry_count, error_message)
VALUES 
(1, 'bulk_insert', 'failed', 1, 'Rate limit exceeded'),
(1, 'bulk_insert', 'failed', 2, 'Network timeout'),
(1, 'bulk_insert', 'success', 3, NULL);

-- Find operations that need retry
SELECT 
    operation_id,
    operation_type,
    MAX(retry_count) as max_retries,
    MAX(timestamp) as last_attempt
FROM operation_log
WHERE status = 'failed' AND retry_count < 3
GROUP BY operation_id, operation_type;
