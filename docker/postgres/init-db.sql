-- Create databases if they do not exist
SELECT 'CREATE DATABASE airflow'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow')\gexec

SELECT 'CREATE DATABASE marquez'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'marquez')\gexec

-- Grant privileges to the default postgres user
GRANT ALL PRIVILEGES ON DATABASE airflow TO postgres;
GRANT ALL PRIVILEGES ON DATABASE marquez TO postgres;

-- Create user marquez with password marquez for Marquez Lineage API
CREATE USER marquez WITH PASSWORD 'marquez';
GRANT ALL PRIVILEGES ON DATABASE marquez TO marquez;
