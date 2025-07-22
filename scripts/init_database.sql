-- Drop and recreate the warehouse_db database in case it already exists. Make sure you are connected to postgres first
DROP DATABASE IF EXISTS warehouse_db;
CREATE DATABASE warehouse_db;

-- Once you run this, switch your active connection to warehouse_db
