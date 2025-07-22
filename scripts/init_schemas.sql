-- These three schemas define medalliation architecture, a standard practice when working with databases
CREATE SCHEMA bronze;
-- Bronze layer will store raw and unprocessed data from our sources, useful for offering traceability and debugging later on
CREATE SCHEMA silver;
-- Silver layer will store clean and standardized data, where we will transform and prepare data for analysis
CREATE SCHEMA gold;
-- Gold layer will store business-ready data: the data that will be used by analysts and businesses to generate insight from
