# SQL-Data-Warehouse-Project
Building a modern data warehouse with PostgreSQL, including an ETL, data cleaning, and preparation for corporate analytics

This project demonstrates a comprehensive data warehouse that is utilized to generate actionable insights and highlight industry best practices in data engineering and analytics. The data used for this project was sample data found online, as this was intended to be a proof of concept that could be applied to any company's financial data to create a similar workflow and data warehouse

I started by utilizing medallion architecture to split the tasks into three schemas - bronze, silver and gold. After defining the skeleton of this project, I used init_database.sql and init_schemas.sql to create the workspace.

Within the bronze layer, my goal was primarily to load in our six datasets each into a table, which was done in load_data.sql. I then took a closer look at each table to understand how the tables connected, what primary keys to use, and how I would go about merging later down the road, which was done in analyze_tables.sql

The silver layer is the most intense and time-consuming, as it was where I cleaned the data and transformed necessary columns to prepare for merging. I started by copying over our tables from the bronze layer in load_data.sql - it was important to do this because, in case something went wrong, I would always be able to access the original raw data from our prior schema. Then, in data_transformation.sql, I highlighted my process for cleaning the data in each individual table, prioritizing clear and descriptive naming conventions and handling discrepencies such as missing values or values that did not make sense in the context of the column they were a part of.

The gold layer was where I finally joined the tables together into three different views: dim_customers, dim_products, and fact_sales. This was done in order to properly group together our six tables and make it easier for analysts to work with later down the line.
