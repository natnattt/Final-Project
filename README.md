# Final Project Data Warehouse - Dibimbing - Kelompok 2

## Objectives
The challenge for you is to build a data infrastructure using synthetic data. You will be provided with a project template to assist you in your role as a Data Engineer. Here are the tasks:
- Participants should implement learned materials through an end-to-end project.
- Participants should construct a data infrastructure using synthetic data.

## Expected Output
1. Build ETL/ELT Jobs using Apache Airflow.
2. Perform Data Modeling in Postgres.
3. Create Dashboards for Data Visualization.
4. Construct a Presentation summarizing the work done.
   
_Note: All data used is synthetic, reflecting real-world data from an online retail company._

---

## Overview
### Full Pipeline
![image](https://github.com/natnattt/Final-Project/assets/164592034/de4bd2b9-745a-4557-81d8-2ea5cda23d8d)



## Setup
- Since our goal is to minimize our resource usage, we will use free cloud databases on [Neon](https://neon.tech/). Once we create a database in it, get the connection string and put it in variable `DW_POSTGRES_URI` at `.env`
    ```.env
    DW_POSTGRES_URI="postgresql://...?sslmode=require"
    ```
- In order to spin up the containers, first you have to build all the Docker images needed using 
    ```sh
    make build
    ```
- Once all the images have been build up, you can try to spin up the containers using
    ```sh
    make spinup
    ```
- Once all the containers ready, you can try to
    - Access the Airflow on port `8081`
    - Access the Metabase on port `3001`, for the username and password, you can try to access the [.env](/.env) file
    - If you didn't find the created tables in the Metabase `Browse data`, you can try to sync it through Metabase admin UI
- Run the DAGs starting from the Ingest Data DAG and proceed to Data Transformation.
- Customize your visualizations:
  * Use Metabase via port `3001` with SQL queries for visualization.
  * Alternatively, connect Tableau to Postgres using the connection from [Neon](https://neon.tech/) with add connection.
- Ready to go!

---

### Tools
* Data Warehouse: Postgresql
* Data Visualization: Tableau
* Containerization: Docker
* Workflow Orchestration: Airflow
* Language: Python, SQL
* Lib: pandas vers 2.1.0, sqlalchemy, fastparquet, fastavro, psycopg2-binary, apache-airflow, openpyxl, xlrd

### Project structure

```
.
├── dags                                  # for airflow dags
│   ├── create_new_table_dag.py                               
│   ├── data_marts_dag.py                 
│   ├── dim_fact_table_dag.py             # dim & fact table for data marts
│   ├── main_dag.py                       # ingest the data
│   └── transform_dag.py                  # for data warehouse transformations
├── data                                  # data source
├── docker                                # for containerizations
└── scripts
├── .env                                  # environment variables (contains usn and pass for metabase)
├── .gitignore
├── makefile
├── README.md
└── requirements.txt                      # library
.
```

## Folder Structure

**main**

In the main folder, you can find `makefile`, so if you want to automate any script, you can try to modify it.

There is also `requirements.txt`, so if you want to add a library to the Airflow container, you can try to add it there. Once you add the library name in the file, make sure you rebuild the image before you spin up the container.

**dags**

This is were you put your `dag` files. This folder is already mounted on the container, hence any updates here will automatically take effect on the container side.

<details>
    
### Ingest Data DAG
![image](https://github.com/natnattt/Final-Project/assets/164592034/60212f39-1101-4ba1-bc96-c03a511b82c7)
### Data Transformation 
![image](https://github.com/natnattt/Final-Project/assets/164592034/d1ca9796-c2ea-42dc-8d7e-5872a622f553)
### Data Marts
![image](https://github.com/natnattt/Final-Project/assets/164592034/f3b8d166-8364-4d1d-975d-ec3dabcdd59c)

</details>

**data**

This flder contains the data needed for your project. If you want to generate or add additional data, you can place them here.

**docker**

Here is the place where you can modify or add a new docker stack if you decide to introduce a new data stack in your data platform. You are free to modify the given `docker-compose.yml` and `Dockerfile.airflow`.

**scripts**

This folder contains script needed in order to automate an initializations process on docker-container setup.

---

### Dimensional Modelling
[![dbt-dag.png](https://i.postimg.cc/Ss1zrZ0Q/dbt-dag.png)](https://postimg.cc/DJsZfPyR)

---

## Data Transformation Project
A detailed description of the data transformation steps performed on various datasets. The transformations include removing unnecessary columns, eliminating duplicates, converting data types, and processing date-time columns to ensure clean and consistent data for further analysis.

#### Supplier Data Transformation

1. **Remove Unnecessary Columns:**
   - Removed the column `Unnamed: 0`.
   
2. **Remove Duplicates:**
   - Removed duplicates based on the `id` column, keeping only the most recent duplicate (`keep='last'`).

#### Product Data Transformation

1. **Remove Unnecessary Columns:**
   - Removed the column `Unnamed: 0`.
   
2. **Remove Duplicates:**
   - Removed duplicates based on the `id` column, keeping only the most recent duplicate (`keep='last'`).

#### Product Category Data Transformation

1. **Remove Unnecessary Columns:**
   - Removed the column `Unnamed: 0`.
   
2. **Remove Duplicates:**
   - Removed duplicates based on the `id` column, keeping only the most recent duplicate (`keep='last'`).

#### Order Data Transformation

1. **Convert Data Types:**
   - Converted the `customer_id` column to float.
   - Ensured the `created_at` column is treated as a string.
   
2. **Remove Duplicates:**
   - Removed duplicates based on the `id` column, keeping only the most recent duplicate (`keep='last'`).

3. **Date-Time Column Processing:**
   - Split the `created_at` column into `created_at` and `period_time` using a space delimiter. If splitting fails, default the `period_time` column to `00:00:00`.

#### Login Attempts Data Transformation

1. **Convert Data Types:**
   - Converted the `customer_id` column to float.
   - Ensured the `attempted_at` column is treated as a string.
   
2. **Remove Duplicates:**
   - Removed duplicates based on the `id` column, keeping only the most recent duplicate (`keep='last'`).

3. **Date-Time Column Processing:**
   - Split the `attempted_at` column into `attempted_at` and `period` using a space delimiter.

#### Customer Data Transformation

1. **Convert Data Types:**
   - Converted the `zip_code` column to a string.
   
2. **Create New Columns:**
   - Created a new `full_name` column by concatenating the `first_name` and `last_name` columns.

#### Coupon Data Transformation

1. **Discount Column Processing:**
   - If the `discount_percent` column exists, converted its values to strings and removed the percent sign (%).
   - Added a new `numeric_value` column containing the numeric value of the converted discount percentages.

#### Order Item Data Transformation

1. **Remove Duplicates:**
   - Removed duplicates based on the `id` column, keeping only the most recent duplicate (`keep='last'`).

##### _Additional Notes_

- Each transformation function uses `XCom` to fetch data from the database.
- Each fetched and transformed dataset is loaded into new tables in PostgreSQL using the `to_sql` function from Pandas.

---

## Data Marts Overview

1. Customer Lifetime Value (CLV): Identify high-value customers and understand their spending patterns. This helps in tailoring marketing strategies and improving customer retention. The `marts_cust_highest_clv` table aggregates customer order data to calculate the lifetime value of each customer. It includes metrics such as total orders, total spending, average order value, and the computed customer lifetime value (CLV). The table highlights the top 7 customers by CLV.

2. Burn Rate: Track the amount of discount given over time to manage promotional costs. The `marts_burn_rate` table records the total discount given each day. This is calculated based on order details, product prices, and applicable coupon values.

3. CLV Quartiles: Segment customers into different CLV groups to identify patterns and target marketing efforts more effectively. The `marts_quartiles_clv` table segments customers into 'Low CLV', 'Medium CLV', and 'High CLV' based on their total spending. It provides the count and percentage of customers in each segment, helping to understand the distribution of customer value.

4. Sales Performance: Monitor daily sales performance to make informed decisions about sales strategies and operations. The `marts_sales_performance` table captures daily sales data, including total orders, total sales, total units sold, and the average price per unit. This helps in tracking sales trends and performance over time.

5. Top Products Sales: Highlight top-performing products to manage inventory effectively and plan product development strategies. The `marts_top_products_sales` table lists the top 5 products by total sales amount. It aggregates sales data by product and ranks them to identify the best sellers.

6. Category Sales Customers: Analyze sales performance across different product categories to optimize product offerings and marketing strategies. The `marts_category_sales_customers` table aggregates sales data by product category, including the total number of customers and total sales for each category. This helps in understanding the popularity and performance of different categories.

---

## Dashboard with Tableau
[![Clean-Shot-2023-12-06-at-14-13-54.png](https://i.postimg.cc/dVmMTpdC/Clean-Shot-2023-12-06-at-14-13-54.png)](https://postimg.cc/4Yd2D8W4)



