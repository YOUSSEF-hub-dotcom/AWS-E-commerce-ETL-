# 🛍️ E-Commerce Data Lakehouse ETL Pipeline

An end-to-end distributed Big Data ETL pipeline built with **PySpark** on **AWS Glue** and **Amazon S3**. This project processes 12 relational raw e-commerce datasets, applies rigorous data quality checks, transforms schemas, models an analytical Star Schema, and outputs optimized Parquet datasets partitioned for cloud querying.

---

## 📐 Architecture Overview

[ Local Python Data Gen ] ➔ [ Boto3 Ingestion ] ➔ [ AWS S3 Raw Zone ]
│
▼
[ Partitioned Parquet Lakehouse ] ◄─ [ Star Schema ] ◄─ [ AWS Glue PySpark ]


1. **Data Generation & Ingestion:** Local Python scripts generated 12 relational datasets (50,000+ orders, 100,000+ line items) uploaded via `boto3` to S3 Raw Zone.
2. **Distributed Compute:** AWS Glue Interactive Session running PySpark with IAM Role authentication.
3. **ETL & Data Quality:** Standardized column naming, string trimming, email/status regex validation, and deduplication across primary keys.
4. **Data Modeling:** Constructed an analytical Star Schema with a centralized `fact_sales` table and clean Dimension tables.
5. **Storage & Partitioning:** Exported curated datasets in **Parquet format with Snappy compression**, partitioning `orders` by `year` and `month`.

---

## 🗂️ Dataset Schema & Structure

The pipeline processes 12 core e-commerce tables:
* **Core Dimensions:** `customers`, `products`, `categories`, `departments`, `employees`, `suppliers`, `shippers`
* **Transactional Facts & Bridges:** `orders`, `order_details`, `payments`, `shipments`, `product_suppliers`

---

## 🛠️ Pipeline Stages & Implementation

### 1. Data Profiling & Data Quality (Parts 1 & 2)
* **Standardization:** Converted all column headers to `snake_case` / lowercase.
* **Cleaning:** Applied `.trim()` on text fields, converted emails to lowercase, and status strings to uppercase.
* **Integrity Enforcement:** Filtered out NULL primary keys and deduplicated records using `.dropDuplicates()`.
* **Validation:** Verified email regex patterns, valid order status categories (`PENDING`, `SHIPPED`, `DELIVERED`, `CANCELLED`), and positive product pricing.

### 2. Type Transformations & Feature Engineering (Parts 3 & 4)
* **Date Casting:** Converted timestamp strings across all 12 tables to explicit `DateType` (`YYYY-MM-DD`).
* **Date Extractions:** Derived `year`, `month`, `quarter`, `day`, and `day_of_week` from `orderdate`.
* **Financial Precision:** Cast prices, unit costs, and total amounts to `Decimal(12,2)`.
* **Business Categorization:**
  * `order_value_category`: Categorized orders into `HIGH` (>= $1000), `MEDIUM` ($500–$999), and `LOW` (< $500).
  * `customer_segment`: Segmented customers into `VIP`, `PREMIUM`, `REGULAR`, and `LOW_VALUE` based on cumulative spend.

### 3. Window Functions & Analytical Joins (Parts 5 & 6)
* Leveraged `PySpark Window` functions to:
  * Extract the first and latest order per customer using `row_number()`.
  * Rank customers by total sales using `rank()`.
  * Identify top 3 and most expensive products per category.
  * Retrieve the latest shipment state per order.
* Executed multi-table joins to assemble the analytical `fact_sales` model and consolidated dimension views.

### 4. Storage Optimization & Partitioning (Part 10)
All curated datasets were saved back to Amazon S3:
* **Format:** Apache Parquet
* **Compression:** Snappy
* **Partition Strategy:** `orders` partitioned by `/year=YYYY/month=MM/` for query pushdown optimization in AWS Athena / Redshift.

---

## 💻 Code Snippet Example (PySpark Execution)

```python
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# 1. Total Amount Calculation
order_details = order_details.withColumn(
    "total_amount", 
    (F.col("quantity") * F.col("unitprice")).cast("decimal(12,2)")
)

# 2. Window Function: Rank Products in Categories
cat_window = Window.partitionBy("categoryid").orderBy(F.col("price").desc())
top_3_products = prod_cat.withColumn("rn", F.row_number().over(cat_window)).filter("rn <= 3")

# 3. Partitioned Parquet Write
orders.write.mode("overwrite") \
    .partitionBy("year", "month") \
    .parquet(f"{PROCESSED_PATH}/orders/")
🚀 Key Technologies Used
Language: Python, PySpark (Spark SQL, DataFrames, DSL)

Cloud Infrastructure: AWS Glue Interactive Sessions, Amazon S3, IAM

File Formats: CSV (Raw), Parquet + Snappy (Curated)

Data Modeling: Dimensional Modeling (Star Schema), Window Functions

👥 Author
Youssef Mahmoud - Data & AI 
