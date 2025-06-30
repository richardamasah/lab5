

#  Lakehouse Data Pipeline on AWS

##  Overview

This project implements a complete end-to-end data pipeline using a Lakehouse architecture on AWS. It ingests raw order data, cleans it using PySpark on AWS Glue, stores the processed data in Delta Lake format, runs a crawler to update metadata, executes queries via Athena, and archives raw files — all orchestrated with AWS Step Functions and triggered automatically using Lambda. 

This pipeline is designed to scale, handle daily file drops, and ensure data quality through validation and structured processing.

---

##  Problem Statement

In real-world e-commerce and retail platforms, order and product data arrives frequently and must be validated, cleaned, stored, and made queryable quickly. Manual ETL doesn't scale, and there's a need for automation, data quality checks, and reproducibility.

This project solves that by building a robust Lakehouse pipeline that:

* Reacts to new data automatically
* Validates and cleans incoming files
* Uses open standards (CSV, Delta Lake, Parquet)
* Runs serverless using AWS Glue, Lambda, and Step Functions
* Supports partitioning and scaling

---

##  Objectives

*  Build a Lakehouse architecture on AWS with automation
*  Process product and order data using PySpark on AWS Glue
*  Store data in Delta Lake format in an S3-based data warehouse
*  Use Glue Crawler + Athena to keep metadata updated and queryable
*  Use Lambda + Step Functions to orchestrate all tasks
*  Automate the full flow with CI/CD using GitHub Actions

---

##  Dataset (Schemas & Tables)

###  `products.csv`

| Column         | Type   |
| -------------- | ------ |
| product\_id    | int64  |
| department\_id | int64  |
| department     | string |
| product\_name  | string |

---

###  `orders_apr_2025.xlsx` → `orders.csv`

| Column           | Type    |
| ---------------- | ------- |
| order\_num       | int64   |
| order\_id        | int64   |
| user\_id         | int64   |
| order\_timestamp | string  |
| total\_amount    | float64 |
| date             | string  |

---

###  `order_items_apr_2025.xlsx` → `order_items.csv`

| Column                    | Type   |
| ------------------------- | ------ |
| id                        | int64  |
| order\_id                 | int64  |
| user\_id                  | int64  |
| days\_since\_prior\_order | int64  |
| product\_id               | int64  |
| add\_to\_cart\_order      | int64  |
| reordered                 | int64  |
| order\_timestamp          | string |
| date                      | string |

These are stored in the following S3 paths:

```
s3://lab5lakehouse/lakehouse/raw/products/
s3://lab5lakehouse/lakehouse/raw/orders/
s3://lab5lakehouse/lakehouse/raw/order_items/
```

---

##  Architecture

###  Key AWS Components Used

| Component          | Purpose                                                                |
| ------------------ | ---------------------------------------------------------------------- |
| **S3**             | Stores raw, clean (Delta), rejected, and archived data                 |
| **Glue Jobs**      | Run PySpark scripts to clean and validate data                         |
| **Glue Crawler**   | Scans clean S3 folders and updates metadata in AWS Glue Data Catalog   |
| **Athena**         | Queries the Delta Lake tables using SQL                                |
| **Lambda**         | Validates files and triggers Step Functions automatically              |
| **Step Functions** | Orchestrates the full ETL flow, including error handling and archiving |
| **GitHub Actions** | Deploys updated scripts and pipeline definitions (CI/CD)               |

---

##  Pipeline Workflow

Here’s the simplified flow:

```
S3 (new file) 
  ⬇
Lambda (validate file)
  ⬇
Step Function
  ├─ Parallel Glue jobs (products, orders, order_items)
  ├─ Glue Crawler
  ├─ Athena query
  └─ Archive raw file
```

###  ETL Sample Logic

Here’s a simplified version of `clean_products.py`:

```python
valid_df = df.filter(col("product_id").isNotNull() & col("product_name").isNotNull())
deduped_df = valid_df.dropDuplicates(["product_id"])
deduped_df.write.format("delta").mode("overwrite").save(clean_output_path)
```

Invalid rows are written to a rejected folder, while clean Delta-formatted data goes to the DWH.



---

##  Step Function Breakdown

The Step Function orchestrates the full pipeline. Once a valid file lands in the S3 raw zone, Lambda triggers the execution. Here's what happens inside:

###  Full Flow:

```
1️ Lambda validates file → starts Step Function

2️ Step Function:
    ├─ Runs 3 Glue jobs in parallel:
    │    • clean-products-job
    │    • clean-orders-job
    │    • clean-order-items-job
    │
    ├─ Then runs Glue Crawler
    │
    ├─ Then triggers Athena Lambda to query "SELECT COUNT(*) FROM products"
    │
    └─ Finally archives the raw file
```

###  Step Function Logic

Each Glue task has error handling (`Catch`) to gracefully fail and log issues.

Athena and Archive Lambdas both take in the same `source_file` path provided at the start.

###  Execution Input:

Example payload sent to the Step Function:

```json
{
  "source_file": "lakehouse/raw/orders/2025-07-01.csv"
}
```

---

##  How to Reproduce the Project

You can rebuild this project end-to-end by following these steps:

### 1.  Upload Files

* Convert all Excel files to CSV
* Place them in appropriate S3 raw folders:

  * `lakehouse/raw/products/`
  * `lakehouse/raw/orders/`
  * `lakehouse/raw/order_items/`

### 2. 🔧 Create Glue ETL Jobs

* Create three Glue jobs using the PySpark scripts in `etl_scripts/`
* Point each job to the corresponding raw folder and clean output folder (Delta)

### 3.  Set Up Step Function

* Use `state_machine/lakehouse-pipeline.json`
* Include:

  * Lambda validation step
  * Parallel Glue jobs
  * Crawler step
  * Athena query Lambda
  * Archive Lambda
  * Error handling

### 4.  Configure Lambda Functions

* `validateFileBeforeETL`: S3 trigger, validates file
* `lakehouseAthenaQuery`: Queries Athena
* `archiveRawFile`: Moves processed file from raw → archived

### 5.  Unit Testing

* Small logic is tested using `pytest`
* Sample: `helpers.py` contains `clean_product_name()` and is tested locally
* PySpark-based tests are excluded due to local Spark limitations

### 6.  CI/CD Pipeline (GitHub Actions)

* `.github/workflows/deploy-etl.yml`: Pushes ETL scripts and updates Step Function
* `.github/workflows/test-etl.yml`: Runs unit tests automatically

---

##  Challenges Faced

*  Spark cannot run on local Windows → PySpark tests were skipped
*  Athena Lambda required correct IAM + query formatting
*  Step Function error handling required trial and error with `.Catch` blocks
*  Lambda trigger + Step Function had race conditions if input was not passed correctly

---

##  Future Enhancements

| Feature                    | Description                                                      |
| -------------------------- | ---------------------------------------------------------------- |
|  Partitioned Delta Tables | Add dynamic partitioning by `date` to improve Athena query speed |
|  DynamoDB logging         | Log each processed file with status, counts, and error messages  |
|  SNS Notifications        | Notify users on success/failure of Step Function via email/SMS   |
|  Additional Data Checks   | Enforce schema validation or use Great Expectations              |
|  Glue Job CI/CD           | Automate creation/updating of Glue jobs using CDK or Terraform   |
|  PySpark test support     | Enable PySpark testing using EMR or WSL with Java                |
|  Dashboard                | Add QuickSight or Streamlit to visualize product sales trends    |

---






#   u p d a t e   s t e p f n 
 
 