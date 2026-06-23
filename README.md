# Data Engineering Lab

A collection of hands-on Data Engineering projects built using Python, PySpark, and SQL. This repository focuses on developing real-world ETL pipelines, data validation frameworks, CDC (Change Data Capture), window functions, aggregations, and data quality monitoring.

Each project simulates a business use case and follows an industry-style pipeline structure with validation, transformation, aggregation, and reporting layers.

---

## Tech Stack

* Python
* PySpark
* SQL
* Git
* GitHub

---

# Projects

## Day 01 - Healthcare Patient Registration Pipeline

### Business Scenario

Process patient registration data and generate healthcare analytics reports.

### Topics Covered

* Null Validation
* Duplicate Validation
* Data Quality Metrics
* Data Transformation
* KPI Aggregation

---

## Day 02 - Retail Sales ETL Pipeline

### Business Scenario

Process retail sales transactions and generate product and revenue analytics.

### Topics Covered

* Product Master Validation
* Business Rule Validation
* Inner Join
* Revenue Calculations
* Sales Reporting

---

## Day 03 - Telecom Recharge Pipeline

### Business Scenario

Analyze telecom recharge transactions and generate customer recharge insights.

### Topics Covered

* Composite Key Validation
* Business Rule Validation
* Left Join
* Window Functions
* Revenue Analytics

---

## Day 04 - Banking Transaction Pipeline

### Business Scenario

Process banking transactions and generate customer activity reports.

### Topics Covered

* Anti Join
* Incremental Loading
* lag() Window Function
* Customer Analytics
* Banking KPI Reporting

---

## Day 05 - E-Commerce Order CDC Pipeline

### Business Scenario

Detect new and updated customer orders using Change Data Capture (CDC).

### Topics Covered

* Change Data Capture (CDC)
* Semi Join
* rank() Window Function
* Customer Segmentation
* Revenue Analytics

---

## Day 06 - Logistics Shipment Tracking CDC Pipeline

### Business Scenario

Track shipment lifecycle events and generate logistics performance reports.

### Topics Covered

* Full CDC (New / Updated / Deleted Records)
* Broadcast Join
* lead() Window Function
* Shipment Tracking KPIs
* Delivery Analytics

---

# Data Engineering Concepts Covered

## Data Quality & Validation

* Null Validation
* Duplicate Validation
* Business Rule Validation
* Customer Master Validation
* Reject Record Handling
* Data Quality Metrics

---

## Spark Transformations

* withColumn()
* when() / otherwise()
* concat_ws()
* to_date()
* datediff()

---

## Join Strategies

* Inner Join
* Left Join
* Semi Join
* Anti Join
* Broadcast Join

---

## Window Functions

* row_number()
* rank()
* lag()
* lead()

---

## Data Engineering Patterns

* Incremental Loading
* Change Data Capture (CDC)
* Customer Master Validation
* Composite Key Validation
* Shipment Lifecycle Analytics

---

# Repository Structure

```text
data-engineering-lab/

├── Day_01_Healthcare_Pipeline/
├── Day_02_Retail_Sales_Pipeline/
├── Day_03_Telecom_Recharge_Pipeline/
├── Day_04_Banking_Transaction_Pipeline/
├── Day_05_Ecommerce_CDC_Pipeline/
├── Day_06_Logistics_Shipment_CDC_Pipeline/
└── README.md
```

---

# Learning Goals

This repository is focused on building practical skills in:

* ETL Pipeline Development
* PySpark Data Processing
* Data Validation Frameworks
* CDC Implementations
* Window Functions
* Data Quality Monitoring
* Analytical Reporting
* Production-Style Data Engineering Workflows

---

# Future Topics

* Apache Airflow
* Apache Kafka
* Delta Lake
* Snowflake
* dbt
* Data Warehousing
* Spark Optimization
* Streaming Pipelines
* Cloud Data Engineering
