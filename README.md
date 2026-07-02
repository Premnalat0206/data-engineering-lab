# Data Engineering Lab

A collection of hands-on Data Engineering projects built using **Python, PySpark, and SQL**. This repository is designed to simulate real-world Data Engineering workflows by building production-style ETL pipelines from scratch.

Each project focuses on solving a real business problem using industry-standard ETL techniques such as data validation, Change Data Capture (CDC), watermark-based incremental processing, joins, window functions, aggregations, and data quality monitoring.

---

# Tech Stack

- Python
- PySpark
- SQL
- Git
- GitHub

---

# Projects

## Day 01 - Healthcare Patient Registration Pipeline

### Business Scenario

Process patient registration data and generate healthcare analytics reports.

### Topics Covered

- Null Validation
- Duplicate Validation
- Data Quality Metrics
- Data Transformation
- KPI Aggregation

---

## Day 02 - Retail Sales ETL Pipeline

### Business Scenario

Process retail sales transactions and generate product and revenue analytics.

### Topics Covered

- Product Master Validation
- Business Rule Validation
- Inner Join
- Revenue Calculations
- Sales Reporting

---

## Day 03 - Telecom Recharge Pipeline

### Business Scenario

Analyze telecom recharge transactions and generate customer recharge insights.

### Topics Covered

- Composite Key Validation
- Business Rule Validation
- Left Join
- Window Functions
- Revenue Analytics

---

## Day 04 - Banking Transaction Pipeline

### Business Scenario

Process banking transactions and generate customer activity reports.

### Topics Covered

- Anti Join
- Incremental Loading
- lag() Window Function
- Customer Analytics
- Banking KPI Reporting

---

## Day 05 - E-Commerce Order CDC Pipeline

### Business Scenario

Detect new and updated customer orders using Change Data Capture (CDC).

### Topics Covered

- Change Data Capture (CDC)
- Left Semi Join
- rank() Window Function
- Customer Segmentation
- Revenue Analytics

---

## Day 06 - Logistics Shipment Tracking CDC Pipeline

### Business Scenario

Track shipment lifecycle events and generate logistics performance reports.

### Topics Covered

- Full CDC (New / Updated / Deleted Records)
- Broadcast Join
- lead() Window Function
- Shipment Tracking KPIs
- Delivery Analytics

---

## Day 07 - HR Employee Payroll CDC Pipeline

### Business Scenario

Process daily HR employee snapshots to identify new hires, employee updates, and employees who have left the organization while generating payroll analytics for HR and Finance teams.

### Topics Covered

- Full Change Data Capture (New / Updated / Deleted Employees)
- Multi-Column CDC Detection
- Data Validation Framework
- Data Quality Metrics
- Department Master Validation
- Salary Band Classification
- dense_rank() Window Function
- Repartition Optimization
- Department-wise Payroll Analytics
- Employee Ranking by Salary

---

## Day 08 - Insurance Claims Fraud Analytics Pipeline

### Business Scenario

Process daily insurance claims to identify high-risk customers, suspicious claim behavior, and generate fraud analytics datasets for reporting and downstream machine learning models.

### Topics Covered

- Watermark-Based Incremental Processing
- Multi-Table Joins
- Customer & Policy Master Enrichment
- Data Validation Framework
- Data Quality Metrics
- Cumulative Sum Window Function
- Fraud Flag Generation
- Claim Size Classification
- Branch-wise Claim Analytics
- Risk Category Analytics
- Customer Claim Analytics

---

# Data Engineering Concepts Covered

## Data Quality & Validation

- Null Validation
- Duplicate Validation
- Business Rule Validation
- Master Data Validation
- Reject Record Handling
- Data Quality Metrics
- Error Reason Generation

---

## Spark Transformations

- withColumn()
- when() / otherwise()
- concat_ws()
- to_date()
- datediff()
- repartition()

---

## Join Strategies

- Inner Join
- Left Join
- Left Anti Join
- Left Semi Join
- Broadcast Join
- Multi-Table Joins

---

## Window Functions

- row_number()
- rank()
- dense_rank()
- lag()
- lead()
- Running Total using sum().over()

---

## Aggregations

- count()
- sum()
- avg()
- max()

---

# Data Engineering Patterns

- Incremental Loading
- Watermark-Based Incremental Processing
- Change Data Capture (CDC)
- Full CDC (New / Updated / Deleted Records)
- Multi-Column Change Detection
- Master Data Validation
- Composite Key Validation
- Data Quality Monitoring
- Fraud Analytics
- Payroll Analytics
- Shipment Lifecycle Analytics

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
├── Day_07_HR_Employee_Payroll_CDC_Pipeline/
├── Day_08_Insurance_Claims_Fraud_Pipeline/
└── README.md
```

---

# Learning Goals

This repository focuses on building practical Data Engineering skills through real-world business scenarios.

## ETL Development

- End-to-End ETL Pipeline Development
- Production-Style Project Structure
- Data Validation Frameworks
- Data Transformation Pipelines
- Analytical Reporting Pipelines

---

## PySpark

- DataFrame API
- Spark SQL
- Window Functions
- Join Strategies
- Aggregations
- Spark Performance Optimization

---

## Data Engineering

- Change Data Capture (CDC)
- Watermark-Based Incremental Processing
- Incremental Data Processing
- Data Quality Monitoring
- Business Rule Validation
- Master Data Validation
- Fraud Analytics
- Operational Analytics

---

## Software Engineering

- Modular Python Design
- Reusable ETL Components
- Git Version Control
- Project Documentation

---

# Future Topics

- Apache Airflow
- Apache Kafka
- Delta Lake
- Snowflake
- dbt
- Data Warehousing
- Spark Optimization
- Partitioning & Bucketing
- Streaming Pipelines
- AWS Data Engineering
- Cloud Data Lakes
- CI/CD for Data Pipelines

---

# Repository Objective

The objective of this repository is to build strong practical Data Engineering skills by implementing production-style ETL pipelines using PySpark. Each project introduces new concepts incrementally, progressing from data validation and transformation to advanced incremental processing, Change Data Capture (CDC), watermark processing, Spark optimizations, window functions, and analytical reporting while following modular software engineering practices.