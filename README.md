# 📊 Volume Monitoring with Business Rules (Rule 7 & Rule 8)

This project implements a data monitoring pipeline to track significant changes in vehicle financing volumes across two processing dates. The goal is to ensure data quality, apply business rules, detect anomalies, and generate monitoring indicators segmented by document type (Individual or Corporate) and vehicle category (Light, Motorcycle, Heavy).

> ⚠️ Note: This is a simulated project inspired by a real use case. All table and column names have been anonymized to protect sensitive information.

---

## 📈 Project Overview

This monitoring system was developed using PySpark and AWS Glue to analyze volumetric trends and ensure data consistency in financial datasets. It is designed to compute and compare metrics across different processing dates, applying domain-specific rules for validation and classification.

### Main Features

- Ingestion from multiple anonymized source tables
- Joins, column transformations, and rule-based outlier filtering
- Categorization of vehicle types and person types
- Calculation of metrics, variation percentages, and classification labels (OK, WARNING, NOT OK)
- Compatibility with AWS Glue Catalog and Athena (decimal types, naming conventions, etc.)
- Export to Amazon S3 in Parquet format with table registration in Glue Catalog

🕒 **Estimated Duration**: ~3 months  
🔧 **Responsibilities**: End-to-end development — analysis, coding, testing, debugging, validation, and documentation

---

## 🎯 Objectives

- Compute **record volumes** for two reference periods
- Apply **business logic** to remove outliers before comparing datasets
- Evaluate **percentage variations** across dimensions
- Generate **monitoring indicators and statuses** for dashboards and auditing

---

## ⚙️ Technologies

- **PySpark**
- **Apache Spark**
- **AWS Glue**
- **Amazon S3**
- **AWS Athena**
- **SQL**

---

## 🧪 Implemented Rules

### ✅ Rule 7 — Total Volume (No Filters)

Compares raw record volumes segmented by person type and vehicle category. No filters are applied.

### ✅ Rule 8 — Volume After Outlier Filtering

Applies business logic to filter out records considered outliers based on:

- Minimum financing value
- Invalid or extreme values
- Invalid vehicle attributes
- Missing required fields
- Domain-specific flags

Outlier removal is performed using conditional logic based on business knowledge.

---

## 📊 Calculated Metrics

- `current_metric_value`: Record count on the current processing date
- `previous_metric_value`: Record count on the previous processing date
- `percentage_variation`: Absolute percentage difference between dates
- `metric_status`: Monitoring label — OK, WARNING, NOT OK
- `metric_status_description`: Describes the variation level

---

## ▶️ Example Execution

```python
rule7_monitoring(sqlContext, "owner", "monitoring", "20241130", "20241031")
rule8_monitoring(sqlContext, "owner", "monitoring", "20241130", "20241031")


📅 Timeline

The project was completed in approximately 3 months, involving:

- Understanding complex business rules
- Designing the data pipeline architecture
- Implementing transformation and validation logic in PySpark
- Testing and refining outputs for consistency and accuracy


📌 Author

Developed by a Data Engineer with expertise in:

- PySpark and distributed data processing
- Data quality monitoring
- Handling large-scale datasets and AWS-based ETL pipelines
- Outlier detection, data validation, and analytics automation
