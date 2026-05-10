# Hilton Multi-Cloud Data Engineering Pipeline

A scalable end-to-end **multi-cloud data engineering pipeline** built using **AWS, Snowflake, Google Cloud Platform (GCP), PySpark, BigQuery, Apache Airflow, and Power BI** for hospitality analytics and business intelligence.

This project processes property, booking, and customer review datasets to generate actionable insights on occupancy trends, revenue performance, customer satisfaction, and geographic analytics.

---

# Project Overview

The objective of this project is to design a robust and scalable **multi-cloud analytics architecture** for hospitality data processing.

The pipeline:
- Ingests raw CSV datasets from AWS S3
- Cleans and transforms data in Snowflake
- Exports curated data to Google Cloud Storage
- Processes analytical models using PySpark on Dataproc
- Loads dimensional datasets into BigQuery
- Visualizes insights using Power BI dashboards
- Automates workflows using Apache Airflow
- Implements monitoring and alerting using Pub/Sub and Cloud Monitoring

---

# Architecture Workflow

## End-to-End Flow

```text
AWS S3 (Raw Layer)
        ↓
Snowflake (Ingestion + Cleaning + Transformation)
        ↓
Google Cloud Storage (Curated Parquet Layer)
        ↓
PySpark on Dataproc (Dimensional Modeling)
        ↓
BigQuery Data Warehouse
        ↓
Power BI Dashboards & Reports
```

---

# Technologies Used

| Category | Tools & Services |
|---|---|
| Cloud Platforms | AWS, Google Cloud Platform |
| Storage | AWS S3, Google Cloud Storage |
| Data Warehouse | Snowflake, BigQuery |
| Processing | PySpark, Google Dataproc |
| Orchestration | Apache Airflow (Cloud Composer) |
| Visualization | Power BI |
| Monitoring | Google Pub/Sub, Cloud Monitoring |
| File Formats | CSV, Parquet |

---

# Datasets Used

## 1. Property Dataset
Contains:
- Property type
- Room type
- Accommodation capacity
- Pricing details
- Location information
- Attendant performance metrics

## 2. Booking Dataset
Contains:
- Booking date
- Availability status
- Booking prices
- Occupancy information

Used for:
- Occupancy trend analysis
- Revenue metrics
- ADR and RevPAR calculations

## 3. Customer Feedback Dataset
Contains:
- Reviewer information
- Ratings
- Review comments
- Review dates

Used for:
- Sentiment analysis
- Service quality analytics
- Customer satisfaction insights

---

# Data Quality Challenges Solved

The pipeline handles multiple real-world data quality problems during transformation.

### Implemented Cleaning Techniques
- Missing value handling
- Duplicate removal
- Data type standardization
- String normalization
- Invalid record removal
- Boolean conversion
- Numeric conversion from formatted strings

### Example Transformations

```sql
TRY_TO_DATE(review_date,'MM/DD/YYYY')
```

```sql
TRY_TO_NUMBER(REPLACE(price, ',', ''))
```

```sql
CASE
WHEN LOWER(availability)='t' THEN TRUE
WHEN LOWER(availability)='f' THEN FALSE
ELSE NULL
END
```

---

# Pipeline Stages

## 1. Raw Data Storage – AWS S3

Raw CSV datasets are stored in Amazon S3 as the primary landing zone.

### S3 Structure

```text
raw/
├── booking/
├── property/
└── feedback/
```

This layer acts as the immutable source of truth for all incoming datasets.

---

## 2. Data Ingestion into Snowflake

Snowflake connects securely to AWS S3 using:
- External stages
- Storage integrations
- COPY INTO commands

### Snowflake Raw Tables
- BOOKING_SUMMARY
- PROPERTY_SUMMARY
- FEEDBACK_SUMMARY

---

## 3. Data Cleaning & Transformation

A Snowflake stored procedure named:

```sql
CLEANING_PROCEDURE
```

is used to:
- Clean raw data
- Standardize formats
- Create staging tables
- Remove invalid records

---

## 4. Export to Google Cloud Storage

Cleaned datasets are exported to GCS in **Parquet format** for optimized analytical processing.

### Curated Layer

```text
curated/
├── bookings/
├── properties/
└── feedback/
```

---

## 5. Advanced Processing with PySpark

PySpark jobs running on Google Dataproc generate:
- Fact tables
- Dimension tables
- Analytics-ready schemas

### Generated Tables

```text
curated/
├── dim_property
├── dim_attendent
├── dim_date
├── fact_bookings
└── fact_reviews
```

---

## 6. BigQuery Data Warehouse

Processed datasets are loaded into Google BigQuery for:
- Large-scale analytical querying
- Data marts
- Reporting workloads
- BI integrations

Optimizations include:
- Partitioning
- Clustering
- Query performance tuning

---

# Analytical Data Marts

| Data Mart | Purpose |
|---|---|
| Property Performance Mart | Pricing, occupancy, ratings |
| Attendant Performance Mart | Staff performance metrics |
| Customer Review Mart | Review analytics & sentiment |
| Geographic Insights Mart | Regional and neighborhood analysis |

---

# Power BI Dashboards

Interactive dashboards provide insights into:
- Occupancy trends
- Revenue analytics
- Booking frequency
- Customer reviews
- Geographic performance

### Key Metrics Implemented
- Average Occupancy Rate
- Average Price per Night
- RevPAR
- Year-to-Date Revenue
- Rolling 30-Day Booking Rate
- Quarter-on-Quarter Trends

Additional features:
- Interactive filtering
- Drill-through analysis
- Custom tooltips
- Time-series visualizations

---

# Pipeline Automation

The workflow is fully automated using **Apache Airflow** running on **Google Cloud Composer**.

### Airflow DAG Responsibilities
- Load raw datasets
- Trigger Snowflake transformations
- Export curated data
- Execute monitoring workflows
- Schedule recurring jobs

This ensures:
- Reliable execution
- Dependency management
- Automated orchestration
- Repeatable workflows

---

# Monitoring & Alerting

The project implements real-time monitoring using:
- Google Pub/Sub
- Cloud Monitoring
- Email Alerting

### Failure Handling Workflow

```text
Pipeline Failure
       ↓
Pub/Sub Topic Triggered
       ↓
Cloud Monitoring Alert
       ↓
Email Notification Sent
```

This improves operational reliability and rapid issue resolution.

---

# Business Impact

This pipeline enables hospitality teams to:
- Optimize revenue
- Improve occupancy management
- Analyze customer sentiment
- Monitor operational efficiency
- Generate real-time business insights
- Scale analytics workloads efficiently across cloud platforms

---

# Future Enhancements

- Real-time streaming ingestion using Kafka
- Machine Learning-based demand forecasting
- Customer sentiment NLP models
- Automated anomaly detection
- CI/CD integration for pipeline deployment
- Data governance and lineage tracking

---

# Repository Structure

```text
hilton-multicloud-pipeline/
│
├── airflow_dags/
├── pyspark_jobs/
├── snowflake_sql/
├── powerbi_dashboards/
├── datasets/
├── screenshots/
├── docs/
└── README.md
```

---

# Screenshots

Include:
- Architecture workflow
- Snowflake transformations
- GCS curated layer
- BigQuery tables
- Power BI dashboards
- Airflow DAG runs
- Monitoring alerts

---

# Key Learnings

- Multi-cloud architecture integration
- Large-scale ETL orchestration
- Data warehouse optimization
- Distributed data processing
- Workflow automation
- Cloud monitoring & alerting
- Business intelligence reporting

---

# Author

**Varun MRN**  
M.Tech – Data Science  
NIT Surat
