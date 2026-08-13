# 🚀 AdventureWorks Data Engineering Pipeline

<center>
<table border="0">
  <tr>
    <td align="center" width="95">
      <img src="https://raw.githubusercontent.com/devicons/devicon/master/icons/python/python-original.svg" alt="Python" width="40" height="40"/><br/>
      <sub><b>Python</b></sub>
    </td>
    <td align="center" width="95">
      <img src="https://raw.githubusercontent.com/devicons/devicon/master/icons/apacheairflow/apacheairflow-original.svg" alt="Airflow" width="40" height="40"/><br/>
      <sub><b>Airflow</b></sub>
    </td>
    <td align="center" width="95">
      <img src="https://cdn.simpleicons.org/databricks/FF3621" alt="Databricks" width="40" height="40"/><br/>
      <sub><b>Databricks</b></sub>
    </td>
    <td align="center" width="95">
      <img src="https://raw.githubusercontent.com/devicons/devicon/master/icons/apachespark/apachespark-original.svg" alt="Spark" width="40" height="40"/><br/>
      <sub><b>PySpark</b></sub>
    </td>
    <td align="center" width="95">
      <img src="https://cdn.simpleicons.org/deltalake/0052CC" alt="Delta Lake" width="40" height="40"/><br/>
      <sub><b>Delta Lake</b></sub>
    </td>
    <td align="center" width="95">
      <img src="https://cdn.simpleicons.org/amazons3/569A31" alt="AWS S3" width="40" height="40"/><br/>
      <sub><b>AWS S3</b></sub>
    </td>
  </tr>
  <tr>
    <td align="center" width="95">
      <img src="https://cdn.simpleicons.org/amazoniam/DD344C" alt="AWS IAM" width="40" height="40"/><br/>
      <sub><b>AWS IAM</b></sub>
    </td>
    <td align="center" width="95">
      <img src="https://upload.wikimedia.org/wikipedia/commons/c/cf/New_Power_BI_Logo.svg" alt="Power BI" width="40" height="40"/><br/>
      <sub><b>Power BI</b></sub>
    </td>
    <td align="center" width="95">
      <img src="https://raw.githubusercontent.com/devicons/devicon/master/icons/docker/docker-original.svg" alt="Docker" width="40" height="40"/><br/>
      <sub><b>Docker</b></sub>
    </td>
    <td align="center" width="95">
      <img src="https://raw.githubusercontent.com/devicons/devicon/master/icons/githubactions/githubactions-original.svg" alt="GH Actions" width="40" height="40"/><br/>
      <sub><b>GH Actions</b></sub>
    </td>
    <td align="center" width="95">
      <img src="https://cdn.simpleicons.org/diagramsnet/F08705" alt="draw.io" width="40" height="40"/><br/>
      <sub><b>draw.io</b></sub>
    </td>
  </tr>
</table>
</center>

---

# 📖 About This Project

This repository contains my first end-to-end Data Engineering project.

I built this project to understand how a modern batch data pipeline works in a real-world environment. Instead of learning each tool separately, I wanted to connect everything together—from data ingestion to reporting.

The project starts by extracting data from the AdventureWorks platform, converting it into CSV files, and loading them into AWS S3. The second phase involves reading raw AdventureWorks data from S3. Apache Airflow orchestrates the pipeline, Databricks processes the data using PySpark, Delta Lake stores each Medallion layer, and the final Gold tables are used to build Power BI dashboards.

While building this project, my goal wasn't just to make the pipeline work. I wanted to learn how different components work together, how data flows through each layer, and how Data Engineers design reliable and maintainable pipelines.

This project covers:

- End-to-end ETL/ELT pipeline
- Medallion Architecture (Bronze, Silver, Validation, Gold)
- Apache Airflow orchestration
- Databricks Workflows
- Delta Lake MERGE operations
- Stateful high-water mark timestamp-based incremental processing
- Data quality validation
- Star Schema data modeling
- Databricks SQL Warehouse
- Power BI dashboards
- Dockerized Airflow
- GitHub Actions CI

Although this is a portfolio project, I tried to follow production-inspired practices wherever possible and document the design decisions throughout the repository.

---

# 🎯 Why I Built This

I created this project to challenge myself with a complete Data Engineering workflow instead of building small isolated examples.

During this project I learned how data moves through a modern data platform, how orchestration works, how Delta Lake handles incremental data via high-water mark timestamps, how to model analytical data, and how to deliver business-ready datasets for reporting.

More importantly, I learned how to debug failures, improve pipeline design, and think about Data Engineering beyond writing code.

---

# 🙌 Feedback Welcome

This is my **first Data Engineering project**, and I'm still learning.

If you notice something that could be improved, an engineering decision that could be better, or a practice that isn't production-ready, I'd genuinely appreciate your feedback.

Constructive criticism helps me become a better Data Engineer, and I'd much rather learn by fixing mistakes than leave them unnoticed.

If you have suggestions, advice, or would simply like to connect and discuss Data Engineering, feel free to reach out to me on **LinkedIn**.

I'm always open to learning from experienced engineers and improving my work.

---

# 🏗️ Project Architecture

One of the main goals of this project was to understand how data moves through a complete Data Engineering pipeline. Instead of processing everything in a single script, I followed the Medallion Architecture to organize the data into different layers, making the pipeline easier to maintain, debug, and scale.

The pipeline starts when raw AdventureWorks CSV files are uploaded to an AWS S3 bucket. Apache Airflow monitors and orchestrates the workflow using lightweight S3 sensors, while Databricks Workflows execute the PySpark notebooks responsible for processing each layer of the pipeline.

Each layer has a specific responsibility:

### 🥉 Bronze Layer

The Bronze layer stores the raw source data exactly as it arrives from Amazon S3 into Delta Lake format.

At this stage I don't perform any business transformations. The main goal is to preserve the original data so it can always be traced back if something goes wrong later in the pipeline.

I also add ingestion metadata such as `ingestion_timestamp` and `source_file_name` to maintain accurate data lineage.

---

### 🥈 Silver Layer

The Silver layer is where the data starts becoming useful.

In this layer I clean inconsistent values, standardize data types, remove duplicates, enforce schema integrity, and apply business transformations using PySpark.

To optimize processing efficiency, I implemented a stateful incremental ingestion model using high-water mark timestamps (`silver_processed_timestamp`). Instead of reprocessing the whole dataset, Delta Lake MERGE operations run against newly ingested records based on timestamp thresholds.

---

### ✅ Validation Layer

Before publishing data to the Gold layer, I run a separate validation step.

The purpose of this layer is to detect common data quality issues before they reach the reporting layer.

Some of the validations include:

- Checking for null values in important primary keys
- Verifying numeric values are valid
- Detecting duplicate records
- Logging validation results for every pipeline execution

Separating validation from transformation makes the pipeline easier to troubleshoot and helps identify data quality issues earlier.

---

### 🥇 Gold Layer

The Gold layer contains business-ready tables designed for reporting and analytics.

Here I build a Star Schema consisting of fact and dimension tables.

These tables are optimized for business users and Power BI dashboards instead of raw data processing.

The final Gold tables are published through Databricks SQL Warehouse, allowing Power BI to connect directly to curated analytical data.

---

# 🔄 End-to-End Pipeline Flow

The complete workflow follows the architecture below.

<img src="docs/Architecture Diagram.png" width="100%" height="750" alt="Pipeline Architecture Diagram">

This separation allows each layer to have a single responsibility, making the pipeline easier to maintain and extend in the future.

---

# ⚡ Incremental Processing

One thing I wanted to avoid was reprocessing the entire dataset every time the pipeline runs.

To solve this, I implemented stateful high-water mark timestamp tracking (`silver_processed_timestamp`).

During each run, the pipeline queries the target Delta Lake table for the max timestamp threshold, reads only newly landed records, and merges them atomically into the Delta Lake tables using MERGE (`UPSERT`) statements.

This approach eliminates unnecessary re-computation while ensuring absolute data consistency.

---

# 🛠️ Technologies Used

This project helped me gain hands-on experience with several tools commonly used in modern Data Engineering.

| Tool | Purpose |
|------|---------|
| Python | Pipeline development & scripting |
| PySpark | Distributed data processing engine |
| Apache Airflow | Workflow orchestration & scheduling |
| Databricks Workflows | Cloud cluster job execution |
| Delta Lake | ACID storage & high-water mark MERGE |
| AWS S3 | Raw data storage / landing zone |
| AWS IAM | Access management and security policies |
| Unity Catalog | Data governance & table management |
| Databricks SQL Warehouse | SQL endpoint for Power BI |
| Power BI | Visual reporting & dashboard creation |
| Docker | Local Airflow environment isolation |
| GitHub Actions | CI/CD automated testing |
| draw.io | Architecture & schema diagramming |

---

# 💡 What I Learned

This project taught me much more than how to write PySpark code.

Some of my biggest takeaways were:

- Breaking a large pipeline into small, maintainable layers.
- Understanding why orchestration is just as important as transformation.
- Designing stateful timestamp-driven incremental pipelines instead of full refresh pipelines.
- Building a dimensional model for analytics.
- Thinking about data quality before reporting.
- Using GitHub Actions to automatically validate changes.
- Documenting architecture instead of only writing code.

Building this project helped me understand how different Data Engineering tools work together to solve a complete business problem instead of learning them in isolation.

---

# 📊 Data Warehouse Design

After transforming and validating the data, I built a Star Schema in the Gold layer to make reporting faster and easier.

Instead of querying raw transactional data, the reporting layer is organized into fact and dimension tables. This approach improves readability and follows a common design used in analytical data warehouses.

### Fact Tables

- **fact_sales**
- **fact_returns**

### Dimension Tables

- **dim_customers**
- **dim_products**
- **dim_date**
- **dim_territories**

This structure makes it simple to analyze business metrics such as sales, profit, returns, customer behavior, product performance, and regional performance.

---

## ⭐ Star Schema

<img src="docs/Start_Schema_diagram.png" width="100%" height="600" alt="Star Schema Diagram">

---

# 📈 Power BI Dashboard

The final Gold tables are connected to Databricks SQL Warehouse and visualized using Power BI.

Instead of creating KPI tables inside the data pipeline, I chose to calculate business metrics in Power BI using DAX measures. This keeps the data pipeline focused on preparing clean and reliable datasets while allowing the reporting layer to handle business calculations.

The dashboard currently includes multiple report pages covering:

- Executive Overview
- Sales Analysis
- Customer Analysis
- Product Performance
- Territory Performance
- Return Analysis

These dashboards allow business users to explore data interactively without querying the underlying warehouse directly.

---

## 📷 Dashboard Preview

<img src="docs/Powerbi Dashboard.png" width="100%" height="800" alt="Dashboard Preview">

---

# 🔄 CI/CD

One of my goals for this project was to learn not only how to build a pipeline but also how to manage it using version control and Continuous Integration.

Every change is tracked with Git, and GitHub Actions automatically validates the repository whenever new code is pushed.

This gives me confidence that future changes don't accidentally break the project.

The current CI pipeline includes:

- Repository validation
- Python dependency installation
- Workflow verification
- Basic project validation before merging changes

Although this is a learning project, adding CI helped me understand how automated validation fits into a real software development workflow.

---

# 📁 Repository Structure

The project is organized to keep orchestration, data processing, documentation, and reporting separate.

```text
Adventureworks-Data-Engineering-Pipeline
│
├── .github/
│   └── workflows/
│
├── dags/
│   └── adventure_work_project/
│
├── databricks/
│   ├── bronze/
│   ├── silver/
│   ├── validation/
│   └── gold/
│
├── docs/
├── Dataset/
├── powerbi/
│
├── Dockerfile
├── docker-compose.yaml
├── requirements.txt
├── README.md
└── .env.example
