# 🚀 AdventureWorks Data Engineering Pipeline

<p align="center">

![Python](https://img.shields.io/badge/Python-blue?style=for-the-badge&logo=python)
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-green?style=for-the-badge&logo=apacheairflow)
![Databricks](https://img.shields.io/badge/Databricks-Lakehouse-orange?style=for-the-badge&logo=databricks)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-PySpark-E25A1C?style=for-the-badge&logo=apachespark)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-Storage-blue?style=for-the-badge)
![AWS S3](https://img.shields.io/badge/AWS-S3-FF9900?style=for-the-badge&logo=amazonaws)
![Power BI](https://img.shields.io/badge/Power%20BI-Dashboard-F2C811?style=for-the-badge&logo=powerbi)
![Docker](https://img.shields.io/badge/Docker-Container-2496ED?style=for-the-badge&logo=docker)
![GitHub Actions](https://img.shields.io/badge/GitHub%20Actions-CI/CD-2088FF?style=for-the-badge&logo=githubactions)

</p>

---

# 📖 About This Project

This repository contains my first end-to-end Data Engineering project.

I built this project to understand how a modern batch data pipeline works in a real-world environment. Instead of learning each tool separately, I wanted to connect everything together—from data ingestion to reporting.

The project starts by extracting data from AdventureWorks platform and convert them into csv and load them into S3 and second fase was reading raw AdventureWorks data from AWS S3. Apache Airflow orchestrates the pipeline, Databricks processes the data using PySpark, Delta Lake stores each Medallion layer, and the final Gold tables are used to build Power BI dashboards.

While building this project, my goal wasn't just to make the pipeline work. I wanted to learn how different components work together, how data flows through each layer, and how Data Engineers design reliable and maintainable pipelines.

This project covers:

- End-to-end ETL/ELT pipeline
- Medallion Architecture (Bronze, Silver, Validation, Gold)
- Apache Airflow orchestration
- Databricks Workflows
- Delta Lake MERGE operations
- Incremental data processing
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

During this project I learned how data moves through a modern data platform, how orchestration works, how Delta Lake handles incremental data, how to model analytical data, and how to deliver business-ready datasets for reporting.

More importantly, I learned how to debug failures, improve pipeline design, and think about Data Engineering beyond writing code.

---

# 🙌 Feedback Welcome

This is my **first Data Engineering project**, and I'm still learning.

If you notice something that could be improved, an engineering decision that could be better, or a practice that isn't production-ready, I'd genuinely appreciate your feedback.

Constructive criticism helps me become a better Data Engineer, and I'd much rather learn by fixing mistakes than leave them unnoticed.

If you have suggestions, advice, or would simply like to connect and discuss Data Engineering, feel free to reach out to me on **LinkedIn**.

I'm always open to learning from experienced engineers and improving my work.

# 🏗️ Project Architecture

One of the main goals of this project was to understand how data moves through a complete Data Engineering pipeline. Instead of processing everything in a single script, I followed the Medallion Architecture to organize the data into different layers, making the pipeline easier to maintain, debug, and scale.

The pipeline starts when raw AdventureWorks CSV files are uploaded to an AWS S3 bucket. Apache Airflow monitors and orchestrates the workflow, while Databricks Workflows execute the PySpark notebooks responsible for processing each layer of the pipeline.

Each layer has a specific responsibility:

### 🥉 Bronze Layer

The Bronze layer stores the raw source data exactly as it arrives from Amazon S3.

At this stage I don't perform any business transformations. The main goal is to preserve the original data so it can always be traced back if something goes wrong later in the pipeline.

I also add ingestion metadata such as processing timestamps and a `pipeline_run_id` that allows the pipeline to process data incrementally.

---

### 🥈 Silver Layer

The Silver layer is where the data starts becoming useful.

In this layer I clean inconsistent values, standardize data types, remove duplicates, and apply business transformations using PySpark.

Instead of rewriting the entire dataset every time, I use Delta Lake MERGE operations to perform incremental updates based on the current pipeline run.

---

### ✅ Validation Layer

Before publishing data to the Gold layer, I run a separate validation step.

The purpose of this layer is to detect common data quality issues before they reach the reporting layer.

Some of the validations include:

- Checking for null values in important columns
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

To solve this, I implemented incremental processing using a unique `pipeline_run_id`.

Each pipeline execution processes only the newly ingested records, which are then merged into Delta Lake tables using MERGE operations.

This approach reduces unnecessary processing while keeping the data up to date.

---

# 🛠️ Technologies Used

This project helped me gain hands-on experience with several tools commonly used in modern Data Engineering.

| Tool | Purpose |
|------|---------|
| Python | Pipeline development |
| PySpark | Distributed data processing |
| Apache Airflow | Workflow orchestration |
| Databricks Workflows | Notebook execution |
| Delta Lake | ACID storage & incremental MERGE |
| AWS S3 | Raw data storage |
| Unity Catalog | Table management |
| Databricks SQL Warehouse | SQL endpoint for reporting |
| Power BI | Dashboard creation |
| Docker | Local Airflow environment |
| GitHub Actions | Continuous Integration |

---

# 💡 What I Learned

This project taught me much more than how to write PySpark code.

Some of my biggest takeaways were:

- Breaking a large pipeline into small, maintainable layers.
- Understanding why orchestration is just as important as transformation.
- Designing incremental pipelines instead of full refresh pipelines.
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

 <img src="docs/Star_Schema_diagram.png" width="100%" height="600" alt="Pipeline Architecture Diagram">

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

<img src="docs/powerbi Visuals.png" width="100%" height="800" alt="Pipeline Architecture Diagram">

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
│── Dataset/
├── powerbi/
│
├── Dockerfile
├── docker-compose.yaml
├── requirements.txt
├── README.md
└── .env.example
```

Keeping the repository organized made it much easier to work on different parts of the project independently and reflects how I wanted to structure a real-world Data Engineering project.

---

# 💭 Engineering Decisions

While building this project, I tried to make decisions that reflect how a real Data Engineering pipeline is organized. Since this was my first end-to-end project, I focused on understanding *why* certain design choices are commonly used instead of simply making the pipeline work.

Here are a few of the decisions I made and the reasoning behind them.

### Why Medallion Architecture?

I chose the Medallion Architecture because it separates raw, cleaned, and business-ready data into different layers.

This makes the pipeline easier to debug, maintain, and extend. If an issue occurs, I can quickly identify which layer introduced the problem without affecting the rest of the pipeline.

---

### Why Apache Airflow?

Instead of manually running notebooks, I used Apache Airflow to orchestrate the pipeline.

This helped me understand how production pipelines manage task dependencies, retries, scheduling, logging, and monitoring.

---

### Why Databricks?

I used Databricks because it provides an environment for running distributed PySpark workloads and integrates well with Delta Lake.

It also allowed me to learn workflows, notebooks, SQL Warehouse, and Unity Catalog in a single platform.

---

### Why Delta Lake?

Delta Lake provides ACID transactions and supports MERGE operations, making incremental updates much easier than rewriting entire datasets.

Using Delta tables also helped me understand how modern Lakehouse architectures manage reliable data pipelines.

---

### Why Incremental Processing?

Reprocessing the entire dataset every time is inefficient.

To avoid unnecessary work, I implemented incremental processing using a `pipeline_run_id`. Each pipeline execution processes only the newly ingested data before merging it into the target tables.

---

### Why Validate Data Separately?

I wanted data quality checks to be independent from transformations.

Keeping validation as its own step makes the pipeline easier to troubleshoot and prevents invalid data from reaching the reporting layer.

---

### Why a Star Schema?

The reporting layer is designed using a Star Schema because it simplifies analytical queries and works well with BI tools like Power BI.

Instead of exposing raw transactional data, business users can work with clean fact and dimension tables.

---

### Why Power BI for KPIs?

I chose to calculate KPIs in Power BI using DAX instead of storing them inside the data pipeline.

This keeps the engineering pipeline focused on preparing reliable datasets while allowing the reporting layer to handle business calculations and visualizations.

---

# 🚧 Challenges I Faced

Building this project wasn't always straightforward. Along the way I ran into several issues that pushed me to understand the tools more deeply.

Some of the challenges included:

- Setting up Apache Airflow locally with Docker.
- Connecting Airflow to Databricks Workflows.
- Learning Delta Lake MERGE operations.
- Designing a Star Schema from transactional data.
- Implementing incremental processing.
- Handling data quality validation.
- Organizing the project into a maintainable structure.
- Understanding how all the technologies work together in a single pipeline.

Most of these problems required reading documentation, experimenting with different approaches, and debugging until I understood the root cause.

Although solving these issues took time, they became some of the most valuable learning experiences during this project.

---

# 🚀 Future Improvements

Although the project is complete, there are several areas I'd like to explore in future versions.

Some ideas include:

- Migrating the pipeline to a cloud-based Airflow deployment.
- Adding automated unit and integration tests.
- Implementing data lineage.
- Adding monitoring and alerting.
- Supporting multiple source systems.
- Introducing Infrastructure as Code (Terraform).
- Exploring streaming pipelines with Spark Structured Streaming.
- Expanding the project with cloud-native services.

I see this project as the foundation for future learning rather than a finished product.

---

# 🎓 What I Learned

This project taught me much more than writing PySpark code.

It helped me understand how different parts of a Data Engineering platform fit together—from ingesting raw data to delivering business-ready insights.

More importantly, I learned that building a reliable pipeline involves much more than transformations. It requires planning, organization, testing, debugging, documentation, and continuously improving the solution.

This project gave me confidence to continue learning and build more complex Data Engineering solutions in the future.

---

# 🤝 Feedback & Suggestions

This is my **first end-to-end Data Engineering project**, and I'm continuously learning.

If you notice something that could be improved, an engineering decision that could be made better, or a best practice I may have missed, I'd genuinely appreciate your feedback. I believe the best way to grow is by understanding mistakes and learning how to solve them.

If you're an experienced Data Engineer or work in the data space, I'd love to hear your suggestions or advice.

Feel free to connect with me on **LinkedIn**—I'm always happy to learn from others, discuss Data Engineering, and improve through constructive feedback.

Thank you for taking the time to explore my project! 🚀

---
# 🚀 Getting Started

If you'd like to explore or run this project on your own machine, follow the steps below.

---

# 📋 Prerequisites

Before getting started, make sure you have the following installed:

- Python 
- Docker Desktop
- Git
- AWS Account
- Databricks Workspace
- Power BI Desktop (Optional)

You'll also need:

- An AWS S3 bucket
- A Databricks Personal Access Token (PAT)
- Access to a Databricks SQL Warehouse

---

# 📥 Clone the Repository

```bash
git clone 

cd Adventureworks-Data-Engineering-Pipeline
```

---

# ⚙️ Configure Environment Variables

This project uses environment variables for credentials and configuration.

Create a `.env` file by copying the example configuration:

```bash
cp .env.example .env
```

Update the `.env` file with your own values.

Example:

```env
AWS_ACCESS_KEY_ID=YOUR_AWS_ACCESS_KEY
AWS_SECRET_ACCESS_KEY=YOUR_AWS_SECRET_KEY
AWS_DEFAULT_REGION=YOUR_REGION

DATABRICKS_HOST=https://<your-workspace>.cloud.databricks.com
DATABRICKS_TOKEN=YOUR_DATABRICKS_PAT

FERNET_KEY=YOUR_FERNET_KEY
```

> **Note:** Never commit your `.env` file or personal credentials to GitHub.

---

# 🐳 Start Apache Airflow

Build and start the Airflow services using Docker Compose.

```bash
docker compose up --build
```

To run in detached mode:

```bash
docker compose up -d --build
```

Once the containers are running, open your browser and navigate to:

```
http://localhost:8085
```

Login using the credentials configured in your Airflow environment.

---

# ☁️ Configure AWS S3

1. Create an S3 bucket.
2. Upload the AdventureWorks dataset.
3. Update any bucket references in the project configuration if needed.

Example folder structure:

```text
airflow-spark-project/

├── incoming/
│   ├── customers/
│   ├── products/
│   ├── sales/
│   ├── returns/
│   ├── territories/
│   ├── calendar/
│   ├── product_category/
│   └── product_sub_category/
│
├── bronze/
├── silver/
├── gold/
├── archive/
├── quarantine/
└── configs/
```

---

# 🧱 Configure Databricks

Before running the pipeline:

- Create a Databricks Workspace.
- Import the notebooks from the `databricks/` folder.
- Create a Databricks Workflow (Job).
- Attach a compute cluster or use Serverless Compute (if available).
- Update the Airflow connection with your Databricks workspace details.
- Configure the correct Job ID in the Airflow DAG.

---

# ▶️ Run the Pipeline

Once everything is configured:

1. Upload source files to Amazon S3.
2. Trigger the Airflow DAG.
3. Airflow starts the Databricks Workflow.
4. Data flows through:
   - Bronze
   - Silver
   - Validation
   - Gold
5. Gold tables are available in Databricks SQL Warehouse.
6. Connect Power BI to the SQL Warehouse and refresh the dashboard.

---

# 📊 Open the Power BI Dashboard

Open the `.pbix` file located in the `powerbi/` directory.

Update the connection details if necessary and refresh the dataset to load the latest Gold tables.

---

# 🧪 CI/CD

GitHub Actions automatically validates the project whenever changes are pushed to the repository.

You can view workflow execution under the **Actions** tab in GitHub.

---

# 🤝 Contributing

Although this repository is primarily a learning project, suggestions and improvements are always welcome.

If you notice a bug, discover a better approach, or have ideas to improve the project, feel free to:

- Open an Issue
- Submit a Pull Request
- Share your feedback

Every suggestion is an opportunity for me to learn something new.

---

# 📬 Connect With Me

If you'd like to discuss Data Engineering, share feedback, or simply connect, feel free to reach out.

- **LinkedIn:** www.linkedin.com/in/abdul-khadir-44876735a
- **Instagram:** https://www.instagram.com/abdulkm_63?igsh=Ymo4cnowcXhjMHVv

I'm always happy to connect with people who are passionate about Data Engineering and continuous learning.

---

# ⭐ Support

If you found this project helpful or interesting, consider giving it a ⭐ on GitHub.

It motivates me to continue building projects, learning new technologies, and sharing my journey with the community.

Thank you for visiting my repository! 🚀

