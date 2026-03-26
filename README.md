# End-to-End-Cloud-ETL-Analytics-Engineering
A complete Azure‑based data engineering and analytics solution showcasing cloud ingestion, ETL orchestration, SQL transformations, dimensional modeling, and Power BI analytics. Demonstrates end‑to‑end skills across ADLS, ADF, Azure SQL, and business insight generation.


---

# 📦 Brazilian E‑Commerce Analytics Pipeline (Azure + SQL + Power BI)

A complete, end‑to‑end cloud data engineering and analytics project demonstrating ingestion, orchestration, transformation, modeling, and business intelligence.

## 🧭 Project Overview

This project implements a modern data engineering workflow using the Brazilian E‑Commerce Public Dataset. It demonstrates the design and execution of a cloud‑native analytics pipeline leveraging:

- **Azure Data Factory** for orchestration and ingestion  
- **Azure Data Lake Storage Gen2** for Bronze‑layer raw file storage  
- **Azure SQL Database** for Bronze/Staging, Silver, and Gold‑layer dimensional modeling  
- **Power BI** for executive‑level analytics and insights  

The goal is to simulate a real‑world enterprise pipeline, transforming raw data into actionable business intelligence.

## 🏗️ Architecture Diagram

*<img width="4124" height="888" alt="image" src="https://github.com/user-attachments/assets/4836513c-aa62-4171-b206-d2ec7dad9ed9" />
*

## 🚀 End‑to‑End Workflow

### 📥 1. Dataset Acquisition
- Downloaded the Brazilian E‑Commerce dataset from Kaggle.  
- Performed initial schema and data quality assessment.

### ☁️ 2. Azure Environment Setup
Provisioned a complete cloud environment including:

- Azure Resource Group  
- Azure Data Lake Storage Gen2  
- Azure Data Factory  
- Azure SQL Server + Azure SQL Database  
- Managed identities and RBAC configuration  

### 📂 3. Raw Data Ingestion (Bronze Layer)
Using Azure Data Factory:

- Configured linked services for local → ADLS ingestion  
- Created pipelines to upload raw CSV files into the Bronze container  
- Mirrored source folder structure  

### 🔄 4. Data Lake → Azure SQL Ingestion
- Built ADF pipelines to load Bronze data into Azure SQL staging tables  
- Applied schema mapping, type enforcement, and validation  
- Ensured idempotent, repeatable loads  

### 🧱 5. Data Transformation (Gold Layer)
Inside Azure SQL:

- Created cleaned, standardized Silver tables  
- Designed a full Star Schema including:  
  - Bridging table Orders  
  - Fact Order Items  
  - Fact Payments  
  - Fact Reviews  
  - Dim Customers  
  - Dim Products  
  - Dim Sellers  
  - Dim Dates  
- Implemented business rules for:  
  - Delivery delays  
  - Review score classification  
  - Product category normalization  

### 📊 6. Analytics Layer (Power BI)
Power BI dashboards include:

- Business Overview  
- Sales & Revenue  
- Delivery and Logistics  
- Review Score Analysis  

## 📁 Repository Structure

```
/data
   /raw — Original raw CSV files from Kaggle.

/sql
   /infrastructure — SQL scripts for Azure SQL access and firewall/auth setup.
   /bronze — SQL scripts creating Bronze layer tables (raw schema).
   /silver — SQL scripts creating Silver layer tables (cleaned data).
      /Transform — Queries used to clean and transform Bronze → Silver.
   /gold — SQL scripts for Gold layer dimensional model (tables + views).

/powerbi
   dashboard.pbix — Power BI report and visuals.

/docs
   architecture-diagram.png — System architecture illustration.
   data-dictionary.md — Table and field definitions.

```

## 🧠 Skills Demonstrated

### ☁️ Cloud Engineering
- Azure Data Factory pipelines  
- ADLS Gen2 hierarchical storage  
- Azure SQL provisioning & security  

### 🛠️ Data Engineering
- ETL/ELT pipeline design  
- Dimensional modeling (Star Schema)  
- SQL transformations & optimization  
- Data quality checks & validation  

### 📈 Analytics
- DAX measures  
- Executive dashboards  
- Business storytelling  

## 📊 Dataset Source
Brazilian E‑Commerce Public Dataset — Kaggle  



---

# 📈 Key Business Insights & Analysis Findings

This analytics pipeline uncovered several high‑impact insights about customer behavior, delivery performance, geographic demand, and product dynamics within the Olist marketplace. These findings reflect real‑world e‑commerce patterns and demonstrate the value of transforming raw data into actionable intelligence.

---

## 🛒 1. Sales and Customer Growth Are Increasing

- Monthly order volume shows a steady upward trend.  
- Customer acquisition is healthy, with consistent growth across the dataset timeline.  

**Business Impact:**  
Olist is scaling, but customer retention strategies could further accelerate revenue growth.

---

## 🚚 2. Delivery Time Strongly Influences Customer Reviews

- On‑time or early deliveries consistently receive 4–5 star reviews.  
- Late deliveries correlate heavily with 1–2 star reviews.  
- Most delivery time is consumed after the carrier picks up the package, not during seller processing.

**Business Impact:**  
Carrier performance optimization and more accurate delivery estimates would significantly improve customer satisfaction.

---

## 🏙️ 3. Demand Is Highly Concentrated in a Few States

Approximately 65% of all orders come from just three states:

- São Paulo (SP): 41%  
- Rio de Janeiro (RJ): ~13%  
- Minas Gerais (MG): ~11%

**Business Impact:**  
These regions should be prioritized for logistics optimization, marketing, and seller onboarding.

---

## ⭐ 4. Review Data Is Collected at the Order Level, Not Product Level

- One order can contain multiple products, but customers leave one review per order.  
- This prevents accurate analysis of product‑level satisfaction.

**Recommendation:**  
Olist should enable product‑specific reviews to unlock deeper insights into product quality, seller performance, and category‑level satisfaction.


## 🛍️ 5. Revenue Is Concentrated in a Few Product Categories

- Categories such as *bed_bath_table*, *health_beauty*, *Watches*, and *sports_leisure* generate a large share of total revenue.  
- Long‑tail categories contribute minimally.

**Business Impact:**  
Olist should focus on expanding inventory and seller partnerships in top‑performing categories.

---

## 📉 6. Review Score Distribution Is Polarized

- Customers tend to give either 5 stars (very satisfied) or 1 star (very dissatisfied).  
- Customers who leave higher ratings typically write longer, more detailed positive comments, while dissatisfied customers tend to shorter complaints.
This reflects a common pattern in e‑commerce sentiment behavior.

**Business Impact:**  
Small improvements in delivery accuracy, communication, and issue resolution can dramatically shift overall ratings.

---

## 🧭 7. Geographic Delivery Performance Varies

- Remote regions experience longer delivery times.  
- Urban centers, especially São Paulo, have faster and more predictable delivery performance.

**Business Impact:**  
Regional carrier partnerships and micro‑fulfillment centers could improve nationwide delivery consistency.

---

## 🎯 Summary

This project demonstrates not only cloud and data engineering skills but also the ability to extract meaningful business insights. The findings highlight:

- Strong sales and customer growth  
- Delivery performance as the primary driver of customer satisfaction  
- Geographic and category‑level concentration of demand  
- Limitations in product‑level review data  
- Clear opportunities for operational and customer experience improvements  

These insights reflect the real value of a modern analytics pipeline: **turning raw data into decisions.**



