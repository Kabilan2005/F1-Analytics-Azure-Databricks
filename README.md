# 🏎️ Formula 1 Data Engineering Project (Azure + Databricks)

## 📖 Overview

A Self paced hands on project under the guidance of Ramesh Ramasamy.
This project uses madallion architecture. Raw, Processed and Presentation.
Used data from Ergast API, Ingested and Transformed for Presentation.
Created Dashboards and Pipelines with Triggers.

The solution includes:
  - Data ingestion using Azure Data Factory
  - Storage in Azure Data Lake Gen2
  - Data transformation using Azure Databricks (PySpark)
  - Delta Lake for ACID-compliant storage
  - Incremental processing using partition overwrite
  - Infrastructure export using ARM templates

## ⚙️ Technologies Used

- Azure Data Factory (ADF)
- Azure Data Lake Storage Gen2 (ADLS)
- Azure Databricks
- PySpark
- Delta Lake
- Azure Key Vault
- ARM Templates (Infrastructure as Code)

## 🚀 Key Engineering Concepts Demonstrated

- Incremental data loading
- Dynamic partition overwrite
- External vs Managed tables
- Delta Lake ACID transactions
- Schema enforcement & evolution
- Job orchestration via ADF triggers
- Infrastructure export via ARM templates

## 🧠 Learnings & Outcomes

Through this project, the following were implemented and understood:

- End-to-end cloud data pipeline design
- Enterprise-level data lake layering
- Governance concepts (Unity Catalog)
- Secure secret management
- Partition-based incremental processing
- Infrastructure-as-Code principles
