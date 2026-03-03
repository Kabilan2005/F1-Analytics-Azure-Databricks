# 🏎️ Formula 1 Data Engineering Pipeline
### Infrastructure-as-Code (Azure ARM Templates)

This repository contains the **Infrastructure-as-Code (ARM Templates)** for an Azure Data Factory (ADF) solution designed to ingest and transform Formula 1 data using **Azure Databricks**.

---

## 🏛️ Project Architecture
The solution follows a standard **Medallion Architecture** (Raw → Processed → Presentation) to ensure data quality and structured lineage.

* **🔄 Ingestion Pipeline (`Ingest_All_Files_F1`)**
    Dynamically ingests F1 data (Circuits, Races, Constructors, etc.) from the **raw container** into the **processed container** of an Azure Data Lake.

* **🛠️ Transformation Pipeline (`Transform_F1_Data`)**
    Orchestrates **Databricks notebooks** to transform ingested data into refined standings and results.

* **📅 Trigger (`tr_process_f1_data`)**
    A **Tumbling Window trigger** configured to run every **168 hours (weekly)** to align with the global Formula 1 race schedule.

---

## 📂 Repository Structure

| Directory | Description |
| :--- | :--- |
| **`/Main`** | Core template and parameters for the Data Factory deployment. |
| **`/Factory-Base`** | Resource definitions for the **ADF instance** and its **Managed Identity**. |
| **`/Linked-Templates`** | Segmented templates for **Linked Services** (Databricks, Storage) and individual pipelines used in master deployments. |

---

## 🛠️ Tech Stack
* **Orchestration:** Azure Data Factory
* **Compute:** Azure Databricks
* **Storage:** Azure Data Lake Storage (ADLS)
* **Deployment:** ARM Templates (JSON)

---

> **Note:** This project is designed for modular deployment, allowing you to update linked services and pipelines independently via the `/Linked-Templates` directory.
