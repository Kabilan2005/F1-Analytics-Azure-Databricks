This repository contains the infrastructure-as-code (ARM Templates) for an Azure Data Factory (ADF) solution designed to ingest and transform Formula 1 data using Azure Databricks.

Project Architecture
The solution follows a standard medallion architecture (Raw → Processed → Presentation).

Ingestion Pipeline (Ingest_All_Files_F1): Dynamically ingest (process) F1 data (Circuits, Races, Constructors, etc.) from the raw container into the processed container of an Azure Data Lake.

Transformation Pipeline (Transform_F1_Data): Orchestrates Databricks notebooks to tranform ingested data into refined standings and results.

Trigger (tr_process_f1_data): A Tumbling Window trigger configured to run every 168 hours (weekly) to align with race schedules.

Repository Structure
/Main: Core template and parameters for the Data Factory.

/Factory-Base: Resource definitions for the ADF instance and its Managed Identity.

/Linked-Templates: Segmented templates for linked services (Databricks, Storage) and individual pipelines used in master deployments.
