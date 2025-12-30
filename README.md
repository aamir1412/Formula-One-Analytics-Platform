# Formula-One-Analytics-Platform
ELT Architecture | Azure Databricks | ADLS Gen2 | Delta Lake | ADF | Power BI


--- Tech Stack --- 

•	Storage: Azure Data Lake Storage Gen2 (ADLSg2)

•	Processing: Azure Databricks (PySpark, Spark SQL)

•	Data Lake Format: Delta Lake

•	Orchestration: Azure Data Factory (ADF)

•	Analytics & Reporting: Power BI

•	Source Data: File-based CSV / JSON (Ergast system)



--- Key Engineering Features ---

•	File-based incremental ingestion using watermark control table

•	Bronze (append), Silver (merge), Gold (append / overwrite) ELT design

•	Delta Lake for ACID transactions and schema enforcement

•	Idempotent pipelines with safe re-runs

•	End-to-end orchestration using Azure Data Factory

•	BI-optimized gold layer for Power BI consumption

