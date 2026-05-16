# Formula One Analytics Platform: Azure Databricks Medallion ELT

### Azure Databricks (hive) | ADLS Gen2 | Delta Lake | RBAC | ADF | Medallion Architecture | Power BI

This project implements an end-to-end ELT pipeline for Formula One racing data. It transforms raw file-based data into a high-performance analytical platform using the Medallion Architecture on Azure, focusing on incremental loading, Delta Lake optimizations, and production-grade orchestration.

---

## 🛠️ Tech Stack

* **Storage:** Azure Data Lake Storage Gen2 (ADLSg2)
* **Processing:** Azure Databricks (PySpark, Spark SQL)
* **Data Format:** Delta Lake (ACID compliant)
* **Orchestration:** Azure Data Factory (ADF)
* **Reporting:** Power BI
* **Source Data:** Ergast API (File-based CSV & JSON)

---

## 🏗️ Data Architecture (Medallion)

The pipeline utilizes a multi-layer Delta Lake architecture to ensure data integrity and query performance.

### Bronze Layer (Raw Ingestion)
* **Strategy:** File-based incremental ingestion.
* **Logic:** Implements watermark control tables to ingest only new/modified files.
* **Storage:** Append-only raw data preserved in Delta format.

### Silver Layer (Cleansed & Conformed)
* **Strategy:** Incremental `MERGE` (Upsert) operations.
* **Logic:** Schema enforcement, handling of late-arriving data, and data cleansing (deduplication, type-casting).
* **Optimization:** Spark-optimized via `partition pruning` and `autoCompact`.

### Gold Layer (Analytical & Reporting)
* **Strategy:** Business-logic transformations using `MERGE` and `Overwrite`.
* **Logic:** Aggregated views for driver standings, constructor performance, and race results.
* **Consumption:** BI-optimized layer specifically modeled for Power BI performance.

---

## 📂 Project Structure

```text
Formula-One-Platform/
├── 1.ingest_bronze/       # Notebooks for raw file ingestion (CSV/JSON)
├── 2.transform_silver/    # Notebooks for data cleansing and normalization
├── 3.analysis_gold/       # Analytical models and reporting views
├── 4.adf_orchestration/   # JSON definitions for ADF pipelines & triggers
├── 5.power_bi_reports/    # Power BI dashboard files and visuals
├── config_env/            # Environment specific configurations
└── utilities/             # Common helper functions (Audit, Mount points)
```
---
## ⚙️ Orchestration & Engineering Features

To ensure production-grade reliability and cost-efficiency, the platform leverages several advanced Spark and Azure features:

* **Idempotency:** Pipelines are architected for **safe re-runs**. Whether a job fails mid-way or is manually restarted, the logic ensures no data duplication or state corruption occurs.
* **Delta Lake Optimizations:** Utilizes `optimizeWrite` and `autoCompact` to solve the "small file problem" automatically. It also employs **column mapping** to support seamless schema evolution over time.
* **End-to-End ADF Integration:** A fully automated Control Plane in **Azure Data Factory** manages notebook execution, complex dependency chains, and event-based or scheduled triggers.
* **Incremental Loading:** Implements a **Watermark-based control logic**. By tracking the last processed timestamp/ID, the pipeline avoids redundant processing of old data, significantly reducing compute costs and execution time.

---
## 🛡️ Security & Governance

The platform adheres to Enterprise-grade security standards to protect sensitive racing data and infrastructure:

* **RBAC (Role-Based Access Control):** Security is enforced through **Azure Active Directory (Azure AD)** integration, combined with **Databricks Access Control Lists (ACLs)** to manage granular permissions at the workspace, cluster, and table levels.
* **Secret Management:** To prevent hardcoded credentials, **Azure Key Vault** is used as the backing store for **Databricks Secret Scopes**. This allows notebooks to securely access ADLS Gen2 storage keys without exposing them in source code.
* **Data Governance:** Utilizes the **Hive Metastore** for structured cataloging. This provides a unified view of metadata across environments, ensuring consistent schema definitions and simplified data discovery for analysts.

---
## 📊 Visuals & Reporting

The platform provides full transparency into the data lifecycle and delivers high-impact insights for stakeholders:

* **End-to-End Lineage:** Every data point is traceable. Using the Databricks Catalog, lineage is tracked from the **raw CSV sources** through Silver cleansing, all the way to the **final Gold standings**. This ensures absolute data auditability and trust.
* **Analytical Reports:** The Gold layer feeds **Power BI dashboards** designed for executive-level analysis. These reports visualize critical metrics such as:
    * **Constructor Dominance:** Historical performance trends of teams.
    * **Driver Career Progressions:** Deep-dives into individual driver statistics and race-day consistency.
