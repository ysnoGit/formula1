# Formula 1 Data Lakehouse on Azure Databricks

This repository contains a hands-on batch data engineering project that builds a Formula 1 data lakehouse on Azure Databricks.

The project demonstrates how raw CSV and JSON files can be ingested from Azure Data Lake Storage, processed through Bronze, Silver, and Gold layers, stored as Delta tables, modelled for analytics, and orchestrated as an incremental pipeline with Lakeflow Jobs.

Two implementations are included:

- **Full-load pipeline** — builds the complete lakehouse by processing all available source data.
- **Incremental-load pipeline** — processes one new batch at a time and uses Delta `MERGE` operations to update existing tables safely.

> This project was completed as part of an Azure Databricks data engineering course. It is published as a learning and portfolio project; see [Acknowledgements](#acknowledgements).

---

## Project Objectives

The project was built to practise the end-to-end responsibilities of a data engineer in Azure Databricks:

- Configure governed access from Databricks to Azure Data Lake Storage.
- Organize data using Unity Catalog catalogs, schemas, tables, and volumes.
- Ingest multiple file formats with explicit Spark schemas.
- Add ingestion metadata for traceability and auditing.
- Apply data-quality and standardization rules with PySpark.
- Store reliable, versioned datasets using Delta Lake.
- Build a dimensional Gold model for reporting and analytics.
- Create driver and constructor standings with Spark SQL.
- Orchestrate dependencies, parameters, retries, and batch status with Lakeflow Jobs.
- Extend a full-refresh pipeline into an incremental and rerunnable batch pipeline.

---

## Technology Stack

| Area | Technology |
|---|---|
| Cloud platform | Microsoft Azure |
| Processing platform | Azure Databricks |
| Distributed processing | Apache Spark / PySpark |
| Query language | Spark SQL |
| Storage | Azure Data Lake Storage Gen2 |
| Table format | Delta Lake |
| Governance | Unity Catalog |
| Orchestration | Databricks Lakeflow Jobs |
| Development | Databricks notebooks |

---

## Source Datasets

The pipeline processes six Formula 1 datasets:

| Dataset | Format | Description |
|---|---|---|
| `circuits` | CSV | Circuit names and geographical information |
| `races` | CSV | Season, round, race date, and circuit references |
| `constructors` | JSON | Constructor names and nationalities |
| `drivers` | JSON | Driver names, dates of birth, and nationalities |
| `results` | JSON | Formula 1 race results |
| `sprints` | JSON | Formula 1 sprint results |

The source data files are not included in the supplied project archives. They must be placed in the configured landing volume before the notebooks are run.

---

## Architecture

```mermaid
flowchart LR
    A[Formula 1 CSV and JSON files] --> B[ADLS Landing Volume]
    B --> C[Bronze Delta Tables]
    C --> D[Silver Delta Tables]
    D --> E[Gold Dimensions and Fact]
    E --> F[SQL Analytical Views]
    F --> G[Dashboards and Analysis]

    H[Unity Catalog] --- B
    H --- C
    H --- D
    H --- E

    I[Lakeflow Jobs] -. orchestrates .-> C
    I -. orchestrates .-> D
    I -. orchestrates .-> E
    I -. monitors .-> F
```

The data lakehouse follows the Medallion Architecture:

### Landing

The Landing layer is the controlled entry point for the original files. Files remain in their source format and are accessed through a Unity Catalog external volume.

### Bronze

The Bronze layer stores source-aligned Delta tables with minimal transformation.

Key operations include:

- Reading CSV and JSON files with explicit schemas.
- Using `FAILFAST` mode to detect malformed data.
- Adding `ingestion_timestamp` and `source_file` audit columns.
- Adding `batch_id` in the incremental implementation.
- Partitioning incremental Bronze tables by `batch_id`.

### Silver

The Silver layer contains cleaned and standardized records that preserve the business keys required by downstream models.

Typical transformations include:

- Converting column names to snake case.
- Renaming fields to more meaningful names.
- Dropping columns that are not required for analytics.
- Flattening nested JSON structures.
- Standardizing text values and date fields.
- Filtering records with missing business keys.
- Removing duplicates.
- Adding `created_timestamp` and `updated_timestamp`.

### Gold

The Gold layer uses a dimensional model that is optimized for analytical queries.

It contains:

- `dim_races`
- `dim_drivers`
- `dim_constructors`
- `fact_session_results`
- `ref_nationality_region`

Race and sprint records are combined into a single fact table by adding a `session_type` column with either `RACE` or `SPRINT`.

The fact table also derives reusable business indicators:

- `is_win`
- `is_podium`
- `has_points`

---

## Gold Data Model

```mermaid
erDiagram
    DIM_RACES ||--o{ FACT_SESSION_RESULTS : "season, round"
    DIM_DRIVERS ||--o{ FACT_SESSION_RESULTS : driver_id
    DIM_CONSTRUCTORS ||--o{ FACT_SESSION_RESULTS : constructor_id

    DIM_RACES {
        int season PK
        int round PK
        string race_name
        date race_date
        string circuit_name
        string locality
        string country
    }

    DIM_DRIVERS {
        string driver_id PK
        string driver_name
        date date_of_birth
        string nationality
        string nationality_region
    }

    DIM_CONSTRUCTORS {
        string constructor_id PK
        string constructor_name
        string nationality
        string nationality_region
    }

    FACT_SESSION_RESULTS {
        int season PK
        int round PK
        string session_type PK
        string constructor_id FK
        string driver_id FK
        int grid_position
        int completed_laps
        int car_number
        double points
        int final_position
        string final_position_text
        string status
        boolean is_win
        boolean is_podium
        boolean has_points
    }
```

This model allows reporting queries to aggregate measures from the fact table while filtering and grouping by descriptive attributes from the dimensions.

---

## Full-Load Pipeline

The first implementation demonstrates the complete flow before incremental processing is introduced.

```text
Landing files
    ↓
Bronze ingestion notebooks
    ↓
Silver transformation notebooks
    ↓
Gold dimensional-model notebooks
    ↓
Driver and constructor standings views
```

Each Bronze table is initially written with an overwrite operation. The Silver and Gold layers are then rebuilt from the complete Bronze dataset.

This version is useful for understanding the core transformation flow and validating the final data model.

---

## Incremental-Load Pipeline

The second implementation extends the project so that only the next unprocessed batch is handled during each run.

Landing data is organized by batch folder:

```text
landing/
├── 2025-01/
│   ├── circuits.csv
│   ├── races.csv
│   ├── constructors.json
│   ├── drivers.json
│   ├── results.json
│   └── sprints.json
└── 2025-02/
    └── ...
```

### Batch Control

A Delta control table tracks pipeline progress:

```text
formula1_incr.control.batch_control
```

| Column | Purpose |
|---|---|
| `batch_id` | Identifies the landing batch |
| `status` | Stores `in_progress` or `completed` |
| `created_timestamp` | Records when processing began |
| `updated_timestamp` | Records the latest status update |

The orchestration notebooks perform the following sequence:

1. List batch folders in the landing volume.
2. Read batches already registered in the control table.
3. Select the earliest unprocessed batch.
4. Pass its ID to downstream tasks with Lakeflow task values.
5. Mark the batch as `in_progress`.
6. Run Bronze, Silver, and Gold processing for that batch.
7. Mark the batch as `completed` after successful processing.

### Incremental Write Strategy

#### Bronze

Bronze tables are partitioned by `batch_id`. The write uses `replaceWhere` so that rerunning one batch replaces only that partition rather than overwriting the entire table.

```python
(
    dataframe.write
        .format("delta")
        .mode("overwrite")
        .partitionBy("batch_id")
        .option("replaceWhere", f"batch_id = '{batch_id}'")
        .saveAsTable(target_table)
)
```

#### Silver

Silver tables use Delta `MERGE` based on each dataset's business key.

- Existing records are updated when the incoming batch is the same as or newer than the stored batch.
- New business keys are inserted.
- `created_timestamp` remains unchanged during an update.
- `updated_timestamp` records the latest modification.
- Older batches cannot overwrite records produced from newer batches.

#### Gold

Gold dimensions and the fact table also use Delta `MERGE`. This makes the analytical model rerunnable while avoiding a complete rebuild during each batch.

---

## Analytical Views

The project creates two Spark SQL views.

### Driver Standings

`v_driver_standing` calculates each driver's seasonal performance using:

- Race or session starts
- Total points
- Number of wins
- Number of podiums
- Seasonal standing calculated with `RANK()`

### Constructor Standings

`v_constructor_standing` calculates each constructor's seasonal performance using the same measures and ranking logic.

These views support questions such as:

- How did each driver perform during a season?
- How did each constructor perform during a season?
- Which drivers accumulated the most wins and podiums?
- Which constructors achieved the strongest long-term results?

---

## Repository Structure

```text
.
├── formula1-project/
│   ├── 00-common/          # Shared configuration and Bronze helper functions
│   ├── 01-setup/           # Unity Catalog and project environment setup
│   ├── 02-bronze/          # Full-load ingestion notebooks
│   ├── 03-silver/          # Cleaning and standardization notebooks
│   ├── 04-gold/            # Dimensions, reference data, and fact table
│   └── 05-analytics/       # Driver and constructor SQL views
│
└── formula1-project-incremental-load/
    ├── 00-common/          # Environment and reusable Delta write helpers
    ├── 01-setup/           # Incremental catalog and storage setup
    ├── 02-bronze/          # Batch-parameterized ingestion notebooks
    ├── 03-silver/          # Batch filtering and Silver MERGE logic
    ├── 04-gold/            # Incremental dimensions and fact table
    ├── 05-analytics/       # Analytical SQL views
    └── 06-orchestration/   # Batch control and task-value notebooks
```

---

## How to Run the Project

### Prerequisites

You need:

- An Azure subscription
- An Azure Databricks workspace with Unity Catalog enabled
- An ADLS Gen2 storage account and container
- An Access Connector for Azure Databricks
- A Unity Catalog storage credential
- Permission to create external locations, catalogs, schemas, volumes, and tables

### 1. Configure Azure Storage Access

Create or confirm the following resources:

1. Azure Databricks Access Connector
2. ADLS Gen2 storage account and container
3. `Storage Blob Data Contributor` role assignment for the connector
4. Unity Catalog storage credential
5. Unity Catalog external location

### 2. Update Project Configuration

The supplied notebooks contain course-specific Azure resource names. Replace the following values with resources from your environment:

- Storage account name
- Container name
- Storage credential name
- External location name
- Catalog name, when required

The main configuration locations are:

```text
01-setup/01.Setup Project Environment.sql
00-common/01.environment-config.py
```

### 3. Upload the Source Data

For the full-load version, place the files directly in the landing volume.

For the incremental version, place each delivery inside a unique batch folder, such as:

```text
/Volumes/formula1_incr/landing/files/2025-01/
```

### 4. Import the Notebooks

Import the project directories into a Databricks workspace while preserving their relative folder structure. The notebooks use `%run` to load shared configuration and helper notebooks.

### 5. Run the Full-Load Version

Run the notebooks in this order:

1. Project environment setup
2. Bronze ingestion notebooks
3. Silver transformation notebooks
4. Nationality-region reference notebook
5. Gold dimension notebooks
6. Gold fact notebook
7. Analytical SQL views

### 6. Run the Incremental Version

Create the control table once, then configure a Lakeflow Job with this logical order:

```text
Identify Next Batch
        ↓
Create New Batch
        ↓
Bronze Ingestion Tasks
        ↓
Silver Transformation Tasks
        ↓
Gold Dimension and Fact Tasks
        ↓
Analytical Views
        ↓
Complete Batch
```

The six Bronze tasks can run in parallel after the batch is created. Silver tasks can also be parallelized when their dependencies allow it.

> The provided files contain the orchestration notebooks, but they do not include an exported Lakeflow Job or Databricks Asset Bundle definition. The task graph and parameter mappings must therefore be configured in the Databricks workspace.

---

## Important Configuration Note

The analytical SQL notebooks in the supplied incremental project currently reference:

```sql
formula1.gold
```

The incremental notebooks otherwise use the `formula1_incr` catalog. Update the SQL views to reference your configured incremental catalog before running them, for example:

```sql
formula1_incr.gold
```

---

## Data Quality and Reliability Features

The implementation includes several practices that improve trust and rerun safety:

- Explicit source schemas instead of schema inference
- Fail-fast handling for malformed input
- Source-file and ingestion-time metadata
- Business-key null validation
- Duplicate removal
- Standardized names and data types
- Delta ACID transactions
- Table history and versioning
- Partition-scoped Bronze reruns
- Silver and Gold upserts with Delta `MERGE`
- Protection against an older batch overwriting newer Silver data
- Batch status tracking for operational recovery

---

## What I Learned

Through this project, I learned how the main components of Azure Databricks work together in an end-to-end data engineering workflow.

In particular, I gained hands-on experience with:

- Designing a multi-layer lakehouse with the Medallion Architecture.
- Connecting Databricks and ADLS through Unity Catalog-managed credentials and external locations.
- Choosing between volumes, schemas, managed locations, and Delta tables.
- Building reusable PySpark ingestion and transformation patterns.
- Understanding why explicit schemas and audit metadata matter in production pipelines.
- Using Delta Lake transaction logs, ACID guarantees, version history, and `MERGE` operations.
- Modelling facts and dimensions for efficient analytical queries.
- Using Spark SQL joins, aggregations, conditional counts, CTEs, and window functions.
- Passing parameters between Lakeflow Job tasks.
- Designing a batch control mechanism that supports incremental processing and safe reruns.
- Separating interactive development compute from production job compute.

---

## Potential Improvements

Possible next steps include:

- Define the Lakeflow Job as code using Databricks Asset Bundles.
- Add automated data-quality tests and pipeline assertions.
- Add a quarantine path for malformed records.
- Introduce schema-evolution handling.
- Add unit tests for transformation helper functions.
- Add data-volume and freshness monitoring.
- Add a CI/CD workflow for notebook deployment.
- Export and version dashboard definitions.
- Add sample output screenshots and query results.

---

## Acknowledgements

This repository is based on the hands-on Formula 1 project from **Azure Databricks for Data Engineers** by **Ramesh Retnasamy / CloudBox Academy**.

The project was used to study Azure Databricks, Apache Spark, Delta Lake, Unity Catalog, Lakeflow Jobs, and lakehouse data engineering concepts.

Before publishing course-provided notebooks or other course assets publicly, confirm that doing so is permitted by the course's copyright and licensing terms.

