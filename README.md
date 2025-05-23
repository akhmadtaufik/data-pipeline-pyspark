# Project Documentation: Unified Startup Data Integration Pipeline

## Introduction: Project Context

In the dynamic world of startups and investments, data is often fragmented across various systems and formats. This project addresses the challenge of integrating disparate data sources to create a unified, reliable, and queryable system for analysis and insights. The primary data sources include:

- **Startup Investment Database**: A structured relational database (assumed PostgreSQL) containing detailed information about companies, funding rounds, investment amounts, investor profiles, and Initial Public Offerings (IPOs).
- **Files with People Information**: Semi-structured files (e.g., CSV, JSON) holding details about key individuals such as founders, CEOs, and board members, including their affiliations and roles within companies.
- **Company Milestone API**: An external API delivering data on significant events and milestones in a company's lifecycle, crucial for tracking growth and development trajectories.

The core objective is to build a robust ETL (Extract, Transform, Load) pipeline using PySpark to ingest, clean, transform, and integrate this data into a centralized data store. This documentation outlines the requirements, design, implementation, and operational aspects of this pipeline.

---

## 1. Requirements Gathering & Solution

### a. Background Problem

The primary challenge lies in the heterogeneity of the data sources:

- **Format Variety**: Data exists in structured (SQL database), semi-structured (CSV/JSON files), and external API (JSON/XML) formats. Each format requires different ingestion and parsing strategies.
- **Data Silos**: Information is isolated. For instance, company financial data is in the database, key personnel details are in files, and company progress markers are from an API. Linking these requires careful integration.
- **Inconsistent Data**: Data from different sources might have varying quality, consistency, and completeness. For example:
  - People's names might be formatted differently across sources.
  - Company identifiers might not be uniform.
  - Timestamps or date formats could vary.
  - API data might have rate limits or availability issues.
- **Scalability**: As the volume of data grows (more companies, investments, people, milestones), the integration process must be scalable and performant.
- **Complexity of Relationships**: Understanding the intricate relationships between companies, people, investments, and milestones requires a well-designed data model and transformation logic.

Without a unified pipeline, data analysis is manual, error-prone, and time-consuming, hindering the ability to derive comprehensive insights.

#### b. Proposed Solutions

The proposed solution is to develop an automated ETL pipeline with the following vision and purpose:

- **Unified Data Repository**: Create a single source of truth by integrating all data into a centralized target database (e.g., a PostgreSQL data warehouse or a data lakehouse structure).
- **Data Harmonization & Cleaning**: Implement processes to standardize formats, resolve inconsistencies, handle missing data, and ensure data quality.
- **Scalable Processing**: Leverage PySpark for its distributed processing capabilities to handle current and future data volumes efficiently.
- **Modular Design**: Develop a modular pipeline that allows for easier maintenance, updates, and potential addition of new data sources.
- **Actionable Insights**: Enable analysts and stakeholders to easily query and analyze comprehensive data, leading to better decision-making regarding investments, market trends, and company performance.
- **Automation**: Automate the entire data flow from extraction to loading, reducing manual intervention and ensuring data freshness.

#### c. Profiling Data

Data profiling is a critical initial step to understand the characteristics of each source. (Based on the project's file structure, profiling scripts like `src/profiling/profiling.py` and outputs in `docs/profiling/` exist).

- **Startup Investment Database (PostgreSQL)**:
  - **Structure**: Multiple related tables (e.g., `companies`, `funding_rounds`, `investments`, `ipos`). Data is generally structured with defined schemas.
  - **Completeness**: Certain fields like `company_description` or `investment_stage` might have missing values. Foreign key relationships ensure some level of relational integrity.
  - **Data Types**: Mostly standard SQL types (VARCHAR, INTEGER, TIMESTAMP, NUMERIC). Dates might need standardization.
  - **Inconsistencies**: Currency formats for investment amounts might vary if data is aggregated from international sources (though typically standardized in a DB). Company names might have slight variations if not strictly curated.
  - **Uniqueness**: Primary keys (e.g., `company_id`, `investment_id`) ensure record uniqueness within tables.

- **Files with People Information (e.g., `data/raw/people.csv`)**:
  - **Structure**: CSV files with headers like `person_id`, `first_name`, `last_name`, `email`, `company_affiliation_id`. JSON files might have nested structures for roles or contact details.
  - **Completeness**: `email` or `company_affiliation_id` might be frequently missing. Middle names might be inconsistently present.
  - **Data Types**: Mostly strings, requiring parsing for dates or numerical IDs. `person_id` should be unique.
  - **Inconsistencies**: Name spellings, inconsistent use of titles (Mr., Dr.), varied date formats for `date_of_birth`. Multiple entries for the same person if IDs are not managed well.
  - **Uniqueness**: `person_id` should be unique, but duplicates might exist if data is manually compiled. Email can be a good candidate for deduplication.

- **Company Milestone Data (API)**:
  - **Structure**: Typically JSON responses. Milestones might be an array of objects, each with `milestone_date`, `description`, `category`, `source_url`.
  - **Completeness**: `description` field might be verbose or too brief. `source_url` for verification might be missing or broken.
  - **Data Types**: Dates are likely strings needing parsing. Categories might be free-text or from a controlled vocabulary.
  - **Inconsistencies**: Milestone descriptions can be highly variable. Date formats from the API need checking.
  - **Uniqueness**: A combination of `company_id` and `milestone_date` and `description_hash` might be needed to define uniqueness if no explicit milestone ID is provided. API rate limits and pagination need to be handled.

#### d. Design Pipeline

The pipeline is designed with distinct layers to manage data flow and transformation effectively:

- **Layers**:
    1. **Raw Layer (Landing Zone)**:
        - **Purpose**: Ingest data from sources with minimal transformation, primarily for archival and reprocessing capabilities.
        - **Storage**: Could be a dedicated schema in PostgreSQL for database dumps, a specific directory in a file system (like `data/raw/` for files), or object storage (e.g., MinIO, AWS S3) for API responses and file uploads.
    2. **Staging Layer**:
        - **Purpose**: Clean, validate, standardize, and partially transform raw data. This layer prepares data for loading into the core analytical database. Schema enforcement and basic data quality checks are applied here.
        - **Storage**: Staging tables in PostgreSQL (e.g., `stg_companies`, `stg_people` as suggested by `schema/staging.sql`) or processed files in a structured layout on object storage (e.g., Parquet format).
        - **Modules**: `src/staging/` directory likely handles this.
    3. **Core/Warehouse Layer (Integration Layer)**:
        - **Purpose**: Integrate data from various staging sources into a well-defined, dimensional or normalized schema optimized for analytics and reporting. This involves complex transformations, joins, aggregations, and business logic application.
        - **Storage**: Final analytical tables in PostgreSQL (e.g., dimension tables like `dim_company`, `dim_person` and fact tables like `fact_investments` as suggested by `schema/warehouse.sql` and `src/warehouse/transformation/`).
        - **Modules**: `src/warehouse/` directory likely handles this.

- **Log System**:
  - **Purpose**: Track pipeline execution, monitor status, log errors, and record metadata for each run.
  - **Implementation**: The `ETLLogger` class in `src/utils/log.py` is used. It logs structured messages (dictionaries) to a dedicated PostgreSQL table (`startup.startup_etl_log`).
  - **Key Logged Info**: `etl_date`, `process_name`, `table_name`, `status` (success/failure), `row_count`, `error_message`, `duration_seconds`.
  - **Schema**: Defined in `schema/log.sql`.

- **Validation System**:
  - **Purpose**: Ensure data integrity and quality at various stages of the pipeline.
  - **Implementation**:
    - **Schema Validation**: PySpark's schema enforcement capabilities (`inferSchema=False` with a predefined schema) during reads.
    - **Custom Rules**: The `src/quality/quality_checker.py` module, likely driven by rules defined in `config/quality_rules.yaml`, applies specific checks (e.g., not null, regex patterns, range checks).
    - **Error Handling**: Modules like `src/staging/load/handle_error.py` and `src/warehouse/load/handle_error.py` suggest mechanisms for isolating or flagging records that fail validation, potentially moving them to an error table or quarantine zone.
    - **Referential Integrity**: Enforced in the target database where possible, or checked during the transformation phase.

---

### 2. Design Target Database

The target database is designed to serve as a centralized, integrated repository for analytical queries.

- **Type of Database Used**: PostgreSQL is used as the target relational database, acting as a data warehouse for this project. This is inferred from `psycopg2_connection.py`, `schema/*.sql` files, and the `ETLLogger` implementation.
- **ERD or Schema Description**:
    (A full ERD would be extensive. A description based on inferred tables from `schema/warehouse.sql` and transformation scripts in `src/warehouse/transformation/` is provided below. The actual DDLs are in `schema/warehouse.sql` and `schema/staging.sql`.)

    The target schema likely follows a dimensional model or a well-normalized relational model:

  - **Dimension Tables**:
    - `dim_company`: Detailed information about companies (e.g., `company_id`, `name`, `industry`, `location`, `founded_date`).
    - `dim_person`: Information about individuals (e.g., `person_id`, `full_name`, `email`, `roles`).
    - `dim_investor`: Details about investors (VC firms, angels).
    - `dim_fund`: Information about investment funds.
    - `dim_location`: Geographic details (city, country, region).
    - `dim_date`: Date dimension for time-based analysis.
    - `dim_milestone_type`: Types of company milestones.
    - `dim_round_type`: Types of funding rounds (Seed, Series A, etc.).
  - **Fact Tables**:
    - `fact_funding_round`: Details of each funding round, linking companies, investors (possibly through a bridge table), and dates (e.g., `funding_round_id`, `company_id`, `round_type_id`, `amount_raised`, `funding_date_sk`).
    - `fact_ipo`: Information about Initial Public Offerings, linking to `dim_company` and `dim_date`.
    - `fact_acquisition`: Data on company acquisitions.
    - `fact_investment`: Granular data on specific investments within rounds, linking investors to funding rounds.
    - `fact_company_milestone`: Links companies to milestones and dates.
    - `fact_person_role`: Links people to companies with specific roles and timeframes.

    **Relationships**: Standard foreign key relationships would link fact tables to dimension tables (e.g., `fact_funding_round.company_id` -> `dim_company.company_id`).

- **Source-to-Target Field Mapping**:
    (This would be a detailed document. An example is provided. The `docs/source-to-target-mapping.md` file in the project structure suggests this exists.)

    **Example: `people.csv` to `dim_person`**

    | Source Field (people.csv) | Target Field (dim_person) | Transformation Logic                                     |
    |---------------------------|---------------------------|----------------------------------------------------------|
    | `person_id`               | `person_id`               | Direct map (after type casting if needed)                |
    | `first_name`, `last_name` | `full_name`               | Concatenate `first_name` and `last_name`                 |
    | `email`                   | `email`                   | Validate format, convert to lowercase                    |
    | `date_of_birth`           | `birth_date`              | Parse string to Date type, handle various input formats  |
    | `company_affiliation_id`  | (Used to link to roles)   | Used in `fact_person_role` or an association table       |
    | N/A                       | `person_sk`               | Surrogate key generated during load                      |
    | N/A                       | `etl_load_date`           | Current timestamp at load time                           |

---

### 3. Design of the ETL Pipeline

The pipeline follows an Extract-Transform-Load (ETL) pattern, with distinct stages for processing.

- **Stages**:
    1. **Extract**:
        - **Startup Investment DB**: Data is extracted from PostgreSQL tables using JDBC connections in Spark.
        - **People Information Files**: CSV/JSON files are read from `data/raw/` using Spark's file reading capabilities.
        - **Company Milestone API**: Data is fetched from the external API using Python's `requests` library (likely within a Spark UDF or a Python script whose output is then read by Spark). Responses (JSON) are parsed.
        - **Output**: Raw DataFrames or RDDs in Spark, potentially landed in the Raw Layer (e.g., HDFS, S3, or a dedicated raw schema in DB if not directly processed).
    2. **Load to Staging (Optional but Recommended for ELT variants or complex pipelines)**:
        - Raw extracted data is loaded into staging tables in PostgreSQL (schema `staging`) or as structured files (e.g., Parquet) in a staging area. This provides a checkpoint and allows transformations to run on a consistent snapshot.
        - Modules: `src/staging/load/load_data.py`.
    3. **Transform**:
        - **Data Cleaning**: Handling nulls, standardizing data types, correcting inconsistencies (e.g., date formats, string casing).
        - **Validation**: Applying quality rules from `config/quality_rules.yaml` via `src/quality/quality_checker.py`.
        - **Schema Mapping**: Aligning source data with staging and then warehouse schemas.
        - **Business Logic**:
            - Joining data from different sources (e.g., linking people to companies, investments to funding rounds).
            - Calculating derived fields (e.g., company age, time between funding rounds).
            - Aggregations for summary tables if needed.
            - Generating surrogate keys for dimension tables.
        - **Modules**: `src/warehouse/transformation/` scripts (e.g., `company.py`, `person.py`, `fact_funding_round.py`).
    4. **Load to Warehouse**:
        - Transformed and integrated data is loaded into the final dimension and fact tables in the PostgreSQL warehouse (schema `warehouse`).
        - **Write Mode**: Typically `overwrite` for full refreshes of dimensions/facts or `append` for incremental loads (if applicable and designed for).
        - **Modules**: `src/warehouse/load/load_warehouse.py`.

- **Order of Execution**:
    1. Extract data from all sources in parallel or sequentially.
    2. Load extracted raw data to Staging tables/area (if this intermediate step is used).
    3. Run transformation jobs. Dimension tables are typically populated first, followed by fact tables due to dependencies.
        - E.g., `dim_company`, `dim_person`, `dim_date` must exist before `fact_funding_round` can be populated.
    4. Load transformed data into Core/Warehouse tables.
    5. Log status and metrics for each step.

- **Data Dependencies**:
  - Fact table transformations depend on the prior successful transformation and loading of related dimension tables.
  - Transformations for enriched views (e.g., a company profile combining DB, file, and API data) depend on the successful staging of all relevant sources.
  - Incremental loads would depend on the last successful run timestamp (retrieved via `ETLLogger.get_last_run`).

- **Tools used for orchestration**:
  - The project structure does not explicitly show a dedicated orchestrator like Airflow, Luigi, or Prefect.
  - Execution might be managed by sequential script calls (e.g., `_staging_pipeline.py` then `_warehouse_pipeline.py`), potentially triggered by cron jobs or manual execution for this project scale.
  - If `docker-compose.yaml` includes an Airflow service, then Airflow would be the orchestrator.

---

### 4. Stack / Tools / Libraries Used

- **Core Language**:
  - Python (Version 3.8+ inferred)
- **Data Processing**:
  - Apache Spark (Version 3.x.x inferred)
  - PySpark (Python API for Spark)
  - Pandas (For `ETLLogger` and potentially for handling smaller datasets or API responses before converting to Spark DataFrames)
- **Database**:
  - PostgreSQL (As source for investment data and target for staging, warehouse, and logs)
  - `psycopg2-binary` (Python adapter for PostgreSQL)
  - `SQLAlchemy` (Used by `ETLLogger` for database interaction, and potentially by Pandas `to_sql`)
- **Configuration**:
  - `PyYAML` (For reading `.yaml` configuration files like `config/api_endpoints.yaml`, `config/quality_rules.yaml`)
- **API Interaction**:
  - `requests` (For fetching data from external Company Milestone API)
- **Development Environment & Version Control**:
  - `venv` (Python virtual environments)
  - Git
  - Docker, Docker Compose (`docker-compose.yaml` present, suggesting containerized setup for Spark/Postgres)
- **Object Storage (Optional/Inferred for larger scale)**:
  - MinIO / AWS S3.
- **Command Line Interface**:
  - `argparse` (Likely used in main pipeline scripts like `_staging_pipeline.py` for parameterizing runs).

---

### 5. How the ETL Pipeline Works and How to Run It

- **File structure of the repository**:

    ```text
    .
    ├── _profiling.py             # Main script for data profiling
    ├── _staging_pipeline.py      # Main script for staging layer ETL
    ├── _warehouse_pipeline.py    # Main script for warehouse layer ETL
    ├── config/                   # Configuration files
    │   ├── api_endpoints.yaml
    │   └── quality_rules.yaml
    ├── data/                     # Data files
    │   ├── processed/            # Output for processed data (local dev)
    │   └── raw/                  # Raw input files (e.g., people.csv)
    ├── docs/                     # Documentation
    │   ├── profiling/            # Profiling reports
    │   └── source-to-target-mapping.md
    ├── logs/                     # Log files
    │   └── pipeline.log
    ├── requirements.txt          # Python dependencies
    ├── schema/                   # SQL DDL files
    │   ├── log.sql
    │   ├── staging.sql
    │   └── warehouse.sql
    ├── src/                      # Source code
    │   ├── profiling/
    │   ├── quality/
    │   ├── staging/              # Staging layer ETL logic (extract, load)
    │   ├── utils/                # Utility modules (config, logger, connections)
    │   └── warehouse/            # Warehouse layer ETL logic (extract, transform, load)
    ├── docker-compose.yaml       # Docker configuration
    └── spark-warehouse/          # Default Spark SQL warehouse directory
    ```

- **Entry point to run the pipeline**:
  - The pipeline is likely run in stages using the main scripts:
        1. `_staging_pipeline.py` (to process raw data into staging)
        2. `_warehouse_pipeline.py` (to process staged data into the warehouse)
  - A master script or an orchestrator (if used) would call these in sequence.
  - Profiling might be run independently using `_profiling.py`.

- **Environment variables or config files required**:
  - **Environment Variables (typically in a `.env` file loaded by `src/utils/config.py`)**:
    - `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_USER`, `POSTGRES_PASSWORD`
    - `DB_LOG` (Name of the log database)
    - `DB_RAW`, `DB_STAGING`, `DB_WAREHOUSE` (Names for different databases, if used, or schemas within a single DB)
    - API keys for external services (e.g., `MILESTONE_API_KEY`)
  - **Configuration Files**:
    - `config/api_endpoints.yaml`: URLs and parameters for external APIs.
    - `config/quality_rules.yaml`: Definitions for data quality checks.

- **CLI example or command to run**:
    (Using `spark-submit`, assuming Spark is installed or managed via Docker)

    **Running Staging Pipeline:**

    ```bash
    # Ensure .env file is configured or environment variables are set
    # Package src directory if needed: zip -r src.zip src/

    spark-submit \
      --master local[*] \ # For local mode
      # --py-files src.zip # If src is packaged
      _staging_pipeline.py \
      --processing-date YYYY-MM-DD # Example custom argument
    ```

    **Running Warehouse Pipeline:**

    ```bash
    spark-submit \
      --master local[*] \
      # --py-files src.zip
      _warehouse_pipeline.py \
      --processing-date YYYY-MM-DD
    ```

- **Optional: How to run in local mode vs distributed mode**:
  - **Local Mode**:
    - Set `spark-submit --master local[*]` or configure in `src/utils/spark_session.py`.
    - PostgreSQL can be run locally or via Docker (as per `docker-compose.yaml`).
    - File paths in `data/raw/` are local.
  - **Distributed Mode (e.g., YARN, Kubernetes)**:
    - `spark-submit --master yarn --deploy-mode client|cluster ...`
    - Requires a configured Spark cluster.
    - Input/output paths would point to distributed file systems (HDFS, S3).
    - Database connections would point to the production database server.
    - `docker-compose.yaml` might be adapted for Kubernetes deployment or a separate K8s manifest used.

---

### 6. Expected Output for Each Process

#### Extract Process

- **Input**:
  - Startup Investment DB: Tables in PostgreSQL.
  - People Information: CSV/JSON files from `data/raw/`.
  - Company Milestones: JSON response from an external API.
- **Expected Output Format**:
  - Spark DataFrames for each source.
- **Sample Output Data (Conceptual - Spark DataFrame `show()` output)**:
  - `people_raw_df.show(5, truncate=False)` would display raw rows from `people.csv`.
- **Location where output is stored**:
  - In-memory Spark DataFrames passed to the next stage.
- **Data Quality Expectations**:
  - All reachable records/files are ingested.
  - Basic schema detection (if `inferSchema=True`) or adherence to a predefined raw schema.
  - Row counts logged match source counts where feasible.

#### Load to Staging Process (if `_staging_pipeline.py` includes this)

- **Input**: Spark DataFrames from the Extract process.
- **Expected Output Format**:
  - Tables in PostgreSQL `staging` schema (e.g., `staging.stg_people`, `staging.stg_companies`).
  - Data types are conformed to staging table definitions (`schema/staging.sql`).
  - Basic cleaning applied (e.g., trimming whitespace).
- **Sample Transformed Data**:

    ```sql
    SELECT person_id, first_name, last_name, email FROM staging.stg_people LIMIT 5;
    ```

- **Location where output is stored**: PostgreSQL `staging` schema tables.
- **Data Quality Expectations**:
  - Row counts match extracted counts (unless invalid records are dropped).
  - No records violating `NOT NULL` constraints in staging tables.
  - Timestamps standardized.
  - Duplicates might still exist if deduplication is deferred to warehouse transformations.

#### Transform & Load to Warehouse Process (`_warehouse_pipeline.py`)

- **Input**: Data from Staging tables (PostgreSQL `staging` schema) or processed Staging DataFrames.
- **Expected Output Format**:
  - Dimension and Fact tables in PostgreSQL `warehouse` schema (e.g., `warehouse.dim_company`, `warehouse.fact_funding_round`).
  - Data is integrated, cleaned, de-duplicated, and conforms to the warehouse schema (`schema/warehouse.sql`).
- **Sample Transformed Data (Conceptual - SQL query from warehouse table)**:
    **`warehouse.dim_person`**

    | person_sk | person_id | full_name      | email_cleaned      | birth_year |
    |-----------|-----------|----------------|--------------------|------------|
    | 1         | p101      | John Doe       | <john.doe@email.com> | 1985       |
    | 2         | p102      | Jane Smith     | <jane.smith@email.com>| 1990       |

    **`warehouse.fact_funding_round`**

    | funding_round_sk | company_sk | round_type_sk | raised_amount_usd | funding_date_sk |
    |------------------|------------|---------------|-------------------|-----------------|
    | 1001             | c1         | rt2           | 5000000           | 20220115        |
    | 1002             | c2         | rt3           | 10000000          | 20220320        |

- **Location where output is stored**: PostgreSQL `warehouse` schema tables.
- **Data Quality Expectations**:
  - Data integrity maintained (referential integrity between facts and dimensions).
  - Duplicates removed based on defined business keys.
  - Nulls handled appropriately (e.g., replaced with 'Unknown' in dimensions, or records filtered).
  - All transformations applied correctly as per business rules.
  - Row counts in dimension and fact tables are consistent with source data after transformations.
  - Metrics from `quality_checker.py` show high pass rates.
