# PostgreSQL to Snowflake Incremental ETL Pipeline

[![Python Tests](https://github.com/Anuj-Chawda/postgres-snowflake-incremental-etl/actions/workflows/pytest.yml/badge.svg)](https://github.com/Anuj-Chawda/postgres-snowflake-incremental-etl/actions/workflows/pytest.yml)

A Python-based ETL framework for synchronizing data from PostgreSQL to Snowflake using incremental loading, schema validation, and metadata-driven synchronization.

The project demonstrates a modular data engineering architecture built with **Python, SQLAlchemy, Pandas, and Snowflake**, with configurable table selection, timestamp-based change detection, and primary-key-based updates.

> **Validation status:** 29 unit tests passed locally after the latest reliability changes. GitHub Actions has passed for an earlier commit; the updated transaction and test changes have not yet been pushed and validated in CI. Live Snowflake integration testing of the latest changes is still pending.

## Architecture

**PostgreSQL -> Python / Pandas -> Snowflake**

The pipeline follows these stages:

1. **Schema validation:** Check source and target table compatibility before loading.
2. **Incremental extraction:** Retrieve PostgreSQL records newer than the stored checkpoint.
3. **Data transformation:** Convert timestamp columns and prepare extracted data in Pandas.
4. **Snowflake staging:** Upload extracted records into staging tables.
5. **Target synchronization:** Replace matching target records using primary-key-based DELETE and INSERT operations.
6. **Transactional checkpointing:** Commit target changes and the corresponding successful checkpoint update together.
7. **Execution tracking:** Record synchronization status and execution timestamps in a Snowflake control table.

## Key Features

- **Incremental data loading:** Uses the `etl_update_dttm` column to identify new and updated records.
- **Initial full extraction:** Defaults to a watermark of `1970-01-01 00:00:00` when no usable checkpoint is found; initial control-table rows must still be provisioned.
- **Configurable table selection:** Processes a defined list of PostgreSQL tables.
- **Schema validation:** Checks source-to-target schemas before loading.
- **SCD Type 1-style updates:** Replaces existing target records based on primary keys.
- **Load integrity checks:** Uses `write_pandas()` with `on_error="ABORT_STATEMENT"` and checks that the reported loaded-row count matches the extracted-row count.
- **Transactional updates:** Executes target DELETE, target INSERT, and successful checkpoint UPDATE within one Snowflake transaction, rolling back on errors before commit.
- **Checkpoint validation:** Requires the successful checkpoint UPDATE to affect exactly one control-table row.
- **Metadata-driven processing:** Tracks checkpoints, execution timestamps, and PASS/FAIL status in Snowflake.
- **Secure authentication:** Uses encrypted credential fields and Snowflake key-pair authentication.
- **Modular design:** Separates connectors, validation, loading, and metadata operations.
- **Automated unit tests:** Uses pytest and mocked database interactions.
- **Continuous integration:** GitHub Actions runs the unit-test suite on pushes and pull requests targeting `main`.
- **Execution logging:** Writes ETL activity and errors to local log files.

## Technology Stack

| Component | Technology |
|---|---|
| Programming language | Python 3.11 |
| Source database | PostgreSQL |
| Target data warehouse | Snowflake |
| Data processing | Pandas |
| Database connectivity | SQLAlchemy, psycopg2, Snowflake Connector |
| Authentication | Fernet encryption, Snowflake key-pair authentication |
| Configuration | YAML |
| Testing | pytest, unittest.mock |
| CI | GitHub Actions |

## Project Structure

```text
.
|-- .github/
|   `-- workflows/
|       `-- pytest.yml
|-- config/
|   `-- config_loader.py
|-- connectors/
|   |-- postgres_connector.py
|   `-- snowflake_connector.py
|-- loaders/
|   `-- table_loader.py
|-- metadata/
|   `-- control_table.py
|-- pipeline/
|   `-- sync_pipeline.py
|-- validation/
|   `-- schema_validator.py
|-- lib/
|   `-- ...
|-- tests/
|   |-- __init__.py
|   |-- test_config_loader.py
|   |-- test_control_table.py
|   |-- test_schema_validator.py
|   `-- test_table_loader.py
|-- delta_load.py
|-- encryption.py
|-- config.example.yaml
|-- requirements.txt
|-- requirements-dev.txt
`-- README.md
```

## Getting Started

### 1. Prerequisites

- Python 3.11 (the version used for local testing)
- Access to PostgreSQL and Snowflake, with appropriate permissions
- Snowflake private key (`rsa_key.p8`)
- A local Fernet encryption key (`encryption_key.key`) compatible with the credential-encryption utility
- Source tables with an `etl_update_dttm` column
- Existing Snowflake target tables with compatible schemas
- A Snowflake control table and initialized entries for the selected tables

### 2. Install Dependencies

Clone the repository and create a virtual environment:

```bash
git clone https://github.com/Anuj-Chawda/postgres-snowflake-incremental-etl.git
cd postgres-snowflake-incremental-etl
python -m venv venv
```

Activate the environment and install the runtime dependencies.

On Windows PowerShell:

```powershell
.\venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
```

On macOS or Linux:

```bash
source venv/bin/activate
python -m pip install -r requirements.txt
```

For development and testing, install `requirements-dev.txt` instead; it includes the runtime requirements and pytest:

```bash
python -m pip install -r requirements-dev.txt
```

### 3. Configure Connections

Copy `config.example.yaml` to `config.yaml` and configure PostgreSQL and Snowflake connection settings.

Specify the tables using a string containing a Python list:

```yaml
tables: "['customers', 'orders']"
```

Place the Snowflake private key at `rsa_key.p8` in the project root. The application also expects a matching local `encryption_key.key` for decrypting configured credential values. Run the credential-encryption utility to populate encrypted fields:

```bash
python encryption.py
```

The utility prompts for credentials that are not already encrypted and saves encrypted values in the local configuration file. **Do not commit real credentials or private keys.** The repository's `.gitignore` excludes `config.yaml`, `encryption_key.key`, and `rsa_key.p8`.

### 4. Initialize Snowflake

Before the first synchronization:

- Create the required target tables in Snowflake.
- Create the synchronization control table.
- Initialize one control-table record for each selected source table, with a starting checkpoint such as `1970-01-01 00:00:00`.

The project includes control-table setup utilities under `lib/`. This initial metadata setup must be completed before running the current pipeline entry point.

The configured table set is established during initialization; onboarding additional tables afterward is not supported by the current workflow.

### 5. Run the ETL

From the project root:

```bash
python delta_load.py
```

The pipeline validates schemas, extracts updated PostgreSQL records, synchronizes them with Snowflake, and updates control-table metadata. Execution logs are written to `logs/`.

## Incremental Loading and Transaction Safety

The ETL uses a timestamp-based high-watermark approach:

```sql
SELECT *
FROM source_schema.source_table
WHERE etl_update_dttm > last_successful_watermark;
```

For a nonempty batch, the pipeline uploads rows to a Snowflake staging table, verifies the reported load result and row count, and applies primary-key-based synchronization. Target changes and the corresponding checkpoint are handled as follows:

```sql
BEGIN;

-- Delete target rows whose primary keys are present in staging.
DELETE FROM target_table
WHERE primary_key IN (SELECT primary_key FROM staging_table);

-- Insert the staged versions of those rows.
INSERT INTO target_table
SELECT * FROM staging_table;

-- Update the successful high-watermark record for this table.
UPDATE control_table
SET max_etl_update_dttm = <maximum_processed_timestamp>
WHERE table_name = '<table_name>';

COMMIT;

-- Drop the staging table after the transaction has completed.
DROP TABLE IF EXISTS staging_table;
```

This is a simplified illustration; the application generates fully qualified SQL and also updates status, timestamp, and comment fields. The checkpoint UPDATE must affect exactly one row. When DELETE, INSERT, or checkpoint UPDATE fails before COMMIT, the application attempts `ROLLBACK`. Staging-table cleanup runs outside the transaction because Snowflake DDL statements can implicitly commit transactions.

For an empty incremental result, the loader records a no-new-data status without advancing the successful high-watermark.

### Timestamp Handling

The loader detects columns whose names contain `_dttm`, parses their values as timestamps, interprets them as UTC, and formats them in `America/New_York` time. Pandas coerces invalid timestamp values to missing values. Tests cover valid timestamps, missing/invalid values, and daylight saving time offsets.

**Important:** The UTC-source assumption must be correct for the source system; timezone and watermark semantics should be reviewed before reuse with other datasets.

## Testing and Continuous Integration

Unit tests run without real PostgreSQL or Snowflake connections by mocking external database calls.

Install development dependencies:

```bash
python -m pip install -r requirements-dev.txt
```

Run all tests:

```bash
python -m pytest -v
```

Run a focused suite:

```bash
python -m pytest tests/test_table_loader.py -v
```

The current locally verified suite contains **29 passing tests**, covering:

| Component | Tests | Coverage areas |
|---|---:|---|
| Configuration loader | 3 | Valid YAML, missing configuration, malformed YAML |
| Control table | 8 | Watermark retrieval, status updates, shared cursor, missing checkpoint record |
| Table loader | 13 | Empty and successful batches, staging failures, row counts, timestamp handling, transaction and rollback behavior |
| Schema validator | 5 | Compatible schema, missing table/columns, datatype mismatch, comparison failure |
| **Total** | **29** | **Local unit tests** |

The workflow at `.github/workflows/pytest.yml` runs on pushes and pull requests targeting `main`, using Python 3.11 and `requirements-dev.txt`.

**CI badge scope:** The badge displays the latest GitHub Actions workflow result for the repository, not necessarily the latest unpushed local changes. The transaction-related changes described above have passed locally but have not yet been verified by a fresh CI run.

## Current Scope and Limitations

- Source tables must include an `etl_update_dttm` column.
- PostgreSQL and Snowflake schemas must be compatible before loading.
- Snowflake target tables and initial control-table entries must be provisioned before execution.
- The selected table set is fixed after initialization.
- Changes are detected using timestamps, not database-native Change Data Capture (CDC).
- Source-side deletions are not automatically propagated to Snowflake.
- The current synchronization implementation uses DELETE and INSERT rather than a single SQL MERGE statement.
- The loading implementation uses staging tables created with `CREATE TABLE`; these are explicitly dropped during normal cleanup rather than declared as Snowflake `TEMPORARY` tables.
- Timestamps are interpreted as UTC before conversion to New York time. The project has not been generalized to support arbitrary source timezone conventions.
- Unit tests verify Python logic and emitted SQL commands using mocks. They do **not** prove live Snowflake rollback, data correctness, or end-to-end behavior of the latest changes.
- The cleaned ETL has been run successfully against databases previously, but the latest transaction and row-count changes still require live integration validation.

## Future Enhancements

- Live PostgreSQL-to-Snowflake integration tests, including transaction rollback and checkpoint consistency
- Automated initial setup and onboarding of new tables
- Expanded handling of timestamp/watermark edge cases
- Support for additional source and target databases
- More detailed load metrics and observability

## Project Purpose

This project demonstrates practical data engineering concepts, including incremental ETL, high-watermark tracking, database synchronization, Snowflake staging and loading, schema validation, transaction-aware checkpoints, automated unit testing, and CI.

It is intended as a reusable foundation for PostgreSQL-to-Snowflake synchronization and as a portfolio project showcasing Python and SQL-based data engineering skills.
