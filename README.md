# PostgreSQL to Snowflake Incremental ETL Pipeline

A Python-based ETL framework for synchronizing data from PostgreSQL to Snowflake using incremental loading, schema validation, and metadata-driven synchronization.

The project demonstrates a modular data engineering architecture built with **Python, SQLAlchemy, Pandas, and Snowflake**, with support for configurable table selection, timestamp-based change detection, and primary-key-based upserts.

## Architecture

**PostgreSQL → Python/Pandas → Snowflake**

The pipeline follows these stages:

1. **Schema Validation:** Validate the source and target table structures before loading.
2. **Incremental Extraction:** Retrieve PostgreSQL records updated since the last successful synchronization.
3. **Data Transformation:** Process timestamp columns and prepare data using Pandas.
4. **Snowflake Staging:** Load extracted records into temporary Snowflake tables.
5. **Upsert Processing:** Apply primary-key-based DELETE and INSERT operations to synchronize target records.
6. **Metadata Tracking:** Maintain synchronization checkpoints, execution timestamps, and load statuses in a Snowflake control table.

## Key Features

- **Incremental Data Loading:** Uses the `etl_update_dttm` column to identify new and updated records.
- **Initial Full Load:** Uses a default watermark of `1970-01-01 00:00:00` for tables without an existing checkpoint.
- **Configurable Table Selection:** Synchronizes a defined list of PostgreSQL tables.
- **Schema Validation:** Performs source-to-target schema checks before loading.
- **SCD Type 1–Style Updates:** Replaces existing target records based on primary keys.
- **Metadata-Driven Processing:** Tracks synchronization timestamps and execution statuses in Snowflake.
- **Secure Authentication:** Supports encrypted credentials and Snowflake key-pair authentication.
- **Modular Design:** Separates connection management, validation, loading, and metadata operations.
- **Execution Logging:** Records ETL activity and errors in local log files.

## Technology Stack

| Component | Technology |
|---|---|
| Programming Language | Python |
| Source Database | PostgreSQL |
| Target Data Warehouse | Snowflake |
| Data Processing | Pandas |
| Database Connectivity | SQLAlchemy, psycopg2, Snowflake Connector |
| Authentication | Fernet encryption, Snowflake key-pair authentication |
| Configuration | YAML |

## Project Structure

```text
.
├── config/
│   └── config_loader.py
├── connectors/
│   ├── postgres_connector.py
│   └── snowflake_connector.py
├── loaders/
│   └── table_loader.py
├── metadata/
│   └── control_table.py
├── pipeline/
│   └── sync_pipeline.py
├── validation/
│   └── schema_validator.py
├── lib/
│   └── ...
├── delta_load.py
├── encryption.py
├── config.example.yaml
├── requirements.txt
└── README.md
```

## Getting Started

### 1. Prerequisites

- Python 3.11 or a compatible Python version
- Access to PostgreSQL and Snowflake
- Appropriate database permissions
- Snowflake private key (`rsa_key.p8`)
- Source tables containing an `etl_update_dttm` column
- Existing Snowflake target tables with compatible schemas

### 2. Install Dependencies

Clone the repository and create a virtual environment.

```bash
git clone <repository-url>
cd <repository-name>

python -m venv venv
```

Activate the environment and install dependencies.

On Windows:

```powershell
.\venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
```

### 3. Configure Connections

Copy `config.example.yaml` to `config.yaml` and update the PostgreSQL and Snowflake connection settings.

Specify the tables to synchronize using a string containing a Python list:

```yaml
tables: "['customers', 'orders']"
```

Add your Snowflake private key as `rsa_key.p8` in the project root.

Run the encryption utility to securely populate encrypted credential fields:

```bash
python encryption.py
```

The utility prompts for credentials that are not already encrypted and stores their encrypted values in the local configuration file.

Keep `config.yaml`, `encryption_key.key`, and `rsa_key.p8` private. These files are excluded from version control.

### 4. Initialize Snowflake

Before the first synchronization:

- Create the required target tables in Snowflake.
- Create the synchronization control table.
- Initialize control-table records for the selected source tables with a starting watermark of `1970-01-01 00:00:00`.

The project includes control-table setup utilities under `lib/`. Initial metadata setup must be completed before running the current pipeline entry point.

The configured table set is established during initialization; adding tables afterward is not supported by the current workflow.

### 5. Run the ETL

From the project root:

```bash
python delta_load.py
```

The pipeline validates table schemas, extracts updated PostgreSQL records, synchronizes them with Snowflake, and updates the control-table metadata.

Execution logs are written to the `logs/` directory.

## Incremental Loading Strategy

The ETL uses a timestamp-based high-watermark approach:

```sql
SELECT *
FROM source_schema.source_table
WHERE etl_update_dttm > last_successful_watermark;
```

After a successful load, the control table records the maximum processed `etl_update_dttm` value.

For existing primary keys, Snowflake target records are replaced using DELETE and INSERT operations. New records are inserted into the target.

This approach supports recurring synchronization without reloading the complete source dataset on every execution.

## Current Scope and Limitations

- Source tables must include an `etl_update_dttm` column.
- The pipeline requires compatible PostgreSQL and Snowflake table schemas.
- Snowflake target tables and initial control-table entries must be provisioned before execution.
- The table selection is fixed after initialization.
- Changes are identified through timestamps rather than database-native Change Data Capture (CDC).
- Deletes made only in the PostgreSQL source are not propagated automatically.
- Full end-to-end validation of the cleaned public repository remains pending.

## Future Enhancements

- Automated initial setup and table onboarding
- Unit and integration tests using pytest
- Improved transaction handling and connection cleanup
- Support for additional source and target databases
- CI/CD integration for automated validation

## Project Purpose

This project demonstrates practical data engineering concepts, including incremental ETL, high-watermark tracking, database synchronization, data warehouse loading, schema validation, and modular pipeline development.

It is intended as a reusable foundation for PostgreSQL-to-Snowflake data synchronization and as a portfolio project showcasing Python and SQL-based data engineering skills.