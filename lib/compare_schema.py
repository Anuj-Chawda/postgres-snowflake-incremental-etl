import pandas as pd
from sqlalchemy import create_engine

def schema_comparison(pg_uri, sf_conn, pg_schema, sf_database, sf_schema, tables):
    """
    Compare column-level details (name and data type) between PostgreSQL and Snowflake schemas
    for all tables in the given PostgreSQL and Snowflake schemas. Returns a dictionary containing:

        - missing_tables_in_snowflake: set of table names present in PostgreSQL but missing in Snowflake.
        - extra_tables_in_snowflake: set of table names present in Snowflake but missing in PostgreSQL.
        - tables_compared: dictionary keyed by table name (for tables existing in both systems) with:
              - missing_columns: set of column names missing in Snowflake.
              - datatype_mismatches: list of dictionaries, each with keys "column_name", "pg_data_type", "sf_data_type".

    Args:
        pg_uri (str): PostgreSQL URI to connect to the database.
        sf_conn: Connection object for Snowflake.
        pg_schema (str): Schema name in PostgreSQL to compare.
        sf_database (str): Snowflake database name.
        sf_schema (str): Schema name in Snowflake to compare.
        ignore_tables : Ignore these tables from PostgreSQL.

    Returns:
        dict: A dictionary with keys "missing_tables_in_snowflake"
              and "tables_compared" as described above.
    """
    # Query to retrieve all columns from the PostgreSQL schema
    pg_columns_query = f"""
    SELECT table_name, column_name, data_type
    FROM information_schema.columns
    WHERE table_schema = '{pg_schema}' and table_name in {tables};
    """

    # Query to retrieve all columns from the Snowflake schema
    sf_columns_query = f"""
    SELECT table_name, column_name, data_type
    FROM {sf_database}.information_schema.columns
    WHERE table_catalog = '{sf_database.upper()}'
      AND table_schema = '{sf_schema.upper()}';
    """

    # Create a PostgreSQL connection and fetch column details
    pg_engine = create_engine(pg_uri)
    with pg_engine.connect() as pg_conn:
        pg_columns = pd.read_sql_query(pg_columns_query, pg_conn)

    # Fetch column details from Snowflake
    with sf_conn.cursor() as cursor:
        cursor.execute(sf_columns_query)
        sf_data = cursor.fetchall()
        sf_columns = pd.DataFrame(sf_data, columns=["table_name", "column_name", "data_type"])

    # Normalize table names, column names, and data types to lowercase and strip whitespace
    def normalize(df):
        df["table_name"] = df["table_name"].str.lower().str.strip()
        df["column_name"] = df["column_name"].str.lower().str.strip()
        df["data_type"] = df["data_type"].str.lower().str.strip()
        return df

    pg_columns = normalize(pg_columns)
    sf_columns = normalize(sf_columns)

    # Map PostgreSQL data types to their Snowflake equivalents
    datatype_mapping = {
        "character varying": "text",
        "integer": "number",
        "timestamp without time zone": "timestamp_ntz",
        "numeric": "number",
        "bigint": "number",
        "text": "text",
        "json": "variant",
        "jsonb": "variant"
    }

    def map_datatype(data_type):
        return datatype_mapping.get(data_type.lower(), data_type.lower())

    # Build a dictionary for PostgreSQL: {table_name: {column_name: mapped_data_type}}
    pg_schema_dict = {}
    for table, group in pg_columns.groupby("table_name"):
        pg_schema_dict[table] = {
            row["column_name"]: map_datatype(row["data_type"])
            for _, row in group.iterrows()
        }

    # Build a dictionary for Snowflake: {table_name: {column_name: data_type}}
    sf_schema_dict = {}
    for table, group in sf_columns.groupby("table_name"):
        sf_schema_dict[table] = {
            row["column_name"]: row["data_type"]
            for _, row in group.iterrows()
        }

    # Determine table sets in each system
    pg_tables = set(pg_schema_dict.keys())
    sf_tables = set(sf_schema_dict.keys())

    missing_tables_in_snowflake = pg_tables - sf_tables
    common_tables = pg_tables & sf_tables

    # Compare columns for each table present in both systems
    tables_compared = {}
    for table in common_tables:
        pg_dict = pg_schema_dict[table]
        sf_dict = sf_schema_dict[table]
        pg_cols = set(pg_dict.keys())
        sf_cols = set(sf_dict.keys())

        # Columns defined in PostgreSQL but missing in Snowflake
        missing_columns = pg_cols - sf_cols

        # Check for datatype mismatches on columns common to both
        datatype_mismatches = []
        for col in pg_cols & sf_cols:
            if pg_dict[col] != sf_dict[col]:
                datatype_mismatches.append({
                    "column_name": col,
                    "pg_data_type": pg_dict[col],
                    "sf_data_type": sf_dict[col]
                })

        tables_compared[table] = {
            "missing_columns": missing_columns,
            "datatype_mismatches": datatype_mismatches
        }

    # Return a dictionary summarizing the schema comparison results
    return {
        "missing_tables_in_snowflake": missing_tables_in_snowflake,
        "tables_compared": tables_compared
    }
