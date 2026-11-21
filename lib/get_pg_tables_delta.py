import psycopg2

def classify_postgres_tables(postgres_uri, schema, ignore_tables):
    """
    Classify tables in the specified PostgreSQL schema into dim_scd1, fct, link, and other categories.

    Args:
        postgres_uri (str): The PostgreSQL connection URI.
        schema (str): The schema name to analyze.

    Returns:
        dict: A dictionary with table classifications as keys and lists of table names as values.
    """
    classifications = {
        'scd1': [],
        'other': []
    }

    # Establish connection to PostgreSQL
    with psycopg2.connect(postgres_uri) as conn:
        with conn.cursor() as cursor:
            # Fetch all table names in the schema
            cursor.execute(f"""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = '{schema}';
            """)
            tables = [row[0] for row in cursor.fetchall()]

            # Iterate over each table to classify them
            for table in tables:
                # Fetch the column names for the current table
                cursor.execute(f"""
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = '{schema}' AND table_name = '{table}' AND table_name NOT IN {ignore_tables};
                """)
                columns = [row[0] for row in cursor.fetchall()]

                # Classification logic
                if 'etl_update_dttm' in columns:
                    classifications['scd1'].append(table)
                else:
                    classifications['other'].append(table)

    return classifications
