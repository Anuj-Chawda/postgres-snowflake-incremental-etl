from sqlalchemy import create_engine, text

def get_primary_keys(pg_uri, schema_name):
    """
    Retrieves the primary key columns for all tables in a given PostgreSQL schema.

    Args:
        pg_uri: PostgreSQL URI to connect to the database.
        schema_name: The schema name to query for primary keys.

    Returns:
        dict: A dictionary where keys are table names and values are lists of primary key columns.
    """
    query = text("""
        SELECT
            kcu.table_name,
            kcu.column_name
        FROM
            information_schema.table_constraints tc
        JOIN
            information_schema.key_column_usage kcu
        ON
            tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
        WHERE
            tc.constraint_type = 'PRIMARY KEY'
            AND tc.table_schema = :schema_name;
    """)
    try:
        # Create PostgreSQL engine
        pg_engine = create_engine(pg_uri)

        with pg_engine.connect() as conn:
            # Execute query
            result = conn.execute(query, {"schema_name": schema_name})
            rows = result.fetchall()  # Fetch all rows as a list of tuples

        # Organize results into a dictionary
        pk_dict = {}
        for row in rows:
            table_name, column_name = row  # Unpack each row
            if table_name not in pk_dict:
                pk_dict[table_name] = []
            pk_dict[table_name].append(column_name)

        return pk_dict

    except Exception as e:
        print(f"Error fetching primary keys: {e}")
        return {}
