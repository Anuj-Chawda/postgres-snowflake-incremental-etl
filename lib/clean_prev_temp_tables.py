import logging

def clean_prev_instance_temp_tables(cursor, sf_database, sf_schema):
    try:
        # Convert schema and database to uppercase to match INFORMATION_SCHEMA storage
        sf_database_upper = sf_database.upper()
        sf_schema_upper = sf_schema.upper()

        # Get all tables that start with 'TEMP_'
        query = f"""
            SELECT TABLE_NAME
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = '{sf_schema_upper}'
            AND TABLE_CATALOG = '{sf_database_upper}'
            AND TABLE_NAME LIKE 'TEMP_%'
        """
        cursor.execute(query)
        tables = [row[0] for row in cursor.fetchall()]

        if not tables:
            logging.info("No previous instance remnant temp tables found.")
        else:
            for table in tables:
                table_upper = table.upper()
                drop_query = f'DROP TABLE IF EXISTS "{sf_schema_upper}"."{table_upper}"'
                cursor.execute(drop_query)
                logging.info(f"Previous temp table {table} deleted")

    except Exception as e:
        logging.error(f"Error: {e}")
