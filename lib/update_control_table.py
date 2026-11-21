import logging

def insert_and_update_control_table(tables, conn, sf_database, sf_schema):
    """
    Inserts the table names into the 'control_table' and updates the 'max_etl_update_dttm'
    column to '1970-01-01 00:00:00' for each table.

    Args:
        tables: A list of table names to insert and update.
        conn: A Snowflake connection object.
        schema: Schema name of the target snowflake database
    """
    try:
        cursor = conn.cursor()

        # Insert the table names into control_table
        for table in tables:
            insert_query = f"""
            INSERT INTO {sf_database}.{sf_schema}.control_table (table_name, max_etl_update_dttm, last_sync_dttm, last_sync_status, comment)
            VALUES ('{table}', '1970-01-01 00:00:00'::TIMESTAMP, NULL, NULL, NULL)
            """
            cursor.execute(insert_query)

        exception_fct_insert_query = f"""
            INSERT INTO {sf_database}.{sf_schema}.control_table (table_name, max_etl_update_dttm, last_sync_dttm, last_sync_status, comment)
            VALUES ('icedw_exception_fct', '1970-01-01 00:00:00'::TIMESTAMP, NULL, NULL, NULL)
        """
        cursor.execute(exception_fct_insert_query)

        # Update the max_etl_update_dttm for all the records in control_table to the specified timestamp
        update_query = f"""
            UPDATE {sf_database}.{sf_schema}.control_table
            SET max_etl_update_dttm = '1970-01-01 00:00:00'::TIMESTAMP
            WHERE max_etl_update_dttm IS NULL
         """
        cursor.execute(update_query)
        logging.info(f"Updated max_etl_update_dttm for all records in control_table.")

    except Exception as e:
        logging.error(f"Error processing tables: {e}")
    finally:
        cursor.close()
