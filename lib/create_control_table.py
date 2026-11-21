import logging

def create_control_table(conn, sf_database, sf_schema):
    """
    Creates the control_table in Snowflake if it doesn't already exist.

    Args:
        conn: An active Snowflake connection object.
    """
    cursor = conn.cursor()
    try:
        query = f"""
        CREATE TABLE IF NOT EXISTS {sf_database}.{sf_schema}.control_table (
            table_name STRING,
            max_etl_update_dttm TIMESTAMP,
            last_sync_dttm TIMESTAMP,
            last_sync_status STRING,
            comment STRING
        )
        """
        cursor.execute(query)

        logging.info("control_table created.")
    finally:
        cursor.close()