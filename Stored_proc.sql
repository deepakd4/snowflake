CREATE OR REPLACE PROCEDURE load_data_from_source_to_target(source_db VARCHAR, target_db VARCHAR)
AS $$
    import snowflake.connector
    import logging

    # Set up logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    # Connect to the Snowflake account
    conn = snowflake.connector.connect(
        user='<user>',
        password='<password>',
        account='<account>',
        warehouse='<warehouse>',
        database=source_db,
        schema='<source_schema>'
    )

    # Get a list of tables in the source schema
    source_tables = []
    cursor = conn.cursor()
    cursor.execute("SHOW TABLES IN SCHEMA {};".format(source_schema))
    for row in cursor:
        source_tables.append(row[0])

    # Iterate through the tables and load data from source to target
    for table in source_tables:
        logger.info("Loading data from {} to {}".format(table, table))
        source_select_query = "SELECT * FROM {}".format(table)
        target_insert_query = "INSERT INTO {}.{}.{} SELECT * FROM {}.{}.{}".format(
            target_db, target_schema, table, source_db, source_schema, table
        )
        try:
            # Truncate the target table to clear any existing data
            cursor.execute("TRUNCATE TABLE {}.{}.{};".format(target_db, target_schema, table))
            conn.commit()

            # Insert data from source to target in parallel
            cursor.execute(source_select_query, parallel=True)
            rows = cursor.fetchall()
            cursor.executemany(target_insert_query, rows, parallel=True)
            conn.commit()
            logger.info("Successfully loaded data from {} to {}".format(table, table))
        except Exception as e:
            logger.error("Failed to load data from {} to {}: {}".format(table, table, e))
            conn.rollback()

    # Close the connection
    conn.close()
$$
LANGUAGE python;
