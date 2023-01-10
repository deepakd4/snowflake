Change the above solution. The target tables have 2 additional columns compared to the source tables. prod_dt which is a date data type column and fivetran_inserted_ts which is timestamp_ntz data type. Declare 2 variables temp_prod_dt which stored current date and temp_fivetran_inserted_ts which stores the current timestamp in pst timezone. before loading any of the table. After inserting data into each target table, the prod_dt should be updated to temp_prod_dt and fivetran_inserted_ts should be updated to temp_fivetran_inserted_ts respectively.

import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import snowflake.connector

def load_data(connection_params, source_schema, target_schema):
    # Declare variables to store current date and timestamp
    temp_prod_dt = datetime.date.today()
    temp_fivetran_inserted_ts = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=-8)))
    # Connect to the Snowflake
    ctx = snowflake.connector.connect(**connection_params)
    cursor = ctx.cursor()
    # Get a list of tables from the source database
    cursor.execute(f"SELECT table_name FROM information_schema.tables where table_schema='{source_schema}'")
    tables = [row[0] for row in cursor.fetchall()]
    # Use ThreadPoolExecutor to load data into the target tables in parallel
    with ThreadPoolExecutor() as executor:
        future_to_table = {executor.submit(load_table, table, temp_prod_dt, temp_fivetran_inserted_ts, cursor, source_schema, target_schema): table for table in tables}
        for future in as_completed(future_to_table):
            table = future_to_table[future]
            try:
                future.result()
                print(f'Successfully loaded {table} into the target database.')
            except Exception as exc:
                print(f'Failed to load {table} into the target database: {exc}')
    cursor.close()
    ctx.close()

def load_table(table, temp_prod_dt, temp_fivetran_inserted_ts, cursor, source_schema, target_schema):
    # Retrieve data from the source table
    cursor.execute(f"SELECT * FROM {source_schema}.{table}")
    rows = cursor.fetchall()
    # Insert the data into the target table
    for row in rows:
        cursor.execute(f"INSERT INTO {target_schema}.{table} (col1, col2, col3, prod_dt, fivetran_inserted_ts) VALUES ({row[0]}, {row[1]}, {row[2]}, {temp_prod_dt}, {temp_fivetran_inserted_ts})")
    ctx.commit()

# connection parameters
connection_params = {
    'user': 'user_name',
    'password': 'password',
    'account': 'account_name',
    'database': 'database_name',
    'warehouse': 'warehouse_name'
}

load_data(connection_params, 'source_schema', 'target_schema')




================
    
    import snowflake.connector
import datetime
import pytz

def load_data_from_source_to_target(source_db, target_db):
    # Connect to the Snowflake account
    ctx = snowflake.connector.connect(
        user='<user>',
        password='<password>',
        account='<account>'
    )

    # Get a list of tables in the source schema
    source_tables = ctx.execute("SHOW TABLES IN SCHEMA {source_schema}").fetchall()
    source_tables = [table[0] for table in source_tables]

    # Declare variables to store current date and timestamp in PST timezone
    temp_prod_dt = datetime.datetime.now().date()
    temp_fivetran_inserted_ts = datetime.datetime.now(pytz.timezone('US/Pacific'))

    # Iterate through the tables and load data from source to target
    for table in source_tables:
        print(f"Loading data from {table} to {table}")
        target_insert_query = f"INSERT INTO {target_db}.{target_schema}.{table} " \
                              f"(SELECT *, '{temp_prod_dt}' as prod_dt, '{temp_fivetran_inserted_ts}' as fivetran_inserted_ts FROM {source_db}.{source_sche

