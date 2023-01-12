import snowflake.connector
import pytz
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

def load_data_from_snowflake(source_database, source_schema, target_database, target_schema, user, password, account):
    # Get the current date and timestamp in PST timezone
    temp_prod_dt = datetime.now().date()
    temp_fivetran_inserted_ts = datetime.now(pytz.timezone('America/Los_Angeles')).strftime('%Y-%m-%d %H:%M:%S.%f')

    # Connect to the source and target databases
    con = snowflake.connector.connect(
        account=account,
        user=user,
        password=password,
    )
    con.cursor().execute(f"USE DATABASE {source_database};")
    con.cursor().execute(f"USE SCHEMA {source_schema};")
    # Get a list of all tables in the source database
    source_tables = con.cursor().execute("SHOW TABLES").fetchall()

    # Use a ThreadPoolExecutor to parallelize the data loading
    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(load_table, table[0], source_database, source_schema, target_database, target_schema, temp_prod_dt,temp_fivetran_inserted_ts, con) for table in source_tables]
        
    con.close()

def load_table(table_name, source_database, source_schema, target_database, target_schema, temp_prod_dt,temp_fivetran_inserted_ts, con):
    con.cursor().execute(f"USE DATABASE {source_database};")
    con.cursor().execute(f"USE SCHEMA {source_schema};")
    # Select all data from the source table
    source_data = con.cursor().execute(f"SELECT * FROM {table_name}").fetchall()
    con.cursor().execute(f"USE DATABASE {target_database};")
    con.cursor().execute(f"USE SCHEMA {target_schema};")
    # Insert the data into the target table, including the additional columns
    con.cursor().execute(f"INSERT INTO {table_name} (SELECT *, '{temp_prod_dt}' as prod_dt, '{temp_fivetran_inserted_ts}' as fivetran_inserted_ts FROM {source_database}.{source_schema}.{table_name})")
    con.commit()


















Write a python function that loads data from all the tables in a source snowflake database to the corresponding tables in target snowflake database.  The target tables have 2 additional columns compared to the source tables. prod_dt which is a date data type column and fivetran_inserted_ts which is timestamp_ntz data type. Declare 2 variables temp_prod_dt which stored current date and temp_fivetran_inserted_ts which stores the current timestamp in pst timezone before loading any of the table. Use pytz library to get the pst timestamp. After inserting data into each target table, the prod_dt should be updated to temp_prod_dt and fivetran_inserted_ts should be updated to temp_fivetran_inserted_ts respectively. Do not truncate any target table before inserting into it.
Improve the previous solution, use concurrent library to parallelize the load instead of multiprocessing library. Also the function should accept the source database and source schema and target database and target schema. Also use a single connection to snowflake. Both source and target databases are in the same snowflake account and domain.
