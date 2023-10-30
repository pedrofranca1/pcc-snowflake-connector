import snowflake.connector
import pandas as pd
from snowflake.connector import DictCursor
import time


def snowflake_lookup(
    account, warehouse, user, authenticator, role, schema, database_name, file_path
):
    start_time = time.time()  # Record the start time

    conn = snowflake.connector.connect(
        account=account,
        warehouse=warehouse,
        user=user,
        authenticator=authenticator,
        role=role,
        schema=schema,
        database=database_name,
    )

    cursor = conn.cursor(DictCursor)

    final_dados = []

    with open(file_path, "r") as file:
        query = file.read()
        print("SQL Query:")
        print(query)

    cursor.execute(query)

    for row in cursor.fetchall():
        final_dados.append(row)

    conn.close()

    dataframe = pd.DataFrame(final_dados)

    end_time = time.time()  # Record the end time
    elapsed_time = end_time - start_time  # Calculate elapsed time in seconds
    print(f"Elapsed Time: {elapsed_time} seconds")

    return dataframe


# account = "technipfmc-data"
# warehouse = "reporting_wh"
# user = "pedro.franca@technipfmc.com"
# authenticator = "externalbrowser"
# role = "REPORTING"
# schema = "PUBLIC"
# database_name = "IDSDEV"
# file_path = "pcc_query.txt"


# df = snowflake_lookup(
#     account, warehouse, user, authenticator, role, schema, database_name, file_path
# )
