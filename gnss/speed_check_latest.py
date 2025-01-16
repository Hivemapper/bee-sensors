import time
import sqlite3

def get_latest_values(database_path, table_name, columns, order_by_column):
    """
    Fetch the latest values from specific columns in a SQLite3 database table.

    :param database_path: Path to the SQLite database file.
    :param table_name: Name of the table to query.
    :param columns: List of column names to retrieve.
    :param order_by_column: Column name used to determine the latest record (e.g., timestamp or ID).
    :return: A dictionary containing the latest values for the specified columns.
    """
    try:
        # Connect to the database
        conn = sqlite3.connect(database_path)
        cursor = conn.cursor()

        # Build the SQL query
        columns_str = ", ".join(columns)
        query = f"SELECT {columns_str} FROM {table_name} ORDER BY {order_by_column} DESC LIMIT 1"

        # Execute the query
        cursor.execute(query)

        # Fetch the result
        result = cursor.fetchone()

        # Check if a result was returned
        if result is None:
            print("No data found in the table.")
            return None

        # Map the result to the column names
        latest_values = dict(zip(columns, result))

        return latest_values

    except sqlite3.Error as e:
        print(f"An error occurred: {e}")
        return None

    finally:
        # Close the connection
        if conn:
            conn.close()

database_path = "/data/recording/redis_handler/fusion-v0-0-2.db" #  5.1.10 <= firmware
gnss_columns = ["id", "system_time","session","eph","time_resolved","gnss_fix_ok","speed"]
order_by_column = "id"

while True:

    latest_gnss = get_latest_values(database_path, "gnss_concise", gnss_columns, order_by_column)
    # print("raw data:",latest_gnss)
    if latest_gnss["time_resolved"] == 1 and latest_gnss["gnss_fix_ok"] == 1 and latest_gnss["eph"] < 10.:
        print(f"Speed [mph]: {2.23694*latest_gnss['speed']}, Speed [km/h]: {3.6 * latest_gnss['speed']}, Speed [m/s]: {latest_gnss['speed']}")
    else:
        print("invalid speed data.")

    time.sleep(0.5)
