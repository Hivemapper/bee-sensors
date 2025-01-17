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

# database_path = "/data/recording/data-logger.v2.0.0.db" # Path to SQLite database file
# database_path = "/data/redis_handler/redis_handler-v0-0-3.db" #  5.0.19 <= firmware < 5.0.26
# database_path = "/data/recording/redis_handler/redis_handler-v0-0-3.db" #  5.026 <= firmware < 5.1.4
# database_path = "/data/recording/redis_handler/sensors-v0-0-1.db" #  5.1.4 <= firmware < 5.1.9
database_path = "/data/recording/redis_handler/sensors-v0-0-2.db" #  5.1.10 <= firmware

nav_pvt_columns = ["id", "system_time",
                        "fully_resolved","gnss_fix_ok",
                        "lat_deg","lon_deg"]
nav_status_columns = ["id", "ttff"]
gnss_columns = ["id", "system_time",
                        "satellites_seen","satellites_used",
                        "rf_jam_ind"]
order_by_column = "id"

while True:

    latest_gnss = get_latest_values(database_path, "gnss", gnss_columns, order_by_column)
    latest_nav_pvt = get_latest_values(database_path, "nav_pvt", nav_pvt_columns, order_by_column)
    latest_nav_status = get_latest_values(database_path, "nav_status", nav_status_columns, order_by_column)
    print("ID should be incrementing: ", latest_gnss["id"])
    print("time should be increasing: ", latest_gnss["system_time"])
    print("Both 1 when fix is achieved. Fully resolved: ", latest_nav_pvt["fully_resolved"], "gnss_fix_ok: ", latest_nav_pvt["gnss_fix_ok"])
    print("Satellites seen should be 15+: ", latest_gnss["satellites_seen"])
    print("Satellites used should be ~60% of seen. 0 seen is very bad: ", latest_gnss["satellites_used"])
    print("RF Jamming Index should be well under 200: ", latest_gnss["rf_jam_ind"])
    print("time-to-first-fix should be < 90 sec: ", latest_nav_status["ttff"])
    print("lat: ", latest_nav_pvt["lat_deg"], "lon:", latest_nav_pvt["lon_deg"])

    time.sleep(1)
