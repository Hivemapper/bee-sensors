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
sensors_path = "/data/recording/redis_handler/sensors-v0-0-2.db" #  5.1.10 <= firmware
fusion_path = "/data/recording/redis_handler/fusion-v0-0-2.db" #  5.1.10 <= firmware

sensors_columns = ["id", "time", "acc_x", "acc_y", "acc_z"]#, "gyro_x", "gyro_y", "gyro_z", "temperature"]
fusion_columns =  ["id", "time", "acc_x", "acc_y", "acc_z"]#, "gyro_x", "gyro_y", "gyro_z", "temperature"]
order_by_column = "id"

while True:

    latest_sensors = get_latest_values(sensors_path, "imu", sensors_columns, order_by_column)
    print("sensors:",latest_sensors)
    latest_fusion = get_latest_values(fusion_path, "imu", fusion_columns, order_by_column)
    print("fusion: ",latest_fusion)
    print("\n")

    time.sleep(1)
