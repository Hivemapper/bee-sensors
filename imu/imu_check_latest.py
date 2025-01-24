import os
import time
import json
import sqlite3

def get_latest_values(cursor, table_name, columns, order_by_column):
    """
    Fetch the latest values from specific columns in a SQLite3 database table.

    :param database_path: Path to the SQLite database file.
    :param table_name: Name of the table to query.
    :param columns: List of column names to retrieve.
    :param order_by_column: Column name used to determine the latest record (e.g., timestamp or ID).
    :return: A dictionary containing the latest values for the specified columns.
    """
    try:
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


def geq(ver1, ver2):
    """ Returns true if ver1 >= ver2"""
    x1,y1,z1 = ver1.split(".")
    x2,y2,z2 = ver2.split(".")
    
    if int(x1) > int(x2):
        return True
    if int(x1) < int(x2):
        return False
    if int(y1) > int(y2):
        return True
    if int(y1) < int(y2):
        return False
    if int(z1) > int(z2):
        return True
    if int(z1) < int(z2):
        return False    
    return True

def less_than(ver1, ver2):
    """ Returns true if ver1 < ver2"""
    return not geq(ver1, ver2)

# read version from /etc/build_info.json variable
with open("/etc/build_info.json") as file:
    build_info = json.load(file)
firmware_version = build_info["odc-version"]

# Choose the appropriate database path based on the firmware version
database_path = None
if geq(firmware_version, "5.0.19") and less_than(firmware_version, "5.0.26"):
    database_path = "/data/redis_handler/redis_handler-v0-0-3.db"
elif geq(firmware_version, "5.0.26") and less_than(firmware_version, "5.1.4"):
    database_path = "/data/recording/redis_handler/redis_handler-v0-0-3.db"
elif geq(firmware_version, "5.1.4"):
    directory_path = "/data/recording/redis_handler/"
    database_path = next((os.path.join(directory_path,x) for x in os.listdir(directory_path) if x.endswith(".db") and "fusion" in x), None)

imu_columns = ["id", "time", "acc_x", "acc_y", "acc_z"]#, "gyro_x", "gyro_y", "gyro_z", "temperature"]
order_by_column = "id"

# Connect to the database
conn = sqlite3.connect(database_path)
cursor = conn.cursor()

while True:
    latest_imu = get_latest_values(cursor, "imu", imu_columns, order_by_column)
    print("imu data:",latest_imu)
    # imu data written in batches every 5 seconds
    time.sleep(5)

# Close the connection
if conn:
    conn.close()
