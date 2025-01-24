"""Check accelerometer, gyroscope, and magnetometer data for quality assurance.

"""

__author__ = "D. Knowles"
__date__ = "24 Oct 2024"


import os
import subprocess

import sqlite3
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

def main():

    # database path
    DB_PATH = "/home/<PATH TO DATABASE FILE>/redis_handler-v0-0-3.db"
    logs, metrics = parse_database(DB_PATH)

    plot_imu_values(logs["imu"])
    plot_magnetometer_values(logs["mag"])
    metrics = check_imu_zeros(logs["imu"], metrics)
    metrics = check_mag_zeros(logs["mag"], metrics)
    print_metrics(metrics)

    plt.show()

def parse_database(db_path):
    """Parse sqlite3 database file.

    Use sqlite3 to connect then read from pandas.

    Attempt recovery if database file is corrupted (requires sqlite3 installed on device).

    Parameters
    ----------
    db_path : str
        Path to the database file.
    
    Returns
    -------
    logs : dict
        Dictionary containing the dataframes for each sensor.
    
    """

    db_not_present = []
    logs = {
            "imu"  : [],
            "mag"  : [],
            }
    metrics = {
            "imu" : {},
            "mag"  : {},
            }

    if not os.path.isfile(db_path):
      print(f"no such file {db_path} found.")
      return None

    # Connect to the database
    conn = sqlite3.connect(db_path)

    # add imu data
    try:
        df = pd.read_sql_query("SELECT * FROM imu", conn)
        logs["imu"] = df
    except Exception as e:
        print(f"imu db error: {e}")
        logger_recovered_path = db_path[:-3] + "_recovered.db"
        if not os.path.isfile(logger_recovered_path):
            # recover file
            print("attempting recovery")
            recover_sqlite_db(db_path,logger_recovered_path)
        else:
            print("using previously recovered db file")
        conn = sqlite3.connect(logger_recovered_path)

        try:
            df = pd.read_sql_query("SELECT * FROM imu", conn)
            logs["imu"] = df
        except Exception as e:
            print(f"imu db error: {e}")
            logs["imu"] = None

    # add magnetometer data
    try:
        df = pd.read_sql_query("SELECT * FROM magnetometer", conn)
        logs["mag"] = df
    except Exception as e:
        print(f"magnetometer db error: {e}")
        logs["mag"] = None

    # Close the connection
    conn.close()

    return logs, metrics

def recover_sqlite_db(corrupt_db_path, recovered_db_path):
    """
    Recovers a corrupt SQLite database using the .recover command.

    Parameters
    ----------
    corrupt_db_path : str
        Path to the corrupt database file.
    recovered_db_path : str
        Path to the recovered database file.

    """

    try:
        subprocess.run(f"sqlite3 {corrupt_db_path} .recover | sqlite3 {recovered_db_path}", shell=True, check=True)
        print("Database recovered successfully.")
    except subprocess.CalledProcessError as e:
        print("Error during recovery:", e)

def plot_imu_values(log_imu):
    """Plot accelerometer and gyroscope data.
    
    Parameters
    ----------
    log_imu : pd.DataFrame
        Dataframe containing the imu data.    
    
    """

    for session in log_imu["session"].unique():
        df_temp = log_imu[log_imu["session"]==session].copy()
        df_temp["time"] = pd.to_datetime(df_temp["time"], format="mixed")
        df_temp["acc_total"] = (df_temp["acc_x"]**2 + df_temp["acc_y"]**2 + df_temp["acc_z"]**2)**0.5

        # plot accelerometer data
        df_temp.plot(x="time",y=["acc_x","acc_y","acc_z","acc_total"], title=f"Accelerometer Session:{session}")
        # plot gyroscope data
        df_temp.plot(x="time",y=["gyro_x","gyro_y","gyro_z"], title=f"Gyroscope Session:{session}")

def plot_magnetometer_values(log_mag):
    """Plot magnetometer data.
    
    Parameters
    ----------
    log_mag : pd.DataFrame
        Dataframe containing the magnetometer data.    
    
    """

    fig = plt.figure()
    ax = fig.add_subplot(111, projection='3d')
    log_mag = log_mag.dropna()
    log_mag = log_mag.reset_index(drop=True)

    # Plot 3D data
    ax.scatter(log_mag["mag_x"], log_mag["mag_y"], log_mag["mag_z"])
    ax.set_xlabel("mag_x")
    ax.set_ylabel("mag_y")
    ax.set_zlabel("mag_z")
    
    plt.title("Magnetometer Data in 3D Should Be Approximately Ellipsoidal")

def check_imu_zeros(logger_imu, metrics):
    if logger_imu is None or len(logger_imu) == 0:
        return metrics

    for key in ["acc_x","acc_y","acc_z","gyro_x","gyro_y","gyro_z"]:
        if len(logger_imu) > 0:
            zero_count_percent = np.round((logger_imu[key].value_counts().get(0.0,0)/float(len(logger_imu)))*100.,2)
        else:
            zero_count_percent = "imu database with zero length"

        metrics["imu"][f"percent rows with zero value {key}"] = zero_count_percent

    return metrics

def check_mag_zeros(logger_mag, metrics):
    if logger_mag is None or len(logger_mag) == 0:
        return metrics

    for key in ["mag_x","mag_y","mag_z"]:
        if len(logger_mag) > 0:
            zero_count_percent = np.round((logger_mag[key].value_counts().get(0.0,0)/float(len(logger_mag)))*100.,2)
        else:
            zero_count_percent = "imu database with zero length"

        metrics["mag"][f"percent rows with zero value {key}"] = zero_count_percent

    return metrics

def print_metrics(metrics):

    print("----------------")
    print("IMU Metrics")
    print("----------------")
    for k, v in metrics["imu"].items():
        print(k,": ",v)

    print("------------")
    print("MAG Metrics")
    print("------------")
    for k, v in metrics["mag"].items():
        print(k,": ",v)

    print("\n"*2)


if __name__ == "__main__":
    main()
