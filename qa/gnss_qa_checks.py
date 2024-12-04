"""Check accelerometer, gyroscope, and magnetometer data for quality assurance.

"""

__author__ = "D. Knowles"
__date__ = "04 Dec 2024"


import os
import subprocess

import sqlite3
import numpy as np
import pandas as pd
import gnss_lib_py as glp

def main():

    # database path
    # DB_PATH = "/home/<PATH TO DATABASE FILE>/redis_handler-v0-0-3.db"
    DB_PATH = "/home/derek/Downloads/test/redis_handler-v0-0-3.db"

    logs, metrics = parse_database(DB_PATH)

    plot_gnss_map(logs["gnss"])
    plot_nav_pvt_map(logs["nav_pvt"])
    plot_landmark_map(logs)
    print_metrics(metrics)

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
            "gnss"  : [],
            "nav_pvt"  : [],
            "drivepath"  : [],
            "landmarks"  : [],
            }
    metrics = {
            "gnss" : {},
            "landmarks" : {},
            }

    if not os.path.isfile(db_path):
      print(f"no such file {db_path} found.")
      return None

    # Connect to the database
    conn = sqlite3.connect(db_path)

    # add imu data
    try:
        df = pd.read_sql_query("SELECT * FROM gnss", conn)
        logs["gnss"] = df
        metrics["gnss"]["% rows w/o GNSS lock"] = np.round(100.0 * float(len(df[df["latitude"] == 0.0])) / len(df),2)
        df = df[df["latitude"] != 0.0]
        gnss_sessions = df["session"].unique()
        
    except Exception as e:
        print(f"gnss db error: {e}")
        logger_recovered_path = db_path[:-3] + "_recovered.db"
        if not os.path.isfile(logger_recovered_path):
            # recover file
            print("attempting recovery")
            recover_sqlite_db(db_path,logger_recovered_path)
        else:
            print("using previously recovered db file")
        conn = sqlite3.connect(logger_recovered_path)

        try:
            df = pd.read_sql_query("SELECT * FROM gnss", conn)
            logs["gnss"] = df
            metrics["gnss"]["% rows w/o GNSS lock"] = np.round(100.0 * float(len(df[df["latitude"] == 0.0])) / len(df),2)
            df = df[df["latitude"] != 0.0]
            gnss_sessions = df["session"].unique()
        except Exception as e:
            print(f"gnss db error: {e}")
            logs["gnss"] = None

    # add nav_pvt
    try:
        df = pd.read_sql_query("SELECT * FROM nav_pvt", conn)
        df = df[df["session"].isin(gnss_sessions)]
        logs["nav_pvt"] = df
    except Exception as e:
        print(f"nav_pvt db error: {e}")
        logs["nav_pvt"] = None


    # add drive path
    try:
        df = pd.read_sql_query("SELECT latitude,longitude FROM packed_framekms", conn)
        logs["drivepath"] = df
    except Exception as e:
      print(f"drivepath db error: {e}")
      logs["drivepath"] = None

    # add landmarks
    try:
      df = pd.read_sql_query("SELECT * FROM map_features", conn)
      logs["landmarks"] = df

      metrics["landmarks"]["unique landmarks : "] = len(df)
      metrics["landmarks"]["landmark counts  : "] = df['class_label'].value_counts().to_dict()
    except Exception as e:
      print(f"landmarks db error: {e}")
      logs["landmarks"] = None

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

def plot_gnss_map(logger_gnss):
    if logger_gnss is None or len(logger_gnss) == 0:
      return
    gnss_sessions = []
    for session in logger_gnss["session"].unique():
      df_temp = logger_gnss
      df_temp = df_temp[df_temp["session"]==session][["latitude","longitude","altitude"]]

      temp = glp.NavData(pandas_df=df_temp)

      if len([ch for ch in session if ch.isalpha()]) == 1 and "e" in session:
        session += "*"

      temp.rename({"latitude":"lat_" + session + "_deg",
                  "longitude":"lon_" + session + "_deg",
                  "altitude":"alt_" + session + "_m",
                  }, inplace=True)

      if len(temp) == 0:
        continue
      gnss_sessions.append(temp)

    if len(gnss_sessions) > 0:
      fig = glp.plot_map(*gnss_sessions)
      fig.show()

def plot_nav_pvt_map(logger_gnss):
    if logger_gnss is None:
      return
    gnss_sessions = []
    for session in logger_gnss["session"].unique():
      df_temp = logger_gnss

      df_temp = df_temp[df_temp["session"]==session][["lat_deg","lon_deg","gnss_fix_ok","fully_resolved"]]

      temp = glp.NavData(pandas_df=df_temp)

      if len([ch for ch in session if ch.isalpha()]) == 1 and "e" in session:
        session += "*"

      ok = temp.rename({"lat_deg":"lat_" + session + "_gnss_valid_deg",
                  "lon_deg":"lon_" + session + "_gnss_valid_deg",
                  }, inplace=False)
      ok = ok.where("gnss_fix_ok",1).where("fully_resolved",1)

      if len(ok) > 0:
        gnss_sessions.append(ok)

      not_ok = temp.rename({"lat_deg":"lat_" + session + "_gnss_invalid_deg",
                  "lon_deg":"lon_" + session + "_gnss_invalid_deg",
                  }, inplace=False)
      not_ok = glp.concat(not_ok.where("gnss_fix_ok",0).where("fully_resolved",0),
                          not_ok.where("gnss_fix_ok",1).where("fully_resolved",0),
                          not_ok.where("gnss_fix_ok",0).where("fully_resolved",1),
                          axis=1)
      if len(not_ok) > 0:
        gnss_sessions.append(not_ok)


    if len(gnss_sessions) > 0:
      fig = glp.plot_map(*gnss_sessions)
        # , mapbox_style="carto-positron")
      fig.show()

def plot_landmark_map(logs):

    figs = {}
    logger_gnss = logs["gnss"]
    logger_landmarks = logs["landmarks"]
    logger_drivepath = logs["drivepath"]

    if logger_gnss is None or logger_landmarks is None or logger_drivepath is None:
        return
    if len(logger_gnss) == 0 or len(logger_landmarks) == 0 or len(logger_drivepath) == 0:
        return

    map_data = []

    # add GNSS
    df_temp = logger_gnss[(logger_gnss["latitude"] >= 37.0) \
                        & (logger_gnss["latitude"] <= 37.9) \
                        & (logger_gnss["longitude"] >= -122.6) \
                        & (logger_gnss["longitude"] <= -121.6)]
    df_temp = df_temp[["latitude","longitude"]]
    temp = glp.NavData(pandas_df=df_temp)
    temp.rename({"latitude":"lat_" + "full_gnss" + "_deg",
                "longitude":"lon_" + "full_gnss" + "_deg",
                }, inplace=True)
    map_data.append(temp)

    # add drive path
    df_temp = logger_drivepath[["latitude","longitude"]]
    temp = glp.NavData(pandas_df=df_temp)
    temp.rename({"latitude":"lat_" + "drive_path" + "_deg",
                "longitude":"lon_" + "drive_path" + "_deg",
                }, inplace=True)
    map_data.append(temp)

    # add landmarks
    df_temp = logger_landmarks[["lat","lon","class_label"]]
    navdata_temp = glp.NavData(pandas_df=df_temp)
    for label in np.unique(navdata_temp["class_label"]):
        temp = navdata_temp.where("class_label",label)
        temp.rename({"lat":"lat_" + label + "_deg",
                    "lon":"lon_" + label + "_deg",
                    }, inplace=True)
        map_data.append(temp)

    fig = glp.plot_map(*map_data)
    fig.show()

    return


def print_metrics(metrics):

    for sensor_type in metrics.keys():
        print("----------------")
        print(sensor_type + " Metrics")
        print("----------------")
        for k, v in metrics[sensor_type].items():
            if type(v) == dict:
                print(k,":")
                for v_k, v_v in v.items():
                    print("     ",v_k,":",v_v)
            else:
                print(k,": ",v)

        print("\n"*2)


if __name__ == "__main__":
    main()
