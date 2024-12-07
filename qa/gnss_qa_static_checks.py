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
import matplotlib.pyplot as plt


def main():

    # database path
    # DB_PATH = "/home/<PATH TO DATABASE FILE>/redis_handler-v0-0-3.db"
    DB_PATH = "/home/derek/bee-sensors/data/redis_handler-v0-0-3.db"

    # Latitude (deg), Longitude (deg), Altitude (m) of test location
    TEST_LOCATION = (37.78804585139535, -122.39925359640425, 150.)

    logs, metrics = parse_database(DB_PATH)

    # plot_gnss_map(logs["gnss"])
    # plot_nav_pvt_map(logs["nav_pvt"])

    # time-to-first-fix
    metrics = time_to_first_fix(logs["nav_status"],metrics)

    # valid fix position error
    metrics = check_valid_fix_position_error(logs["nav_pvt"], metrics, TEST_LOCATION)

    # check cn0 and sats seen
    metrics = check_cn0_and_sats_seen(logs["gnss"], metrics)
    
    
    print_metrics(metrics)
    qa_static_checks(metrics)



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
            "nav_status" : [],
            }
    metrics = {
            "gnss" : {},
            "nav_status" : {},
            "nav_pvt" : {},
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
        logs["nav_pvt"] = df
    except Exception as e:
        print(f"nav_pvt db error: {e}")
        logs["nav_pvt"] = None

    # add nav_status
    try:
        df = pd.read_sql_query("SELECT * FROM nav_status", conn)
        logs["nav_status"] = df
    except Exception as e:
        print(f"nav_status db error: {e}")
        logs["nav_status"] = None

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
        df_temp = df_temp[df_temp["latitude"] != 0.0]
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

def time_to_first_fix(logger_status,metrics):
    if logger_status is None or len(logger_status) == 0:
        return metrics

    #Identify time difference between 
    logger_status['boot_id'] = (logger_status['msss'].diff() < 0).cumsum()
    first_fixes = logger_status[logger_status['ttff'] > 0].groupby('boot_id').first()
    all_boots = logger_status['boot_id'].unique()
    results = pd.DataFrame(index=all_boots, columns=['first_fix_time', 'start_time'], dtype=float)
    results['start_time'] = logger_status.groupby('boot_id').first()['msss']
    results.loc[first_fixes.index, 'first_fix_time'] = first_fixes['ttff']
    results['time_difference'] = (results['first_fix_time'] - results['start_time'])/1000.
    # results['time_difference'] = results['time_difference'].fillna("No fix")

    metrics["nav_status"]["time-to-first-fixes (secs)"] = list(results['first_fix_time']/1000.)
    metrics["nav_status"]["time-to-first-fix Logging Start (secs)"] = list((results['time_difference']))

    return metrics

def check_valid_fix_position_error(nav_pvt, metrics, test_location, plot=False):
    """Checks for valid fix and calculates error.

    """
    if nav_pvt is None:
        return

    df_temp = nav_pvt[["lat_deg","lon_deg","height_m","gnss_fix_ok","fully_resolved"]]
    df_temp = df_temp[df_temp["gnss_fix_ok"]==1]
    df_temp = df_temp[df_temp["fully_resolved"]==1]

    true_ecef = glp.geodetic_to_ecef(np.array([[test_location[0]],[test_location[1]],[test_location[2]]]))
    test_ecef = glp.geodetic_to_ecef(np.array([df_temp["lat_deg"].values,df_temp["lon_deg"].values,df_temp["height_m"].values]))

    error = np.linalg.norm(test_ecef - true_ecef,axis=0)
    
    if plot:
        fig, ax = plt.subplots()
        ax.hist(error, bins=100)
        ax.set_xlabel("Error (m)")
        ax.set_ylabel("Frequency")
        plt.show()

    metrics["nav_pvt"]["Valid Fix Position Error (m)"] = {
            "mean" : np.mean(error),
            "std" : np.std(error),
            "max" : np.max(error),
            "min" : np.min(error),
            "median" : np.median(error),
            "95_percentile" : np.percentile(error,95),
            }

    return metrics

def check_cn0_and_sats_seen(logger_gnss, metrics):
    """ Check for cn0 value and satellites seen

    """

    # get cn0 >= 0
    cn0 = logger_gnss["cno"].to_numpy()
    cn0 = cn0[cn0 != 0.]
    metrics["gnss"]["avg CN0"] = {
            "mean" : np.mean(cn0),
            "std" : np.std(cn0),
            "max" : np.max(cn0),
            "min" : np.min(cn0),
            "median" : np.median(cn0),
            "5_percentile" : np.percentile(cn0,5),
            }

    # get cn0 >= 0
    df_temp = logger_gnss[logger_gnss["cno"] > 0]

    # get metrics for number of satellites seen
    metrics["gnss"]["satellites seen"] = {
            "mean" : np.mean(df_temp["satellites_seen"]),
            "std" : np.std(df_temp["satellites_seen"]),
            "max" : np.max(df_temp["satellites_seen"]),
            "min" : np.min(df_temp["satellites_seen"]),
            "median" : np.median(df_temp["satellites_seen"]),
            "5_percentile" : np.percentile(df_temp["satellites_seen"],5),
            }
    
    # get metrics for number of satellites used
    metrics["gnss"]["satellites used"] = {
            "mean" : np.mean(df_temp["satellites_used"]),
            "std" : np.std(df_temp["satellites_used"]),
            "max" : np.max(df_temp["satellites_used"]),
            "min" : np.min(df_temp["satellites_used"]),
            "median" : np.median(df_temp["satellites_used"]),
            "5_percentile" : np.percentile(df_temp["satellites_used"],5),
            }

    return metrics


def print_metrics(metrics):

    for sensor_type in metrics.keys():
        print("----------------")
        print(sensor_type.upper() + " Metrics")
        print("----------------")
        for k, v in metrics[sensor_type].items():
            if type(v) == dict:
                print(k,":")
                for v_k, v_v in v.items():
                    print("     ",v_k,":",v_v)
            else:
                print(k,": ",v)

        print("\n")

def qa_static_checks(metrics):
    """Run quality assurance checks on the GNSS data.

    """

    # check there's at least three nonzero time-to-first-fix values
    nonzero_ttff = len([x for x in metrics["nav_status"]["time-to-first-fixes (secs)"] if x > 0])
    if nonzero_ttff >= 3:
        print("[PASS] At least three nonzero time-to-first-fix values.")
    else:
        print("[FAIL] Less than three nonzero time-to-first-fix values.")

    # check that the final two ttff that aren't NaN are less than 60 seconds
    non_nan_ttff = [x for x in metrics["nav_status"]["time-to-first-fixes (secs)"] if not np.isnan(x)]
    if non_nan_ttff[-1] < 60. and non_nan_ttff[-2] < 60.:
        print("[PASS] Final two time-to-first-fix values are less than 60 seconds.")
    else:
        print("[FAIL] Final two time-to-first-fix values are greater than 60 seconds.")

    # check valid fix position error
    if metrics["nav_pvt"]["Valid Fix Position Error (m)"]["95_percentile"] < 50.:
        print("[PASS] Valid Fix Position Error (m) 95% percentile < 50m")
    else:
        print("[FAIL] Valid Fix Position Error (m) 95% percentile >= 50m")

    # check CN0 5th percentile is >= 20
    if metrics["gnss"]["avg CN0"]["5_percentile"] >= 20.:
        print("[PASS] CN0 5% percentile >= 20")
    else:
        print("[FAIL] CN0 5% percentile < 20")

    # check satellites seen 5th percentile is >= 15
    if metrics["gnss"]["satellites seen"]["5_percentile"] >= 15.:
        print("[PASS] Satellites seen 5% percentile >= 15")
    else:
        print("[FAIL] Satellites seen 5% percentile < 15")

    # check satellites used 5th percentile is >= 5
    if metrics["gnss"]["satellites used"]["5_percentile"] >= 5.:
        print("[PASS] Satellites used 5% percentile >= 5")
    else:
        print("[FAIL] Satellites used 5% percentile < 5")


if __name__ == "__main__":
    main()
