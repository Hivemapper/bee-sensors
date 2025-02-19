""" Plot replayed data nicely.

# Source for fill between:
https://github.com/betaBison/aa273/blob/main/hw7/p2_ekf_slam.py

"""

import os
import warnings
import subprocess

import sqlite3
import numpy as np
import pandas as pd
import gnss_lib_py as glp
import plotly.graph_objects as go
import matplotlib.pyplot as plt

warnings.simplefilter(action='ignore', category=pd.errors.SettingWithCopyWarning)

DB_DIRECTORY_PATH = "/data/recording/redis_handler/"
SENSORS_DB_NAME = "sensors"
FUSION_DB_NAME = "fusion"
ODC_API_DB_NAME = "odc-api"
STATES = [
          ["ecef_x_m",   "pos_cov_x_x","q00","r00"],
          ["ecef_y_m",   "pos_cov_y_y","q11","r11"],
          ["ecef_z_m",   "pos_cov_z_z","q22","r22"],
          ["ecef_vx_m_s","vel_cov_x_x","q33","r33"],
          ["ecef_vy_m_s","vel_cov_y_y","q44","r44"],
          ["ecef_vz_m_s","vel_cov_z_z","q55","r55"],
        #   ["course_deg","","",""],
          ]


def compute_results():
    comparisons = [
        "",
                   ]
    logs, _, _ = parse_database(comparisons)


    plot_fusion_map(logs["fusion_filtered"],comparisons)

    # plot_states_with_covariance(logs["fusion_filtered"],comparisons)

    # plot_acc_gyro_values(logs,comparisons)

    plt.show()


"""Parse Databases"""

def recover_sqlite_db(corrupt_db_path, recovered_db_path):
    """
    Recovers a corrupt SQLite database using the .recover command.
    """

    try:
        subprocess.run(f"sqlite3 {corrupt_db_path} .recover | sqlite3 {recovered_db_path}", shell=True, check=True)
        print("Database recovered successfully.")
    except subprocess.CalledProcessError as e:
        print("Error during recovery:", e)

def parse_database(date_dirs):

    logs = {
            "gnss" : [],
            "nav_pvt" : [],
            "drivepath" : [],
            "landmarks" : [],
            "nav_status" : [],
            "fusion_filtered" : [],
            "fusion_gnss" : [],
            "sensors_gnss" : [],
            "fusion_gnss" : [],
            "logger_gnss" : [],
            "sensors_nav_pvt" : [],
            "odc_packed_fmkms" : [],
            "landmarks"  : [],
            "fusion_filtered" : [],
            "fusion_gnss_concise" : [],
            "sensors_nav_posecef" : [],
            "sensors_imu" : [],
            "fusion_imu" : [],
            }
    metrics = {
            "gnss" : {},
            "nav_pvt" : {},
            "drivepath" : {},
            "landmarks" : {},
            "nav_status" : {},
            "fusion_filtered" : {},
            "sensors_gnss" : {},
            "fusion_gnss"  : {},
            "logger_gnss"  : {},
            "sensors_nav_pvt"  : {},
            "odc_packed_fmkms"  : {},
            "landmarks"  : {},
            "fusion_filtered" : {},
            "fusion_gnss_concise" : {},
            "sensors_imu" : {},
            "fusion_imu" : {},
            }

    for date_dir in sorted(date_dirs):
        print(f"analyzing {date_dir}")

        sensors_file = [x for x in os.listdir(os.path.join(DB_DIRECTORY_PATH,date_dir)) if ((x[-3:] == ".db") and (SENSORS_DB_NAME in x))][0]
        sensors_path = os.path.join(DB_DIRECTORY_PATH,date_dir,sensors_file)

        fusion_file = [x for x in os.listdir(os.path.join(DB_DIRECTORY_PATH,date_dir)) if ((x[-3:] == ".db") and (FUSION_DB_NAME in x))][0]
        fusion_path = os.path.join(DB_DIRECTORY_PATH,date_dir,fusion_file)

        metrics["gnss"][date_dir] = {}
        metrics["nav_pvt"][date_dir] = {}
        metrics["landmarks"][date_dir] = {}
        metrics["drivepath"][date_dir] = {}
        metrics["nav_status"][date_dir] = {}
        metrics["fusion_filtered"][date_dir] = {}
        metrics["sensors_gnss"][date_dir] = {}
        metrics["fusion_gnss"][date_dir] = {}
        metrics["logger_gnss"][date_dir] = {}
        metrics["sensors_nav_pvt"][date_dir] = {}
        metrics["odc_packed_fmkms"][date_dir] = {}
        metrics["landmarks"][date_dir] = {}
        metrics["fusion_filtered"][date_dir] = {}
        metrics["fusion_gnss_concise"][date_dir] = {}
        metrics["sensors_imu"][date_dir] = {}
        metrics["fusion_imu"][date_dir] = {}

    # Connect to the database
        conn = sqlite3.connect(fusion_path)
        print(fusion_path)
        # add gnss
        df = pd.read_sql_query("SELECT * FROM gnss", conn)
        if len(df) == 0:
            logs["fusion_gnss"].append(pd.DataFrame())
        else:
            metrics["fusion_gnss"][date_dir]["% rows w/o GNSS lock"] = np.round(100.0 * float(len(df[df["latitude"] == 0.0])) / len(df),2)
        if len(df) == 0:
            logs["fusion_gnss"].append(pd.DataFrame())
        else:
            outside_bay_count = len(df[(df["latitude"] < 37.0) | (df["latitude"] > 37.9) | (df["longitude"] < -122.6) | (df["longitude"] > -121.6)])
            metrics["fusion_gnss"][date_dir]["% rows w/ fix outside of Bay"] = np.round((float(outside_bay_count)/len(df)) * 100,2)
            gnss_sessions = df["session"].unique()
            logs["fusion_gnss"].append(df)
        # add fusion_filtered
        try:
            df = pd.read_sql_query("SELECT * FROM filtered", conn)
            logs["fusion_filtered"].append(df)
        except Exception as e:
            print(f"fusion_filtered db error: {e}")
            logs["fusion_filtered"].append(None)
        # add filtered
        try:
            df = pd.read_sql_query("SELECT * FROM gnss_concise", conn)
            logs["fusion_gnss_concise"].append(df)
        except Exception as e:
            print(f"fusion_gnss_concise db error: {e}")
            logs["fusion_gnss_concise"].append(None)
        try:
            df = pd.read_sql_query("SELECT * FROM imu", conn)
            logs["fusion_imu"].append(df)
        except Exception as e:
            print(f"fusion_imu db error: {e}")
            logs["fusion_imu"].append(None)

        # Close the connection
        conn.close()

        # Connect to the database
        conn = sqlite3.connect(sensors_path)
        # add nav_pvt
        try:
            df = pd.read_sql_query("SELECT * FROM nav_pvt", conn)
            df = df[df["session"].isin(gnss_sessions)]
            logs["nav_pvt"].append(df)
        except Exception as e:
            print(f"nav_pvt db error: {e}")
            logs["nav_pvt"].append(None)

        # add nav_status
        try:
            df = pd.read_sql_query("SELECT * FROM nav_status", conn)
            df = df[df["session"].isin(gnss_sessions)]
            logs["nav_status"].append(df)
        except Exception as e:
            print(f"nav_status db error: {e}")
            logs["nav_status"].append(None)
        # add imu
        try:
            df = pd.read_sql_query("SELECT * FROM imu", conn)
            df = df[df["session"].isin(gnss_sessions)]
            logs["sensors_imu"].append(df)
        except Exception as e:
            print(f"sensors_imu db error: {e}")
            logs["sensors_imu"].append(None)
        conn.close()

    return logs, metrics, date_dirs

def generate_ellipses(df, type="cov", num_points=100, sigma=3):
    """
    Vectorized function to generate covariance ellipses for multiple points in a DataFrame.

    Parameters:
        df (pd.DataFrame): DataFrame with columns ['lat', 'lon', 'cov_11', 'cov_12', 'cov_22']
        num_points (int): Number of points for the ellipse perimeter

    Returns:
        dict: Mapping (lat, lon) -> (lat_ellipse, lon_ellipse)
    """
    earth_radius = 6378137  # Radius of Earth in meters

    if type == "cov":
        # Create covariance matrices
        cov_matrices = np.array([
            [[row.pos_cov_n_n, row.pos_cov_n_e],
             [row.pos_cov_n_e, row.pos_cov_e_e]]
            for _, row in df.iterrows()
        ])
    elif type == "Q":
        # Create covariance matrices
        cov_matrices = np.array([
            [[row.q_cov_n_n, row.q_cov_n_e],
             [row.q_cov_n_e, row.q_cov_e_e]]
            for _, row in df.iterrows()
        ])
    elif type == "R":
        # Create covariance matrices
        cov_matrices = np.array([
            [[row.r_cov_n_n, row.r_cov_n_e],
             [row.r_cov_n_e, row.r_cov_e_e]]
            for _, row in df.iterrows()
        ])

    # Eigen decomposition
    eigenvalues, eigenvectors = np.linalg.eigh(cov_matrices)

    # Scale by sigma (3-sigma for 99.7% confidence)
    scaled_eigenvalues = np.sqrt(eigenvalues) * sigma

    # Generate unit circle points
    theta = np.linspace(0, 2 * np.pi, num_points)
    unit_circle = np.array([np.cos(theta), np.sin(theta)])  # Shape: (2, num_points)
    scaled_unit_circle = (scaled_eigenvalues[..., None] * unit_circle[None, :, :])  # (45875, 2, 100)
    ellipses = np.einsum('...ij,...jk->...ik', eigenvectors, scaled_unit_circle)  # CORRECT


    # Convert north/east displacements to lat/lon displacements
    dlat = ellipses[:, 0, :] / earth_radius * (180 / np.pi)
    dlon = ellipses[:, 1, :] / (earth_radius * np.cos(np.radians(df['lat_deg'].values[:, None]))) * (180 / np.pi)

    # Compute final lat/lon for each ellipse
    lat_ellipses = df['lat_deg'].values[:, None] + dlat
    lon_ellipses = df['lon_deg'].values[:, None] + dlon

    # Convert to lists and add `None` separator for fast plotting
    lat_list = []
    lon_list = []
    for i in range(len(df)):
        lat_list.extend(lat_ellipses[i].tolist() + [None])  # Add `None` to separate ellipses
        lon_list.extend(lon_ellipses[i].tolist() + [None])

    return lat_list, lon_list


def plot_fusion_map(logger_drivepaths, comparisons):
    gnss_sessions = []
    cov_ellipses = []
    r_ellipses = []
    q_ellipses = []
    for ii,logger_drivepath in enumerate(logger_drivepaths):
        if logger_drivepath is None or len(logger_drivepath) == 0:
            continue
        
        print("fusion map",logger_drivepath.shape)
        df_temp = logger_drivepath[logger_drivepath["lat_deg"]!= 0.0]
        df_temp = logger_drivepath[(logger_drivepaths[ii]["lat_deg"] >= 37.0) \
                                & (logger_drivepaths[ii]["lat_deg"] <= 37.9) \
                                & (logger_drivepaths[ii]["lon_deg"] >= -122.6) \
                                & (logger_drivepaths[ii]["lon_deg"] <= -121.6)]
        df_temp = df_temp[["lat_deg","lon_deg",
                           "pos_cov_n_n","pos_cov_n_e","pos_cov_e_e",
                           "q_cov_n_n","q_cov_n_e","q_cov_e_e",
                            "r_cov_n_n","r_cov_n_e","r_cov_e_e",
                           ]]

        print(df_temp.shape)
        temp = glp.NavData(pandas_df=df_temp)
        if len(temp) == 0:
            continue

        temp.rename({"lat_deg":f"lat_fusion_{comparisons[ii]}_deg",
                     "lon_deg":f"lon_fusion_{comparisons[ii]}_deg",
                    }, inplace=True)

        gnss_sessions.append(temp)

        lat_ellipses, lon_ellipses = generate_ellipses(df_temp.iloc[::10], type="cov", sigma=2)
        cov_ellipses.append([lat_ellipses, lon_ellipses])
        lat_ellipses, lon_ellipses = generate_ellipses(df_temp.iloc[::10], type="R", sigma=2)
        r_ellipses.append([lat_ellipses, lon_ellipses])
        lat_ellipses, lon_ellipses = generate_ellipses(df_temp.iloc[::10], type="Q", sigma=2)
        q_ellipses.append([lat_ellipses, lon_ellipses])

    if len(gnss_sessions) > 0:
        print("FUSION MAP")
        fig = glp.plot_map(*gnss_sessions)

        for jj, (lat_ellipses, lon_ellipses) in enumerate(cov_ellipses):
            fig.add_trace(go.Scattermapbox(
                mode="lines",
                lon=lon_ellipses,
                lat=lat_ellipses,
                line=dict(color=glp.style.STANFORD_COLORS[(jj+len(cov_ellipses)) % 13]), # Use Matplotlib "C0" color
                name=f"{comparisons[jj]} 2-sigma EKF Covariance",
                ),  
            )
            fig.add_trace(go.Scattermapbox(
                mode="lines",
                lon=r_ellipses[jj][1],
                lat=r_ellipses[jj][0],
                line=dict(color=glp.style.STANFORD_COLORS[(jj+2*len(cov_ellipses)) % 13]),
                name=f"{comparisons[jj]} 2-sigma R Covariance",
                ),  
            )
            fig.add_trace(go.Scattermapbox(
                mode="lines",
                lon=q_ellipses[jj][1],
                lat=q_ellipses[jj][0],
                line=dict(color=glp.style.STANFORD_COLORS[(jj+3*len(cov_ellipses)) % 13]),
                name=f"{comparisons[jj]} 2-sigma Q Covariance",
                ),  
            )

        fig.update_layout(
            autosize=False,
            width=1800,
            height=1000,
        )
        fig.show()

def plot_states_with_covariance(filtered_loggers, comparisons):
    for ii,filtered_logger in enumerate(filtered_loggers):
        if filtered_loggers is None or len(filtered_loggers) == 0:
            continue
        filtered_logger["time"] = pd.to_datetime(filtered_logger["system_time"], format="mixed")
        filtered_logger = filtered_logger[filtered_logger["pos_cov_x_x"] != 1000.0]

        fig = plt.figure(figsize=(12, 8))
        for ss, state in enumerate(STATES):
            ax = fig.add_subplot(3, 2, ss+1)
            ax.plot(filtered_logger["time"], filtered_logger[state[0]], label=state[0], marker="None")
            ax.fill_between(filtered_logger["time"],
                            filtered_logger[state[0]] - 2.*np.sqrt(filtered_logger[state[3]]),
                            filtered_logger[state[0]] + 2.*np.sqrt(filtered_logger[state[3]]),
                            alpha=0.5, label="R 2-$\sigma$ Covariance")
            ax.fill_between(filtered_logger["time"],
                            filtered_logger[state[0]] - 2.*np.sqrt(filtered_logger[state[1]]),
                            filtered_logger[state[0]] + 2.*np.sqrt(filtered_logger[state[1]]),
                            alpha=0.5,label="P 2-$\sigma$ Covariance")#, color="C"+str(ss))
            ax.fill_between(filtered_logger["time"],
                            filtered_logger[state[0]] - 2.*np.sqrt(filtered_logger[state[2]]),
                            filtered_logger[state[0]] + 2.*np.sqrt(filtered_logger[state[2]]),
                            alpha=0.5, label="Q 2-$\sigma$ Covariance")
            ax.set_title(state[0])
            plt.legend()
        plt.suptitle(f"States with Covariance {comparisons[ii]}")
        plt.tight_layout()

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


def plot_acc_gyro_values(logs, date_dirs):
  for ii,logger in enumerate(logs["sensors_imu"]):
    if logger is None:
      continue

    for session in logger["session"].unique():
      df_temp = logger[logger["session"]==session]
      df_temp["time"] = pd.to_datetime(df_temp["time"], format="mixed")
      df_temp["acc_total"] = (df_temp["acc_x"]**2 + df_temp["acc_y"]**2 + df_temp["acc_z"]**2)**0.5

      df_fusion = logs["fusion_imu"][ii]
      df_fusion = df_fusion[df_fusion["session"] == session]
      df_fusion["time"] = pd.to_datetime(df_fusion["time"], format="mixed")
      df_fusion["acc_total"] = (df_fusion["acc_x"]**2 + df_fusion["acc_y"]**2 + df_fusion["acc_z"]**2)**0.5

      plt.figure()
      axis_ys = ["acc_z","acc_y","acc_x"]

      plt.plot(df_temp["time"], df_temp["acc_x"], label="unfiltered x")
      plt.plot(df_temp["time"], df_temp["acc_y"], label="unfiltered y")
      plt.plot(df_temp["time"], df_temp["acc_z"], label="unfiltered z")
      plt.plot(df_temp["time"], df_temp["acc_total"], label="unfiltered total")
      plt.plot(df_fusion["time"], df_fusion["acc_x"], label="filtered x")
      plt.plot(df_fusion["time"], df_fusion["acc_y"], label="filtered y")
      plt.plot(df_fusion["time"], df_fusion["acc_z"], label="filtered z")
      plt.plot(df_fusion["time"], df_fusion["acc_total"], label="filtered total")
      plt.legend()
      plt.title(f"{date_dirs[ii]} {session}")

      HARSH_BRAKING_THRESHOLD = 0.72
      AGGRESSIVE_ACCEL_THRESHOLD = 0.51
      SWERVING_THRESHOLD = 0.5
    
      # harsh braking figures.
      def min_previous_400(series):
        return series.rolling(window=400, min_periods=1).min()
      def max_previous_400(series):
        return series.rolling(window=400, min_periods=1).max()
      
      df_fusion["braking_threshold"] = min_previous_400(df_fusion["acc_x"] + HARSH_BRAKING_THRESHOLD)
      df_fusion["accel_threshold"] = max_previous_400(df_fusion["acc_x"] - AGGRESSIVE_ACCEL_THRESHOLD)

      plt.figure()
      plt.plot(df_temp["time"], df_temp["acc_x"], label="unfiltered x")
      plt.plot(df_temp["time"], df_temp["acc_total"], label="unfiltered total")
      plt.plot(df_fusion["time"], df_fusion["acc_x"], label="filtered x")
      plt.plot(df_fusion["time"], df_fusion["acc_total"], label="filtered total")
      plt.plot(df_fusion["time"], df_fusion["braking_threshold"], label="harsh braking threshold x",
               color='red', linestyle='--', marker='None')
      plt.plot(df_fusion["time"], df_fusion["accel_threshold"], label="sudden accel threshold x",
               color='blue', linestyle='--', marker='None')
      plt.axhline(y=(np.sqrt(1 + HARSH_BRAKING_THRESHOLD**2)), color='red', linestyle='--', label='harsh braking threshold total')
      plt.axhline(y=(np.sqrt(1 + AGGRESSIVE_ACCEL_THRESHOLD**2)), color='blue', linestyle='--', label='sudden accel threshold total')
      plt.legend()
      plt.title(f"Harsh braking {date_dirs[ii]} {session}")

      # swerving figures.
      def avg_previous_6000(series):
        return series.rolling(window=6000, min_periods=1).mean()
      
      df_fusion["steady_state"] = avg_previous_6000(df_fusion["acc_y"])
      df_fusion["swerving_threshold"] = df_fusion["steady_state"] + SWERVING_THRESHOLD
      df_fusion["end_swerving"] = df_fusion["steady_state"] + 0.25*SWERVING_THRESHOLD
      
      plt.figure()
      plt.plot(df_temp["time"], df_temp["acc_y"], label="unfiltered y")
      plt.plot(df_temp["time"], df_temp["acc_total"], label="unfiltered total")
      plt.plot(df_fusion["time"], df_fusion["acc_y"], label="filtered y")
      plt.plot(df_fusion["time"], np.abs(df_fusion["acc_y"] - df_fusion["steady_state"]), label="filtered y diff")
      plt.plot(df_fusion["time"], df_fusion["acc_total"], label="filtered total")
      plt.plot(df_fusion["time"], df_fusion["steady_state"], label="steady state y",
               color='red', linestyle='--', marker='None')
      plt.plot(df_fusion["time"], df_fusion["swerving_threshold"], label="swerving threshold y",
               color='blue', linestyle='--', marker='None')
      plt.plot(df_fusion["time"], df_fusion["end_swerving"], label="end swerving threshold y",
               color='green', linestyle='--', marker='None')
      plt.legend()
      plt.title(f"Swerving {date_dirs[ii]} {session}")

      # ys = []
      # if "acc_norm" in df_temp.columns:
      #   ys.append("acc_norm")
      # if "acc_norm_filtered" in df_temp.columns:
      #   ys.append("acc_norm_filtered")
      # if "acc_norm_filtered_colab" in df_temp.columns:
      #   ys.append("acc_norm_filtered_colab")
      # if len(ys) > 0:
      #   df_temp.plot(x="time",y=ys)
      #   plt.title(f"Accel Magnitude {date_dirs[ii]} {session}")

    #   plt.figure()
    #   df_temp.plot(x="time",y=["gyro_x","gyro_y","gyro_z"])
      plt.title(f"{date_dirs[ii]} {session}")


      # df_temp["gyro_norm"] = np.sqrt(df_temp['gyro_x']**2 + df_temp['gyro_y']**2 + df_temp['gyro_z']**2)
      # df_temp.plot(x="time",y=["gyro_norm"])
      # plt.title(f"Gyro Magnitude {date_dirs[ii]} {session}")


if __name__ == "__main__":
    compute_results()