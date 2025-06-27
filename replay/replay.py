"""Simulator to replay Bee data.

Setup:
1. pip install redis pandas protobuf
2. protoc --python_out=. sensordata.proto


Only pushes data the is after the first nav-pvt system time.

"""

import sys
import time
import signal
import base64
import sqlite3
import argparse
import threading
import subprocess

import redis
import numpy as np
import pandas as pd
from tqdm import tqdm

import sensordata_pb2 as sensordata
import framemetadata_pb2 as framemetadata


def main():
    """Main function to fetch, serialize, and push data."""

    # get path to database from argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--sensors-db-path", type=str, default="sensors-v0-0-2.db", help="Path to the SQLite database file")
    parser.add_argument("--gnss-db-path", type=str, default="gnss-raw-v0-0-2.db", help="Path to the SQLite database file")
    parser.add_argument("--session", type=str, default="", help="Session ID to replay")
    args = parser.parse_args()

    sr = SensorReplay(args.sensors_db_path, args.gnss_db_path, args.session)
    signal.signal(signal.SIGINT, sr.handle_exit)
    thread = threading.Thread(target=sr.run_map_ai_handshake, daemon=True)
    thread.start()
    sr.run_replay()

class SensorReplay():
    def __init__(self, sensor_db_path, gnss_db_path, session = ""):


        # Redis configuration
        self.redis_host = "127.0.0.1"
        self.redis_port = 6379
        self.redis_conf_file = "redis.conf"
        self.sensor_db_path = sensor_db_path
        self.gnss_db_path = gnss_db_path
        self.session = session

        
        self.redis_table_to_list = {
                                "nav_pvt" : "NavPvt", # must be first for sync time to work out
                                "gnss_auth" : "GnssAuthData",
                                "imu" : "ImuData",
                                "magnetometer" : "MagnetometerData",
                                "nav_cov" : "NavCov",
                                "nav_posecef" : "NavPosecef",
                                "nav_status" : "NavStatus",
                                "nav_timegps" : "NavTimegps",
                                "nav_velecef" : "NavVelecef",
                                "tim_tp" : "TimTp",
                                "nav_sat" : "NavSat",
                              }
        
        self.serializers = {
                                "gnss_auth" : self.serialize_gnss_auth,
                                "imu" : self.serialize_imu,
                                "magnetometer" : self.serialize_mag,
                                "nav_cov" : self.serialize_nav_cov,
                                "nav_posecef" : self.serialize_nav_posecef,
                                "nav_pvt" : self.serialize_nav_pvt,
                                "nav_status" : self.serialize_nav_status,
                                "nav_timegps" : self.serialize_nav_timegps,
                                "nav_velecef" : self.serialize_nav_velecef,
                                "nav_sat" : self.serialize_nav_sat,
                                "tim_tp" : self.serialize_tim_tp,
                            }

        self.system_time_columns = {
                                "gnss_auth" : "system_time",
                                "imu" : "time",
                                "magnetometer" : "system_time",
                                "tim_tp" : "system_time",
                                "nav_pvt" : "system_time",
                              }
        self.itow_ms_tables = ["nav_cov",
                               "nav_posecef",
                               "nav_status",
                               "nav_timegps",
                               "nav_velecef",
                               "nav_sat",
                               ]
        
        self.sql_data = {}
        self.row_index = {}
        self.system_timestamps = {}
        nav_pvt_start_time = None
        nav_pvt_start_itow_ms = None
        for table in self.redis_table_to_list:
            self.sql_data[table] = self.fetch_sqlite_table(table)
            self.row_index[table] = 0
            if table in self.system_time_columns:
                self.sql_data[table]["sync_time"] = pd.to_datetime(self.sql_data[table][self.system_time_columns[table]],
                                                                   errors='coerce', format="%Y-%m-%d %H:%M:%S.%f")
                self.sql_data[table] = self.sql_data[table].dropna(subset=["sync_time"])
                if table == "nav_pvt":
                    nav_pvt_start_time = self.sql_data[table]["sync_time"][0]
                    nav_pvt_start_itow_ms = self.sql_data[table]["itow_ms"][0]
                elif nav_pvt_start_time is not None:
                    self.sql_data[table] = self.sql_data[table][self.sql_data[table]["sync_time"] >= nav_pvt_start_time]
                    self.sql_data[table].reset_index(drop=True, inplace=True)
                if len(self.sql_data[table]) > 0:
                    self.system_timestamps[table] = self.sql_data[table]["sync_time"][0]
            else:
                if nav_pvt_start_itow_ms is not None:
                    self.sql_data[table] = self.sql_data[table][self.sql_data[table]["itow_ms"] >= nav_pvt_start_itow_ms]
                    self.sql_data[table].reset_index(drop=True, inplace=True)

        self.start_redis_server()
        self.nav_pvt_start_itow_ms = nav_pvt_start_itow_ms

    def adjust_itow_ms(self, itow_ms):
        
        return itow_ms
        # return int((itow_ms + 371345500) % 6.048E8)
        # return int((itow_ms + 107628500) % 6.048E8)

    def run_replay(self):
        """Runs the replay loop."""

        pbar = tqdm(total=sum([len(self.sql_data[table]) for table in self.sql_data]))
        nav_sat_rows = self.sql_data["nav_sat"][self.sql_data["nav_sat"]["itow_ms"] == self.sql_data["nav_sat"]["itow_ms"][0]]
        
        while True:

            # find the minimum timestamp
            valid_timestamps = {k: v for k, v in self.system_timestamps.items() if v is not None}
            if len(valid_timestamps) == 0:
                break
            min_key = min(valid_timestamps, key=valid_timestamps.get)
            
            # add the current min key to Redis
            serialized_data = self.serializers[min_key](self.sql_data[min_key].iloc[self.row_index[min_key]])
            self.push_to_redis(serialized_data, self.redis_table_to_list[min_key])
            pbar.update(1)
            
            # if it's a navigation message, also add the other navigation messages
            if min_key == "nav_pvt":
                nav_pvt_itow_ms = self.sql_data[min_key].iloc[self.row_index[min_key]]["itow_ms"]
                nav_pvt_system_time = self.sql_data[min_key].iloc[self.row_index[min_key]]["system_time"]

                for table in self.itow_ms_tables:
                    data_row = self.sql_data[table].loc[self.sql_data[table]["itow_ms"] == nav_pvt_itow_ms]
                    if len(data_row) > 0:
                        # if self.adjust_itow_ms(nav_pvt_itow_ms) >= 604799750:
                        # if (self.adjust_itow_ms(nav_pvt_itow_ms) >= 604799750 or self.adjust_itow_ms(nav_pvt_itow_ms) == 0) and table == "nav_cov":
                            # print("skipping nav_cov")
                            # continue
                        serialized_data = self.serializers[table](data_row.iloc[0])
                        self.push_to_redis(serialized_data, self.redis_table_to_list[table])
                        pbar.update(1)
                self.push_to_redis(self.serialize_nav_dop(nav_pvt_system_time, nav_pvt_itow_ms), "NavDop")
                self.push_to_redis(self.serialize_nav_sat(nav_sat_rows, nav_pvt_system_time, nav_pvt_itow_ms), "NavSat")
                self.push_to_redis(self.serialize_nav_sig(nav_pvt_system_time, nav_pvt_itow_ms), "NavSig")
                self.push_to_redis(self.serialize_mon_rf(nav_pvt_system_time), "MonRf")
                time.sleep(0.25)

            # update to the next timestamp
            self.row_index[min_key] += 1
            if self.row_index[min_key] >= len(self.sql_data[min_key]):
                self.system_timestamps[min_key] = None
            else:
                self.system_timestamps[min_key] = self.sql_data[min_key]["sync_time"][self.row_index[min_key]]

        self.clear_redis()

    def serialize_gnss(self, row):
        message = sensordata.GnssData()
        message.system_time = row.system_time
        message.timestamp = row.time
        message.fix = row.fix
        message.ttff = row.ttff
        message.latitude = row.latitude
        message.longitude = row.longitude
        message.altitude = row.altitude
        message.speed = row.speed
        message.heading = row.heading
        message.satellites.seen = row.satellites_seen
        message.satellites.used = row.satellites_used
        message.eph = row.eph
        message.horizontal_accuracy = row.horizontal_accuracy
        message.vertical_accuracy = row.vertical_accuracy
        message.heading_accuracy = row.heading_accuracy
        message.speed_accuracy = row.speed_accuracy
        message.dop.hdop = row.hdop
        message.dop.vdop = row.vdop
        message.dop.xdop = row.xdop
        message.dop.ydop = row.ydop
        message.dop.tdop = row.tdop
        message.dop.pdop = row.pdop
        message.dop.gdop = row.gdop
        message.rf.jamming_state = row.rf_jamming_state
        message.rf.ant_status = row.rf_ant_status
        message.rf.ant_power = row.rf_ant_power
        message.rf.post_status = row.rf_post_status
        message.rf.noise_per_ms = row.rf_noise_per_ms
        message.rf.agc_cnt = row.rf_agc_cnt
        message.rf.jam_ind = row.rf_jam_ind
        message.rf.ofs_i = row.rf_ofs_i
        message.rf.mag_i = row.rf_mag_i
        message.rf.ofs_q = row.rf_ofs_q
        message.cno = row.cno
        message.actual_system_time = row.actual_system_time
        message.time_resolved = row.time_resolved
        return message.SerializeToString()
    
    def serialize_gnss_auth(self, row):
        message = sensordata.GnssData()
        message.sec_ecsign_buffer = row.buffer
        message.sec_ecsign.msg_num = row.buffer_message_num
        message.sec_ecsign.session_id = base64.b64decode(row.gnss_session_id)
        message.sec_ecsign.final_hash = base64.b64decode(row.buffer_hash)
        message.sec_ecsign.ecdsa_signature = base64.b64decode(row.signature)
        message.system_time = row.system_time
        return message.SerializeToString()

    def serialize_imu(self, row):
        message = sensordata.ImuData()
        message.time = row.time
        message.accelerometer.x = row.acc_x
        message.accelerometer.y = row.acc_y
        message.accelerometer.z = row.acc_z
        message.gyroscope.x = row.gyro_x
        message.gyroscope.y = row.gyro_y
        message.gyroscope.z = row.gyro_z
        message.temperature = row.temperature
        return message.SerializeToString()
    
    def serialize_mag(self, row):
        message = sensordata.MagnetometerData()
        message.system_time = row.system_time
        message.x = row.mag_x
        message.y = row.mag_y
        message.z = row.mag_z
        return message.SerializeToString()
    
    def serialize_nav_cov(self, row):
        message = sensordata.NavCov()
        message.itow_ms = self.adjust_itow_ms(row.itow_ms)
        message.version = row.version
        message.pos_cov_valid = row.posCovValid
        message.vel_cov_valid = row.velCovValid
        message.pos_cov_n_n = row.pos_cov_n_n
        message.pos_cov_n_e = row.pos_cov_n_e
        message.pos_cov_n_d = row.pos_cov_n_d
        message.pos_cov_e_e = row.pos_cov_e_e
        message.pos_cov_e_d = row.pos_cov_e_d
        message.pos_cov_d_d = row.pos_cov_d_d
        message.vel_cov_n_n = row.vel_cov_n_n
        message.vel_cov_n_e = row.vel_cov_n_e
        message.vel_cov_n_d = row.vel_cov_n_d
        message.vel_cov_e_e = row.vel_cov_e_e
        message.vel_cov_e_d = row.vel_cov_e_d
        message.vel_cov_d_d = row.vel_cov_d_d
        return message.SerializeToString()
    
    def serialize_nav_posecef(self, row):
        message = sensordata.NavPosecef()
        message.itow_ms = self.adjust_itow_ms(row.itow_ms)
        message.ecef_x_cm = int(np.rint(row.ecef_x * 100.))
        message.ecef_y_cm = int(np.rint(row.ecef_y * 100.))
        message.ecef_z_cm = int(np.rint(row.ecef_z * 100.))
        message.p_acc_cm = int(np.rint(row.p_acc * 100.))
        return message.SerializeToString()
    
    def serialize_nav_pvt(self, row):
        message = sensordata.NavPvt()
        message.system_time = row.system_time
        message.itow_ms = self.adjust_itow_ms(row.itow_ms)
        message.uptime_ms = self.uptime_milliseconds()
        message.valid = (row.valid_date << 0) | (row.valid_time << 1) | (row.fully_resolved << 2) | (row.valid_mag << 3)
        message.fix_type = row.fix_type
        message.flags = (row.gnss_fix_ok << 0) | (row.diff_soln << 1) | (row.psm_state << 2) | (row.head_veh_valid << 5) | (row.carr_soln << 6)
        message.num_sv = row.num_sv
        message.lon_dege7 = int(np.rint(row.lon_deg * 1e7))
        message.lat_dege7 = int(np.rint(row.lat_deg * 1e7))
        message.height_mm = int(np.rint(row.height_m * 1000))
        message.hmsl_mm = int(np.rint(row.hmsl_m * 1000))
        message.h_acc_mm = int(np.rint(row.h_acc_m * 1000))
        message.v_acc_mm = int(np.rint(row.v_acc_m * 1000))
        message.vel_n_mm_s = int(np.rint(row.vel_n_m_s * 1000))
        message.vel_e_mm_s = int(np.rint(row.vel_e_m_s * 1000))
        message.vel_d_mm_s = int(np.rint(row.vel_d_m_s * 1000))
        message.g_speed_mm_s = int(np.rint(row.g_speed_m_s * 1000))
        message.head_mot_dege5 = int(np.rint(row.head_mot_deg * 1e5))
        message.s_acc_mm_s = int(np.rint(row.s_acc_m_s * 1000))
        message.head_acc_dege5 = int(np.rint(row.head_acc_deg * 1e5))
        message.pdop = int(np.rint(row.pdop * 100))
        message.flags3 = (row.invalid_llh << 0) | (row.last_correction_age << 1) | (row.auth_time << 13) | (row.nma_fix_status << 14)
        return message.SerializeToString()
    
    def serialize_nav_status(self, row):
        message = sensordata.NavStatus()
        message.itow_ms = self.adjust_itow_ms(row.itow_ms)
        message.gps_fix = row.gps_fix
        message.flags = (row.gps_fix_ok << 0) | (row.diff_soln << 1) | (row.wkn_set << 2) | (row.tow_set << 3)
        message.fix_stat = (row.diff_corr << 0) | (row.carr_soln_valid << 1)
        message.flags2 = (row.psm_state << 0) | (row.spoof_det_state << 3) | (row.carr_soln << 6)
        message.ttff = row.ttff
        message.msss = row.msss
        return message.SerializeToString()
    
    def serialize_nav_timegps(self, row):
        message = sensordata.NavTimegps()
        message.itow_ms = self.adjust_itow_ms(row.itow_ms)
        message.ftow_ns = row.ftow_ns
        message.week = row.week
        message.leap_s = row.leap_s
        message.valid = row.valid
        message.t_acc_ns = np.uint32(row.t_acc_ns)
        return message.SerializeToString()
    
    def serialize_tim_tp(self, row):
        message = sensordata.TimTp()
        message.system_time = row.system_time
        message.itow_ms = self.adjust_itow_ms(row.itow_ms)
        message.itow_sub_ms = row.itow_sub_ms
        message.q_err_ps = row.q_err_ps
        message.week = row.week
        # message.flags = 
        # message.ref_info = 
        return message.SerializeToString()
    
    def serialize_nav_velecef(self, row):
        message = sensordata.NavVelecef()
        message.itow_ms = self.adjust_itow_ms(row.itow_ms)
        message.ecef_vx_cm_s = int(np.rint(row.ecef_vx * 100.))
        message.ecef_vy_cm_s = int(np.rint(row.ecef_vy * 100.))
        message.ecef_vz_cm_s = int(np.rint(row.ecef_vz * 100.))
        message.s_acc_cm_s = int(np.rint(row.s_acc * 100.))
        return message.SerializeToString()
    
    def serialize_nav_dop(self, nav_pvt_system_time, nav_pvt_itow_ms):
        message = sensordata.NavDop()
        message.system_time = nav_pvt_system_time
        message.itow_ms = self.adjust_itow_ms(nav_pvt_itow_ms)
        message.gdop = 1
        message.pdop = 1
        message.tdop = 1
        message.vdop = 1
        message.hdop = 1
        message.ndop = 1
        message.edop = 1

        return message.SerializeToString()
    
    def serialize_nav_sat(self, rows, nav_pvt_system_time, nav_pvt_itow_ms):
        message = sensordata.NavSat()
        message.system_time = nav_pvt_system_time
        message.itow_ms = self.adjust_itow_ms(nav_pvt_itow_ms)
        message.version = 1
        message.num_svs = len(rows)
        for idx, row in rows.iterrows():
            dummy_sv = message.svs.add()
            dummy_sv.gnss_id = row.gnss_id         # Example GNSS system ID
            dummy_sv.sv_id = row.sv_id          # Example satellite ID
            dummy_sv.cno_dbhz = int(row.cno_dbhz)       # Carrier-to-noise ratio in dB-Hz
            dummy_sv.elev_deg = int(row.elev_deg)       # Elevation angle in degrees
            dummy_sv.azim_deg = int(row.azim_deg)      # Azimuth angle in degrees
            dummy_sv.pr_res_me1 = int(row.pr_res_m)   # Pseudorange residual in meters * 0.1
            dummy_sv.flags = 0b11111111111111111111111111111111    # Example flags

        return message.SerializeToString()
    
    def serialize_nav_sig(self, nav_pvt_system_time, nav_pvt_itow_ms):
        message = sensordata.NavSig()
        message.system_time = nav_pvt_system_time
        message.itow_ms = self.adjust_itow_ms(nav_pvt_itow_ms)
        message.version = 1
        message.num_sigs = 10  # Two satellites in the list

        # Create the first dummy Svs entry
        for sig in range(message.num_sigs):
            dummy_sig = message.sigs.add()
            dummy_sig.gnss_id = 1         # Example GNSS system ID
            dummy_sig.sv_id = 10          # Example satellite ID
            dummy_sig.sig_id = 1           # Signal ID
            dummy_sig.freq_id = 1          # Frequency ID
            dummy_sig.pr_res_me1 = sig   # Pseudorange residual in meters * 0.1
            dummy_sig.cno_dbhz = sig       # Carrier-to-noise ratio in d
            dummy_sig.quality_ind = 1  # Quality indicator
            dummy_sig.corr_source = 1
            dummy_sig.iono_model = 1
            dummy_sig.sig_flags = 0b11111111 # Example flags

        return message.SerializeToString()
    
    def uptime_milliseconds(self):
        with open("/proc/uptime", "r") as f:
            uptime_str = f.readline().split()[0]
            return float(uptime_str)*1000.
    
    def serialize_mon_rf(self, nav_pvt_system_time):
        message = sensordata.MonRf()
        message.system_time = nav_pvt_system_time
        return message.SerializeToString()

    def push_to_redis(self, serialized_data, list_name):
        """Pushes the serialized data to a Redis list."""
        redis_client = redis.StrictRedis(host=self.redis_host, port=self.redis_port, decode_responses=True)
        redis_client.lpush(list_name, serialized_data)

    def fetch_sqlite_table(self, table_name):


        if table_name in ["nav_sat"]:
            # For nav_sat, we need to fetch all rows at once
            conn = sqlite3.connect(self.gnss_db_path)
        else:
            conn = sqlite3.connect(self.sensor_db_path)

        query = f"SELECT * FROM {table_name}"

        df = pd.read_sql_query(query, conn)

        if self.session != "":
            if table_name == "gnss_auth":
                df = df[df["session_id"] == self.session]
            else:
                df = df[df["session"] == self.session]

        df.reset_index(drop=True, inplace=True)

        conn.close()
        return df

    def clear_redis(self):
        try:
            # Connect to Redis on localhost
            client = redis.Redis(host=self.redis_host, port=self.redis_port, db=0)
            
            # Flush all data from Redis
            client.flushall()
            print("All data cleared from Redis.")
        except Exception as e:
            print(f"Error: {e}")


    def start_redis_server(self):
        """Starts a Redis server as a subprocess."""
        try:
            if self.redis_conf_file:
                process = subprocess.Popen(["redis-server", self.redis_conf_file], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            else:
                process = subprocess.Popen(["redis-server"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)

            # Wait a bit to ensure the Redis server starts
            time.sleep(2)

            self.clear_redis()

            print("Redis server started successfully.")
            return process
        except Exception as e:
            print(f"Failed to start Redis server: {e}")
            raise e
        
    def run_map_ai_handshake(self):

        # push timestamp to MapAiStarted
        redis_client = redis.StrictRedis(host=self.redis_host, port=self.redis_port, decode_responses=True)

        self.push_to_redis(str(self.uptime_milliseconds()),"MapAiStarted")

        while True:
            # Handle FrameRequest → FrameChosen
            if redis_client.llen("FrameRequest") > 0:
                val = redis_client.rpop("FrameRequest")
                try:
                    # Add one second
                    new_val = str(float(val) + 1.0)
                    redis_client.lpush("FrameChosen", new_val)
                except ValueError:
                    print(f"Invalid timestamp in FrameRequest: {val}")

            # Handle FrameMetadata cleanup
            if redis_client.llen("FrameMetadata") > 0:
                raw_data = redis_client.rpop("FrameMetadata")
                if raw_data is not None:
                    message = framemetadata.FrameMetadata()  # Replace with actual message class
                    message.ParseFromString(raw_data)

                    print(message.latitude, message.longitude)
                else:
                    print("No data in FrameMetadata list.")

            time.sleep(0.1)

    def handle_exit(self, signal, frame):
        print("Clearing data on exit...")
        self.clear_redis()
        sys.exit(0)

if __name__ == "__main__":
    main()
