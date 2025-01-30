"""Simulator to replay Bee data.

Setup:
1. pip install redis pandas protobuf
2. protoc --python_out=. sensordata.proto

"""

import time
import base64
import sqlite3
import argparse
import subprocess

import redis
import pandas as pd
from tqdm import tqdm

import sensordata_pb2 as sensordata


def main():
    """Main function to fetch, serialize, and push data."""

    # get path to database from argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--db_path", type=str, default="sensors-v0-0-2.db", help="Path to the SQLite database file")
    args = parser.parse_args()

    sr = SensorReplay(args.db_path)
    sr.run_replay()

class SensorReplay():
    def __init__(self, sensor_db_path):


        # Redis configuration
        self.redis_host = "127.0.0.1"
        self.redis_port = 6379
        self.redis_conf_file = "redis.conf"
        self.sensor_db_path = sensor_db_path

        
        self.redis_table_to_list = {
                                "gnss" : "gnss_data",
                                "gnss_auth" : "gnss_auth_data",
                                "imu" : "imu_data",
                                "magnetometer" : "magnetometer_data",
                                "nav_cov" : "NavCov",
                                "nav_posecef" : "NavPosecef",
                                "nav_pvt" : "NavPvt",
                                "nav_status" : "NavStatus",
                                "nav_timegps" : "NavTimegps",
                                "nav_velecef" : "NavVelecef",
                              }
        
        self.serializers = {
                                "gnss" : self.serialize_gnss,
                                "gnss_auth" : self.serialize_gnss_auth,
                                "imu" : self.serialize_imu,
                                "magnetometer" : self.serialize_mag,
                                "nav_cov" : self.serialize_nav_cov,
                                "nav_posecef" : self.serialize_nav_posecef,
                                "nav_pvt" : self.serialize_nav_pvt,
                                "nav_status" : self.serialize_nav_status,
                                "nav_timegps" : self.serialize_nav_timegps,
                                "nav_velecef" : self.serialize_nav_velecef,
                            }
        

        self.redis_unsaved_lists = ["NavDop","MonRf"]

        self.system_time_columns = {
                                "gnss" : "system_time",
                                "gnss_auth" : "system_time",
                                "imu" : "time",
                                "magnetometer" : "system_time",
                                "nav_pvt" : "system_time",
                              }
        self.itow_ms_tables = ["nav_cov", "nav_posecef", "nav_status", "nav_timegps", "nav_velecef"]
        
        self.sql_data = {}
        self.row_index = {}
        self.system_timestamps = {}
        for table in self.redis_table_to_list:
            self.sql_data[table] = self.fetch_sqlite_table(table)
            self.row_index[table] = 0
            if table in self.system_time_columns:
                self.sql_data[table][self.system_time_columns[table]] = pd.to_datetime(self.sql_data[table][self.system_time_columns[table]], format="mixed")
                if len(self.sql_data[table]) > 0:
                    self.system_timestamps[table] = self.sql_data[table][self.system_time_columns[table]][0]

        self.start_redis_server()

    def run_replay(self):
        """Runs the replay loop."""

        pbar = tqdm(total=sum([len(self.sql_data[table]) for table in self.sql_data]))
        
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
                for table in self.itow_ms_tables:
                    data_row = self.sql_data[table].loc[self.sql_data[table]["itow_ms"] == self.sql_data[min_key].iloc[self.row_index[min_key]]["itow_ms"]]
                    if len(data_row) > 0:
                        serialized_data = self.serializers[table](data_row.iloc[0])
                        self.push_to_redis(serialized_data, self.redis_table_to_list[table])
                        pbar.update(1)
            
            # update to the next timestamp
            self.row_index[min_key] += 1
            if self.row_index[min_key] >= len(self.sql_data[min_key]):
                self.system_timestamps[min_key] = None
            else:
                self.system_timestamps[min_key] = self.sql_data[min_key][self.system_time_columns[min_key]][self.row_index[min_key]]

    def serialize_gnss(self, row):
        message = sensordata.GnssData()
        message.system_time = row.system_time.strftime('%Y-%m-%d %H:%M:%S.') + str(row.system_time.microsecond).ljust(6, '0')
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
        message.system_time = row.system_time.strftime('%Y-%m-%d %H:%M:%S.') + str(row.system_time.microsecond).ljust(6, '0')
        return message.SerializeToString()

    def serialize_imu(self, row):
        message = sensordata.ImuData()
        message.time = row.time.strftime('%Y-%m-%d %H:%M:%S.') + str(row.time.microsecond).ljust(6, '0')
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
        message.system_time = row.system_time.strftime('%Y-%m-%d %H:%M:%S.') + str(row.system_time.microsecond).ljust(6, '0')
        message.x = row.mag_x
        message.y = row.mag_y
        message.z = row.mag_z
        return message.SerializeToString()
    
    def serialize_nav_cov(self, row):
        message = sensordata.NavCov()
        return message.SerializeToString()
    
    def serialize_nav_posecef(self, row):
        message = sensordata.NavPosecef()
        return message.SerializeToString()
    
    def serialize_nav_pvt(self, row):
        message = sensordata.NavPvt()
        return message.SerializeToString()
    
    def serialize_nav_status(self, row):
        message = sensordata.NavStatus()
        return message.SerializeToString()
    
    def serialize_nav_timegps(self, row):
        message = sensordata.NavTimegps()
        return message.SerializeToString()
    
    def serialize_nav_velecef(self, row):
        message = sensordata.NavVelecef()
        return message.SerializeToString()

    def push_to_redis(self, serialized_data, list_name):
        """Pushes the serialized data to a Redis list."""
        redis_client = redis.StrictRedis(host=self.redis_host, port=self.redis_port, decode_responses=True)
        redis_client.lpush(list_name, serialized_data)

    def fetch_sqlite_table(self, table_name):

        conn = sqlite3.connect(self.sensor_db_path)
        cursor = conn.cursor()

        query = f"SELECT * FROM {table_name}"

        df = pd.read_sql_query(query, conn)

        conn.close()
        return df

    def start_redis_server(self):
        """Starts a Redis server as a subprocess."""
        try:
            if self.redis_conf_file:
                process = subprocess.Popen(["redis-server", self.redis_conf_file], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            else:
                process = subprocess.Popen(["redis-server"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)

            # Wait a bit to ensure the Redis server starts
            time.sleep(2)

            print("Redis server started successfully.")
            return process
        except Exception as e:
            print(f"Failed to start Redis server: {e}")
            raise e

if __name__ == "__main__":
    main()
