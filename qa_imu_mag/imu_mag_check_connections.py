""" Imu/Mag Automatic QA script.

"""

import time
import sqlite3
import argparse
import subprocess

class ImuMagQa():
    def __init__(self, db_path, name="", sn=""):
        self.database_path = db_path
        self.name = name
        self.sn = sn

        self.imu_columns = ["id", "time", "session",
                            "acc_x","acc_y", "acc_z",
                            "gyro_x", "gyro_y", "gyro_z",
                            ]
        self.mag_columns = ["id", "system_time", "session",
                            "mag_x","mag_y", "mag_z",
                            ]
        
        self.checked_imu = False
        self.check_acc_zeros = False
        self.check_gyro_zeros = False
        self.checked_mag = False
        self.check_mag_zeros = False
        self.check_fsync_connection = False
        self.fsync_waits = []
        
        self.imu_ids_seen = set()
        self.mag_ids_seen = set()

        self.imu_data = {"acc_x": [], "acc_y": [], "acc_z": [],
                         "gyro_x": [], "gyro_y": [], "gyro_z": []}
        self.mag_data = {"mag_x": [], "mag_y": [], "mag_z": []}
        
        self.count = 0
        self.num_rows_to_fetch = 20
        self.secs_to_fetch = 1

    def run(self):

        while True:

            if self.count >= 60:
                print("60s Timeout. Exiting...")
                self._write_results()
                break

            latest_imu = self._get_latest_values("imu", self.imu_columns)
            if not latest_imu or len(latest_imu) == 0:
                print("empty imu database.")
                self.count += 1
                time.sleep(1)
                continue
            if len(self.imu_ids_seen.intersection(set(latest_imu["id"]))) > 0:
                print("stale imu data.")
                self.count += 1
                time.sleep(1)
                continue
            self.imu_ids_seen.update(latest_imu["id"])
            for key in self.imu_data.keys():
                self.imu_data[key].extend(latest_imu[key])

            latest_mag = self._get_latest_values("magnetometer", self.mag_columns)
            if not latest_mag or len(latest_mag) == 0:
                print("empty mag database.")
                self.count += 1
                time.sleep(1)
                continue
            if len(self.mag_ids_seen.intersection(set(latest_mag["id"]))) > 0:
                print("stale mag data.")
                self.count += 1
                time.sleep(1)
                continue
            self.mag_ids_seen.update(latest_mag["id"])
            for key in self.mag_data.keys():
                self.mag_data[key].extend(latest_mag[key])

            if len(self.imu_data["acc_x"]) >= self.secs_to_fetch*self.num_rows_to_fetch and not self.checked_imu:
                self.check_acc_zeros = self._check_acc_zeros()
                self.check_gyro_zeros = self._check_gyro_zeros()
                self.checked_imu = True
            if len(self.mag_data["mag_x"]) >= self.secs_to_fetch*self.num_rows_to_fetch and not self.checked_mag:
                self.check_mag_zeros = self._check_mag_zeros()
                self.checked_mag = True

            if self.checked_imu and self.checked_mag:
                subprocess.run(["systemctl", "disable", "hivemapper-data-logger"])
                subprocess.run(["systemctl", "stop", "hivemapper-data-logger"])
                time.sleep(2)
                subprocess.run(["chmod", "+x", "/home/root/datalogger"])
                self.check_fsync_connection = self._check_fsync_connection()
                subprocess.run(["systemctl", "enable", "hivemapper-data-logger"])
                subprocess.run(["systemctl", "start", "hivemapper-data-logger"])
                break
            
            self.count += 1
            time.sleep(1)

        self._write_results()


    def _check_acc_zeros(self):
        """Check less than 1% of data is zero.

        """
        # check that less than 1% of time CW jamming >= 250
        for key in ["acc_x", "acc_y", "acc_z"]:
            if sum([1 if x == 0.0 else 0 for x in self.imu_data[key]])/float(len(self.imu_data[key])) > 0.01:
                return False
        return True
    
    def _check_gyro_zeros(self):
        """Check less than 1% of data is zero.

        """
        # check that less than 1% of time CW jamming >= 250
        for key in ["gyro_x", "gyro_y", "gyro_z"]:
            if sum([1 if x == 0.0 else 0 for x in self.imu_data[key]])/float(len(self.imu_data[key])) > 0.01:
                return False
        return True
    
    def _check_mag_zeros(self):
        """Check less than 1% of data is zero.

        """
        # check that less than 1% of time CW jamming >= 250
        for key in ["mag_x", "mag_y", "mag_z"]:
            if sum([1 if x == 0.0 else 0 for x in self.mag_data[key]])/float(len(self.mag_data[key])) > 0.01:
                return False
        return True
    
    def _check_fsync_connection(self):
        """Check FSYNC connection.

        """
        print("Checking FSYNC connection")

        for _ in range(3):
            fsync_waits = self._run_data_logger()
            if len(fsync_waits) > 0:
                break

        self.fsync_waits = fsync_waits
        
        if len(fsync_waits) < 500:
            return False
        
        # check that 90% of time fsync_waits are less than 5
        if sum([1 if x < 5 else 0 for x in fsync_waits])/float(len(fsync_waits)) < 0.9:
            return False

        return True
    
    def _run_data_logger(self):

        fsync_waits = []
        fsync_wait_count = 0

        command = ["/home/root/datalogger", "log",
                   "--gnss-mga-offline-file-path", "/data/mgaoffline.ubx",
                   "--imu-json-destination-folder=/data/recording/imu",
                   "--gnss-json-destination-folder=/data/recording/gps",
                   "--db-output-path=/data/recording/data-logger.v2.0.0.db",
                   "--db-log-ttl=30m",
                   "--gnss-dev-path=/dev/ttyS2",
                   "--imu-dev-path=/dev/spidev0.0",
                   "--gnss-initial-baud-rate=460800",
                   "--gnss-json-save-interval=30s",
                   "--imu-json-save-interval=5s",
                   "--imu-axis-map=CamX:Y,CamY:X,CamZ:Z",
                   "--imu-inverted",
                   "X:true,Y:false,Z:false",
                   "--enable-magnetometer",
                   "--enable-redis-logs"]

        try:
            process = subprocess.Popen(
                command,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1
            )

            # Run the process for 15 seconds and print output in real-time
            start_time = time.time()
            while True:
                # Check if 15 seconds have passed
                if time.time() - start_time > 15:
                    process.terminate()  # Stop the process
                    break

                # Print output from stdout
                if process.stdout:
                    line = process.stdout.readline()
                    if line:
                        if "Fsync{FSYNC interrupt: false," in line:
                            fsync_wait_count += 1
                        elif "Fsync{FSYNC interrupt: true," in line:
                            fsync_waits.append(fsync_wait_count)
                            fsync_wait_count = 0

            # Print any remaining stderr output after termination
            if process.stderr:
                for line in process.stderr:
                    print(line, end="")

            process.wait()  # Ensure the process has terminated


        except FileNotFoundError:
            print("The command or executable was not found.")
        except Exception as e:
            print(f"An unexpected error occurred: {e}")

        return fsync_waits

    def _write_results(self):
        """Write results to txt file /data/qa_imu_mag_results.txt

        """

        filename = "/data/qa_imu_mag_results"
        if self.name != "":
            filename += "_"+self.name
        if self.sn != "":
            filename += "_"+self.sn
        filename += ".txt"

        with open(filename, "w") as f:            
            if self.check_acc_zeros:
                f.write("[PASS] accel values 0.0 for less than 1% of time\n")
            else:
                f.write("[FAIL] accel values 0.0 for greater than 1% of time\n")
            if self.check_gyro_zeros:
                f.write("[PASS] gyro values 0.0 for less than 1% of time\n")
            else:
                f.write("[FAIL] gyro values 0.0 for greater than 1% of time\n")

            if self.check_mag_zeros:
                f.write("[PASS] mag values 0.0 for less than 1% of time\n")
            else:
                f.write("[FAIL] mag values 0.0 for greater than 1% of time\n")

            if self.check_fsync_connection:
                f.write("[PASS] IMU/GNSS FSYNC connection verified\n")
            else:
                f.write("[FAIL] IMU/GNSS FSYNC connection not verified\n")

            # f.write("FSYNC wait values: ")
            # f.write(str(self.fsync_waits))
            # f.write("\n")

    def _get_latest_values(self, table_name, columns, order_by_column = "id"):
        """
        Fetch the last values from specific columns in a SQLite3 database table.

        :param database_path: Path to the SQLite database file.
        :param table_name: Name of the table to query.
        :param columns: List of column names to retrieve.
        :param order_by_column: Column name used to determine the order (e.g., timestamp or ID).
        :return: A list of dictionaries containing the latest values for the specified columns.
        """
        try:
            # Connect to the database
            conn = sqlite3.connect(self.database_path)
            cursor = conn.cursor()

            # Build the SQL query
            columns_str = ", ".join(columns)
            query = f"SELECT {columns_str} FROM {table_name} ORDER BY {order_by_column} DESC LIMIT {self.num_rows_to_fetch}"

            # Execute the query
            cursor.execute(query)

            # Fetch the results
            results = cursor.fetchall()

            # Check if any results were returned
            if not results:
                print("No data found in the table.")
                return {}

            # Transpose rows into columns
            column_data = {column: [] for column in columns}
            for row in results:
                for col_name, value in zip(columns, row):
                    column_data[col_name].append(value)

            return column_data

        except sqlite3.Error as e:
            print(f"An error occurred: {e}")
            return {}

        finally:
            # Close the connection
            if conn:
                conn.close()

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Process device information.")
    parser.add_argument("--name", default="", help="Name of the technician.")
    parser.add_argument("--sn", default="", help="Serial number of the Bee device.")
    args = parser.parse_args()

    # database path
    DB_PATH = "/data/redis_handler/redis_handler-v0-0-3.db" # <= firmware 5.0.19

    qa = ImuMagQa(DB_PATH, args.name, args.sn)
    qa.run()