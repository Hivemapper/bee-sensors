""" GNSS Automatic QA script.

"""

import time
import sqlite3
import argparse
import subprocess
import multiprocessing

import numpy as np

class GnssQa():
    def __init__(self, db_path, test_location, name="", sn=""):
        self.database_path = db_path
        self.test_location = test_location
        self.name = name
        self.sn = sn

        self.nav_pvt_columns = ["id", "system_time", "session",
                                "fully_resolved","gnss_fix_ok",
                                "lat_deg","lon_deg","hmsl_m"]
        self.nav_status_columns = ["id", "itow_ms", "session", 
                                   "ttff","msss"]
        self.gnss_columns = ["id", "system_time", "session",
                             "satellites_seen","satellites_used",
                             "cno","rf_jam_ind"]
        
        self.states = {
            0: "Step 1/4 Waiting for first fix",
            1: "Step 2/4 Waiting for checks on first fix",
            2: "Step 3/4 Waiting for second fix",
            3: "Step 4/4 waiting for third fix",
            4: "Saving results to file.",
        }

        
        self.check_sats_seen = False
        self.check_sats_used = False
        self.check_pos_error = False
        self.check_cn0 = False
        self.check_cw_jamming = False
        self.check_ttff = False
        
        self.ttff = []
        self.cw_jamming = []
        self.avg_error = 999999.
        self.ids_seen = set()
        
        self.state = 0
        self.count = 0
        self.first_fix_count = None

    def run(self):

        while True:

            if self.count >= 480:
                print("Timeout. Exiting...")
                self._write_results()
                break

            # print current state
            print(self.states[self.state])

            if self.state in (0,2,3):

                latest_gnss = self._get_latest_values("gnss", self.gnss_columns)
                if not latest_gnss or len(latest_gnss) == 0:
                    print("empty database.")
                    self.count += 1
                    time.sleep(1)
                    continue
                if len(self.ids_seen.intersection(set(latest_gnss["id"]))) > 0:
                    print("stale data.")
                    self.count += 1
                    time.sleep(1)
                    continue

                if self._fix_acquired():
                    self._add_ttff()

                    if self.state == 2:
                        self._cold_reboot()
                    if self.state == 3:
                        self.check_ttff = self._check_ttff()
                    self.state += 1

            elif self.state == 1:
                if self.first_fix_count is None:
                    self.first_fix_count = self.count

                latest_gnss = self._get_latest_values("gnss", self.gnss_columns)
                if not latest_gnss or len(latest_gnss) == 0:
                    print("empty database.")
                    self.count += 1
                    time.sleep(1)
                    continue
                if len(self.ids_seen.intersection(set(latest_gnss["id"]))) > 0:
                    print("stale data.")
                    self.count += 1
                    time.sleep(1)
                    continue
                self.ids_seen.update(latest_gnss["id"])

                if not self.check_sats_seen:
                    self.check_sats_seen = self._check_satellites_seen(latest_gnss)
                if not self.check_sats_used:
                    self.check_sats_used = self._check_satellites_used(latest_gnss)
                if not self.check_pos_error:
                    self.check_pos_error = self._check_pos_error()
                if not self.check_cn0:
                    self.check_cn0 = self._check_cn0(latest_gnss)
                
                self.cw_jamming += latest_gnss["rf_jam_ind"]
                if self.count - self.first_fix_count >= 60:
                    self.check_cw_jamming = self._check_cw_jamming()
                    if not self.check_cw_jamming:
                        self._write_results()
                        break

                if self.check_sats_seen and self.check_sats_used and self.check_pos_error and self.check_cn0 and self.check_cw_jamming:
                    self.state += 1
                    self._cold_reboot()
                else:
                    if not self.check_sats_seen:
                        print("Waiting for at least 15 satellites to be seen...")
                    if not self.check_sats_used:
                        print("Waiting for at least 5 satellites to be used...")
                    if not self.check_pos_error:
                        print("Waiting for position error to be less than 50m...")
                    if not self.check_cn0:
                        print("Waiting for CN0 to be greater than 30...")
                    if not self.check_cw_jamming:
                        print("Waiting for 60 secs of data for jamming check...")
                    
                
            elif self.state == 4:
                self._write_results()
                break
            
            self.count += 1
            time.sleep(1)

    def _check_satellites_seen(self, latest_gnss):
        """Check that the number of satellites seen are all greater than 15.

        """
        if all([x >= 15 for x in latest_gnss["satellites_seen"]]):
            return True
        return False
    
    def _check_satellites_used(self, latest_gnss):
        """Check that the number of satellites used are all greater than 5.

        """
        if all([x >= 5 for x in latest_gnss["satellites_used"]]):
            return True
        return False
    
    def _check_pos_error(self):
        """Check position error.

        """
        latest_nav_pvt = self._get_latest_values("nav_pvt", self.nav_pvt_columns)
        if latest_nav_pvt and len(latest_nav_pvt) > 0:

            
            lat = latest_nav_pvt["lat_deg"][-1]
            lon = latest_nav_pvt["lon_deg"][-1]
            alt = latest_nav_pvt["hmsl_m"][-1]

            true_ecef = self._geodetic_to_ecef(np.array([self.test_location]))
            test_ecef = self._geodetic_to_ecef(np.array([latest_nav_pvt["lat_deg"],
                                                        latest_nav_pvt["lon_deg"],
                                                        latest_nav_pvt["hmsl_m"]]))
            error = np.linalg.norm(test_ecef - true_ecef,axis=0)

            self.avg_error = np.mean(error)

            if np.all(error < 50.):
                return True
        return False

    def _check_cn0(self, latest_gnss):
        """Check CN0 values.

        """
        if all([x >= 30 for x in latest_gnss["cno"]]):
            return True
        return False
    
    def _check_cw_jamming(self):
        """Check CW jamming values.

        """
        
        # check that less than 1% of time CW jamming >= 250
        if sum([1 if x > 250 else 0 for x in self.cw_jamming])/float(len(self.cw_jamming)) < 0.01:
            return True
        return False

    def _geodetic_to_ecef(self, lla):
        """Convert geodetic coordinates to ECEF coordinates.

        Notes: copied from https://github.com/Stanford-NavLab/gnss_lib_py/blob/main/gnss_lib_py/utils/coordinates.py

        Based on code from https://github.com/commaai/laika whose license is
        copied below:

        MIT License

        Copyright (c) 2018 comma.ai

        Permission is hereby granted, free of charge, to any person obtaining a copy
        of this software and associated documentation files (the "Software"), to deal
        in the Software without restriction, including without limitation the rights
        to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
        copies of the Software, and to permit persons to whom the Software is
        furnished to do so, subject to the following conditions:

        The above copyright notice and this permission notice shall be included in all
        copies or substantial portions of the Software.

        THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
        IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
        FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
        AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
        LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
        OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
        SOFTWARE.

        """

        lla = lla.reshape(3,-1)

        ratio = (np.pi / 180.0)
        lat = lla[0]*ratio
        lon = lla[1]*ratio
        alt = lla[2]

        E1SQ = 6.69437999014 * 0.001
        """float : First esscentricity squared of Earth (not orbit)."""

        A = 6378137.
        """float : Semi-major axis (radius) of the Earth [m]."""

        xi = np.sqrt(1 - E1SQ * np.sin(lat)**2)
        x = (A / xi + alt) * np.cos(lat) * np.cos(lon)
        y = (A / xi + alt) * np.cos(lat) * np.sin(lon)
        z = (A / xi * (1 - E1SQ) + alt) * np.sin(lat)
        ecef = np.array([x, y, z]).reshape(3,-1)

        return ecef

    def _check_ttff(self):
        """ Check that the final two ttff are less than 90 seconds

        """
        if self.ttff[-1] <= 90. and self.ttff[-2] <= 90.:
            return True
        return False

    def _add_ttff(self):
        """Add time to first fix value.

        """
        time.sleep(2)
        latest_nav_status = self._get_latest_values("nav_status", self.nav_status_columns)
        if latest_nav_status and len(latest_nav_status) > 0:
            self.ttff.append(latest_nav_status["ttff"][-1]/1000.)

    def _fix_acquired(self):
        """Check if a fix has been acquired.
        """
        
        latest_nav_pvt = self._get_latest_values("nav_pvt", self.nav_pvt_columns)
        if latest_nav_pvt and len(latest_nav_pvt) > 0:
            # check that no 0 value in fully_resolved
            if not(0 in latest_nav_pvt["fully_resolved"]) and not(0 in latest_nav_pvt["gnss_fix_ok"]):
                return True
        return False
    
    def _run_script(self,script_path, capture_output=True):
        subprocess.run(["bash", script_path],capture_output=capture_output)

    
    def _cold_reboot(self):
        """Cold reboot the device.
        """

        # Start gpsd in a non-blocking manner
        print("starting gpsd")
        gpsd_script = "/data/qa_gnss/gpsd_command.sh"
        p1 = multiprocessing.Process(target=self._run_script, args=(gpsd_script,))
        p1.start()
        time.sleep(5)

        
        # Cold reboot the device
        print("running ublox cold reboot")
        ubxtool_script = "/data/qa_gnss/ubxtool_command.sh"
        p = multiprocessing.Process(target=self._run_script, args=(ubxtool_script,))
        p.start()
        time.sleep(5)

        p1.kill()
        p1.terminate()
        gpsd_kill_script = "/data/qa_gnss/gpsd_kill_command.sh"
        p = multiprocessing.Process(target=self._run_script, args=(gpsd_kill_script,))
        p.start()


        # restart hivemapper-data-logger
        print("restarting data-logger")
        logger_script = "/data/qa_gnss/logger_command.sh"
        p = multiprocessing.Process(target=self._run_script, args=(logger_script,))
        p.start()
        time.sleep(10)

    def _write_results(self):
        """Write results to txt file /data/qa_gnss_results.txt
        
        self.check_sats_seen = False
        self.check_sats_used = False
        self.check_pos_error = False
        self.check_cn0 = False
        self.check_cw_jamming = False
        self.check_ttff = False

        Also write TTFF values.

        """

        filename = "/data/qa_gnss_results"
        if self.name != "":
            filename += "_"+self.name
        if self.sn != "":
            filename += "_"+self.sn
        filename += ".txt"

        with open(filename, "w") as f:
            if self.check_sats_seen:
                f.write("[PASS] Saw at least 15 satellites\n")
            else:
                f.write("[FAIL] Did not see at least 15 satellites\n")
            
            if self.check_sats_used:
                f.write("[PASS] Used at least 5 satellites\n")
            else:
                f.write("[FAIL] Did not use at least 5 satellites\n")
            
            if self.check_pos_error:
                f.write("[PASS] Position error is less than 50m\n")
            else:
                f.write("[FAIL] Position error is greater than 50m\n")
            
            if self.check_cn0:
                f.write("[PASS] Achieved CN0 greater than 30\n")
            else:
                f.write("[FAIL] Did not achieve CN0 greater than 30\n")
            
            if self.check_cw_jamming:
                f.write("[PASS] CW jamming >=250 for less than 1% of time\n")
            else:
                f.write("[FAIL] CW jamming >=250 for more than 1% of time\n")
            
            if self.check_ttff:
                f.write("[PASS] TTFF values less than 90s\n")
            else:
                f.write("[FAIL] TTFF values greater than 90s\n")

            f.write("TTFF values: ")
            f.write(str(self.ttff))
            f.write("\n")
            f.write("Jamming indicator values: ")
            f.write(str(self.cw_jamming))
            f.write("\n")
            f.write("Avg position error [m]: ")
            f.write(str(self.avg_error))
            f.write("\n")


    def _get_latest_values(self, table_name, columns, order_by_column = "id"):
        """
        Fetch the last 10 values from specific columns in a SQLite3 database table.

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
            query = f"SELECT {columns_str} FROM {table_name} ORDER BY {order_by_column} DESC LIMIT 5"

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
    # DB_PATH = "/data/recording/redis_handler/redis_handler-v0-0-3.db" #  5.0.20 >= firmware < 5.026
    # DB_PATH = "/data/recording/data-logger.v2.0.0.db" # > 5.0.26

    # Latitude (deg), Longitude (deg), Altitude above Mean Sea Level (m) of test location
    # TEST_LOCATION = (37.787976671122664, -122.3983670259852, 20.) # SalesForce Park, San Francisco
    TEST_LOCATION = (40.54570923442922, -79.82677996611702, 260.)   # Hellbender, Pittsburgh, PA
    # TEST_LOCATION = (37.4692648, -122.2920581, 165.)                # Edgewood park and ride

    gnss_qa = GnssQa(DB_PATH, TEST_LOCATION, args.name, args.sn)
    gnss_qa.run()
