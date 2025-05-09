import os
import re
import ast
import time
import subprocess


import boto3
import sqlite3
import numpy as np
import pandas as pd
import seaborn as sns
from datetime import datetime
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches

class S3BucketBrowser:
    def __init__(self, bucket_name, region_name=None):
        self.bucket_name = bucket_name
        self.s3 = boto3.client('s3', region_name=region_name)
        self.data = None

        self.col_names = [
                     "sats_seen",
                     "sats_used",
                     "position_error",
                     "cno",
                     "cw_jamming",
                     "ttff",
                     "fsync",
                     "ttff_values",
                     "jamming_indicator_values",
                     "avg_position_error_m",
                     "fsync_wait_counts",
                    ]
        
        self.dir_name = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))),
                                        "results",f"gnss_eol_meta_analysis_{datetime.now().strftime("%Y%m%d%H%M%S")}")
        os.makedirs(self.dir_name, exist_ok=True)


    def list_objects(self, prefix=""):
        """List all files and directories under a prefix."""
        paginator = self.s3.get_paginator('list_objects_v2')
        result = []
        for page in paginator.paginate(Bucket=self.bucket_name, Prefix=prefix):
            contents = page.get('Contents', [])
            result.extend([obj['Key'] for obj in contents])
        return result

    def list_subdirectories(self, prefix=""):
        s3 = boto3.client('s3')
        paginator = s3.get_paginator('list_objects_v2')

        result = []
        for page in paginator.paginate(Bucket=self.bucket_name, Prefix=prefix, Delimiter='/'):
            prefixes = page.get('CommonPrefixes', [])
            result.extend([p['Prefix'].rstrip('/') for p in prefixes])
                
        return result

    def download_file(self, s3_key, local_path):
        """Download a single file."""
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        # print(f"Downloading s3://{self.bucket_name}/{s3_key} -> {local_path}")
        self.s3.download_file(self.bucket_name, s3_key, local_path)

    def download_directory(self, s3_prefix, local_dir):
        """Download all files under a 'directory' prefix."""
        objects = self.list_objects(s3_prefix)
        for s3_key in objects:
            rel_path = os.path.relpath(s3_key, s3_prefix)
            local_path = os.path.join(local_dir, rel_path)
            self.download_file(s3_key, local_path)

    def parse_gnss_directories(self):
        dirs = self.list_subdirectories()

        dirs = [d for d in dirs if "GNSS" in d]
        names = [d.split("_")[2] for d in dirs]
        sns = [d.split("_")[3] for d in dirs]
        date = [d.split("_")[4] for d in dirs]
        time = [d.split("_")[5] for d in dirs]

        parsed_data = {col : [] for col in self.col_names}
        for ii, my_dir in enumerate(dirs):
            log_file = os.path.join(my_dir, f"qa_gnss_results_{names[ii]}_{sns[ii]}.log")
            # check if the log file exists in the bucke          
            log_data = self.stream_log_from_s3(log_file)

            if log_data is not None:
                for key in self.col_names:
                    parsed_data[key].append(log_data.get(key, None))
            else:
                for key in self.col_names:
                    parsed_data[key].append(None)

        data_input = {
            "dir": dirs,
            "name": names,
            "sn": sns,
            "date": date,
            "time": time
        }
        data_input.update(parsed_data)

        self.data = pd.DataFrame(data_input)

        self.clean_data_entries()

        self.save_data_to_csv()

    def save_data_to_csv(self):

        self.data.to_csv(os.path.join(self.dir_name,f"gnss_eol_meta_analysis_{datetime.now().strftime("%Y%m%d%H%M%S")}.csv"))

    def load_data_from_csv(self, csv_path):
        """Load data from a CSV file."""
        if os.path.exists(csv_path):
            self.data = pd.read_csv(csv_path)
        else:
            print(f"File {csv_path} does not exist.")
            self.data = None

    def stream_log_from_s3(self, key):
        s3 = boto3.client('s3')

        try:
            response = s3.get_object(Bucket=self.bucket_name, Key=key)
        except Exception as e:
            print(f"Error with {key}: {e}")
            return None

        # print(f"Streaming log file from s3://{self.bucket_name}/{key}")
        
        data = {}

        # Read line by line without saving to disk
        for line in response['Body'].iter_lines():
            decoded_line = line.decode('utf-8')
            # Do your parsing here

            key = ""
            if "at least 15 satellites" in decoded_line:
                key = "sats_seen"
            elif "at least 5 satellites" in decoded_line:
                key = "sats_used"
            elif "Position error" in decoded_line:
                key = "position_error"
            elif "CN0 greater than 30" in decoded_line:
                key = "cno"
            elif "CW jamming >=250" in decoded_line:
                key = "cw_jamming"
            elif "TTFF values" in decoded_line and "than 90s" in decoded_line:
                key = "ttff"
            elif "IMU/GNSS FSYNC connection" in decoded_line:
                key = "fsync"
            elif "TTFF values:" in decoded_line:
                key = "ttff_values"
            elif "Jamming indicator values:" in decoded_line:
                key = "jamming_indicator_values"
            elif "Avg position error" in decoded_line:
                key = "avg_position_error_m"
            elif "FSYNC wait counts" in decoded_line:
                key = "fsync_wait_counts"
            else:
                print("Unknown line:", decoded_line)
                continue

            # Handle [PASS]/[FAIL] lines
            pass_fail_match = re.match(r"\[(PASS|FAIL)\] (.+)", decoded_line)
            if pass_fail_match:
                status, description = pass_fail_match.groups()
                # Convert description to a snake_case key
                # key = re.sub(r'[^a-zA-Z0-9]+', '_', description.strip().lower()).strip('_')
                data[key] = True if status == 'PASS' else False
                continue

            # Handle TTFF values
            if decoded_line.startswith("TTFF values:"):
                values = ast.literal_eval(decoded_line.split(":", 1)[1].strip())
                data[key] = values
                continue

            # Handle Jamming indicator values
            if decoded_line.startswith("Jamming indicator values:"):
                values = ast.literal_eval(decoded_line.split(":", 1)[1].strip())
                data[key] = values
                continue

            # Handle Avg position error
            if decoded_line.startswith("Avg position error"):
                value = float(re.search(r"[-+]?\d*\.\d+|\d+", decoded_line).group())
                data[key] = value
                continue

        return data

    # Fix the time format, removing leading dashes and ensuring two-digit formatting
    def fix_time(self, t):
        parts = t.replace("-", " ").lstrip().split()
        hour = parts[0].zfill(2)
        minute = parts[1].zfill(2)
        return f"{hour}:{minute}"

    def clean_data_entries(self):
        """Clean data entries in the DataFrame."""

        # remove HDC-Bee_GNSS_Niessl_FEB_ENG_05_2025-02-05_11-10 without a timestamp
        self.data = self.data[self.data["dir"].str.contains("HDC-Bee_GNSS_Niessl_FEB_ENG_05_2025-02-05_11-10") == False]

        # remove eng, test, empty, 1P0014545
        self.data["sn"] = self.data["sn"].astype(str).str.upper()
        self.data = self.data[~self.data["sn"].str.contains("ENG")]
        self.data = self.data[~self.data["sn"].str.contains("TEST")]
        self.data = self.data[~self.data["sn"].str.contains("1P000C")]
        self.data = self.data[~self.data["sn"].str.contains("1P0014545")]
        self.data = self.data[~self.data["sn"].str.contains("2P0016165")]
        self.data = self.data[~self.data["sn"].str.contains("2P00181")]
        self.data = self.data[~self.data["sn"].str.contains("2P00197-")]
        self.data = self.data[~self.data["sn"].str.contains("2P009B52A")]
        self.data = self.data[~self.data["sn"].str.contains("2P0081722")]
        self.data = self.data[~self.data["sn"].str.contains("6AA64")]
        self.data = self.data[~self.data["sn"].str.contains("2P00640C8")]
        self.data = self.data[~self.data["sn"].str.contains("2P009B52B")]
        self.data = self.data[~self.data["sn"].str.contains("2P0084127")]
        self.data = self.data[~(self.data["sn"].str.upper() == "NAN")]
        self.data = self.data[~(self.data["sn"] == "2")]

        # fix sn names where we can
        self.data["sn"] = self.data["sn"].replace("1P390", "1P000390")
        self.data["sn"] = self.data["sn"].replace("538", "1P000538")
        self.data["sn"] = self.data["sn"].replace("847", "2P000847")
        self.data["sn"] = self.data["sn"].replace("0854", "2P000854")
        self.data["sn"] = self.data["sn"].replace("0855", "2P000855")
        self.data["sn"] = self.data["sn"].replace("0857", "2P000857")
        self.data["sn"] = self.data["sn"].replace("0860", "2P000860")
        self.data["sn"] = self.data["sn"].replace("0864", "2P000864")
        self.data["sn"] = self.data["sn"].replace("0867", "2P000867")
        self.data["sn"] = self.data["sn"].replace("2P1417", "2P001417")
        self.data["sn"] = self.data["sn"].replace("2P1525", "2P001525")
        self.data["sn"] = self.data["sn"].replace("2P1773", "2P001773")
        self.data["sn"] = self.data["sn"].replace("2P002-002584", "2P002584")
        self.data["sn"] = self.data["sn"].replace("SP002785", "2P002785")
        self.data["sn"] = self.data["sn"].replace("SP002784", "2P002784")
        self.data["sn"] = self.data["sn"].replace("SP002769", "2P002769")
        self.data["sn"] = self.data["sn"].replace("D2P004621", "2P004621")
        self.data["sn"] = self.data["sn"].replace("SP002760", "2P002760")
        self.data["sn"] = self.data["sn"].replace("SP002119", "2P002119")
        self.data["sn"] = self.data["sn"].replace("1P0000187", "1P000187")
        self.data["sn"] = self.data["sn"].replace("1P0000764", "1P000764")
        self.data["sn"] = self.data["sn"].replace("D2P002948", "2P002948")
        self.data["sn"] = self.data["sn"].replace("2P0002995", "2P002995")
        self.data["sn"] = self.data["sn"].replace("2P04431", "2P004431")

        # List of values to convert
        to_convert = [
            "1260", "1960", "1842",
            "2623", "2629", "2686", "2642", "2643", "2640", "2630", "2632", "2664",
            "2667", "2694", "2669", "2649", "2646", "2695", "2648", "2644", "2394",
            "2652", "2654", "2650", "2666", "2639", "2908", "2996", 
            "3070", "3096", "3044", "3037", "3036", "3092", "3036", "3067", "3102", 
            "3052", "3114", "3035", "3116", "3095", "3110", "3110", "3100", "3634",
            "3568", "2751"
                    ]
        self.data["sn"] = self.data["sn"].apply(
            lambda x: f"2P00{x}" if x in to_convert else x
        )

        # test = self.data[self.data["dir"].str.contains("HDC-Bee_GNSS_Bri_0864_2025-03-08_10-11")]
        # print(test)
        # print(test["sn"])
        # print(test["sn"].dtype)
        # print(test["sn"].values[0])
        # print(type(test["sn"].values[0]))
        # print(len(test["sn"].values[0]))

        # combine date and time into a single datetime column
        self.data["time_fixed"] = self.data["time"].apply(self.fix_time)

        # Combine and convert to datetime
        self.data["datetime"] = pd.to_datetime(self.data["date"] + " " + self.data["time_fixed"])

        test_columns = ['sats_seen', 'sats_used', 'position_error', 'cno', 'cw_jamming', 'fsync']
        self.data["has_log"] = self.data[test_columns].notna().any(axis=1)

        # Optional: drop intermediate column
        self.data.drop(columns="time_fixed", inplace=True)

        # sort by datetime column
        self.data.sort_values(by="datetime", inplace=True)
        # reset index
        self.data.reset_index(drop=True, inplace=True)

        # Reorder columns: dir, datetime, then the rest
        cols = ["dir", "datetime"] + [col for col in self.data.columns if col not in ("dir", "datetime")]
        self.data = self.data[cols]

    def analyze_users(self):

        print("total length:", len(self.data))
        missing_log = self.data[self.data["sats_seen"].isna()]
        print("missing log length:", len(missing_log))
        print("missing log percentage:", len(missing_log) / len(self.data) * 100)
        print(missing_log["name"].unique())
        existing_log = self.data[self.data["sats_seen"].notna()]
        print("existing log length:", len(existing_log))
        print(existing_log["name"].unique())

    def meta_analysis(self):

        # 1. Determine test outcome using waterfall logic
        
        print("has_log:", self.data["has_log"].sum())

        def evaluate_test(row):
            if not row["has_log"]:
                return "no_log"
            if all(row[col] == False for col in ['sats_seen', 'sats_used', 'position_error', 'cno']):
                return "timeout"
            if row['sats_seen'] == False:
                return "fail_sats_seen"
            if row['sats_used'] == False:
                return "fail_sats_used"
            if row['position_error'] == False:
                return "fail_position_error"
            if row['cno'] == False:
                return "fail_cno"
            if row["cw_jamming"] == False:
                return "fail_cw_jamming"
            if row["ttff"] == False:
                return "fail_ttff"
            if row["fsync"] == False:
                return "fail_fsync"
            return "pass"

        self.data["test_result"] = self.data.apply(evaluate_test, axis=1)

        # Extract date from datetime
        self.data["date_only"] = pd.to_datetime(self.data["datetime"]).dt.date

        # === 1. Plot number of pass/fail/no log per day ===
        fig1 = plt.figure(figsize=(12, 6))
        sns.countplot(data=self.data, x="date_only", hue="has_log", order=sorted(self.data["date_only"].unique()))
        plt.xticks(rotation=45)
        plt.title("Has log or not per Day")
        plt.xlabel("Date")
        plt.ylabel("Count")
        plt.legend(title="Has Log")
        plt.tight_layout()
        fig1.savefig(os.path.join(self.dir_name,"has_log_per_day.png"))


        fail_result = self.data[(self.data["test_result"] != "pass") & (self.data["test_result"] != "no_log")]
        fig2 = plt.figure(figsize=(12, 6))
        sns.countplot(data=fail_result, x="date_only", hue="test_result", order=sorted(fail_result["date_only"].unique()))
        plt.xticks(rotation=45)
        plt.title("Test Results per Day")
        plt.xlabel("Date")
        plt.ylabel("Count")
        plt.legend(title="Test Result")
        plt.tight_layout()
        fig2.savefig(os.path.join(self.dir_name,"test_result_per_day.png"))

        # === 2. Count how many SN devices failed ===
        failed_sn_count = fail_result["sn"].nunique()
        print(f"Number of devices that failed: {failed_sn_count}")
        print(f"Devices that failed: {fail_result['sn'].unique()}")

        # === 3. Count how many devices were tested multiple times ===
        multiple_tests_count = (self.data[self.data["test_result"] != "no_log"]["sn"].value_counts() > 1).sum()
        print(f"Number of devices tested multiple times: {multiple_tests_count}")
        print(f"Devices tested multiple times: {self.data[self.data['test_result'] != 'no_log']['sn'].value_counts()[self.data[self.data['test_result'] != 'no_log']['sn'].value_counts() > 1].index.tolist()}")

        # === 4. Plot distributions for pass/fail ===
        # Convert numeric columns
        self.data["avg_position_error_m"] = pd.to_numeric(self.data["avg_position_error_m"], errors='coerce')

        self.data_pass = self.data[self.data["test_result"] == "pass"]
        self.data_fail = self.data[self.data["test_result"] != "pass"]

        fig, axes = plt.subplots(1, 1, figsize=(18, 10))
        metrics = [
            ("avg_position_error_m", "Avg. Position Error (m)"),
        ]

        ax = axes
        col = metrics[0][0]
        title = metrics[0][1]
        sns.histplot(self.data_pass[col], bins=30, color="green", label="Pass", kde=True, ax=ax, stat="density", alpha=0.6)
        ax.set_title(title)
        ax.legend()
        plt.tight_layout()


        # === Extract TTFF values into separate columns ===
        def extract_ttff(row, index):
            try:
                vals = ast.literal_eval(row)
                return vals[index]
            except:
                return np.nan

        self.data["ttff_1"] = self.data["ttff_values"].apply(lambda x: extract_ttff(x, 0))
        self.data["ttff_2"] = self.data["ttff_values"].apply(lambda x: extract_ttff(x, 1))
        self.data["ttff_3"] = self.data["ttff_values"].apply(lambda x: extract_ttff(x, 2))

        # === Plot TTFF values in separate subplots ===
        fig, axes = plt.subplots(1, 3, figsize=(18, 5))
        ttff_cols = ["ttff_1", "ttff_2", "ttff_3"]
        titles = ["TTFF 1", "TTFF 2", "TTFF 3"]
        colors = ["blue", "green", "orange"]

        for ax, col, title, color in zip(axes, ttff_cols, titles, colors):
            sns.histplot(self.data[col].dropna(), kde=True, bins=30, ax=ax, color=color)
            ax.set_title(f"Distribution of {title}")
            ax.set_xlabel("Seconds")
            ax.set_ylabel("Density")

        plt.tight_layout()
        fig.savefig(os.path.join(self.dir_name,"ttff_individual_subplots.png"))

        # === Plot all TTFF values together ===
        ttff_all = pd.concat([
            self.data["ttff_1"].dropna(),
            self.data["ttff_2"].dropna(),
            self.data["ttff_3"].dropna()
        ], axis=0)

        fig, ax = plt.subplots(figsize=(8, 5))
        sns.histplot(ttff_all, bins=40, kde=True, color="purple", ax=ax)
        ax.set_title("Combined TTFF Distribution")
        ax.set_xlabel("Seconds")
        ax.set_ylabel("Density")
        plt.tight_layout()
        fig.savefig(os.path.join(self.dir_name,"ttff_combined.png"))

        # === Jamming indicator analysis ===
        def parse_jamming(jamming):
            try:
                j = ast.literal_eval(jamming) if isinstance(jamming, str) else jamming
                if isinstance(j, list) and j:
                    return pd.Series({
                        "jamming_min": min(j),
                        "jamming_median": np.median(j),
                        "jamming_max": max(j)
                    })
            except:
                pass
            return pd.Series({"jamming_min": np.nan, "jamming_median": np.nan, "jamming_max": np.nan})

        jamming_stats = self.data["jamming_indicator_values"].apply(parse_jamming)
        self.data = pd.concat([self.data, jamming_stats], axis=1)

        # === Plot jamming min, median, max in subplots ===
        fig, axes = plt.subplots(1, 3, figsize=(18, 5))
        for ax, col, title in zip(axes, ["jamming_min", "jamming_median", "jamming_max"],
                                ["Min Jamming", "Median Jamming", "Max Jamming"]):
            sns.histplot(self.data[col].dropna(), bins=30, ax=ax, kde=True)
            ax.set_title(title)
            ax.set_xlabel("Value")
            ax.set_ylabel("Density")

        plt.tight_layout()
        fig.savefig(os.path.join(self.dir_name,"jamming_distribution_subplots.png"))

        # === 5. Waterfall Breakdown ===
        waterfall_counts = {
            "No Log": (~self.data["has_log"]).sum(),
            "Timed Out": self.data["test_result"].eq("timeout").sum(),
            "Failed Sats Seen": self.data["test_result"].eq("fail_sats_seen").sum(),
            "Failed Sats Used": self.data["test_result"].eq("fail_sats_used").sum(),
            "Failed Position Error": self.data["test_result"].eq("fail_position_error").sum(),
            "Failed CNO": self.data["test_result"].eq("fail_cno").sum(),
            "Failed CW Jamming": self.data["test_result"].eq("fail_cw_jamming").sum(),
            "Failed TTFF": self.data["test_result"].eq("fail_ttff").sum(),
            "Failed FSYNC": self.data["test_result"].eq("fail_fsync").sum(),
            "Passed": self.data["test_result"].eq("pass").sum(),
        }

        # Plot waterfall
        fig3 = plt.figure(figsize=(10, 6))
        sns.barplot(x=list(waterfall_counts.keys()), y=list(waterfall_counts.values()), palette="Set2", hue=list(waterfall_counts.keys()), legend=False)
        plt.ylabel("Number of Devices")
        plt.yscale("log")
        plt.title("Waterfall Breakdown of Device Test Outcomes")
        plt.xticks(rotation=15)
        plt.tight_layout()
        fig3.savefig(os.path.join(self.dir_name,"waterfall_breakdown.png"))
        


        # pass attempts


        # Filter out devices without logs
        logged_data = self.data[self.data["test_result"] != "no_log"]

        # Group devices tested multiple times
        grouped = logged_data.groupby("sn")
        tested_multiple_times = grouped.filter(lambda x: len(x) > 1)
        device_groups = tested_multiple_times.groupby("sn")

        # Classify devices
        categories = {
            "Always Passed": [],
            "Always Failed": [],
            "First Passed Then Failed": [],
            "First Failed Then Passed": []
        }

        # Function to classify each device
        for sn, group in device_groups:
            results = group.sort_values(by="datetime" if "datetime" in group.columns else group.index)["test_result"].tolist()
            result_set = set(results)
            if all(r == "pass" for r in results):
                categories["Always Passed"].append((sn, results))
            elif all(r != "pass" for r in results):
                categories["Always Failed"].append((sn, results))
            elif results[0] == "pass" and any(r != "pass" for r in results):
                categories["First Passed Then Failed"].append((sn, results))
            elif results[0] != "pass" and any(r == "pass" for r in results):
                categories["First Failed Then Passed"].append((sn, results))

        # Plot setup
        fig, axs = plt.subplots(1, 4, figsize=(16, 10), sharex=True)
        fig.suptitle(f"Test Result Sequences for {len(device_groups)} Devices Tested Multiple Times "
                    f"(Total Unique Devices: {logged_data['sn'].nunique()})", fontsize=14)

        # Helper function for plotting
        def plot_category(ax, title, entries, y_start):
            yticks = []
            ylabels = []
            for i, (sn, results) in enumerate(entries):
                colors = ['green' if r == "pass" else 'red' for r in results]
                ax.barh(y=[i]*len(results), width=1, left=list(range(len(results))), color=colors, height=0.6)
                yticks.append(i)
                ylabels.append(sn)
            ax.set_yticks(yticks)
            ax.set_yticklabels(ylabels, fontsize=8)
            ax.set_title(f"{title} — {len(entries)} devices", loc='left')
            ax.set_ylabel("SN")

        # Plot each category
        plot_category(axs[0], "Always Failed", categories["Always Failed"], 0)
        plot_category(axs[1], "First Failed Then Passed", categories["First Failed Then Passed"], 0)
        plot_category(axs[2], "Always Passed", categories["Always Passed"], 0)
        plot_category(axs[3], "First Passed Then Failed", categories["First Passed Then Failed"], 0)
        axs[3].set_xlabel("Test Sequence")

        # Legend
        pass_patch = mpatches.Patch(color='green', label='Pass')
        fail_patch = mpatches.Patch(color='red', label='Fail')
        axs[0].legend(handles=[pass_patch, fail_patch], loc='upper right')

        plt.tight_layout(rect=[0, 0.03, 1, 0.9])  # Leave space for suptitle
        plt.savefig(os.path.join(self.dir_name,"test_result_categories.png"))

    def download_dbs_without_logs(self):
        """Download all DBs without logs."""
        # Filter out devices without logs
        no_log_data = self.data[self.data["has_log"] == False]

        # Get unique dirs that have no logs
        no_log_dirs = no_log_data["dir"]
        print(len(no_log_dirs), "dirs without logs")
        print(no_log_data["dir"].nunique())


        # Download each DB
        for dir in no_log_dirs:
            print("Downloading DBs from", dir)

            contents = self.list_objects(dir)

            dir_name = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))),
                                        "data",dir)
            os.makedirs(dir_name, exist_ok=True)

            # check if any of the contents has "sensors" in the name
            if any(["sensors" in c for c in contents]):
                for c in contents:
                    if "sensors" in c:
                        self.download_file(c, os.path.join(dir_name, os.path.basename(c)))
                continue
            # check if any files has "redis-handler" in the name
            elif any(["redis_handler" in c for c in contents]):
                for c in contents:
                    if "redis_handler" in c:
                        self.download_file(c, os.path.join(dir_name, os.path.basename(c)))
                continue
            # check if any files has "fusion" in the name
            elif any(["fusion" in c for c in contents]):
                print(f"[WARNING] Only fusion files found: {dir}")
                for c in contents:
                    if "fusion" in c:
                        self.download_file(c, os.path.join(dir_name, os.path.basename(c)))
                continue
            else:
                print(f"[WARNING] No files found in {dir} with 'sensors', 'redis-handler', or 'fusion' in the name.")
                continue

    def rerun_no_log_analysis(self):
        """Rerun the analysis for devices without logs."""
        # Filter out devices without logs
        no_log_data = self.data[self.data["has_log"] == False]

        # Get unique dirs that have no logs
        no_log_dirs = no_log_data["dir"].unique()

        for d_idx, dir in enumerate(no_log_dirs):

            print(f"{d_idx+1}/{len(no_log_dirs)} : {dir}")

            dir_name = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))),
                                        "data",dir)
            
            contents = os.listdir(dir_name)
            sensor_file = None

            # check if any of the contents has "sensors" in the name
            if any(["sensors" in c for c in contents]):
                sensor_files = [x for x in contents if ((x[-3:] == ".db") and ("sensors" in x))]
                if len(sensor_files) > 0:
                    sensor_file = sensor_files[0]
            # check if any files has "redis-handler" in the name
            elif any(["redis_handler" in c for c in contents]):
                sensor_files = [x for x in contents if ((x[-3:] == ".db") and ("redis_handler" in x))]
                if len(sensor_files) > 0:
                    sensor_file = sensor_files[0]
            # check if any files has "fusion" in the name
            elif any(["fusion" in c for c in contents]):
                sensor_files = [x for x in contents if ((x[-3:] == ".db") and ("fusion" in x))]
                if len(sensor_files) > 0:
                    sensor_file = sensor_files[0]
            else:
                print(f"[WARNING] No files found in {dir} with 'sensors', 'redis-handler', or 'fusion' in the name.")
                continue

            if sensor_file is None:
                continue

            self.perform_gnss_qa(os.path.join(dir_name,sensor_file), dir)

            self.save_data_to_csv()

    def perform_gnss_qa(self, sensor_file, dir):
        # Perform GNSS QA on the downloaded DB
        logs = {}

        # Connect to the database
        conn = sqlite3.connect(sensor_file)

        # add gnss
        try:
            df = pd.read_sql_query("SELECT * FROM gnss", conn)
            logs["gnss"] = df
        except Exception as e:
            print(f"gnss db error: {e}")
            conn.close()
            time.sleep(1)

            # Fallback: try exporting valid rows via sqlite3 CLI to CSV
            csv_path = os.path.join(os.path.dirname(sensor_file), "gnss_fallback.csv")
            try:
                # Pass SQL commands via input (stdin)
                dump_sql = (
                        ".headers on\n"
                        ".mode csv\n"
                        f".output {csv_path}\n"
                        "SELECT * FROM gnss;\n"
                        ".exit\n"
                    )
                subprocess.run(
                    ["sqlite3", sensor_file],
                    input=dump_sql,
                    text=True,
                    check=True
                )
            except Exception as subproc_err:
                print(f"sqlite3 CLI fallback failed: {subproc_err}")
                logs["gnss"] = None

            if os.path.exists(csv_path):
                df = pd.read_csv(csv_path)
                logs["gnss"] = df
                print(f"Loaded {len(df)} rows from gnss fallback CSV.")
            else:
                print("CSV fallback failed: file not created.")
                logs["gnss"] = None

            conn = sqlite3.connect(sensor_file)

        # add nav_pvt
        try:
            df = pd.read_sql_query("SELECT * FROM nav_pvt", conn)
            logs["nav_pvt"] = df
        except Exception as e:
            print(f"nav_pvt db error: {e}")
            conn.close()
            time.sleep(1)

            # Fallback: try exporting valid rows via sqlite3 CLI to CSV
            csv_path = os.path.join(os.path.dirname(sensor_file), "nav_pvt_fallback.csv")
            try:
                # Pass SQL commands via input (stdin)
                dump_sql = (
                        ".headers on\n"
                        ".mode csv\n"
                        f".output {csv_path}\n"
                        "SELECT * FROM nav_pvt;\n"
                        ".exit\n"
                    )
                subprocess.run(
                    ["sqlite3", sensor_file],
                    input=dump_sql,
                    text=True,
                    check=True
                )
            except Exception as subproc_err:
                print(f"sqlite3 CLI fallback failed: {subproc_err}")
                logs["nav_pvt"] = None

            if os.path.exists(csv_path):
                df = pd.read_csv(csv_path)
                logs["nav_pvt"] = df
                print(f"Loaded {len(df)} rows from gnss fallback CSV.")
            else:
                print("CSV fallback failed: file not created.")
                logs["nav_pvt"] = None

            conn = sqlite3.connect(sensor_file)

        # add nav_status
        try:
            df = pd.read_sql_query("SELECT * FROM nav_status", conn)
            logs["nav_status"] = df
            conn.close()
        except Exception as e:
            print(f"nav_status db error: {e}")
            conn.close()
            time.sleep(1)

            # Fallback: try exporting valid rows via sqlite3 CLI to CSV
            csv_path = os.path.join(os.path.dirname(sensor_file), "nav_status_fallback.csv")
            try:
                # Pass SQL commands via input (stdin)
                dump_sql = (
                        ".headers on\n"
                        ".mode csv\n"
                        f".output {csv_path}\n"
                        "SELECT * FROM nav_status;\n"
                        ".exit\n"
                    )
                subprocess.run(
                    ["sqlite3", sensor_file],
                    input=dump_sql,
                    text=True,
                    check=True
                )
            except Exception as subproc_err:
                print(f"sqlite3 CLI fallback failed: {subproc_err}")
                logs["nav_status"] = None

            if os.path.exists(csv_path):
                df = pd.read_csv(csv_path)
                logs["nav_status"] = df
                print(f"Loaded {len(df)} rows from gnss fallback CSV.")
            else:
                print("CSV fallback failed: file not created.")
                logs["nav_status"] = None
        

        check_sats_seen = self.evaluate_sats_seen(logs["gnss"])
        check_sats_used = self.evaluate_sats_used(logs["gnss"])
        check_pos_error, avg_position_error_m = self.evaluate_position_error(logs["nav_pvt"])
        check_cn0 = self.evaluate_cno(logs["gnss"])
        check_cw_jamming, cw_jamming = self.evaluate_cw_jamming(logs["gnss"])
        check_ttff, ttff = self.evaluate_ttff(logs["nav_status"])
        
        if check_sats_seen is not None:
            self.data.loc[self.data["dir"] == dir, "sats_seen"] = check_sats_seen
        if check_sats_used is not None:
            self.data.loc[self.data["dir"] == dir, "sats_used"] = check_sats_used
        if check_pos_error is not None:
            self.data.loc[self.data["dir"] == dir, "position_error"] = check_pos_error
        if check_cn0 is not None:
            self.data.loc[self.data["dir"] == dir, "cno"] = check_cn0
        if check_cw_jamming is not None:
            self.data.loc[self.data["dir"] == dir, "cw_jamming"] = check_cw_jamming
        if check_ttff is not None:
            self.data.loc[self.data["dir"] == dir, "ttff"] = check_ttff
        if ttff is not None:
            self.data.loc[self.data["dir"] == dir, "ttff_values"] = ttff
        if cw_jamming is not None:
            self.data.loc[self.data["dir"] == dir, "jamming_indicator_values"] = cw_jamming
        if avg_position_error_m is not None:
            self.data.loc[self.data["dir"] == dir, "avg_position_error_m"] = avg_position_error_m

    def evaluate_sats_seen(self, df):

        if df is None:
            return None

        nonzero = df['latitude'] != 0.0
        if nonzero.sum() == 0:
            return False

        # Find start and end of nonzero runs
        change = nonzero.ne(nonzero.shift()).cumsum()
        groups = df[nonzero].groupby(change)

        # Get the first nonzero group
        first_nonzero_group = next(iter(groups))

        seen_values = first_nonzero_group[1]['satellites_seen']
        min_rolling = seen_values.rolling(window=5, min_periods=5).min().shift(-4)

        if max(min_rolling) >= 15:
            return True
        return False

    def evaluate_sats_used(self, df):
        if df is None:
            return None

        nonzero = df['latitude'] != 0.0
        if nonzero.sum() == 0:
            return False

        # Find start and end of nonzero runs
        change = nonzero.ne(nonzero.shift()).cumsum()
        groups = df[nonzero].groupby(change)

        # Get the first nonzero group
        first_nonzero_group = next(iter(groups))

        values = first_nonzero_group[1]['satellites_used']
        min_rolling = values.rolling(window=5, min_periods=5).min().shift(-4)

        if max(min_rolling) >= 5:
            return True
        return False

    def evaluate_cno(self, df):
        if df is None:
            return None

        nonzero = df['latitude'] != 0.0
        if nonzero.sum() == 0:
            return False

        # Find start and end of nonzero runs
        change = nonzero.ne(nonzero.shift()).cumsum()
        groups = df[nonzero].groupby(change)

        # Get the first nonzero group
        first_nonzero_group = next(iter(groups))

        values = first_nonzero_group[1]['cno']
        min_rolling = values.rolling(window=5, min_periods=5).min().shift(-4)

        if max(min_rolling) >= 30:
            return True
        return False
    
    def evaluate_cw_jamming(self, df):
        if df is None:
            return None, None

        nonzero = df['latitude'] != 0.0
        if nonzero.sum() == 0:
            return False, "[]"

        # Find start and end of nonzero runs
        change = nonzero.ne(nonzero.shift()).cumsum()
        groups = df[nonzero].groupby(change)

        # Get the first nonzero group
        first_nonzero_group = next(iter(groups))

        values = first_nonzero_group[1]['rf_jam_ind']
        
        if (values < 250).mean() >= 0.99:
            return True, str(values.tolist())
        return False, str(values.tolist())
    
    def evaluate_position_error(self, df):
        if df is None:
            return None, None

        nonzero = df['lat_deg'] != 0.0
        if nonzero.sum() == 0:
            return False, 999999.

        # Find start and end of nonzero runs
        change = nonzero.ne(nonzero.shift()).cumsum()
        groups = df[nonzero].groupby(change)

        # Get the first nonzero group
        first_nonzero_group = next(iter(groups))

        lats = first_nonzero_group[1]['lat_deg']
        lons = first_nonzero_group[1]['lon_deg']
        alts = first_nonzero_group[1]['hmsl_m']

        true_ecef = self._geodetic_to_ecef(np.array([(40.54584991471907 ,  -79.82566018301341, 260. )]))
        test_ecef = self._geodetic_to_ecef(np.array([lats,
                                                     lons,
                                                     alts]))
        error = np.linalg.norm(test_ecef - true_ecef,axis=0)

        errors = pd.Series(error)

        max_rolling = errors.rolling(window=5, min_periods=5).max().shift(-4)
        if min(max_rolling) <= 50.:
            first_index = (max_rolling < 50).idxmax() if (max_rolling < 50).any() else None

            # Get the next 5 values after that index (inclusive or exclusive)
            if first_index is not None:
                mean_value = errors.iloc[first_index : first_index + 5].mean()
            else:
                mean_value = 999999.
            return True, mean_value
        
        # mean of last 5 error values
        if len(errors) >= 5:
            mean_value = errors.iloc[-5:].mean()
        elif len(errors) > 0:
            mean_value = errors.mean()
        else:
            mean_value = 999999.

        return False, mean_value

    def evaluate_ttff(self, df):
        if df is None:
            return None, None
        
        ttffs = df["ttff"].unique().tolist()
        ttffs = [x/1000. for x in ttffs if x > 0]

        if len(ttffs) < 2:
            return False, str(ttffs)
        if ttffs[-1] <= 90. and ttffs[-2] <= 90.:
            return True, str(ttffs)
        return False, str(ttffs)

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

    def recover_sqlite_db(self, corrupt_db_path, recovered_db_path):
        """
        Recovers a corrupt SQLite database using the .recover command.
        """

        try:
            subprocess.run(f"sqlite3 {corrupt_db_path} .recover | sqlite3 {recovered_db_path}", shell=True, check=True)
            print("Database recovered successfully.")
        except subprocess.CalledProcessError as e:
            print("Error during recovery:", e)

# Example usage:
if __name__ == "__main__":
    bucket = S3BucketBrowser("hb-calib-assets-2025", 'us-west-2')

    # parse GNSS directories
    # bucket.parse_gnss_directories()
    # bucket.load_data_from_csv("/home/derekhive/bee-sensors/gnss/gnss_eol_meta_analysis_20250506133212.csv")
    bucket.load_data_from_csv("/home/derekhive/bee-sensors/results/gnss_eol_meta_analysis/gnss_eol_meta_analysis_20250506151536.csv")
    # print("Parsed GNSS directories:\n", bucket.data)
    # clean data entries
    # bucket.clean_data_entries()

    # download all DBs without logs
    test_columns = ['sats_seen', 'sats_used', 'position_error', 'cno', 'cw_jamming', 'fsync']
    bucket.data["has_log"] = bucket.data[test_columns].notna().any(axis=1)
    # bucket.download_dbs_without_logs()

    bucket.rerun_no_log_analysis()
    
    # # analyze users
    # bucket.analyze_users()

    # # meta analysis
    # bucket.meta_analysis()
    # bucket.save_data_to_csv()

    # plt.show()

