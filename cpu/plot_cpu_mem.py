import os
from collections import defaultdict

import matplotlib.pyplot as plt

def parse_log_file(file_path):
    """
    Parse the log file to extract CPU and memory usage over time for each process.
    """
    data_collection = []
    timestamp_colleciton = []

    data = defaultdict(list)  # Stores data as {timestamp: {process: {cpu, memory}}}
    timestamps = []

    with open(file_path, "r") as file:

        for line in file:
            # header line.
            if "timestamp" in line:
                data_collection.append(data)
                timestamp_colleciton.append(timestamps)
                data = defaultdict(list)  # Stores data as {timestamp: {process: {cpu, memory}}}
                timestamps = []
                continue
            
            elements = line.split(",")
            timestamp = elements[0]
            if timestamp not in timestamps:
                timestamps.append(timestamp)
                data[timestamp] = {}
            data[timestamp][elements[2]] = {"cpu": float(elements[3]), "memory": float(elements[4])}

    data_collection.append(data)
    timestamp_colleciton.append(timestamps)

    return data_collection, timestamp_colleciton

def plot_stacked_bar(data, timestamps, index, dir=None):
    """
    Generate stacked bar charts for total CPU and memory usage over time.
    """
    # Prepare data for plotting
    cpu_stacks = defaultdict(list)
    memory_stacks = defaultdict(list)
    process_names = set()
    required_services = [
                    "depthai_gate",
                    "map-ai.sh",
                    "folder-purger",
                    "beekeeper-plugin",
                    "odc-api",
                    "datalogger",
                    "redis-server",
                    "RedisHandler",
                    "depthai-device",
                    "map-ai.py",
                    ]
    for serv in required_services:
        process_names.add(serv)

    for timestamp in timestamps:
        for process, metrics in data[timestamp].items():
            process = process.strip()
            process_names.add(process)
            cpu_stacks[process].append(metrics["cpu"])
            memory_stacks[process].append(metrics["memory"])

        # Ensure every process has a value at every timestamp (fill missing with 0)
        for process in process_names:
            if process not in data[timestamp]:
                cpu_stacks[process].append(0)
                memory_stacks[process].append(0)

    # Plot CPU usage
    plt.figure(figsize=(12, 6))
    bottom = [0] * len(timestamps)  # To stack bars
    for process in required_services:
        values = cpu_stacks[process]
        plt.bar(timestamps, values, bottom=bottom, label=process)
        bottom = [sum(x) for x in zip(bottom, values)]
    plt.title("Total CPU Usage Over Time")
    plt.xlabel("Timestamp")
    plt.ylabel("CPU %")
    plt.xticks(rotation=45, ha="right")
    plt.legend(loc="upper left", bbox_to_anchor=(1, 1), title="Processes")
    plt.tight_layout()
    if dir is not None:
        plt.savefig(f"{dir}/stacked_cpu_usage_{index}.png")
    else:
        plt.savefig(f"stacked_cpu_usage_{index}.png")

    plt.close()

    # Plot Memory usage
    plt.figure(figsize=(12, 6))
    bottom = [0] * len(timestamps)  # Reset bottom for memory
    for process in required_services:
        values = memory_stacks[process]
        plt.bar(timestamps, values, bottom=bottom, label=process)
        bottom = [sum(x) for x in zip(bottom, values)]
    plt.title("Total Memory Usage Over Time")
    plt.xlabel("Timestamp")
    plt.ylabel("Memory %")
    plt.xticks(rotation=45, ha="right")
    plt.legend(loc="upper left", bbox_to_anchor=(1, 1), title="Processes")
    plt.tight_layout()
    if dir is not None:
        plt.savefig(f"{dir}/stacked_memory_usage_{index}.png")
    else:
        plt.savefig(f"stacked_memory_usage_{index}.png")

    plt.close()

def main():
    # Path to the log file
    log_file_path = "cpu-mem-logger-ambititious-plum-beaver-20250303-102134.log"
    dir_path = log_file_path.split(".")[0]
    os.makedirs(dir_path, exist_ok=True)


    # Parse the log file
    data, timestamps = parse_log_file(log_file_path)

    # Plot the data
    for i in range(len(data)):
        plot_stacked_bar(data[i], timestamps[i],i+1, dir=dir_path)

    plt.show()

if __name__ == "__main__":
    main()
