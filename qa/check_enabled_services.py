#!/usr/bin/python3
#
# check_enabled_services.py
#
# Check script for modem and enabled services
#
# Copyright 2024, 2025 Hivemapper, Hellbender Inc
# Some Rights Reserved, see README/LICENSE
#
# Changelog:
# Author Email, Date,     , Comment
# derek       , 2024-12-27, Created
# cniessl-HB  , 2025-01-23, Improve the LTE check
#
# Formatted with flake8 --indent-size 4 --max-line-length 119
#

import time
import json
import sqlite3
import subprocess


def test_LTE():
    """Stops ODC-API, and LTE restarter. Clears the lte log. Checks that the modem is responsive"""
    subprocess.run(["systemctl", "stop", "odc-api"])
    time.sleep(3)
    subprocess.run(["systemctl", "stop", "lte"])
    time.sleep(2)
    subprocess.run(["rm", "-f", "/data/recording/lte-status.log"])
    time.sleep(1)
    subprocess.run(["systemctl", "enable", "lte"])
    time.sleep(2)
    subprocess.run(["systemctl", "start", "lte"])
    print("Waiting 30 seconds for initial LTE log write")
    time.sleep(35)
    subprocess.run(["sync"])
    subprocess.run(["cp", "/data/recording/lte-status.log", "/tmp/lte_capture.txt"])


def get_enabled_services():
    """Gets a list of enabled systemctl services."""

    result = subprocess.run(["systemctl", "list-unit-files", "--type=service", "--state=enabled"],
                            capture_output=True, text=True)

    if result.returncode == 0:
        services = []
        for line in result.stdout.splitlines():
            if "enabled" not in line:
                continue
            services.append(line.split()[0])
        return services
    else:
        raise Exception(f"Error running systemctl: {result.stderr}")


def check_enabled_services():
    """Checks if the required services are enabled."""

    required_services = [
                         "depthai_gate",
                         "hivemapper-data-logger",
                         "hivemapper-folder-purger",
                         "lte",
                         "map-ai",
                         "odc-api",
                         "redis",
                         "redis-handler",
                         "cpu-mem-logger",
                         ]
    
    disabled_services = [
                         "gnss-eol-test",
                        ]
    enabled_services = get_enabled_services()
    for service in required_services:
        service_name = f"{service}.service"
        if service_name not in enabled_services:
            print(f"[FAIL] Service {service} is not enabled.")
        else:
            print(f"[PASS] Service {service} is enabled.")
    for service in disabled_services:
        service_name = f"{service}.service"
        if service_name in enabled_services:
            print(f"[FAIL] Service {service} is enabled.")
        else:
            print(f"[PASS] Service {service} is disabled.")


def lte_file_check():
    """Checks for OK responses from initial AT commands for modem config"""
    command_set_check = ["'AT#USBCFG?'",
                         "'AT+GMM'"]
    command_result_map = {}
    contents = []
    with open("/tmp/lte_capture.txt", "r") as lte_file:
        contents = lte_file.readlines()

    # Go through the log, and verify responses were valid for each command
    previous_command = ""
    for line in contents:
        if previous_command != "":
            is_ok = ("\\r\\nOK\\r\\n" in line)
            if previous_command not in command_result_map:
                command_result_map[previous_command] = is_ok
            else:
                was_ok = command_result_map[previous_command]
                command_result_map[previous_command] = is_ok and was_ok
            previous_command = ""
            continue
        for at_command in command_set_check:
            if at_command in line:
                previous_command = at_command
                break

    # Confirm we got OK results for the minimum configuration commands
    for command in command_set_check:
        if command not in command_result_map:
            print(f"{command} not found")
            print("[FAIL] LTE command didn't respond")
            return
        if not command_result_map[command]:
            print(f"{command} didn't return OK")
            print("[FAIL] LTE command didn't return OK")
            return

    print("[PASS] LTE responsive.")

def geq(ver1, ver2):
    """ Returns true if ver1 >= ver2"""
    x1,y1,z1 = ver1.split(".")
    x2,y2,z2 = ver2.split(".")
    
    if int(x1) > int(x2):
        return True
    if int(x1) < int(x2):
        return False
    if int(y1) > int(y2):
        return True
    if int(y1) < int(y2):
        return False
    if int(z1) > int(z2):
        return True
    if int(z1) < int(z2):
        return False    
    return True

def less_than(ver1, ver2):
    """ Returns true if ver1 < ver2"""
    return not geq(ver1, ver2)

def get_json_config(key):
    """Get a value from the JSON configuration file.

    Returns None if key is not found or 
    file doesn't exist.

    Parameters
    ----------
    key : str
        The key to get from the JSON configuration file

    Returns
    -------
    value
        The value of the key in the JSON configuration file
    
    """

    config_file_path = "/opt/dashcam/bin/config.json"
    try:
        with open(config_file_path, "r") as f:
            config = json.load(f)
            value = config[key]
    except FileNotFoundError:
        return None
    except KeyError:
        return None

    return value

def enable_bk(db_path, plugin_name, state):
    conn = None
    try:   
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        sql_command = "INSERT OR REPLACE INTO plugins (plugin, state) VALUES (?, ?);"
        cursor.execute(sql_command, (plugin_name, state))

        conn.commit()
        print(f"Rows affected: {cursor.rowcount}")
    except Exception as e:
        print(f"An error occurred while changing {plugin_name} plugin: {e}")
        print(f"[FAIL] Failed to switch {plugin_name} plugin to {state}.")
        return
    finally:
        if conn:
            conn.close()

    print(f"[PASS] {plugin_name} plugin {state}.")

def main():
    
    with open("/etc/build_info.json") as file:
        build_info = json.load(file)
    firmware_version = build_info["odc-version"]

    if geq(firmware_version, "5.2.0"):
        db_path = get_json_config("ODC_API_DB_PATH")
        plugin_name = "beekeeper-plugin"
        state = "enabled"
        enable_bk(db_path, plugin_name, state)

    print("Re-enabling and testing LTE:")
    test_LTE()
    lte_file_check()
    print("Checking all services:")
    check_enabled_services()

if __name__ == "__main__":
    main()
