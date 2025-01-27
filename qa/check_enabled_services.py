#!/usr/bin/python3
#
# check_enabled_services.py
#
# Check script for 
# Get the modem type from assembly
# Final system check before ready to push
#
# Copyright 2024, 2025 Hivemapper, Hellbender Inc
# Some Rights Reserved, see README/LICENSE
#
# Changelog:
# Author Email, Date,     , Comment
# derek       , 2024-12-27, Created
# niessl-HB   , 2025,01-23, Improve the LTE check
#
# Formatted with flake8 --indent-size 4 --max-line-length 119
#

import time
import subprocess


def test_LTE():
    """Stops ODC-API, and LTE restarter. Clears the lte log. Checks that the modem is responsive"""
    subprocess.run(["systemctl", "stop", "odc-api"])
    time.sleep(3)
    subprocess.run(["systemctl", "stop", "lte"])
    time.sleep(2)
    subprocess.run(["rm", "-f", "/data/recording/lte-status.log"])
    time.sleep(1)
    subprocess.run(["systemctl", "start", "lte"])
    print("Waiting 10 seconds for initial LTE log write")
    time.sleep(10)
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
                         ]

    enabled_services = get_enabled_services()
    for service in required_services:
        service_name = f"{service}.service"
        if service_name not in enabled_services:
            print(f"[FAIL] Service {service} is not enabled.")
        else:
            print(f"[PASS] Service {service} is enabled.")


def lte_file_check():
    """Checks for OK responses from initial AT commands for modem config"""
    command_set_check = ["'AT'",
                         "'AT#USBCFG?'",
                         "'AT+GMM'",
                        ]
    command_result_map = {}
    contents = []
    with open("/tmp/lte_capture.txt", "r") as lte_file:
        contents = lte_file.readlines()
    
    # Go through the log, and verify responses were valid for each command
    previous_command = ""
    for line in contents:
        if previous_command is not "":
            print(line)
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


def main():
    print("Checking all services:")
    check_enabled_services()
    print("Re-enabling and testing LTE:")
    test_LTE()
    lte_file_check()
    
if __name__ == "__main__":
    main()
