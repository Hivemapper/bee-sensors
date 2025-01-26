
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
    file_len = 0
    with open("/tmp/lte_capture.txt", "r") as lte_file:
        contents = lte_file.read()
        print(contents)
        file_len = len(contents)
        print(file_len)
    if file_len < 75:
        print("[FAIL] LTE didn't respond.")
    else:
        print("[PASS] LTE responsive.")


if __name__ == "__main__":
    print("Checking all services:")
    check_enabled_services()
    print("Re-enabling and testing LTE:")
    test_LTE()
    lte_file_check()
