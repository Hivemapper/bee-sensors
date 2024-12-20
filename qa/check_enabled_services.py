import subprocess

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

if __name__ == "__main__":
    check_enabled_services()