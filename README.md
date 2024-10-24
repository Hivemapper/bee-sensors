# bee-sensors

Utilities for sensor (GNSS, IMU, etc.) processing and fusion on the Bee.

### Table of Contents
- [GNSS](#gnss)
    - [Parse U-blox Messages](#parse-u-blox-messages)
    - [Flash U-blox Configuration](#flash-u-blox-configuration)
    - [Save U-blox Configuration to File](#save-u-blox-configuration-to-file)
    - [Compare Two U-blox Configurations](#compare-two-u-blox-configurations)

## GNSS


### Parse U-blox Messages

Use the `ubx_parser.py` file to convert all messages from `.ubx` to `.csv` files. Will correlate the GPS time to each navigation epoch and discard any epoch without a valid GPS time message. It takes a single parameter:
- `-i`, `--input` is the path to the UBX file to parse

Example use:
```
python3 ubx_parser.py -i UBX_MESSAGES.ubx
```

### Flash U-blox Configuration

Use the `ubx_flash_cfg.sh` file to flash updated configuration parameters to the dashcam with `gpsd`.

Before running, update the `ubxtoolCmd`, `address`, and `port` variables that correspond to the location of the installed `ubxtool` executable and dashcam address/port

### Save U-blox Configuration to File

Use the `ubx_get_cfg.py` file to save all possible configuration parameters on the u-blox GNSS module to a csv file.

#### Requirements
- installation of `ubxtool` that's included in [gpsd](https://gpsd.gitlab.io/gpsd/building.html) version 3.25 (version is important)
 - `gpsd` running on the dascham:  
    On the old Bee devices:
    ```
    gpsd -G -S 9090 --speed 460800 -D 4 -n -N /dev/ttyS2
    ```
    On the newer Bee devices (do not need to specify baudrate):
    ```
    gpsd -G -S 9090 -D 4 -n -N /dev/ttyS2
    ```
#### Running
The user must three parameters when running from the command line:
- `-d` or `--dashcam` is the address + port of the dashcam from which to read
- `-u` or `--ublox` is the **full** filepath to the ubxtool command. Relative paths don't seem to work. It'll need to be the full path, i.e. you can copy whatever `realpath ./ubxtool` outputs
- `-n` or `--name` is the name to append to the cfg file. It will default to "dashcam" if nothing is input

Example use:
```
python3 get_ubx_cfg.py --dashcam 192.168.0.10:9090 --ublox /home/derek/gnss/gpsd-3.25/gpsd-3.25/clients/ubxtool --name active
```

### Compare Two U-blox Configurations

Use the `ubx_compare_cfg.py` file to compare two csv configuration files. The user must pass in the paths to two configuration files and that paths may be relative.
- `--cfg1` is the path to the first configuration file
- `--cfg2` is the path to the second configuration file

Example use:
```
python3 compare_ubx_cfg.py --cfg1 ubx_cfg_passive.csv --cfg2 ubx_cfg_active.csv
```
