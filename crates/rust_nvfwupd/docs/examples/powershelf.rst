PowerShelf Platform Update Examples
------------------------------------

NVFWUPD 2.0.9 introduces support for Delta and LiteOn PowerShelf platforms. NVFWUPD 2.1.2 adds support for Megmeet PowerShelf platforms. Firmware updates use Redfish APIs.

Supported PowerShelf Platforms
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

PowerShelf platforms from various manufacturers are supported, including:

- **Delta PowerShelf**: Delta power distribution and management systems
- **LiteOn PowerShelf**: LiteOn power distribution and management systems
- **Megmeet PowerShelf**: Megmeet power distribution and management systems

All PowerShelf platforms use the same servertype: ``Powershelf``


Displaying Current PowerShelf Firmware Versions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To display the current firmware versions on a PowerShelf platform, use the ``show_version`` command with servertype ``Powershelf``:

Example 1: Delta PowerShelf
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block::

    $ nvfwupd -t ip=<PowerShelf BMC IP> user=***** password=***** servertype=Powershelf show_version -p nvidia-pmc-3.2.3-RELEASE.FOR.UPDATE.CEC-CFG.fwpkg

    System Model: AHE50V660A33KWL
    Part number: $TRAY_PART_NUMBER
    Serial number: $TRAY_SERIAL_NUMBER
    Packages: ['3.2.3']
    Connection Status: Successful

    Firmware Devices:
    AP Name           Sys Version     Pkg Version   Up-To-Date
    -------           -----------     -----------   ----------
    FW_ERoT_BMC_0     1203            N/A           No        
    FW_PMC_alt        3.2.4           3.2.3         Yes       
    FW_PMC_primary    3.2.4           3.2.3         Yes       
    FW_PSU_1          01050105        N/A           No        
    FW_PSU_2          01050105        N/A           No        
    FW_PSU_3          01050105        N/A           No        
    FW_PSU_4          01050105        N/A           No        
    FW_PSU_5          01050105        N/A           No        
    FW_PSU_6          01050105        N/A           No        
    ----------------------------------------------------------
    Error Code: 0

Example 2: LiteOn PowerShelf
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block::

    $ nvfwupd -t ip=<PowerShelf BMC IP> user=*** password=*** servertype=Powershelf show_version -p cm14mp2rd-r1.3.9-rc1.tar 
    System Model: PF-1333-13RD
    Part number: $TRAY_PART_NUMBER
    Serial number: $TRAY_SERIAL_NUMBER
    Packages: ['LiteOnPMCImages/cm14mp2rd-r1.3.9-rc1.tar']
    Connection Status: Successful

    Firmware Devices:
    AP Name         Sys Version    Pkg Version   Up-To-Date
    -------         -----------    -----------   ----------
    mcu             0              N/A           No        
    pmc_86ae677a    r1.3.9         r1.3.9-rc1    No        
    powerdevice0    0x1 / 0x1      N/A           No        
    powerdevice1    0x1 / 0x1      N/A           No        
    powerdevice2    0x1 / 0x1      N/A           No        
    powerdevice3    0x1 / 0x1      N/A           No        
    powerdevice4    0x1 / 0x1      N/A           No        
    powerdevice5    0x1 / 0x1      N/A           No        
    ----------------------------------------------------------
    Error Code: 0

Example 3: Megmeet PowerShelf
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. note::
    Megmeet PowerShelf PMC package versioning is not supported in the show_version output. As a result, the package version field displays N/A. Support will be added in a future release.

.. code-block::

    $ nvfwupd -t ip=<PowerShelf BMC IP> user=*** password=*** servertype=Powershelf show_version -p PMCUpgrade/PMC-V03B02D00.fwpkg
    System Model: MPSC6000-MCU
    Part number: $TRAY_PART_NUMBER
    Serial number: $TRAY_SERIAL_NUMBER
    Packages: ['V03B02']
    Connection Status: Successful

    Firmware Devices:
    AP Name            Sys Version            Pkg Version     Up-To-Date
    -------            -----------            -----------     ----------
    backupbmc_active   V99B02                 N/A             No
    bios_active        NA                     N/A             No
    bmc_active         V03B02                 N/A             No
    cpld_active        NA                     N/A             No
    de3fdd82           V01                    N/A             No
    mcu_active         V02B01                 N/A             No
    psu_active_1       PFC:V00B05/DCDC:V00B05 N/A             No
    psu_active_2       PFC:V00B05/DCDC:V00B05 N/A             No
    psu_active_3       PFC:V00B05/DCDC:V00B05 N/A             No
    psu_active_4       PFC:V00B05/DCDC:V00B05 N/A             No
    psu_active_5       PFC:V00B05/DCDC:V00B05 N/A             No
    psu_active_6       PFC:V00B05/DCDC:V00B05 N/A             No
    ------------------------------------------------------------------------------------
    Error Code: 0

Updating PowerShelf Firmware
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To update components on a PowerShelf platform, run the ``update_fw`` command without specifying individual targets:

**LiteOn PMC Update:**

LiteOn PMC updates begin immediately and return a task id that cannot be used to monitor the update progress as the PMC becomes unresponsive until the update is complete.

.. code-block::

    $ nvfwupd -t ip=<PowerShelf BMC IP> user=*** password=*** servertype=Powershelf update_fw -p cm14mp2rd-r1.3.9-rc1.tar
    Updating ip address: ip=XXXX
    FW package: ['LiteOnPMCImages/cm14mp2rd-r1.3.9-rc1.tar']
    Ok to proceed with firmware update? <Y/N>
    y
    FW update started, Task Id: 1
    Firmware update in progress
    use show_update_progress command to monitor the progress
    ------------------------------------------------------------------------------------
    Error Code: 0

    ~ Wait approximately 20 minutes for the PMC update to complete.

    $ nvfwupd -t ip=<PowerShelf BMC IP> user=*** password=*** servertype=Powershelf show_version -p cm14mp2rd-r1.3.9-rc1.tar 
    System Model: PF-1333-13RD
    Part number: $TRAY_PART_NUMBER
    Serial number: $TRAY_SERIAL_NUMBER
    Packages: ['LiteOnPMCImages/cm14mp2rd-r1.3.9-rc1.tar']
    Connection Status: Successful

    Firmware Devices:
    AP Name         Sys Version    Pkg Version   Up-To-Date
    -------         -----------    -----------   ----------
    mcu             0              N/A           No        
    pmc_86ae677a    r1.3.9-rc1     r1.3.9-rc1    Yes       
    powerdevice0    0x1 / 0x1      N/A           No        
    powerdevice1    0x1 / 0x1      N/A           No        
    powerdevice2    0x1 / 0x1      N/A           No        
    powerdevice3    0x1 / 0x1      N/A           No        
    powerdevice4    0x1 / 0x1      N/A           No        
    powerdevice5    0x1 / 0x1      N/A           No        
    ----------------------------------------------------------
    Error Code: 0

**Delta PMC Update:**

Delta PMC updates may be initially monitored using the ``show_update_progress`` command with task ID, but eventually the PMC will become unresponsive after performing an auto reboot for activation.

.. code-block::

    $ nvfwupd -t ip=<PowerShelf BMC IP> user=*** password=*** show_version -p DeltaPR3PSUS/nvidia-pmc-3.2.3-RELEASE.FOR.UPDATE.CEC-CFG.fwpkg
    System Model: AHE50V660A33KWL
    Part number: $TRAY_PART_NUMBER
    Serial number: $TRAY_SERIAL_NUMBER
    Packages: ['3.2.3']
    Connection Status: Successful

    Firmware Devices:
    AP Name          Sys Version    Pkg Version   Up-To-Date
    -------          -----------    -----------   ----------
    FW_ERoT_BMC_0    1203           N/A          No        
    FW_PMC_alt       3.2.4         3.2.3         Yes       
    FW_PMC_primary   3.2.4         3.2.3         Yes       
    FW_PSU_1         01050105      N/A           No        
    FW_PSU_2         01050105      N/A           No        
    FW_PSU_3         01050105      N/A           No        
    FW_PSU_4         01050105      N/A           No        
    FW_PSU_5         01050105      N/A           No        
    FW_PSU_6         01050105      N/A           No        
    ----------------------------------------------------------
    Error Code: 0

    $ nvfwupd -t ip=<PowerShelf BMC IP> user=*** password=*** update_fw -p DeltaPR3PSUS/nvidia-pmc-3.2.3-RELEASE.FOR.UPDATE.CEC-CFG.fwpkg
    Updating ip address: ip=XXXX
    FW package: ['DeltaPR3PSUS/nvidia-pmc-3.2.3-RELEASE.FOR.UPDATE.CEC-CFG.fwpkg']
    Ok to proceed with firmware update? <Y/N>
    y
    FW update started, Task Id: 3
    Firmware update in progress
    use show_update_progress command to monitor the progress
    ------------------------------------------------------------------------------------
    Error Code: 0

    $ nvfwupd -t ip=<PowerShelf BMC IP> user=*** password=*** show_update_progress -i 3
    Task Info for Id: 3
    StartTime: 2024-05-02T15:45:44+00:00
    TaskState: Running
    PercentComplete: 49
    TaskStatus: OK
    Overall Task Status: {
        "@odata.id": "/redfish/v1/TaskService/Tasks/3",
        "@odata.type": "#Task.v1_7_4.Task",
        "HidePayload": false,
        "Id": "3",
        "Messages": [
            {
                "@odata.type": "#Message.v1_1_1.Message",
                "Message": "The task with Id '3' has started.",
                "MessageArgs": [
                    "0"
                ],
                "MessageId": "TaskEvent.1.0.3.TaskStarted",
                "MessageSeverity": "OK",
                "Resolution": "None."
            },
            {
                "@odata.type": "#Message.v1_1_1.Message",
                "Message": "The task with Id '3' has changed to progress 10 percent complete.",
                "MessageArgs": [
                    "0",
                    "10"
                ],
                "MessageId": "TaskEvent.1.0.3.TaskProgressChanged",
                "MessageSeverity": "OK",
                "Resolution": "None."
            },
            {
                "@odata.type": "#Message.v1_1_1.Message",
                "Message": "The task with Id '3' has changed to progress 23 percent complete.",
                "MessageArgs": [
                    "0",
                    "23"
                ],
                "MessageId": "TaskEvent.1.0.3.TaskProgressChanged",
                "MessageSeverity": "OK",
                "Resolution": "None."
            },
            {
                "@odata.type": "#Message.v1_1_1.Message",
                "Message": "The task with Id '3' has changed to progress 36 percent complete.",
                "MessageArgs": [
                    "0",
                    "36"
                ],
                "MessageId": "TaskEvent.1.0.3.TaskProgressChanged",
                "MessageSeverity": "OK",
                "Resolution": "None."
            },
            {
                "@odata.type": "#Message.v1_1_1.Message",
                "Message": "The task with Id '3' has changed to progress 49 percent complete.",
                "MessageArgs": [
                    "0",
                    "49"
                ],
                "MessageId": "TaskEvent.1.0.3.TaskProgressChanged",
                "MessageSeverity": "OK",
                "Resolution": "None."
            }
        ],
        "Name": "Task 3",
        "Payload": {
            "HttpHeaders": [],
            "HttpOperation": "POST",
            "JsonBody": "null",
            "TargetUri": "/redfish/v1/UpdateService/update"
        },
        "PercentComplete": 49,
        "StartTime": "2024-05-02T15:45:44+00:00",
        "TaskMonitor": "/redfish/v1/TaskService/Tasks/3/Monitor",
        "TaskState": "Running",
        "TaskStatus": "OK"
    }
    Update is still running.
    ------------------------------------------------------------------------------------
    Error Code: 0

    $ nvfwupd -t ip=<PowerShelf BMC IP> user=*** password=*** show_version -p DeltaPR3PSUS/nvidia-pmc-3.2.3-RELEASE.FOR.UPDATE.CEC-CFG.fwpkg
    System Model: AHE50V660A33KWL
    Part number: $TRAY_PART_NUMBER
    Serial number: $TRAY_SERIAL_NUMBER
    Packages: ['3.2.3']
    Connection Status: Successful

    Firmware Devices:
    AP Name          Sys Version   Pkg Version   Up-To-Date
    -------          -----------   -----------   ----------
    44ecaa94         01050105      N/A           No        
    62031a50         01040104      N/A           No        
    FW_ERoT_BMC_0    1203          N/A           No        
    FW_PMC_alt       3.2.4         3.2.3         Yes       
    FW_PMC_primary   3.2.3         3.2.3         Yes       
    ----------------------------------------------------------
    Error Code: 0

**Megmeet PMC Update:**

Megmeet PMC updates start immediately and return a task ID. That task ID cannot be used to monitor progress because the PMC remains unresponsive until the update completes.

.. code-block::

    $ nvfwupd -t ip=<PowerShelf BMC IP> user=*** password=*** show_version -p PMC-V99B02D00-test.fwpkg
    System Model: MPSC6000-MCU
    Part number: $TRAY_PART_NUMBER
    Serial number: $TRAY_SERIAL_NUMBER
    Packages: ['V99B02']
    Connection Status: Successful

    Firmware Devices:
    AP Name            Sys Version                Pkg Version      Up-To-Date
    -------            -----------                -----------      ----------
    backupbmc_active   V03B02                     N/A              No        
    bios_active        NA                         N/A              No        
    bmc_active         V03B02                     N/A              No        
    cpld_active        NA                         N/A              No        
    mcu_active         V02B01                     N/A              No        
    psu_active_1       PFC:V99B05/DCDC:V99B05     N/A              No        
    psu_active_2       PFC:V99B05/DCDC:V99B05     N/A              No        
    psu_active_3       PFC:V99B05/DCDC:V99B05     N/A              No        
    psu_active_4       PFC:V99B05/DCDC:V99B05     N/A              No        
    psu_active_5       PFC:V99B05/DCDC:V99B05     N/A              No        
    psu_active_6       PFC:V99B05/DCDC:V99B05     N/A              No        
    -------------------------------------------------------------------------
    Error Code: 0


    $ nvfwupd -t ip=<PowerShelf BMC IP> user=*** password=*** update_fw -p PMC-V99B02D00-test.fwpkg
    Updating ip address: ip=XXXX
    FW package: ['PMC-V99B02D00-test.fwpkg']
    Ok to proceed with firmware update? <Y/N>
    y
    ApplyTime set to 'Immediate' successfully.
    FW update started, Task Id: 1
    Firmware update in progress
    use show_update_progress command to monitor the progress
    ------------------------------------------------------------------------------------
    Error Code: 0

    ~ Wait approximately 10 minutes for the PMC update to complete. bmc_active will be updated.

    $ nvfwupd -t ip=<PowerShelf BMC IP> user=*** password=*** show_version -p PMC-V99B02D00-test.fwpkg
    System Model: MPSC6000-MCU
    Part number: $TRAY_PART_NUMBER
    Serial number: $TRAY_SERIAL_NUMBER
    Packages: ['V99B02']
    Connection Status: Successful

    Firmware Devices:
    AP Name             Sys Version                Pkg Version      Up-To-Date
    -------             -----------                -----------      ----------
    backupbmc_active    V03B02                     N/A              No        
    bios_active         NA                         N/A              No        
    bmc_active          V99B02                     N/A              No        
    cpld_active         NA                         N/A              No        
    mcu_active          V02B01                     N/A              No        
    psu_active_1        PFC:V99B05/DCDC:V99B05     N/A              No        
    psu_active_2        PFC:V99B05/DCDC:V99B05     N/A              No        
    psu_active_3        PFC:V99B05/DCDC:V99B05     N/A              No        
    psu_active_4        PFC:V99B05/DCDC:V99B05     N/A              No        
    psu_active_5        PFC:V99B05/DCDC:V99B05     N/A              No        
    psu_active_6        PFC:V99B05/DCDC:V99B05     N/A              No        
    -------------------------------------------------------------------------
    Error Code: 0


**LiteOn PSU Update:**

LiteOn PSUs may only be updated one at a time. To select the PSU to update, a special JSON file containing the "LiteOnPowerDeviceId" value is required. The index value matches exactly to the powerdevice index in the firmware inventory.

.. note::
    Currently, LiteOn PSU package versioning in the ``show_version`` output is not supported, therefore "N/A" is expected for the package version field. This will be improved in a future release.

.. code-block::

    $ nvfwupd -t ip=<PowerShelf BMC IP> user=*** password=*** show_version -p LiteOnPSUPackages/SP-2552-7RD_F1F1_Bootloader_20250611.D2DAppA.tar
    System Model: PF-1333-13RD
    Part number: $TRAY_PART_NUMBER
    Serial number: $TRAY_SERIAL_NUMBER
    Packages: ['LiteOnPSUPackages/SP-2552-7RD_F1F1_Bootloader_20250611.D2DAppA.tar']
    Connection Status: Successful

    Firmware Devices:
    AP Name          Sys Version   Pkg Version   Up-To-Date
    -------          -----------   -----------   ----------
    mcu              0             N/A           No        
    pmc_6afb1cf5     r1.3.9-rc1    N/A           No        
    powerdevice0     0x1 / 0x1     N/A           No        
    powerdevice1     0x1 / 0x1     N/A           No        
    powerdevice2     0x1 / 0x1     N/A           No        
    powerdevice3     0x1 / 0x1     N/A           No        
    powerdevice4     0x1 / 0x1     N/A           No        
    powerdevice5     0x1 / 0x1     N/A           No        
    ----------------------------------------------------------
    Error Code: 0

.. note::
    1. Setting the "LiteOnPowerDeviceId" value in the special JSON file is mandatory for LiteOn PSU updates.
    2. Only a single LiteOn PSU can be updated at a time.
    3. The index value must match exactly to the powerdevice index in the firmware inventory.
        - For example, to update "powerdevice0", set the "LiteOnPowerDeviceId" value to "0".
        - To update "powerdevice1", set the "LiteOnPowerDeviceId" value to "1", and so on.

.. code-block::

    cat liteon_psu_device0.json
    {
        "LiteOnPowerDeviceId": "0"
    }

    $ nvfwupd -t ip=<PowerShelf BMC IP> user=*** password=*** update_fw -p LiteOnPSUPackages/SP-2552-7RD_F1F1_Bootloader_20250611.D2DAppA.tar -s liteon_psu_device0.json
    Updating ip address: ip=XXXX
    yFW package: ['LiteOnPSUPackages/SP-2552-7RD_F1F1_Bootloader_20250611.D2DAppA.tar']
    Ok to proceed with firmware update? <Y/N>
    y
    Performing LiteOn PSU firmware update (Device ID: 0)...
    LiteOn PSU firmware update started successfully (Device ID: 0)
    Run "show_update_progress -i powerdevice0" to monitor update progress.
    ------------------------------------------------------------------------------------
    Error Code: 0

    $ nvfwupd -t ip=<PowerShelf BMC IP> user=*** password=*** show_update_progress -i powerdevice0
    LiteOn PSU Update Info for Device: powerdevice0
    Name: power unit 0
    Model: SP-2552-7RD
    SerialNumber: 6PL7Y01X15284JW
    Version: 0x1 / 0x1
    Status: OK
    State: Boot
    UpdateProgress: 3%
    Update is in progress.
    Full Response: {
        "@odata.id": "/redfish/v1/Chassis/powershelf/PowerUnits/powerdevice0",
        "@odata.type": "#LiteOn.v1_0_0.PowerUnit",
        "Id": "0",
        "Model": "SP-2552-7RD",
        "Name": "power unit 0",
        "SerialNumber": "6PL7Y01X15284JW",
        "Version": "0x1 / 0x1",
        "ambient": {
            "Unit": "\u00b0C",
            "Value": 26.0
        },
        "exhaust": {
            "Unit": "\u00b0C",
            "Value": 30.25
        },
        "fan1": {
            "Unit": "RPMS",
            "Value": 14720.0
        },
        "freqin": {
            "Unit": "Hz",
            "Value": 60.0
        },
        "hotspot": {
            "Unit": "\u00b0C",
            "Value": 31.0
        },
        "iin": {
            "Unit": "A",
            "Value": 0.5
        },
        "iout": {
            "Unit": "A",
            "Value": 0.64
        },
        "pin": {
            "Unit": "W",
            "Value": 56.68
        },
        "pout": {
            "Unit": "W",
            "Value": 41.5
        },
        "state": "Boot",
        "status": "OK",
        "updateProgress": 3,
        "vin": {
            "Unit": "V",
            "Value": 242.0
        },
        "vout": {
            "Unit": "V",
            "Value": 50.38
        }
    }
    ------------------------------------------------------------------------------------
    Error Code: 0

After the update is complete, the PSU version is updated to 0xf1.

.. code-block::

    $ nvfwupd -t ip=<PowerShelf BMC IP> user=*** password=*** show_version -p LiteOnPSUPackages/SP-2552-7RD_F1F1_Bootloader_20250611.D2DAppA.tar
    System Model: PF-1333-13RD
    Part number: $TRAY_PART_NUMBER
    Serial number: $TRAY_SERIAL_NUMBER
    Packages: ['LiteOnPSUPackages/SP-2552-7RD_F1F1_Bootloader_20250611.D2DAppA.tar']
    Connection Status: Successful

    Firmware Devices:
    AP Name          Sys Version   Pkg Version   Up-To-Date
    -------          -----------   -----------   ----------
    mcu              0             N/A           No        
    pmc_6afb1cf5     r1.3.9-rc1    N/A           No        
    powerdevice0     0xf1 / 0xf1   N/A           No        
    powerdevice1     0x1 / 0x1     N/A           No        
    powerdevice2     0x1 / 0x1     N/A           No        
    powerdevice3     0x1 / 0x1     N/A           No        
    powerdevice4     0x1 / 0x1     N/A           No        
    powerdevice5     0x1 / 0x1     N/A           No        
    ----------------------------------------------------------
    Error Code: 0

**Delta PSU Update:**

Delta PSUs update simultaneously and no special JSON file is required. You can monitor the update progress using the ``show_update_progress`` command with task ID.

.. code-block::

    $ nvfwupd -t ip=<PowerShelf BMC IP> user=*** password=*** show_version -p DeltaPR3PSUS/Nvidia_GB300_5500W_APP_01050105_20251013.hex.tar 
    System Model: AHE50V660A33KWL
    Part number: $TRAY_PART_NUMBER
    Serial number: $TRAY_SERIAL_NUMBER
    Packages: ['DeltaPR3PSUS/Nvidia_GB300_5500W_APP_01050105_20251013.hex.tar']
    Connection Status: Successful

    Firmware Devices:
    AP Name          Sys Version   Pkg Version   Up-To-Date
    -------          -----------   -----------   ----------
    FW_ERoT_BMC_0    1203          N/A           No        
    FW_PMC_alt       3.2.4         N/A           No        
    FW_PMC_primary   3.2.4         N/A           No        
    FW_PSU_1         01050105      01050105      Yes       
    FW_PSU_2         01040104      01050105      No        
    FW_PSU_3         01040104      01050105      No        
    FW_PSU_4         01040104      01050105      No        
    FW_PSU_5         01040104      01050105      No        
    FW_PSU_6         01040104      01050105      No        
    -------------------------------------------------------
    Error Code: 0

    $ nvfwupd -t ip=<PowerShelf BMC IP> user=*** password=*** update_fw -p DeltaPR3PSUS/Nvidia_GB300_5500W_APP_01050105_20251013.hex.tar
    Updating ip address: ip=XXXX
    FW package: ['DeltaPR3PSUS/Nvidia_GB300_5500W_APP_01050105_20251013.hex.tar']
    Ok to proceed with firmware update? <Y/N>
    y
    FW update started, Task Id: 0
    Firmware update in progress
    use show_update_progress command to monitor the progress
    ------------------------------------------------------------------------------------
    Error Code: 0

    $ nvfwupd -t ip=<PowerShelf BMC IP> user=*** password=*** show_update_progress -i 0
    Task Info for Id: 0
    StartTime: 2024-05-02T15:45:44+00:00
    TaskState: Running
    PercentComplete: 49
    TaskStatus: OK
    Overall Task Status: {
        "@odata.id": "/redfish/v1/TaskService/Tasks/0",
        "@odata.type": "#Task.v1_7_4.Task",
        "HidePayload": false,
        "Id": "0",
        "Messages": [
            {
                "@odata.type": "#Message.v1_1_1.Message",
                "Message": "The task with Id '0' has started.",
                "MessageArgs": [
                    "0"
                ],
                "MessageId": "TaskEvent.1.0.3.TaskStarted",
                "MessageSeverity": "OK",
                "Resolution": "None."
            },
            {
                "@odata.type": "#Message.v1_1_1.Message",
                "Message": "The task with Id '0' has changed to progress 10 percent complete.",
                "MessageArgs": [
                    "0",
                    "10"
                ],
                "MessageId": "TaskEvent.1.0.3.TaskProgressChanged",
                "MessageSeverity": "OK",
                "Resolution": "None."
            },
            {
                "@odata.type": "#Message.v1_1_1.Message",
                "Message": "The task with Id '0' has changed to progress 23 percent complete.",
                "MessageArgs": [
                    "0",
                    "23"
                ],
                "MessageId": "TaskEvent.1.0.3.TaskProgressChanged",
                "MessageSeverity": "OK",
                "Resolution": "None."
            },
            {
                "@odata.type": "#Message.v1_1_1.Message",
                "Message": "The task with Id '0' has changed to progress 36 percent complete.",
                "MessageArgs": [
                    "0",
                    "36"
                ],
                "MessageId": "TaskEvent.1.0.3.TaskProgressChanged",
                "MessageSeverity": "OK",
                "Resolution": "None."
            },
            {
                "@odata.type": "#Message.v1_1_1.Message",
                "Message": "The task with Id '0' has changed to progress 49 percent complete.",
                "MessageArgs": [
                    "0",
                    "49"
                ],
                "MessageId": "TaskEvent.1.0.3.TaskProgressChanged",
                "MessageSeverity": "OK",
                "Resolution": "None."
            }
        ],
        "Name": "Task 0",
        "Payload": {
            "HttpHeaders": [],
            "HttpOperation": "POST",
            "JsonBody": "null",
            "TargetUri": "/redfish/v1/UpdateService/update"
        },
        "PercentComplete": 49,
        "StartTime": "2024-05-02T15:45:44+00:00",
        "TaskMonitor": "/redfish/v1/TaskService/Tasks/0/Monitor",
        "TaskState": "Running",
        "TaskStatus": "OK"
    }
    Update is still running.
    ------------------------------------------------------------------------------------
    Error Code: 0

    $ nvfwupd -t ip=<PowerShelf BMC IP> user=*** password=*** show_version -p DeltaPR3PSUS/Nvidia_GB300_5500W_APP_01050105_20251013.hex.tar 
    System Model: AHE50V660A33KWL
    Part number: $TRAY_PART_NUMBER
    Serial number: $TRAY_SERIAL_NUMBER
    Packages: ['DeltaPR3PSUS/Nvidia_GB300_5500W_APP_01050105_20251013.hex.tar']
    Connection Status: Successful

    Firmware Devices:
    AP Name          Sys Version   Pkg Version   Up-To-Date
    -------          -----------   -----------   ----------
    FW_ERoT_BMC_0    1203          N/A           No        
    FW_PMC_alt       3.2.4         N/A           No        
    FW_PMC_primary   3.2.4         N/A           No        
    FW_PSU_1         01050105      01050105      Yes       
    FW_PSU_2         01050105      01050105      Yes       
    FW_PSU_3         01050105      01050105      Yes       
    FW_PSU_4         01050105      01050105      Yes       
    FW_PSU_5         01050105      01050105      Yes       
    FW_PSU_6         01050105      01050105      Yes       
    -------------------------------------------------------
    Error Code: 0

**Megmeet PSU Update:**

Megmeet PSUs update simultaneously and do not require a special JSON file. To monitor update progress, use the ``show_update_progress`` command with the returned task ID.

.. note::
    The ``Pkg Version`` field in the ``show_version`` output currently displays incorrect version numbers for Megmeet PowerShelf PSUs. This issue will be addressed in future firmware packages from the vendor.

.. code-block::

    $ nvfwupd -t ip=<PowerShelf BMC IP> user=*** password=*** servertype=Powershelf show_version -p PSUUpgrade/PSU_PFCV00B05_DCDCV00B05.tar
    System Model: MPSC6000-MCU
    Part number: $TRAY_PART_NUMBER
    Serial number: $TRAY_SERIAL_NUMBER
    Packages: ['PSUUpgrade/PSU_PFCV00B05_DCDCV00B05.tar']
    Connection Status: Successful

    Firmware Devices:
    AP Name            Sys Version            Pkg Version     Up-To-Date
    -------            -----------            -----------     ----------
    backupbmc_active   V99B02                 N/A             No
    bios_active        NA                     N/A             No
    bmc_active         V03B02                 N/A             No
    cpld_active        NA                     N/A             No
    de3fdd82           V01                    V01             Yes
    mcu_active         V02B01                 N/A             No
    psu_active_1       PFC:V99B05/DCDC:V99B05 V01             Yes
    psu_active_2       PFC:V99B05/DCDC:V99B05 V01             Yes
    psu_active_3       PFC:V99B05/DCDC:V99B05 V01             Yes
    psu_active_4       PFC:V99B05/DCDC:V99B05 V01             Yes
    psu_active_5       PFC:V99B05/DCDC:V99B05 V01             Yes
    psu_active_6       PFC:V99B05/DCDC:V99B05 V01             Yes
    ------------------------------------------------------------------------------------
    Error Code: 0

    $ nvfwupd -t ip=<PowerShelf BMC IP> user=*** password=*** servertype=Powershelf update_fw -p PSUUpgrade/PSU_PFCV00B05_DCDCV00B05.tar
    Updating ip address: ip=XXXX
    FW package: ['PSUUpgrade/PSU_PFCV00B05_DCDCV00B05.tar']
    Ok to proceed with firmware update? <Y/N>
    y
    ApplyTime set to 'Immediate' successfully.
    FW update started, Task Id: 1
    Firmware update in progress
    use show_update_progress command to monitor the progress
    ------------------------------------------------------------------------------------
    Error Code: 0

    $ nvfwupd -t ip=<PowerShelf BMC IP> user=*** password=*** servertype=Powershelf show_update_progress -i 1
    Task Info for Id: 1
    StartTime: 2026-06-03T22:30:33+00:00
    TaskState: Completed
    PercentComplete: 100
    TaskStatus: OK
    EndTime: 2026-06-03T23:04:43+00:00
    Overall Time Taken: 0:34:10
    Overall Task Status: {
        "@odata.id": "/redfish/v1/TaskService/Tasks/1",
        "@odata.type": "#Task.v1_6_0.Task",
        "EndTime": "2026-06-03T23:04:43+00:00",
        "Id": "1",
        "Messages": [
            {
                "@odata.type": "#Message.v1_0_0.Message",
                "Message": "The task with id 1 has started.",
                "MessageArgs": [
                    "1"
                ],
                "MessageId": "TaskEvent.1.0.1.TaskStarted",
                "Resolution": "None.",
                "Severity": "OK"
            },
            {
                "@odata.type": "#Message.v1_0_0.Message",
                "Message": "The task with id 1 has changed to progress 49 percent complete.",
                "MessageArgs": [
                    "1",
                    "49"
                ],
                "MessageId": "TaskEvent.1.0.1.TaskProgressChanged",
                "Resolution": "None.",
                "Severity": "OK"
            },
            {
                "@odata.type": "#Message.v1_0_0.Message",
                "Message": "The task with id 1 has completed.",
                "MessageArgs": [
                    "1"
                ],
                "MessageId": "TaskEvent.1.0.1.TaskCompletedOK",
                "Resolution": "None.",
                "Severity": "OK"
            }
        ],
        "PercentComplete": 100,
        "StartTime": "2026-06-03T22:30:33+00:00",
        "TaskState": "Completed",
        "TaskStatus": "OK"
    }
    Update is successful.
    ------------------------------------------------------------------------------------
    Error Code: 0

    $ nvfwupd -t ip=<PowerShelf BMC IP> user=*** password=*** servertype=Powershelf show_version -p PSUUpgrade/PSU_PFCV00B05_DCDCV00B05.tar
    System Model: MPSC6000-MCU
    Part number: $TRAY_PART_NUMBER
    Serial number: $TRAY_SERIAL_NUMBER
    Packages: ['PSUUpgrade/PSU_PFCV00B05_DCDCV00B05.tar']
    Connection Status: Successful

    Firmware Devices:
    AP Name            Sys Version            Pkg Version     Up-To-Date
    -------            -----------            -----------     ----------
    backupbmc_active   V99B02                 N/A             No
    bios_active        NA                     N/A             No
    bmc_active         V03B02                 N/A             No
    cpld_active        NA                     N/A             No
    de3fdd82           V01                    V01             Yes
    mcu_active         V02B01                 N/A             No
    psu_active_1       PFC:V00B05/DCDC:V00B05 V01             Yes
    psu_active_2       PFC:V00B05/DCDC:V00B05 V01             Yes
    psu_active_3       PFC:V00B05/DCDC:V00B05 V01             Yes
    psu_active_4       PFC:V00B05/DCDC:V00B05 V01             Yes
    psu_active_5       PFC:V00B05/DCDC:V00B05 V01             Yes
    psu_active_6       PFC:V00B05/DCDC:V00B05 V01             Yes
    ------------------------------------------------------------------------------------
    Error Code: 0


Configuration File for PowerShelf Updates
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

PowerShelf updates can be configured using a YAML configuration file for easier management:

.. code-block:: yaml

    TargetPlatform: 'Powershelf'

    FWUpdateFilePath:
    - "DeltaPR3PSUS/Nvidia_GB300_5500W_APP_01040104_20250807.hex.tar"

    BMC_IP: "POWERSHELF_BMC_IP"
    RF_USERNAME: "PMC_USERNAME"
    RF_PASSWORD: "PMC_PASSWORD"


Using the configuration file:

.. code-block::

    $ nvfwupd -c powershelf_config.yaml show_version
    $ nvfwupd -c powershelf_config.yaml update_fw

Monitoring Update Progress
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Monitor the progress of PowerShelf firmware updates using the ``show_update_progress`` command:

.. code-block::

    $ nvfwupd -t ip=<PowerShelf BMC IP> user=*** password=*** servertype=Powershelf show_update_progress -i 2

      Task Info for Id: 2
     StartTime: 2024-04-29T16:21:51+00:00
     TaskState: Running
     PercentComplete: 0
     TaskStatus: OK
     Overall Task Status: {
        "@odata.id": "/redfish/v1/TaskService/Tasks/2",
        "@odata.type": "#Task.v1_7_4.Task",
        "HidePayload": false,
        "Id": "2",
        "Messages": [
            {
                "@odata.type": "#Message.v1_1_1.Message",
                "Message": "The task with Id '2' has started.",
                "MessageArgs": [
                    "2"
                ],
                "MessageId": "TaskEvent.1.0.3.TaskStarted",
                "MessageSeverity": "OK",
                "Resolution": "None."
            }
        ],
        "Name": "Task 2",
        "Payload": {
            "HttpHeaders": [],
            "HttpOperation": "POST",
            "JsonBody": "null",
            "TargetUri": "/redfish/v1/UpdateService/update"
        },
        "PercentComplete": 0,
        "StartTime": "2024-04-29T16:21:51+00:00",
        "TaskMonitor": "/redfish/v1/TaskService/Tasks/2/Monitor",
        "TaskState": "Running",
        "TaskStatus": "OK"
    }
      Update is still running.
    ------------------------------------------------------------------------------------
    Error Code: 0

.. note::
    1. Delta Powershelf PMC updates may be monitored to around 60% completion before the PMC reboots itself for activation. This is normal and expected behavior.
    2. If the PMC becomes unresponsive, LiteOn Powershelf PMC updates might not be monitored until the update is complete and the new PMC firmware is activated.
    3. Delta Powershelf PSU updates may be monitored with a provided task ID and the PSU will reboot itself after the update is complete.
    4. LiteOn Powershelf PSU updates do not utilize the Redfish taskservice, so a LiteOn OEM page displays the UpdateProgress as part of the PSU power status output.
    5. Monitor Megmeet Powershelf PSU updates with a provided task ID.

Verifying Update Completion
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

After the update completes, PowerShelf components automatically activate the new firmware. Wait for any component reboots to complete, then verify the firmware versions:

.. code-block::

    $ nvfwupd -t ip=<PowerShelf BMC IP> user=*** password=*** servertype=Powershelf show_version -p nvidia-pmc-3.2.3-RELEASE.FOR.UPDATE.CEC-CFG.fwpkg

    System Model: AHE50V660A33KWL
    Part number: $TRAY_PART_NUMBER
    Serial number: $TRAY_SERIAL_NUMBER
    Packages: ['3.2.3']
    Connection Status: Successful

    Firmware Devices:
    AP Name         Sys Version  Pkg Version  Up-To-Date
    -------         -----------  -----------  ----------
    44ecaa94        01050105     N/A           No        
    62031a50        01040104     N/A           No        
    FW_ERoT_BMC_0   1203         N/A           No        
    FW_PMC_alt      3.2.4        3.2.3         Yes       
    FW_PMC_primary  3.2.3        3.2.3         Yes       
    -------------------------------------------------------
    Error Code: 0

.. note::
    In this example, the FW_PMC_primary firmware was successfully downgraded from 3.2.4 to 3.2.3. The component automatically rebooted and activated the new firmware version. Note that the component ordering may change after updates, and new components may appear in the inventory.

Update Workflow Summary
~~~~~~~~~~~~~~~~~~~~~~~

Based on the Delta PowerShelf PMC update example, here is the typical workflow:

1. **Check Current Versions**: Use ``show_version`` to see current firmware versions before updating
2. **Start Update**: Run ``update_fw`` command - the tool returns a Task ID for monitoring
3. **Monitor Progress**: Use ``show_update_progress`` with the Task ID to check update status
4. **Wait for Completion**: PowerShelf components will reboot and automatically activate new firmware
5. **Verify Update**: Run ``show_version`` again to confirm the new firmware is active and up to date

The entire process typically takes several minutes, during which the PowerShelf PMC may become temporarily unreachable during PMC reboots.

Activation and Power Management
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

All PowerShelf components activate themselves automatically after the update completes. No additional activation steps are required for PowerShelf platforms.

.. important::
    - Wait for the complete update process to finish before verifying firmware versions
    - The PMC will reboot during the update, causing temporary loss of connectivity
    - Updates can take several minutes to complete - use ``show_update_progress`` to monitor status
    - Component ordering in the firmware inventory may change after updates


OnReset Update Workflows
~~~~~~~~~~~~~~~~~~~~~~~~

.. note::
    Starting with NVFWUPD 2.1.0, ``OnReset`` activation is supported on Delta and LiteOn PowerShelf platforms. Starting with NVFWUPD 2.1.2, ``OnReset`` activation is also supported on Megmeet PowerShelf platforms. By default, all updates use immediate activation.

.. important::

    ``OnReset`` activation applies only to PMC firmware updates on Delta, LiteOn, and Megmeet PowerShelf platforms. PSU firmware updates always use immediate activation.

To use OnReset activation, complete the following steps:

1. Create a JSON file with the following contents to set the ``ApplyTime`` parameter:

.. code-block::

    cat OnResetActivation.json
    {
        “ApplyTime” : “OnReset”
    }

Valid values for "ApplyTime" are "Immediate" and "OnReset". If not provided, the default value is "Immediate".

2. Run the ``update_fw`` command with the JSON file:

.. code-block::

    $ nvfwupd -t ip=<PowerShelf BMC IP> user=*** password=*** update_fw -p nvidia-pmc-3.2.9-RELEASE.FOR.UPDATE.CEC-CFG.fwpkg -s OnResetActivation.json
    Updating ip address: ip=XXXX
    FW package: ['nvidia-pmc-3.2.9-RELEASE.FOR.UPDATE.CEC-CFG.fwpkg']
    Ok to proceed with firmware update? <Y/N>
    Y
    ApplyTime set to 'OnReset' successfully.
    FW update started, Task Id: 0
    Firmware update in progress
    use show_update_progress command to monitor the progress
    ================================================================================
    Error Code: 0

3. After the update begins, monitor it's progress using the ``show_update_progress`` command with the Task ID:

.. code-block::

    $ nvfwupd -t ip=<PowerShelf BMC IP> user=*** password=*** show_update_progress -i 0
     Task Info for Id: 0
     StartTime: 2024-04-29T16:21:51+00:00
     TaskState: Completed
     PercentComplete: 100
     TaskStatus: OK
     Overall Task Status: {
        "@odata.id": "/redfish/v1/TaskService/Tasks/0",
        "@odata.type": "#Task.v1_7_4.Task",
        "HidePayload": false,
        "Id": "0",
        "Messages": [
        {
            "@odata.type": "#Message.v1_1_1.Message",
            "Message": "The task with Id '0' has started.",
            "MessageArgs": [
                "0"
            ],
            "MessageId": "TaskEvent.1.0.3.TaskStarted",
            "MessageSeverity": "OK",
            "Resolution": "None."
        },
        {
            "MessageId": "TaskEvent.1.0.3.TaskProgressChanged",
            "MessageSeverity": "OK",
            "Resolution": "None."
        },
        {
            "@odata.type": "#Message.v1_1_1.Message",
            "Message": "The task with Id '0' has changed to progress 10 percent complete.",
            "MessageArgs": [
                "0",
                "10"
            ],
            "MessageId": "TaskEvent.1.0.3.TaskProgressChanged",
            "MessageSeverity": "OK",
            "Resolution": "None."
        },
        {
            "@odata.type": "#Message.v1_1_1.Message",
            "Message": "The task with Id '0' has changed to progress 20 percent complete.",
            "MessageArgs": [
                "0",
                "20"
            ],
            "MessageId": "TaskEvent.1.0.3.TaskProgressChanged",
            "MessageSeverity": "OK",
            "Resolution": "None."
        },
        {
            "@odata.type": "#Message.v1_1_1.Message",
            "Message": "The task with Id '0' has changed to progress 30 percent complete.",
            "MessageArgs": [
                "0",
                "30"
            ],
            "MessageId": "TaskEvent.1.0.3.TaskProgressChanged",
            "MessageSeverity": "OK",
            "Resolution": "None."
        },
        {
            "@odata.type": "#Message.v1_1_1.Message",
            "Message": "The task with Id '0' has changed to progress 40 percent complete.",
            "MessageArgs": [
                "0",
                "40"
            ],
            "MessageId": "TaskEvent.1.0.3.TaskProgressChanged",
            "MessageSeverity": "OK",
            "Resolution": "None."
        },
        {
            "@odata.type": "#Message.v1_1_1.Message",
            "Message": "The task with Id '0' has changed to progress 50 percent complete.",
            "MessageArgs": [
                "0",
                "50"
            ],
            "MessageId": "TaskEvent.1.0.3.TaskProgressChanged",
            "MessageSeverity": "OK",
            "Resolution": "None."
        },
        {
            "@odata.type": "#Message.v1_1_1.Message",
            "Message": "The task with Id '0' has changed to progress 100 percent complete.",
            "MessageArgs": [
                "0",
                "100"
            ],
            "MessageId": "TaskEvent.1.0.3.TaskProgressChanged",
            "MessageSeverity": "OK",
            "Resolution": "None."
        },
        {
            "@odata.type": "#Message.v1_1_1.Message",
            "Message": "The task with Id '0' has completed.",
            "MessageArgs": [
                "0"
            ],
            "MessageId": "TaskEvent.1.0.3.TaskCompletedOK",
            "MessageSeverity": "OK",
            "Resolution": "None."
        }
    ],
    "Name": "Task 0",
    "Payload": {
        "HttpHeaders": [],
        "HttpOperation": "POST",
        "JsonBody": "null",
        "TargetUri": "/redfish/v1/UpdateService/update"
    },
    "PercentComplete": 100,
    "StartTime": "2025-12-10T19:54:55+00:00",
    "TaskMonitor": "/redfish/v1/TaskService/Tasks/0/Monitor",
    "TaskState": "Completed",
    "TaskStatus": "OK"
    }
    Update is successful.
    ================================================================================
    Error Code: 0

4. After the update completes, manually activate the PMC firmware using the activate_fw command with RF_PWRSHELF_RESET:

.. code-block::

    $ nvfwupd -t ip=<PowerShelf BMC IP> user=*** password=*** activate_fw -c RF_PWRSHELF_RESET
    RF_PWRSHELF_RESET requested successfully.
    PowerShelf Manager reset (GracefulRestart) initiated at /redfish/v1/Managers/PMC_0/Actions/Manager.Reset
    Server response:
    {
        "@Message.ExtendedInfo": [
            {
                "@odata.type": "#Message.v1_1_1.Message",
                "Message": "The request completed successfully.",
                "MessageArgs": [],
                "MessageId": "Base.1.18.1.Success",
                "MessageSeverity": "OK",
                "Resolution": "None."
            },
            {
                "@odata.type": "#Message.v1_1_1.Message",
                "Message": "The request completed successfully.",
                "MessageArgs": [],
                "MessageId": "Base.1.18.1.Success",
                "MessageSeverity": "OK",
                "Resolution": "None."
            }
        ]
    }
    ================================================================================
