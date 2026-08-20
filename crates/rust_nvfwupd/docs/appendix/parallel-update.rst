Simultaneously Updating the Firmware for Multiple Target Systems
----------------------------------------------------------------------------

nvfwupd v2.0.5 and later supports parallel firmware updates through the YAML configuration file, which allows users to update multiple servers at the same time.

To simultaneously update the firmware for multiple target systems:

1. Create an nvfwupd configuration yaml file. Set ``"ParallelUpdate": true`` to enable parallel updates. Optionally, set ``ParallelThreadCount`` to the maximum number of threads you want to allow for parallel operations. The default value is 10; you can raise or lower it based on the host system's capabilities.

2. Create a list of Targets, each of which contains ``BMC_IP``, ``RF_USERNAME``, ``RF_PASSWORD``, and ``PACKAGE`` as mandatory parameters.

    - For platforms that require ``servertype``, include ``TARGET_PLATFORM``.

    - For platforms that require the ``-s`` special json file inclusion for updates, include ``UPDATE_PARAMETERS_TARGETS``.

    - The ``PACKAGE`` field supports both single packages (string) and multiple packages (list) for version comparison and updates.

    - To prevent BMC memory exhaustion when pushing multiple packages to the same system, include ``UPDATE_DELAY`` (in seconds) to add a delay between package pushes. Default is 0 seconds if not specified.

Here is an example of a configuration file:

.. code-block::

    # To enable parallel update in nvfwupd, set the ParallelUpdate key to true
    ParallelUpdate: true

    # Optional max number of threads for parallel operations (show version, update, task polling).
    # If not set, defaults to 10.
    ParallelThreadCount: 15

    # When ParallelUpdate is enabled, "Targets" becomes a list of systems alongside their packages
    # BMC_IP, RF_USERNAME, RF_PASSWORD, PACKAGE are mandatory when using parallel update
    # TARGET_PLATFORM would be needed for any systems that would normally require it from the command line (that is: GB200, gb200switch, and so on)
    # UPDATE_PARAMETERS_TARGETS is optional, but uses the same exact params as the special target file for nvfwupd "-s" option in json format with a given system
    # SYSTEM_NAME is an entirely optional, but recommended user defined string. It is used to more easily distinguish systems as it is used in task printouts
    # PACKAGE supports both single packages (string) and multiple packages (list) for version comparison and updates
    # UPDATE_DELAY is optional and specifies a delay in seconds before pushing a package to a system (default: 0) - recommended value of 30 to prevent BMC memory exhaustion when pushing multiple packages to the same system
    Targets: 

          - BMC_IP: "192.0.2.10" 
            RF_USERNAME: "****" 
            RF_PASSWORD: "****"
            SYSTEM_NAME: "Example DGX"
            TARGET_PLATFORM: 'DGX'
            PACKAGE: "nvfw_DGX_0005_241205.1.0_custom_dbg-signed.fwpkg"
            UPDATE_PARAMETERS_TARGETS: {}
          - BMC_IP: "198.51.100.10"
            RF_USERNAME: "****" 
            RF_PASSWORD: "****" 
            TARGET_PLATFORM: 'DGX'
            PACKAGE: ["nvfw_DGX_0003_241205.1.0_custom_prod-signed.fwpkg", "nvfw_DGX_0004_241205.1.1_custom_prod-signed.fwpkg"]
            UPDATE_PARAMETERS_TARGETS: {}
          - BMC_IP: "203.0.113.10"
            RF_USERNAME: "****" 
            RF_PASSWORD: "****" 
            TARGET_PLATFORM: 'gb200switch'
            PACKAGE: "nvfw_GB200-P4978_0004_241119.1.1_dbg-signed.fwpkg"
            UPDATE_PARAMETERS_TARGETS: {"Targets": ["BMC"]}
          - BMC_IP: "192.0.2.11"
            RF_USERNAME: "****" 
            RF_PASSWORD: "****"
            SYSTEM_NAME: "HMC"
            TARGET_PLATFORM: 'GB200'
            PACKAGE: ["nvfw_GB200-P4975_0009_241206.1.4_custom_dbg-signed.fwpkg", "nvfw_GB200-P4972_0009_241206.1.3_custom_dbg-signed.fwpkg"]
            UPDATE_PARAMETERS_TARGETS: {"Targets": ["/redfish/v1/Chassis/HGX_Chassis_0"]}
          - BMC_IP: "198.51.100.11"
            RF_USERNAME: "****" 
            RF_PASSWORD: "****" 
            TARGET_PLATFORM: 'GB200'
            PACKAGE: "nvfw_GB200-P4972_0009_241206.1.3_custom_dbg-signed.fwpkg"
            UPDATE_DELAY: 30 # Add a 30 second delay before pushing this package
            UPDATE_PARAMETERS_TARGETS: {"Targets": [], "ForceUpdate": true}
          - BMC_IP: "198.51.100.11"
            RF_USERNAME: "****" 
            RF_PASSWORD: "****" 
            TARGET_PLATFORM: 'GB200'
            PACKAGE: "nvfw_GB200-P4975_0009_241206.1.4_custom_dbg-signed.fwpkg"
            UPDATE_PARAMETERS_TARGETS: {"Targets": [/redfish/v1/Chassis/HGX_Chassis_0], "ForceUpdate": true}

3. To check the system and package versions in parallel, run the ``show_version`` command with the configuration file.

.. code-block::

  $ cat parallel_update_config.yaml

  ParallelUpdate: true
  Targets: 
    - BMC_IP: "192.0.2.10" 
      RF_USERNAME: "*******" 
      RF_PASSWORD: "***************" 
      TARGET_PLATFORM: 'gb200switch'
      PACKAGE: "nvfw_GB200-P4978_0004_241209.1.7_dbg-signed.fwpkg"
      UPDATE_PARAMETERS_TARGETS: {"Targets": ["BMC"]}
      SYSTEM_NAME: "GB200Switch System"
    - BMC_IP: "198.51.100.10"
      RF_USERNAME: "*******"
      RF_PASSWORD: "***************" 
      TARGET_PLATFORM: 'MGX'
      PACKAGE: "nvfw_GH200-P5042_0004_241218.1.0_custom_prod-signed.fwpkg"
      UPDATE_PARAMETERS_TARGETS: {"HttpPushUriTargets":["/redfish/v1/UpdateService/FirmwareInventory/CPU_0"]}
      SYSTEM_NAME: "Lab MGX System"

  $ nvfwupd -c parallel_update_config.yaml show_version
  System Model: N5200_LD
  Part number: $TRAY_PART_NUMBER
  Serial number: $TRAY_SERIAL_NUMBER
  Packages: ['GB200-P4978_0004_241209.1.7']
  Connection Status: Successful

  Firmware Devices:
  AP Name           Sys Version             Pkg Version             Up-To-Date
  -------           -----------             -----------             ----------
  ASIC              35.2014.1610            N/A                     No        
  BIOS              0ACTV_00.01.010d        N/A                     No        
  BMC               88.0002.0600            88.0002.0600            Yes       
  CPLD1             CPLD000370_REV0300      N/A                     No        
  CPLD2             CPLD000377_REV0500      N/A                     No        
  CPLD3             CPLD000373_REV0205      N/A                     No        
  CPLD4             CPLD000390_REV0200      N/A                     No        
  EROT              01.04.0000.0000_n04     01.03.0235.0000_n04     Yes       
  EROT-ASIC1        01.04.0000.0000_n04     01.03.0235.0000_n04     Yes       
  EROT-ASIC2        01.04.0000.0000_n04     01.03.0235.0000_n04     Yes       
  EROT-BMC          01.04.0000.0000_n04     01.03.0235.0000_n04     Yes       
  EROT-CPU          01.04.0000.0000_n04     01.03.0235.0000_n04     Yes       
  EROT-FPGA         01.04.0000.0000_n04     01.03.0235.0000_n04     Yes       
  FPGA              0.1A                    0.19                    Yes       
  SSD               CE00A400                N/A                     No        
  transceiver       N/A                     N/A                     No        
  ----------------------------------------------------------------------------
  System Model: P3809
  Part number: $TRAY_PART_NUMBER
  Serial number: $TRAY_SERIAL_NUMBER
  Packages: ['GH200-P5042_0004_241218.1.0_custom']
  Connection Status: Successful

  Firmware Devices:
  AP Name           Sys Version              Pkg Version              Up-To-Date
  -------           -----------              -----------              ----------
  BMC_Firmware      GH-24.03-14              24.03.14                 No        
  CPLD_0            6                        N/A                      No        
  CPU_0             02.03.14                 02.03.13                 Yes       
  ERoT_CPU_0        01.03.0252.0000_n01      01.03.0252.0000_n01      Yes       
  ERoT_GPU_0        01.03.0252.0000_n01      01.03.0252.0000_n01      Yes       
  FW_FPGA_0         0.9A                     0.9A                     Yes       
  FW_GPU_0          96.00.8D.00.03           96.00.8D.00.03           Yes       
  InfoROM_GPU_0     G530.0206.00.02          N/A                      No        
  ------------------------------------------------------------------------------
  Error Code: 0

4. To start the firmware update in parallel, run the ``update_fw`` command with the configuration file.

.. code-block::

  $ cat parallel_update_config.yaml

  ParallelUpdate: true
  Targets: 
    - BMC_IP: "192.0.2.10" 
      RF_USERNAME: "*******" 
      RF_PASSWORD: "***************" 
      TARGET_PLATFORM: 'gb200switch'
      PACKAGE: "nvfw_GB200-P4978_0004_241209.1.7_dbg-signed.fwpkg"
      UPDATE_PARAMETERS_TARGETS: {"Targets": ["BMC"]}
      SYSTEM_NAME: "GB200Switch System"
    - BMC_IP: "198.51.100.10"
      RF_USERNAME: "*******"
      RF_PASSWORD: "***************" 
      TARGET_PLATFORM: 'MGX'
      PACKAGE: "nvfw_GH200-P5042_0004_241218.1.0_custom_prod-signed.fwpkg"
      UPDATE_PARAMETERS_TARGETS: {"HttpPushUriTargets":["/redfish/v1/UpdateService/FirmwareInventory/CPU_0"]}
      SYSTEM_NAME: "Lab MGX System"

  $ nvfwupd -c parallel_update_config.yaml update_fw
  Updating ip address: ip=XXXX
  Updating ip address: ip=XXXX
  FW package: ['nvfw_GH200-P5042_0004_241218.1.0_custom_prod-signed.fwpkg']
  FW package: ['nvfw_GB200-P4978_0004_241209.1.7_dbg-signed.fwpkg']
  The following targets will be updated ['BMC']
    FW update started, Task Id: 2
  ------------------------------------------------------------------------------------------------------------------------
  Update file nvfw_GB200-P4978_0004_241209.1.7_dbg-signed.fwpkg was uploaded successfully
  Starting FW update for: BMC
  FW update task was created with ID 5
  Status for Job Id 5:
  {'detail': '',
  'http_status': 200,
  'issue': [],
  'percentage': '',
  'state': 'running',
  'status': '',
  'timeout': 1800,
  'type': '',
  'warnings': []}

  ------------------------------------------------------------------------------------------------------------------------
  Printing Task status for IP: XXXX
  Printing Task status for system: GB200Switch System
  ------------------------------------------------------------------------------------------------------------------------
  Status for Job Id: 5
  {'detail': 'Installing firmware: '
            'nvfw_GB200-P4978_0004_241209.1.7_dbg-signed.fwpkg',
  'http_status': 200,
  'issue': [],
  'percentage': '',
  'state': 'running',
  'status': 'Installing firmware: '
            'nvfw_GB200-P4978_0004_241209.1.7_dbg-signed.fwpkg',
  'timeout': 1800,
  'type': '',
  'warnings': []}

  Printing Task status for IP: XXXX
  Printing Task status for system: Lab MGX System
    ------------------------------------------------------------------------------------------------------------------------
    Task Info for Id: 2
  StartTime: 2025-01-06T22:31:51+00:00
  TaskState: Running
  PercentComplete: 0
  TaskStatus: OK
  Overall Task Status: {
      "@odata.id": "/redfish/v1/TaskService/Tasks/2",
      "@odata.type": "#Task.v1_4_3.Task",
      "Id": "2",
      "Messages": [
          {
              "@odata.type": "#Message.v1_0_0.Message",
              "Message": "The task with id 2 has started.",
              "MessageArgs": [
                  "2"
              ],
              "MessageId": "TaskEvent.1.0.1.TaskStarted",
              "Resolution": "None.",
              "Severity": "OK"
          },
          {
              "@odata.type": "#MessageRegistry.v1_4_1.MessageRegistry",
              "Message": "The target device 'CPU_0' will be updated with image '02.03.13'.",
              "MessageArgs": [
                  "CPU_0",
                  "02.03.13"
              ],
              "MessageId": "Update.1.0.TargetDetermined",
              "Resolution": "None.",
              "Severity": "OK"
          },
          {
              "@odata.type": "#MessageRegistry.v1_4_1.MessageRegistry",
              "Message": "Image '02.03.13' is being transferred to 'CPU_0'.",
              "MessageArgs": [
                  "02.03.13",
                  "CPU_0"
              ],
              "MessageId": "Update.1.0.TransferringToComponent",
              "Resolution": "None.",
              "Severity": "OK"
          }
      ],
      "Name": "Task 2",
      "Payload": {
          "HttpHeaders": [
              "Host: 192.0.2.10",
              "User-Agent: python-requests/2.27.1",
              "Accept-Encoding: gzip, deflate",
              "Accept: */*",
              "Connection: keep-alive",
              "Content-Length: 102712798"
          ],
          "HttpOperation": "POST",
          "JsonBody": "null",
          "TargetUri": "/redfish/v1/UpdateService"
      },
      "PercentComplete": 0,
      "StartTime": "2025-01-06T22:31:51+00:00",
      "TaskMonitor": "/redfish/v1/TaskService/Tasks/2/Monitor",
      "TaskState": "Running",
      "TaskStatus": "OK"
  }
    Update is still running.
  Printing Task status for IP: XXXX
  Printing Task status for system: GB200Switch System
  ------------------------------------------------------------------------------------------------------------------------
  Status for Job Id: 5
  {'detail': 'Installing firmware: '
            'nvfw_GB200-P4978_0004_241209.1.7_dbg-signed.fwpkg',
  'http_status': 200,
  'issue': [],
  'percentage': '',
  'state': 'running',
  'status': 'Installing firmware: '
            'nvfw_GB200-P4978_0004_241209.1.7_dbg-signed.fwpkg',
  'timeout': 1800,
  'type': '',
  'warnings': []}

  Printing Task status for IP: XXXX
  Printing Task status for system: Lab MGX System
    ------------------------------------------------------------------------------------------------------------------------
    Task Info for Id: 2
  StartTime: 2025-01-06T22:31:51+00:00
  TaskState: Completed
  PercentComplete: 100
  TaskStatus: OK
  EndTime: 2025-01-06T22:37:05+00:00
  Overall Time Taken: 0:05:14
  Overall Task Status: {
      "@odata.id": "/redfish/v1/TaskService/Tasks/2",
      "@odata.type": "#Task.v1_4_3.Task",
      "EndTime": "2025-01-06T22:37:05+00:00",
      "Id": "2",
      "Messages": [
          {
              "@odata.type": "#Message.v1_0_0.Message",
              "Message": "The task with id 2 has started.",
              "MessageArgs": [
                  "2"
              ],
              "MessageId": "TaskEvent.1.0.1.TaskStarted",
              "Resolution": "None.",
              "Severity": "OK"
          },
          {
              "@odata.type": "#MessageRegistry.v1_4_1.MessageRegistry",
              "Message": "The target device 'CPU_0' will be updated with image '02.03.13'.",
              "MessageArgs": [
                  "CPU_0",
                  "02.03.13"
              ],
              "MessageId": "Update.1.0.TargetDetermined",
              "Resolution": "None.",
              "Severity": "OK"
          },
          {
              "@odata.type": "#MessageRegistry.v1_4_1.MessageRegistry",
              "Message": "Image '02.03.13' is being transferred to 'CPU_0'.",
              "MessageArgs": [
                  "02.03.13",
                  "CPU_0"
              ],
              "MessageId": "Update.1.0.TransferringToComponent",
              "Resolution": "None.",
              "Severity": "OK"
          },
          {
              "@odata.type": "#Message.v1_0_0.Message",
              "Message": "The task with id 2 has changed to progress 20 percent complete.",
              "MessageArgs": [
                  "2",
                  "20"
              ],
              "MessageId": "TaskEvent.1.0.1.TaskProgressChanged",
              "Resolution": "None.",
              "Severity": "OK"
          },
          {
              "@odata.type": "#MessageRegistry.v1_4_1.MessageRegistry",
              "Message": "Device 'CPU_0' successfully updated with image '02.03.13'.",
              "MessageArgs": [
                  "CPU_0",
                  "02.03.13"
              ],
              "MessageId": "Update.1.0.UpdateSuccessful",
              "Resolution": "None.",
              "Severity": "OK"
          },
          {
              "@odata.type": "#MessageRegistry.v1_4_1.MessageRegistry",
              "Message": "Awaiting for an action to proceed with activating image '02.03.13' on 'CPU_0'.",
              "MessageArgs": [
                  "02.03.13",
                  "CPU_0"
              ],
              "MessageId": "Update.1.0.AwaitToActivate",
              "Resolution": "DC power cycle or AC power cycle",
              "Severity": "OK"
          },
          {
              "@odata.type": "#Message.v1_0_0.Message",
              "Message": "The task with id 2 has changed to progress 100 percent complete.",
              "MessageArgs": [
                  "2",
                  "100"
              ],
              "MessageId": "TaskEvent.1.0.1.TaskProgressChanged",
              "Resolution": "None.",
              "Severity": "OK"
          },
          {
              "@odata.type": "#Message.v1_0_0.Message",
              "Message": "The task with id 2 has Completed.",
              "MessageArgs": [
                  "2"
              ],
              "MessageId": "TaskEvent.1.0.1.TaskCompletedOK",
              "Resolution": "None.",
              "Severity": "OK"
          }
      ],
      "Name": "Task 2",
      "Payload": {
          "HttpHeaders": [
              "Host: 192.0.2.10",
              "User-Agent: python-requests/2.27.1",
              "Accept-Encoding: gzip, deflate",
              "Accept: */*",
              "Connection: keep-alive",
              "Content-Length: 102712798"
          ],
          "HttpOperation": "POST",
          "JsonBody": "null",
          "TargetUri": "/redfish/v1/UpdateService"
      },
      "PercentComplete": 100,
      "StartTime": "2025-01-06T22:31:51+00:00",
      "TaskMonitor": "/redfish/v1/TaskService/Tasks/2/Monitor",
      "TaskState": "Completed",
      "TaskStatus": "OK"
  }
    Update is successful.
  Printing Task status for IP: XXXX
  Printing Task status for system: GB200Switch System
  ------------------------------------------------------------------------------------------------------------------------
  Status for Job Id: 5
  {'detail': 'Installing firmware: '
            'nvfw_GB200-P4978_0004_241209.1.7_dbg-signed.fwpkg',
  'http_status': 200,
  'issue': [],
  'percentage': '',
  'state': 'running',
  'status': 'Installing firmware: '
            'nvfw_GB200-P4978_0004_241209.1.7_dbg-signed.fwpkg',
  'timeout': 1800,
  'type': '',
  'warnings': []}

  Printing Task status for IP: XXXX
  Printing Task status for system: GB200Switch System
  ------------------------------------------------------------------------------------------------------------------------
  Status for Job Id: 5
  {'detail': 'Next reboot will perform a power cycle to load the new firmware',
  'http_status': 200,
  'issue': [],
  'percentage': '',
  'state': 'action_success',
  'status': 'Next reboot will perform a power cycle to load the new firmware',
  'timeout': 1800,
  'type': '',
  'warnings': []}

  Error Code: 0

5. To activate the firmware, follow the activation steps for each target. Activating firmware in parallel is not currently supported.
