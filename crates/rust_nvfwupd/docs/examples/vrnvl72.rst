Vera Rubin NVL72 Update Examples
--------------------------------

The firmware update mechanism for Vera Rubin NVL72 is different from the update mechanism for MGX as follows:

-  Vera Rubin NVL72 uses 3 fwpkg bundles.

    - **P4110** - BMC tray.
    - **P4109** - compute tray.
    - **N6100** - switch tray.

-  Pass the update targets file using the -s option to specify the update target for the BMC and compute trays. See the sample outputs in the next section for examples.

-  To downgrade Vera Rubin NVL72 BMC or compute tray firmware, set the ``ForceUpdate`` flag in the update target JSON file passed with -s. Vera Rubin NVL72 Switch tray downgrades are allowed by default.

Updating the Vera Rubin NVL72 BMC Tray
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. note::
    Starting with NVFWUPD 2.1.0, the tool prevents concurrent BMC tray updates and returns an error if you attempt to start a new update while another is in progress.
    To allow an update to proceed while another is in progress, pass the ``--skip_pre_flight_checks`` or ``-k`` option to ``update_fw``.

To update the complete BMC tray:

1. Create a JSON file like the ``BMC_Full.json`` file in the example.

2. Use the nvfwupd tool and run the update_fw command.

In the package name, the BMC tray update packages is identified by ``VR-NVL72-P4110``.

1. After the update successfully completes, complete a power cycle to activate the firmware.

2. After the BMC and Redfish service are running, verify that the BMC tray components match the package versions using the show_version command.


Here is the output:

.. code-block::

    $ cat BMC_Full.json

    {
        "Targets": []
    }

    $ nvfwupd -t ip=<BMC IP> user=*** password=*** update_fw -s BMC_Full.json -p nvfw_VR-NVL72-P4110-BMC_0001_260130.1.1_custom_prod-signed.fwpkg

    Updating ip address: ip=XXXX
    FW package: ['nvfw_VR-NVL72-P4110-BMC_0001_260130.1.1_custom_prod-signed.fwpkg']
    Ok to proceed with firmware update? <Y/N>
    y
    {"@odata.id": "/redfish/v1/TaskService/Tasks/0", "@odata.type": "#Task.v1_4_3.Task", "Id": "0", "TaskState": "Running", "TaskStatus": "OK"}
    FW update started, Task Id: 0
    Wait for Firmware Update to Start...
    TaskState: Running
    PercentComplete: 20
    TaskStatus: OK
    TaskState: Running
    PercentComplete: 40
    TaskStatus: OK
    TaskState: Completed
    PercentComplete: 100
    TaskStatus: OK
    Firmware update successful!
    Overall Time Taken: 0:10:41
    Refer to 'NVIDIA Firmware Update Document' on activation steps for new firmware to take effect.
    -------------------------------------------------------------------------------------
    Error Code: 0

Updating the Vera Rubin NVL72 Compute Tray
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. note::
    Starting with NVFWUPD 2.1.0, the tool prevents concurrent compute tray updates and returns an error if you attempt to start a new update while another is in progress. To allow concurrent updates, pass --skip_pre_flight_checks (or -k) to update_fw.
    To allow an update to proceed while another is in progress, pass the ``--skip_pre_flight_checks`` or ``-k`` option to ``update_fw``.

To update the compute tray:

1. Create a JSON file, like the ``Compute_Full.json`` file, in the example.

2. Use the ``nvfwupd`` tool and run the ``update_fw`` command.

In the package name, the Compute tray update packages is identified by ``VR-NVL72-P4109``.

1. After the update successfully completes, to activate the firmware, complete an AC cycle.

2. After the BMC and Redfish service are running, verify that the compute tray components match the package versions using the ``show_version`` command.

Here is an example:

.. code-block::

    $ cat Compute_Full.json

    {
        "Targets": ["/redfish/v1/Chassis/HGX_Chassis_0"]
    }

    $ nvfwupd -t ip=<BMC IP> user=*** password=*** servertype=VRNVL72 update_fw -s Compute_Full.json -p nvfw_VR-NVL72-P4109-HMC_0001_260130.1.1_custom_prod-signed.fwpkg

    Updating ip address: ip=XXXX
    FW package: ['nvfw_VR-NVL72-P4109-HMC_0001_260130.1.1_custom_prod-signed.fwpkg']
    Ok to proceed with firmware update? <Y/N>
    y
    {"@odata.id": "/redfish/v1/TaskService/Tasks/HGX_0", "@odata.type": "#Task.v1_4_3.Task", "Id": "HGX_0", "TaskState": Running", "TaskStatus": "OK"}
    FW update started, Task Id: HGX_0
    Wait for Firmware Update to Start...
    TaskState: Running
    PercentComplete: 20
    TaskStatus: OK
    TaskState: Running
    PercentComplete: 40
    TaskStatus: OK
    TaskState: Completed
    PercentComplete: 100
    TaskStatus: OK
    Firmware update successful!
    Overall Time Taken: 0:09:46
    Refer to 'NVIDIA Firmware Update Document' on activation steps for new firmware to take effect.
    -----------------------------------------------------------------------------------
    Error Code: 0

Vera Rubin NVL72 Firmware Downgrades Using the Force Update Option
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To downgrade Vera Rubin NVL72 firmware, enable the force update multipart option in the update parameters JSON file and pass it using the -s option. If a firmware update fails with the following error message:

``Component comparison stamp is lower than the firmware component comparison stamp in the FD.``

Retry the update with force firmware update enabled and change the Targets value to specify the tray you want to update.

For example, to force update the BMC tray on the target:

1. Create a JSON file, like the ``force_BMC_Full.json`` file, in the example.

2. Run the tool.

Here is an example:

.. code-block::
    :emphasize-lines: 4

    $ cat force_BMC_Full.json

    {
        "ForceUpdate":true,
        "Targets":[]

    }

    $ nvfwupd -t ip=<BMC IP> user=*** password=**** servertype=VRNVL72 update_fw -s force_BMC_Full.json -p nvfw_VR-NVL72-P4110-BMC_0001_260130.1.1_custom_prod-signed.fwpkg

    Updating ip address: ip=XXXX
    FW package: ['nvfw_VR-NVL72-P4110-BMC_0001_260130.1.1_custom_prod-signed.fwpkg']
    Ok to proceed with firmware update? <Y/N>
    y
    {"@odata.id": "/redfish/v1/TaskService/Tasks/0", "@odata.type": "#Task.v1_4_3.Task", "Id": "0", "TaskState": "Running", "TaskStatus": "OK"}
    FW update started, Task Id: 0
    Wait for Firmware Update to Start...
    TaskState: Running
    PercentComplete: 20
    TaskStatus: OK
    TaskState: Running
    PercentComplete: 40
    TaskStatus: OK
    TaskState: Completed
    PercentComplete: 100
    TaskStatus: OK
    Firmware update successful!
    Overall Time Taken: 0:10:38

    Refer to 'NVIDIA Firmware Update Document' on activation steps for new firmware to take effect.
    -------------------------------------------------------------------------------------
    Error Code: 0

Vera Rubin NVL72 Firmware Updates for Selected Components
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To complete a firmware update of a component:

1. Identify the inventory name of the component.

   -  Use the ``show_version`` option to list all components in the inventory with their current versions.

   -  Update components prefixed with **HGX** using the compute tray package. Update all other components using the BMC tray package.

.. code-block ::

    $ nvfwupd -t ip=<BMC IP> user=*** password=*** servertype=VRNVL72 show_version -p nvfw_VR-NVL72-P4110-BMC_0001_260130.1.1_custom_prod-signed.fwpkg nvfw_VR-NVL72-P4109-HMC_0001_260130.1.1_custom_prod-signed.fwpkg

    System Model: VR NVL72
    Part number: $TRAY_PART_NUMBER
    Serial number: $TRAY_SERIAL_NUMBER
    Packages: ['VR-NVL72-P4110-BMC_0001_260130.1.1_custom', 'VR-NVL72-P4109-HMC_0001_260130.1.1_custom']
    Connection Status: Successful

    Firmware Devices:
    AP Name                       Sys Version         Pkg Version           Up-To-Date
    -------                       -----------         -----------           ----------
    FW_BMC_0                      VR-2512-07.01       VR-2512-07.00         Yes       
    FW_CX_0                       82.48.0756          N/A                   No        
    FW_CX_1                       82.48.0756          N/A                   No        
    FW_CX_2                       82.48.0756          N/A                   No        
    FW_CX_3                       82.48.0756          N/A                   No        
    FW_CX_4                       82.48.0756          N/A                   No        
    FW_CX_5                       82.48.0756          N/A                   No        
    FW_CX_6                       82.48.0756          N/A                   No        
    FW_CX_7                       82.48.0756          N/A                   No        
    FW_ERoT_BMC_0                 02.00.0016.0000_n05 N/A                   No        
    FW_IO_Board_SMA_0             0027.01.0116.1002   N/A                   No        
    FW_IO_Board_SMA_1             0027.01.0116.1002   N/A                   No        
    FW_IO_Board_SMA_2             0027.01.0116.1002   N/A                   No        
    FW_IO_Board_SMA_3             0027.01.0116.1002   N/A                   No        
    FW_NVMe_E1S_210               LDDJ3U2Q            N/A                   No        
    FW_NVMe_E1S_65                G77YG100            N/A                   No        
    FW_NVMe_E1S_75                G77YG100            N/A                   No        
    FW_NVMe_E1S_85                G77YG100            N/A                   No        
    FW_NVMe_E1S_95                G77YG100            N/A                   No        
    HGX_FW_BMC_0                  VR-2512-6.A         N/A                   No        
    HGX_FW_CPLD_0                 0.22                N/A                   No        
    HGX_FW_CPLD_1                 0.22                N/A                   No        
    HGX_FW_ERoT_BMC_0             02.00.0016.0000_n05 02.00.0016.0000_n05   Yes       
    HGX_FW_GPU_0                  99.00.01.53.27      99.00.01.53.2B        No        
    HGX_FW_GPU_1                  99.00.01.53.27      99.00.01.53.2B        No        
    HGX_FW_GPU_2                  99.00.01.53.27      99.00.01.53.2B        No        
    HGX_FW_GPU_3                  99.00.01.53.27      99.00.01.53.2B        No        
    HGX_FW_GPU_SMA_0              0023.01.0125.0001   0023.01.0125.0001     Yes       
    HGX_FW_GPU_SMA_1              0023.01.0125.0001   0023.01.0125.0001     Yes       
    HGX_FW_ProcessorModule_CPLD_0 0048.46.0050.0050   N/A                   No        
    HGX_FW_ProcessorModule_CPLD_1 0048.46.0050.0050   N/A                   No        
    HGX_FW_ProcessorModule_SMA_0  0026.01.0139.0013   0026.01.0131.1000     Yes       
    HGX_FW_ProcessorModule_SMA_1  0026.01.0139.0013   0026.01.0131.1000     Yes       
    HGX_InfoROM_GPU_0             G558.0200.00.01     N/A                   No        
    HGX_InfoROM_GPU_1             G558.0200.00.01     N/A                   No        
    HGX_InfoROM_GPU_2             G558.0200.00.01     N/A                   No        
    HGX_InfoROM_GPU_3             G558.0200.00.01     N/A                   No        
    HGX_SBIOS_FMC_0               00.02.00.10         00.02.00.10           Yes       
    HGX_SBIOS_FMC_1               00.02.00.10         00.02.00.10           Yes       
    HGX_SBIOS_FW_0                00.02.00.10         00.02.00.10           Yes       
    HGX_SBIOS_FW_1                00.02.00.10         00.02.00.10           Yes       
    -------------------------------------------------------------------------------------
    Error Code: 0


2. After identifying the inventory name, create the JSON file with the Redfish inventory URI of that component (``/redfish/v1/UpdateService/FirmwareInventory/<component name>``).

   The example in step 3 shows a sample ``GPU.json`` file that is used to update only the ``HGX_FW_GPU_0`` component on the tray.

3.  Run the ``update_fw`` command with the ``GPU.json`` file and compute tray bundle as the inputs.

.. code-block:: 

    $ cat GPU.json

    {
        "Targets":["/redfish/v1/UpdateService/FirmwareInventory/HGX_FW_GPU_0"]
    }

    $ nvfwupd --target ip=<BMC IP> user=*** password=*** servertype=VRNVL72 update_fw -s GPU.json -p nvfw_VR-NVL72-P4109-HMC_0001_260130.1.1_custom_prod-signed.fwpkg

    Updating ip address: ip=XXXX
    FW package: ['nvfw_VR-NVL72-P4109-HMC_0001_260130.1.1_custom_prod-signed.fwpkg']
    Ok to proceed with firmware update? <Y/N>
    y
    {"@odata.id": "/redfish/v1/TaskService/Tasks/HGX_3", "@odata.type": "#Task.v1_4_3.Task", "Id": "HGX_3", "TaskState": Running", "TaskStatus": "OK"}
      FW update started, Task Id: HGX_3
    Wait for Firmware Update to Start...
      TaskState: Running
      PercentComplete: 20
      TaskStatus: OK
      TaskState: Running
      PercentComplete: 40
      TaskStatus: OK
      TaskState: Completed
      PercentComplete: 100
      TaskStatus: OK
      Firmware update successful!

    Overall Time Taken: 0:09:50

    Refer to 'NVIDIA Firmware Update Document' on activation steps for new firmware to take effect.
    -------------------------------------------------------------------------------------
    Error Code: 0


Activating the Firmware
~~~~~~~~~~~~~~~~~~~~~~~

After updating a component or full bundle, complete an AC power cycle to activate the new firmware. The BMC and Redfish service can take up to 5 minutes to restart after the power cycle completes.
NVFWUPD 2.0.3 and later supports the ``activate_fw`` command to complete this task. The ``activate_fw`` command returns immediately after issuing the power cycle and does not wait for the BMC to restart. After the BMC Redfish service is running, verify the system versions using the ``show_version`` command.

Here is an example that uses the ``activate_fw`` command:

.. code-block::

    $ nvfwupd -t ip=<BMC IP> user=**** password=**** servertype=VRNVL72 activate_fw -c PWR_OFF

    IPMI Command Status: Success

    Chassis Power Control: Down/Off

    -------------------------------------------------------------------------------------

.. code-block::

    $ nvfwupd -t ip=<BMC IP> user=**** password=**** servertype=VRNVL72 activate_fw –c RF_AUX_PWR_CYCLE

    AUX Power Cycle requested successfully.

    Server response:

    ""

    -------------------------------------------------------------------------------------

After running these commands, the system reboots. When the system is accessible, run the ``power_on`` command to power on all components.

.. code-block::
    
    $ nvfwupd -t ip=<BMC IP> user=**** password=**** activate_fw -c PWR_ON

    IPMI Command Status: Success

    Chassis Power Control: Up/On

    -------------------------------------------------------------------------------------

Alternatively, starting with NVFWUPD 2.0.6, use Redfish activation commands:

.. code-block::

    $ nvfwupd -t ip=<BMC IP> user=**** password=**** servertype=VRNVL72 activate_fw -c RF_PWR_OFF

    RF_PWR_OFF requested successfully.
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
            }
        ]
    }
    ------------------------------------------------------------------------------------


.. code-block::

    $ nvfwupd -t ip=<BMC IP> user=**** password=**** servertype=VRNVL72 activate_fw –c RF_AUX_PWR_CYCLE

    AUX Power Cycle requested successfully.

    Server response:

    ""

    ------------------------------------------------------------------------------------

After running these commands, the system reboots. When the system is accessible, run the ``power_on`` command to power on all components.

.. code-block::
    
    $ nvfwupd -t ip=<BMC IP> user=**** password=**** servertype=VRNVL72 activate_fw -c RF_PWR_ON

    RF_PWR_ON requested successfully.
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
            }
        ]
    }
    ------------------------------------------------------------------------------------

Check the power status of system components using the RF_PWR_STATUS activate_fw command.

.. code-block::

    $ nvfwupd -t ip=<BMC IP> user=*** password=*** activate_fw -c RF_PWR_STATUS
    Power Status:
    System: System_0
    URI: /redfish/v1/Systems/System_0
    PowerState: On

    System: HGX_Baseboard_0
    URI: /redfish/v1/Systems/HGX_Baseboard_0
    PowerState: On

Vera Rubin NVL72 Switch Tray Update
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

..  table:: Vera Rubin NVL72 Switch Tray Packages
    :name: vera_rubin_nvl72_switch_tray_packages
    :widths: auto

    +------------------------------+-------------------------------+
    | **Package ID**               | **Components**                |
    +------------------------------+-------------------------------+
    | nvfw_VR-NVL72-N6100-LD_0001  | BMC, EROT, SBIOS, CPLD, SMA   |
    +------------------------------+-------------------------------+

Displaying the Current Versions of the Switch Tray Components
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To display the current versions of switch tray components, run the show_version command. The **System Version** column shows the current firmware versions on the system and the **Package** version column shows the versions in the packages after you run the ``–p`` option.

.. code-block::

    $ nvfwupd -t ip=<NVOS IP> user=*** password=*** servertype=vrnvl72switch show_version -p nvfw_VR-NVL72-N6100-LD_0001_260128.1.0_prod-signed.fwpkg
    
    System Model: N6100_LD
    Part number: $TRAY_PART_NUMBER
    Serial number: $TRAY_SERIAL_NUMBER
    Packages: ['VR-NVL72-N6100-LD_0001_260128.1.0']
    Connection Status: Successful

    Firmware Devices:
    AP Name              Sys Version           Pkg Version            Up-To-Date
    -------              -----------           -----------            ----------
    ASIC                 41.2018.0254          N/A                    No        
    BIOS                 0ACTV_00.01.028       0ACTV_00.01.026        Yes       
    BMC                  88.0060.0077          88.0002.2000           Yes       
    CPLD1                CPLD000449_REV0012    CPLD000449_REV0012     Yes       
    CPLD2                CPLD000450_REV0007    CPLD000450_REV0007     Yes       
    EROT                 02.00.0016.0000_n05   02.00.0016.0000_n05    Yes       
    EROT-BMC             02.00.0016.0000_n05   02.00.0016.0000_n05    Yes       
    EROT-CPU             02.00.0016.0000_n05   02.00.0016.0000_n05    Yes       
    SMA                  0020.01.0125.0000     0020.01.0125.0000      Yes       
    SMA1                 0020.01.0125.0000     0020.01.0125.0000      Yes       
    SMA2                 0020.01.0125.0000     0020.01.0125.0000      Yes       
    SSD                  ETFIPV.2              N/A                    No        
    transceiver          N/A                   N/A                    No        
    ----------------------------------------------------------------------------
    Error Code: 0


.. note::
    The ``SSD``, ``transceiver``, and ``ASIC`` can only be updated using inband update methods. These components cannot be updated using nvfwupd.

Full Bundle Firmware Update for Vera Rubin NVL72 Switch Components
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To update all Vera Rubin NVL72 Switch Tray components, pass the bundle without specifying targets. After the update completes, activate the firmware.

1. To update BMC, SMA, ERoT, SBIOS, and all CPLD components, use the .fwpkg file with the 0001 substring in the filename. Vera Rubin NVL72 Switch Tray uses a single .fwpkg file for all components.

2. Pass the .fwpkg file to the update_fw command as shown in the following example. The update runs in parallel for all components.

.. code-block::

    $ ./nvfwupd -t ip=<NVOS IP> user=**** password=**** servertype=vrnvl72switch update_fw -p nvfw_VR-NVL72-N6100-LD_0001_260121.1.0_prod-signed.fwpkg

    Updating IP address: ip-XXXX
    FW package: ['nvfw_VR-NVL72-N6100-LD_0001_260121.1.0_prod-signed.fwpkg']
    Ok to proceed with firmware update: <Y/N>
    Y
    The following targets will be updated ['BMC', 'BIOS', 'SMA', 'EROT', 'CPLD1']
    Attempting parallel update method for VRNVL72...
    Step 1: Uploading firmware package to /tmp...
    Update: fetched firmware file for: nvfw_VR-NVL72-N6100-LD_0001_260121.1.0_prod-signed.fwpkg was uploaded successfully
    Step 2: Fetching firmware file for all components: ['BMC', 'BIOS', 'SMA', 'EROT', 'CPLD1']
    Parallel fetch task was created with ID 1
    Status for Job Id 1:
    {'detail': '',
    'http_status': 200,
    'is_range_action': False,
    'issue': [],
    'percentage': '',
    'phase': 'running',
    'status': '',
    'statuses': [],
    'timeout': 1800,
    'type': '',
    'warnings': []}

    Status for Job Id 1:
    {'detail': 'File fetched successfully',
    'http_status': 200,
    'is_range_action': False,
    'issue': [],
    'percentage': '',
    'state': 'action_success',
    'status': 'File fetched successfully',
    'statuses': [],
    'timeout': 1800,
    'type': '',
    'warnings': []}

    Parallel fetch completed successfully
    Step 3: Installing firmware for all components: ['BMC', 'BIOS', 'SMA', 'EROT', 'CPLD1']
    Parallel FW update task was created with ID 2
    Status for Job Id 2:
    {'detail': 'Installing firmware: '
            'nvfw_VR-NVL72-N6100-LD_0001_260121.1.0_prod-signed.fwpkg',
    'http_status': 200,
    'is_range_action': False,
    'issue': [],
    'percentage': '0%',
    'state': 'running',
    'status': 'Installing firmware: '
            'nvfw_VR-NVL72-N6100-LD_0001_260121.1.0_prod-signed.fwpkg',
    'statuses': [],
    'timeout': 1800,
    'type': '',
    'warnings': []}

    Status for Job Id 2:
    {'detail': 'Installing firmware: '
            'nvfw_VR-NVL72-N6100-LD_0001_260121.1.0_prod-signed.fwpkg',
    'http_status': 200,
    'is_range_action': False,
    'issue': [],
    'percentage': '0%',
    'state': 'running',
    'status': 'Installing firmware: '
            'nvfw_VR-NVL72-N6100-LD_0001_260121.1.0_prod-signed.fwpkg',
    'statuses': [],
    'timeout': 1800,
    'type': '',
    'warnings': []}

    Status for Job Id 2:
    {'detail': 'EROT-BMC        ... Successfully updated\n'
            'BMC             ... In progress',
    'http_status': 200,
    'is_range_action': False,
    'issue': [],
    'percentage': '52%',
    'state': 'running',
    'status': 'EROT-BMC        ... Successfully updated\n'
            'BMC             ... In progress',
    'statuses': [],
    'timeout': 1800,
    'type': '',
    'warnings': []}

    Status for Job Id 2:
    {'detail': 'EROT-BMC        ... Successfully updated\n'
            'BMC             ... In progress',
    'http_status': 200,
    'is_range_action': False,
    'issue': [],
    'percentage': '55%',
    'state': 'running',
    'status': 'EROT-BMC        ... Successfully updated\n'
            'BMC             ... In progress',
    'statuses': [],
    'timeout': 1800,
    'type': '',
    'warnings': []}

    Status for Job Id 2:
    {'detail': 'EROT-BMC        ... Successfully updated\n'
            'BMC             ... In progress',
    'http_status': 200,
    'is_range_action': False,
    'issue': [],
    'percentage': '58%',
    'state': 'running',
    'status': 'EROT-BMC        ... Successfully updated\n'
            'BMC             ... In progress',
    'statuses': [],
    'timeout': 1800,
    'type': '',
    'warnings': []}

    Status for Job Id 2:
    {'detail': 'EROT-BMC        ... Successfully updated\n'
            'BMC             ... In progress',
    'http_status': 200,
    'is_range_action': False,
    'issue': [],
    'percentage': '61%',
    'state': 'running',
    'status': 'EROT-BMC        ... Successfully updated\n'
            'BMC             ... In progress',
    'statuses': [],
    'timeout': 1800,
    'type': '',
    'warnings': []}
    
    Status for Job Id 2:
    {'detail': 'EROT-BMC        ... Successfully updated\n'
            'BMC             ... In progress',
    'http_status': 200,
    'is_range_action': False,
    'issue': [],
    'percentage': '100%',
    'state': 'running',
    'status': 'EROT-BMC        ... Successfully updated\n'
            'BMC             ... In progress',
    'statuses': [],
    'timeout': 1800,
    'type': '',
    'warnings': []}

    Status for Job Id 2:
    {'detail': 'EROT-BMC        ... Successfully updated\n'
            'BMC             ... In progress',
    'http_status': 200,
    'is_range_action': False,
    'issue': [],
    'percentage': '100%',
    'state': 'running',
    'status': 'EROT-BMC        ... Successfully updated\n'
            'BMC             ... In progress',
    'statuses': [],
    'timeout': 1800,
    'type': '',
    'warnings': []}

    Status for Job Id 2:
    {'detail': 'EROT-BMC        ... Successfully updated\n'
            'BMC             ... In progress',
    'http_status': 200,
    'is_range_action': False,
    'issue': [],
    'percentage': '100%',
    'state': 'running',
    'status': 'EROT-BMC        ... Successfully updated\n'
            'BMC             ... In progress',
    'statuses': [],
    'timeout': 1800,
    'type': '',
    'warnings': []}

    Status for Job Id 2:
    {'detail': 'EROT-BMC        ... Successfully updated\n'
            'BMC             ... In progress',
    'http_status': 200,
    'is_range_action': False,
    'issue': [],
    'percentage': '100%',
    'state': 'running',
    'status': 'EROT-BMC        ... Successfully updated\n'
            'BMC             ... In progress',
    'statuses': [],
    'timeout': 1800,
    'type': '',
    'warnings': []}

    Status for Job Id 2:
    {'detail': 'EROT-BMC        ... Successfully updated\n'
            'BMC             ... In progress',
    'http_status': 200,
    'is_range_action': False,
    'issue': [],
    'percentage': '100%',
    'state': 'action_success',
    'status': 'EROT-BMC        ... Successfully updated\n'
            'BMC             ... In progress',
    'statuses': [],
    'timeout': 1800,
    'type': '',
    'warnings': []}

    Parallel update method completed successfully for all components
    ================================================================================
    Error Code: 0

3. After the update for all components is complete, to activate the installed versions, run the ``activate_fw`` command.

.. code-block::

    $ nvfwupd -t ip=<NVOS IP> user=*** password=*** servertype=vrnvl72switch activate_fw -c NVUE_PWR_CYCLE

    Power cycle task was created with ID 3

    Status for Job Id 3:
    {'detail': '',
    'http_status': 200,
    'issue': [],
    'percentage': '',
    'state': 'running',
    'status': '',
    'timeout': 5,
    'type': ''}

Targeted Firmware Updates for Vera Rubin NVL72 Switch Components
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To update a component in the Vera Rubin NVL72 Switch Tray, use the ``–s`` or the ``--special`` option with the ``update_fw`` command shown in examples in step 2.

.. note::
    Parallel component update method is not supported for targeted firmware updates.

1. To update the ``BMC``, use the ``fwpkg`` file with ``0001`` sub-string in the name.

2. Create a JSON file, like ``targets.json`` in the following example, and pass these two files as input to the ``update_fw`` command as shown below.

.. code-block:: 

    $ cat targets.json
    {"Targets": ["BMC"]}

    $ ./nvfwupd -t ip=<NVOS IP> user=**** password=**** servertype=vrnvl72switch update_fw -s targets.json -p nvfw_VR-NVL72-N6100-LD_0001_260128.1.0_prod-signed.fwpkg

    Updating ip address: ip=XXXX
    FW package: ['nvfw_VR-NVL72-N6100-LD_0001_260128.1.0_prod-signed.fwpkg']
    Ok to proceed with firmware update? <Y/N>
    y
    The following targets will be updated ['BMC']
    Update file nvfw_VR-NVL72-N6100-LD_0001_260128.1.0_prod-signed.fwpkg was uploaded successfully
    Starting FW update for: BMC
    FW update task was created with ID 10

    Status for Job Id 10:
    {'detail': '',
    'http_status': 200,
    'issue': [],
    'percentage': '',
    'state': 'start',
    'status': '',
    'timeout': 1200,
    'type': ''}

    Status for Job Id 10:
    {'detail': 'Installing firmware: '
    'nvfw_VR-NVL72-N6100-LD_0001_260128.1.0_prod-signed.fwpkg',
    'http_status': 200,
    'issue': [],
    'percentage': '',
    'state': 'running',
    'status': 'Installing firmware: '
    'nvfw_VR-NVL72-N6100-LD_0001_260128.1.0_prod-signed.fwpkg',
    'timeout': 1200,
    'type': ''}


    Status for Job Id 10:
    {'detail': 'Firmware nvfw_VR-NVL72-N6100-LD_0001_260128.1.0_prod-signed.fwpkg '
    'installed successfully',
    'http_status': 200,
    'issue': [],
    'percentage': '',
    'state': 'action_success',
    'status': 'Firmware nvfw_VR-NVL72-N6100-LD_0001_260128.1.0_prod-signed.fwpkg '
    'installed successfully',
    'timeout': 1200,
    'type': ''}

    -------------------------------------------------------------------------------------

    Error Code: 0

3. After the update for all desired components is complete, run the ``activate_fw`` command to activate the installed firmware.

.. code-block::

    $ nvfwupd -t ip=<NVOS IP> user=*** password=*** servertype=vrnvl72switch activate_fw -c NVUE_PWR_CYCLE

    Power cycle task was created with ID 11
    Status for Job Id 11:
    {'detail': '',
    'http_status': 200,
    'issue': [],
    'percentage': '',
    'state': 'running',
    'status': '',
    'timeout': 5,
    'type': ''}

4. To update any other component, replace the ``BMC`` in the ``targets.json`` file from the example above with the component name. To update the ``CPLD``, use a component name of ``CPLD1``.

5. Pass the ``targets.json`` and update package as described in step 2.

IST Vector Support
~~~~~~~~~~~~~~~~~~

NVFWUPD v2.1.1 and later supports IST vector uploads to Vera CPUs for platforms such as Vera Rubin NVL72, and MGX Vera C2.

IST Vector Inventory
~~~~~~~~~~~~~~~~~~~~

Display IST vector versions using the ``show_version`` command. Include the ``-i`` option to display software inventory.

.. code-block::

    $ nvfwupd -t ip=<BMC_IP> user=*** password=**** show_version -i -p nvfw_IST-Vectors_0000_260304.1.0.fwpkg
    System Model: VR NVL72
    Part number: $TRAY_PART_NUMBER
    Serial number: $TRAY_SERIAL_NUMBER
    Packages: ['IST-Vectors_0000_260304.1.0']
    Connection Status: Successful

    Firmware Devices:

    AP Name                       Sys Version         Pkg Version           Up-To-Date
    --------                      -----------         -----------           ----------
    FW_BMC_0                      VR-2512-07.01       N/A                   No
    FW_CX_0                       82.48.0756          N/A                   No
    FW_CX_1                       82.48.0756          N/A                   No
    FW_CX_2                       82.48.0756          N/A                   No
    FW_CX_3                       82.48.0756          N/A                   No
    FW_CX_4                       82.48.0756          N/A                   No
    FW_CX_5                       82.48.0756          N/A                   No
    FW_CX_6                       82.48.0756          N/A                   No
    FW_CX_7                       82.48.0756          N/A                   No
    FW_ERoT_BMC_0                 02.00.0016.0000_n05 N/A                   No
    FW_IO_Board_SMA_0             0027.01.0116.1002   N/A                   No
    FW_IO_Board_SMA_1             0027.01.0116.1002   N/A                   No
    FW_IO_Board_SMA_2             0027.01.0116.1002   N/A                   No
    FW_IO_Board_SMA_3             0027.01.0116.1002   N/A                   No
    FW_NVMe_E1S_210               LDDJ3U2Q            N/A                   No
    FW_NVMe_E1S_65                G77YG100            N/A                   No
    FW_NVMe_E1S_75                G77YG100            N/A                   No
    FW_NVMe_E1S_85                G77YG100            N/A                   No
    FW_NVMe_E1S_95                G77YG100            N/A                   No
    HGX_FW_BMC_0                  VR-2512-6.A         N/A                   No
    HGX_FW_CPLD_0                 0.22                N/A                   No
    HGX_FW_CPLD_1                 0.22                N/A                   No
    HGX_FW_ERoT_BMC_0             02.00.0016.0000_n05 N/A                   No
    HGX_FW_GPU_0                  99.00.01.53.27      N/A                   No
    HGX_FW_GPU_1                  99.00.01.53.27      N/A                   No
    HGX_FW_GPU_2                  99.00.01.53.27      N/A                   No
    HGX_FW_GPU_3                  99.00.01.53.27      N/A                   No
    HGX_FW_GPU_SMA_0              0023.01.0125.0001   N/A                   No
    HGX_FW_GPU_SMA_1              0023.01.0125.0001   N/A                   No
    HGX_FW_ProcessorModule_CPLD_0 0048.46.0050.0050   N/A                   No
    HGX_FW_ProcessorModule_CPLD_1 0048.46.0050.0050   N/A                   No
    HGX_FW_ProcessorModule_SMA_0  0026.01.0139.0013   N/A                   No
    HGX_FW_ProcessorModule_SMA_1  0026.01.0139.0013   N/A                   No
    HGX_InfoROM_GPU_0             G558.0200.00.01     N/A                   No
    HGX_InfoROM_GPU_1             G558.0200.00.01     N/A                   No
    HGX_InfoROM_GPU_2             G558.0200.00.01     N/A                   No
    HGX_InfoROM_GPU_3             G558.0200.00.01     N/A                   No
    HGX_SBIOS_FMC_0               00.02.00.10         N/A                   No
    HGX_SBIOS_FMC_1               00.02.00.10         N/A                   No
    HGX_SBIOS_FW_0                00.02.00.10         N/A                   No
    HGX_SBIOS_FW_1                00.02.00.10         N/A                   No
    IST_Vectors                   2_TP27              2_TP28                No

    Error Code: 0

The IST_Vectors AP is for the IST vector software running on the BMC.

IST Vector Updates
~~~~~~~~~~~~~~~~~~

Run the ``update_fw`` command to upload IST vectors. Specify the IST vector package and the corresponding UpdateParameters file.

Depending on the platform, there may be both IST_Vectors and HGX_IST_Vectors targets.
Each AP can be targeted with "/redfish/v1/UpdateService/SoftwareInventory/IST_Vectors" or "/redfish/v1/UpdateService/SoftwareInventory/HGX_IST_Vectors" URIs respectively.

To update the IST_Vectors AP, the UpdateParameters JSON file must specify the IST_Vectors target:

.. code-block:: 

    $ cat ist_vector_update.json
    {
        "Targets": ["/redfish/v1/UpdateService/SoftwareInventory/IST_Vectors"]
    }

After the JSON file is created, upload the IST vector package using the ``update_fw`` command:

.. code-block::

    $ nvfwupd -t ip=<BMC IP> user=*** password=*** update_fw -p nvfw_IST-Vectors_0000_260304.1.0.fwpkg -s ist_vector_update.json
    Updating ip address: ip=XXXX
    Fw packages ['nvfw_IST-Vectors_0000_260304.1.0.fwpkg']

    {"@odata.id": "/redfish/v1/TaskService/Tasks/5", "@odata.type": "#Task.v1_4_1.Task", "hidePayload": true, "Id": "5", "Messages": [{"@odata.type": "#Message.v1_1_1.Message", "Message": "The task with Id '5' has started.", "MessageArgs": ["5"], "MessageId": "TaskEvent.1.0.TaskStarted", "MessageSeverity": "OK", "Resolution": "None."}], "Name": "Task 5", "PercentComplete":
    0, "TaskMonitor": "/redfish/v1/TaskService/TaskMonitors/5", "TaskState": "Running", "TaskStatus": "OK"}
    FW update started, Task Id: 5
    Wait for firmware Update to Start...

    PercentComplete: 10
    TaskStatus: Running
    PercentComplete: 20
    TaskStatus: Running
    PercentComplete: 40
    TaskStatus: Running
    PercentComplete: 60
    TaskStatus: Running
    PercentComplete: 80
    TaskStatus: Running
    PercentComplete: 80
    TaskStatus: Completed
    PercentComplete: 100
    TaskStatus: OK
    Firmware update successful!

    Overall Time Taken: 0:02:50
    ------------------------------------------------------------------------------------------------------------------------
    Error Code: 0
