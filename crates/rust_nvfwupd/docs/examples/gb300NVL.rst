GB300 NVL Update Examples
-------------------------

The firmware update mechanism for GB300 NVL is different from the update mechanism for MGX in the following ways:

-  GB300 NVL uses two fwpkg bundles.

    - The **P4058** packages are for the BMC tray.
    - The **P4059** packages are for the compute tray.

-  The update targets file are passed with the ``–s`` option, which can be used to specify the update target for the BMC and compute trays (refer to the sample outputs in the :ref:`next section <gb300_updates>`).

-  To downgrade the GB300 NVL BMC or compute tray firmware, set the ``ForceUpdate`` flag in the update target JSON file that is passed with the ``–s`` option.

.. _gb300_updates:

Updating the GB300 NVL BMC Tray
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. note::
    Starting with NVFWUPD 2.1.0, the tool prevents concurrent BMC tray updates. NVFWUPD returns an error if you attempt to start a new update while another is in progress.
    If a user prefers the old behavior of still pushing the update even if an existing update is in progress, they can additionally pass the ``--skip_pre_flight_checks`` or ``-k`` option to the ``update_fw`` command.

To update the complete BMC tray:

1. Create a JSON file like the ``BMC_Full.json`` file in the example.

2. Use the nvfwupd tool and run the ``update_fw`` command.

In the package name, the BMC tray update packages can be identified by ``GB300-P4058``.

1. After the update successfully completes, to activate the firmware, complete a power cycle.

2. After the BMC is up and the Redfish service is running, to determine whether the BMC tray components were updated and they match the versions in the package, run the ``show_version`` command.

Here is the output:

.. code-block::

    $ cat BMC_Full.json

    {
        "Targets": []
    }

    nvfwupd -t ip=<BMC_IP> user=**** password=**** servertype=GB300 update_fw -p nvfw_GB300-P4058_0001_250422.1.0_prod-signed.fwpkg -s BMC_Full.json

    Updating ip address: ip=XXXX
    FW package: ['nvfw_GB300-P4058_0001_250422.1.0_prod-signed.fwpkg ']
    Ok to proceed with firmware update? <Y/N>
    y
    {"@odata.id": "/redfish/v1/TaskService/Tasks/1", "@odata.type": "#Task.v1_4_3.Task", "Id": "1", "TaskState": "Running", "TaskStatus": "OK"}
    FW update started, Task Id: 1
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
    Overall Time Taken: 0:10:35
    Refer to 'NVIDIA Firmware Update Document' on activation steps for new firmware to take effect.
    ------------------------------------------------------------------------------------
    Error Code: 0

.. note::
    For NVFWUPD 2.0.6 and later, the ``-s`` option is no longer required when updating the entire BMC tray. The default "Targets" are all components without ``force_update``.

Updating the GB300 NVL Compute Tray
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. note::
    For NVFWUPD 2.1.0 and later, it is no longer possible to start a new compute tray update while an existing compute tray update is in progress. NVFWUPD will now return an error if this is attempted.
    If a user prefers the old behavior of still pushing the update even if an existing update is in progress, they can additionally pass the ``--skip_pre_flight_checks`` or ``-k`` option to the ``update_fw`` command.

To update the compute tray:

1. Create a JSON file, like the ``Compute_Full.json`` file, in the example.

2. Use the ``nvfwupd`` tool and run the ``update_fw`` command.

In the package name, the Compute tray update packages can be identified by ``GB300-P4059``.

1. After the update successfully completes, to activate the firmware, complete an AC cycle.

2. After the BMC is up and the Redfish service is running again, to determine whether the compute tray components were updated and they match the versions in the package, run the ``show_version`` command.

Here is an example:

.. code-block::

    $ cat Compute_Full.json

    {
        "Targets": ["/redfish/v1/Chassis/HGX_Chassis_0"]
    }

    $ nvfwupd -t ip=<BMC IP> user=*** password=*** servertype=GB300 update_fw -s Compute_Full.json -p nvfw_GB300-P4059_0002_250422.1.0_custom_prod-signed.fwpkg

    Updating ip address: ip=XXXX
    FW package: ['nvfw_GB300-P4059_0002_250422.1.0_custom_prod-signed.fwpkg']
    Ok to proceed with firmware update? <Y/N>
    y
    {"@odata.id": "/redfish/v1/TaskService/Tasks/HGX_1", "@odata.type": "#Task.v1_4_3.Task", "Id": "HGX_1", "TaskState": "Running", "TaskStatus": "OK"}
    FW update started, Task Id: HGX_1
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
    Overall Time Taken: 0:11:20
    Refer to 'NVIDIA Firmware Update Document' on activation steps for new firmware to take effect.
    ------------------------------------------------------------------------------------
    Error Code: 0


.. note::
    For NVFWUPD 2.0.6 and later, the ``-s`` option is no longer required when updating the entire Compute tray. The default "Targets" are all components without ``force_update``.

GB300 NVL Firmware Downgrades Using the Force Update Option
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To downgrade the GB300 NVL firmware, you must use the force update multipart option, which can be set in the update parameters JSON file targets, and are passed in the JSON file with the ``–s`` option. If you try firmware updates as described in the previous sections, and you see the following error message in the firmware update log:

.. note::
    The Component comparison stamp is lower than the firmware component comparison stamp in the FD.

Retry with a force firmware update but change the Targets value based on the tray you want to force update.

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

    $ nvfwupd -t ip=<BMC IP> user=*** password=**** servertype=GB300 update_fw -s force_BMC_Full.json -p nvfw_GB300-P4058_0001_250422.1.0_prod-signed.fwpkg

    Updating ip address: ip=XXXX
    FW package: ['nvfw_GB300-P4058_0001_250422.1.0_prod-signed.fwpkg']
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

GB300 NVL Firmware Updates for Selected Components
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To complete a firmware update of a component:

1. Identify the inventory name of the component.

   -  The ``show_version`` option can be used to list all the components in the inventory with their current versions.

   -  Components names that are prefixed with **HGX** can be updated using a compute tray package, and the rest of the components will need the BMC tray package.

.. code-block ::

    nvfwupd -t ip=<BMC_IP> user=**** password=**** show_version -p nvfw_GB300-P4058_0000_250422.1.0_prod-signed.fwpkg nvfw_GB300-P4059_0002_250422.1.0_custom_prod-signed.fwpkg

    System Model: GB300 NVL
    Part number: $TRAY_PART_NUMBER
    Serial number: $TRAY_SERIAL_NUMBER
    Packages: ['GB300-P4058_0000_250422.1.0', 'GB300-P4059_0002_250422.1.0_custom']
    Connection Status: Successful

    Firmware Devices:
    AP Name                 Sys Version                 Pkg Version           Up-To-Date
    -------                 -----------                 -----------           ----------
    FW_BMC_0                GB3-2503-02.0               GB3-2503-04.0         No        
    FW_CPLD_0               0x00 0x0c 0x06 0x04         C_06_04               Yes       
    FW_CPLD_1               0x00 0x0c 0x06 0x04         C_06_04               Yes       
    FW_CPLD_2               0x00 0x0c 0x06 0x04         C_06_04               Yes        
    FW_CPLD_3               0x00 0x0c 0x06 0x04         C_06_04               Yes        
    FW_ERoT_BMC_0           01.04.0008.0000_n04         01.04.0008.0000_n04   Yes       
    NIC_1                   32.41.1300                  N/A                   No        
    SMA_0                   0010.00.0131.0000           0010.00.0145.0000     No        
    SMA_1                   0010.00.0131.0000           0010.00.0145.0000     No        
    UEFI                    buildbrain-gcid-39352899    N/A                   No        
    HGX_FW_BMC_0            GB3-2503-04.0               GB3-2503-04.0         Yes       
    HGX_FW_CPLD_0           0.1C                        0.1C                  Yes       
    HGX_FW_CPU_0            3.0.4_dot                   03.00.05              Yes       
    HGX_FW_CPU_1            3.0.4_dot                   03.00.05              Yes       
    HGX_FW_ERoT_BMC_0       01.04.0008.0000_n04         01.04.0008.0000_n04   Yes       
    HGX_FW_ERoT_CPU_0       01.04.0008.0000_n04         01.04.0008.0000_n04   Yes       
    HGX_FW_ERoT_CPU_1       01.04.0008.0000_n04         01.04.0008.0000_n04   Yes       
    HGX_FW_ERoT_FPGA_0      01.04.0008.0000_n04         01.04.0008.0000_n04   Yes       
    HGX_FW_FPGA_0           0.30                        0.32                  No        
    HGX_FW_GPU_0            97.10.12.00.00              97.10.12.00.01        No        
    HGX_FW_GPU_1            97.10.12.00.00              97.10.12.00.01        No        
    HGX_FW_GPU_2            97.10.06.00.00              N/A                   No        
    HGX_FW_GPU_3            97.10.06.00.00              N/A                   No        
    HGX_InfoROM_GPU_0       G540.0211.00.05             N/A                   No        
    HGX_InfoROM_GPU_1       G540.0211.00.05             N/A                   No        
    HGX_InfoROM_GPU_2       G540.0211.00.05             N/A                   No        
    HGX_InfoROM_GPU_3       G540.0211.00.05             N/A                   No        
    HGX_PCIeSwitchConfig_0  01300524                    N/A                   No        
    HGX_SXM_MCU_0           0004.00.0123.0000           N/A                   No        
    HGX_SXM_MCU_1           0004.00.0123.0000           N/A                   No        
    HGX_SXM_MCU_2           0004.00.0123.0000           N/A                   No        
    HGX_SXM_MCU_3           0004.00.0123.0000           N/A                   No        
    -----------------------------------------------------------------------------------
    Error Code: 0


2. After identifying the inventory name, create the JSON file with the Redfish inventory URI of that component (``/redfish/v1/UpdateService/FirmwareInventory/<component name>``).

   The example in step 4 shows a sample ``CPU.json`` file that is used to update only the ``HGX_FW_CPU_0`` component on the tray.

3.  Run the ``update_fw`` command with the ``CPU.json`` file and compute tray bundle as the inputs.

4.  To perform a downgrade, add the ``"ForceUpdate": true`` field to this JSON file.

.. code-block:: 

    $ cat CPU.json

    {
        "Targets":["/redfish/v1/UpdateService/FirmwareInventory/HGX_FW_CPU_0"]
    }

    $ nvfwupd --target ip=<BMC IP> user=*** password=*** servertype=GB300 update_fw -s CPU.json -p nvfw_GB300-P4059_0002_250422.1.0_custom_prod-signed.fwpkg

    Updating ip address: ip=XXXX
    FW package: ['nvfw_GB300-P4059_0002_250422.1.0_custom_prod-signed.fwpkg']
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

After performing firmware update of a component, or a full bundle, complete an AC power cycle to activate the new firmware. It can take up to five minutes for the BMC and Redfish service to come up after power cycle is complete. The ``activate_fw`` command returns immediately after issuing a command and does not wait for the BMC to come back up. To check new system versions after the BMC Redfish service is back, run the show version command. Version 2.0.3 and later of the tool supports the ``activate_fw`` command to complete this task.

Here is an example that uses the ``activate_fw`` command:

.. code-block::

    $ nvfwupd -t ip=<BMC IP> user=**** password=**** servertype=GB300 activate_fw -c PWR_OFF

    IPMI Command Status: Success

    Chassis Power Control: Down/Off

    -------------------------------------------------------------------------------------

.. code-block::

    $ nvfwupd -t ip=<BMC IP> user=**** password=**** servertype=GB300 activate_fw –c RF_AUX_PWR_CYCLE

    AUX Power Cycle requested successfully.

    Server response:

    ""

    -------------------------------------------------------------------------------------

After running the above commands, the system will reboot. Once the system is reachable again, use the Power On command to enable everything.

.. code-block::
    
    $ nvfwupd -t ip=<BMC IP> user=**** password=**** servertype=GB300 activate_fw -c PWR_ON

    IPMI Command Status: Success

    Chassis Power Control: Up/On

    -------------------------------------------------------------------------------------


Alternatively, as of NVFWUPD 2.0.6, Redfish activation commands can be used:

.. code-block::

    $ nvfwupd -t ip=<BMC IP> user=**** password=**** servertype=GB300 activate_fw -c RF_PWR_OFF

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
    -------------------------------------------------------------------------------------


.. code-block::

    $ nvfwupd -t ip=<BMC IP> user=**** password=**** servertype=GB300 activate_fw –c RF_AUX_PWR_CYCLE

    AUX Power Cycle requested successfully.

    Server response:

    ""

    -------------------------------------------------------------------------------------

After running the above commands, the system will reboot. Once the system is reachable again, use the Redfish Power On command to enable everything.

.. code-block::
    
    $ nvfwupd -t ip=<BMC IP> user=**** password=**** servertype=GB300 activate_fw -c RF_PWR_ON

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

The power status of system components can be checked with the RF_PWR_STATUS activate_fw command.

.. code-block::

    $ nvfwupd -t ip=<BMC IP> user=*** password=*** activate_fw -c RF_PWR_STATUS
    Power Status:
    System: System_0
    URI: /redfish/v1/Systems/System_0
    PowerState: On

    System: HGX_Baseboard_0
    URI: /redfish/v1/Systems/HGX_Baseboard_0
    PowerState: On