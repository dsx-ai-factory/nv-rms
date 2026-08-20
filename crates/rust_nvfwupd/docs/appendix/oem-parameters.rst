.. _oem-parameters:

OEM Parameters Support for Firmware Update Operations
======================================================

NVFWUPD 2.0.9 introduces support for OEM (Original Equipment Manufacturer) specific parameters during firmware update operations. This feature allows users to pass vendor-specific parameters to customize the firmware update behavior according to OEM requirements.

Overview
--------

OEM parameters provide a flexible mechanism to pass vendor-specific configuration options during firmware updates. These parameters are passed through a JSON file and used to control various aspects of the update process based on manufacturer specifications.

Using OEM Parameters
--------------------

OEM parameters are specified using the ``--oem_parameters`` or ``-o`` option with the ``update_fw`` command. The parameters are defined in a JSON file that is passed as an argument.

Syntax
^^^^^^

.. code-block::

    $ nvfwupd -t ip=<BMC IP> user=<username> password=<password> servertype=<servertype> update_fw -p <package> -o <oem_params.json>

or

.. code-block::

    $ nvfwupd -t ip=<BMC IP> user=<username> password=<password> servertype=<servertype> update_fw -p <package> --oem_parameters <oem_params.json>

OEM Parameters File Format
---------------------------

The OEM parameters file is a JSON file containing key-value pairs specific to the OEM requirements. The structure and accepted parameters vary by platform and vendor.

Basic Structure
^^^^^^^^^^^^^^^

.. code-block:: json

    {
        "ImageType": "PLDM",
        "Platform": "HGX"
    }


Example Usage
-------------

Example 1: Dell SBIOS Update with OEM Parameters
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

This example demonstrates updating a Dell SBIOS on GB200 NVL using OEM parameters to specify the image type and platform.

First, check the current firmware versions:

.. code-block::

    $ nvfwupd -t ip=<BMC_IP> user=<username> password=<password> servertype=GB200 show_version -p DellServer9712a_BIOS216.fwpkg

    System Model: GB200 NVL
    Part number: $TRAY_PART_NUMBER
    Serial number: $TRAY_SERIAL_NUMBER
    Packages: ['cec1736ApFwPkg-216']
    Connection Status: Successful

    Firmware Devices:
    AP Name                  Sys Version            Pkg Version   Up-To-Date
    -------                  -----------            -----------   ----------
    BMC                      25.07.29001500         N/A           No        
    HGX_FW_BMC_0             GB200Nvl-25.02-E       N/A           No        
    HGX_FW_CPLD_0            0.1D                   N/A           No    
    HGX_FW_CPU_0             12                     N/A           No    
    HGX_FW_CPU_1             12                     N/A           No        
    HGX_FW_ERoT_BMC_0        01.04.0008.0000_n04    N/A           No        
    HGX_FW_ERoT_CPU_0        01.04.0008.0000_n04    N/A           No        
    HGX_FW_ERoT_CPU_1        01.04.0008.0000_n04    N/A           No        
    HGX_FW_ERoT_FPGA_0       01.04.0008.0000_n04    N/A           No        
    HGX_FW_ERoT_FPGA_1       01.04.0008.0000_n04    N/A           No        
    HGX_FW_FPGA_0            1.30                   N/A           No        
    HGX_FW_FPGA_1            1.30                   N/A           No        
    HGX_FW_GPU_0             97.00.A2.00.02         N/A           No        
    HGX_FW_GPU_1             97.00.A2.00.02         N/A           No        
    HGX_FW_GPU_2             97.00.A2.00.02         N/A           No        
    HGX_FW_GPU_3             97.00.A2.00.02         N/A           No        
    HGX_PCIeSwitchConfig_0   01151024               N/A           No        
    StorageUnit_0            1.0.0                  N/A           No        
    StorageUnit_2            E2MU290                N/A           No        
    StorageUnit_3            E2MU290                N/A           No        
    StorageUnit_4            E2MU290                N/A           No        
    StorageUnit_5            E2MU290                N/A           No        
    StorageUnit_6            E2MU290                N/A           No        
    StorageUnit_7            E2MU290                N/A           No        
    StorageUnit_8            E2MU290                N/A           No        
    StorageUnit_9            E2MU290                N/A           No        
    ------------------------------------------------------------------------
    Error Code: 0

.. note::
    An exact component match could not be made for this SBIOS firmware package due to metadata mismatches. This will be improved in future releases.

Create an OEM parameters file specifying the image type and platform:

.. code-block::

    $ cat oem_params.json

    {
        "ImageType": "PLDM",
        "Platform": "HGX"
    }

Use the OEM parameters file during the SBIOS update:

.. code-block::

    $ nvfwupd -t ip=<BMC_IP> user=**** password=***** servertype=GB200 update_fw -p DellServer9712a_BIOS216.fwpkg -o oem_params.json

    Updating ip address: ip=XXXX
    FW package: ['DellServer9712a_BIOS216.fwpkg']
    Ok to proceed with firmware update? <Y/N>
    y
    {"@odata.id": "/redfish/v1/TaskService/Tasks/2", "@odata.type": "#Task.v1_7_4.Task", "Description": "Task for Update Service Task", "Id": "2", "Messages": [{"@odata.type": "#Message.v1_3_0.Message", "Message": "The action UpdateService.MultipartPush was submitted to do firmware update.", "MessageArgs": ["UpdateService.MultipartPush"], "MessageId": "UpdateService.1.0.StartFirmwareUpdate", "Resolution": "None", "Severity": "OK"}], "Name": "Update Service Task", "StartTime": "2025-10-21T21:20:32+04:00", "TaskMonitor": "/redfish/v1/TaskService/TaskMonitors/2", "TaskState": "New", "TaskStatus": "OK"}
      FW update started, Task Id: 2
    Wait for Firmware Update to Start...
      TaskState: Running
      PercentComplete: 20
      TaskStatus: OK
      TaskState: Completed
      PercentComplete: 100
      TaskStatus: OK
      Firmware update successful!
    Overall Time Taken: 0:04:55
    Refer to 'NVIDIA Firmware Update Document' on activation steps for new firmware to take effect.
    ------------------------------------------------------------------------------------
    Error Code: 0

.. note::
    In this example, the OEM parameters specify ``ImageType: PLDM`` and ``Platform: HGX`` which are required for Dell SBIOS updates on GB200 NVL systems. The update completed successfully in approximately 5 minutes.

Post Activation Validation:

Validate that the firmware update completed successfully by checking the updated firmware versions (CPU version should be 16):

.. code-block::

    $ nvfwupd -t ip=<BMC_IP> user=**** password=***** servertype=GB200 show_version -p DellServer9712a_BIOS216.fwpkg

    System Model: GB200 NVL
    Part number: $TRAY_PART_NUMBER
    Serial number: $TRAY_SERIAL_NUMBER
    Packages: ['cec1736ApFwPkg-216']
    Connection Status: Successful

    Firmware Devices:
    AP Name                   Sys Version          Pkg Version  Up-To-Date
    -------                   -----------          -----------  ----------
    BMC                       25.07.29001500       N/A          No        
    HGX_FW_BMC_0              GB200Nvl-25.02-E     N/A          No        
    HGX_FW_CPLD_0             0.1D                 N/A          No
    HGX_FW_CPU_0              16                   N/A          No       
    HGX_FW_CPU_1              16                   N/A          No        
    HGX_FW_ERoT_BMC_0         01.04.0008.0000_n04  N/A          No        
    HGX_FW_ERoT_CPU_0         01.04.0008.0000_n04  N/A          No        
    HGX_FW_ERoT_CPU_1         01.04.0008.0000_n04  N/A          No        
    HGX_FW_ERoT_FPGA_0        01.04.0008.0000_n04  N/A          No        
    HGX_FW_ERoT_FPGA_1        01.04.0008.0000_n04  N/A          No        
    HGX_FW_FPGA_0             1.30                 N/A          No        
    HGX_FW_FPGA_1             1.30                 N/A          No        
    HGX_FW_GPU_0              97.00.A2.00.02       N/A          No        
    HGX_FW_GPU_1              97.00.A2.00.02       N/A          No        
    HGX_FW_GPU_2              97.00.A2.00.02       N/A          No        
    HGX_FW_GPU_3              97.00.A2.00.02       N/A          No        
    HGX_PCIeSwitchConfig_0    01151024             N/A          No        
    StorageUnit_0             1.0.0                N/A          No        
    StorageUnit_2             E2MU290              N/A          No        
    StorageUnit_3             E2MU290              N/A          No        
    StorageUnit_4             E2MU290              N/A          No        
    StorageUnit_5             E2MU290              N/A          No        
    StorageUnit_6             E2MU290              N/A          No        
    StorageUnit_7             E2MU290              N/A          No        
    StorageUnit_8             E2MU290              N/A          No        
    StorageUnit_9             E2MU290              N/A          No        
    ----------------------------------------------------------------------
    Error Code: 0


Configuration File Integration
-------------------------------

OEM parameters can also be specified directly in the YAML configuration file using the ``OemParameters`` field:

.. code-block:: yaml

    TargetPlatform: 'GB200'

    BMC_IP: "BMC_IP"
    RF_USERNAME: "BMC_USERNAME"
    RF_PASSWORD: "BMC_PASSWORD"

    FWUpdateFilePath:
    - "DellServer9712a_BIOS216.fwpkg"

    FwUpdateMethod: "MultipartHttpPushUri"

    UpdateParametersTargets: []

    OemParameters: {"ImageType": "PLDM", "Platform": "HGX"}


Using the configuration file:

.. code-block::

    $ nvfwupd -c config.yaml show_version
    $ nvfwupd -c config.yaml update_fw

GB200/GB300 NVL Platforms
^^^^^^^^^^^^^^^^^^^^^^^^^^

GB200 and GB300 NVL platforms support platform-specific parameters for special firmware types:

Dell SBIOS Updates
"""""""""""""""""""

For Dell SBIOS updates on GB200 NVL systems, use the following OEM parameters:

.. code-block:: json

    {
        "ImageType": "PLDM",
        "Platform": "HGX"
    }

These parameters specify the firmware image type (PLDM) and the target platform (HGX) required for Dell SBIOS firmware updates.

