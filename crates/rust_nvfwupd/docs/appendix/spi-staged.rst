SPI Staged Update
----------------------------------------------------------------------------
SPI Staged Updates allow you to download firmware component images in advance for the supported components, which reduces the downtime during firmware updates and activation.

GB200 NVL and GB300 NVL BMC/Compute trays support staged updates.

To begin SPI staging an update, run the ``update_fw`` command with the ``-u`` flag or the ``--staged_update`` option. This will only stage, but not activate, the firmware images.

.. code-block::

    nvfwupd -t ip=192.0.2.10 user=<username> password=<password> update_fw -p nvfw_GB200-P4972_0011_250404.1.0_prod-signed.fwpkg -s BMC_Full.json -u
    Updating ip address: ip=XXXX
    FW package: ['nvfw_GB200-P4972_0011_250404.1.0_prod-signed.fwpkg']
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
    Overall Time Taken: 0:11:35
    Refer to 'NVIDIA Firmware Update Document' on activation steps for new firmware to take effect.
    -------------------------------------------------------------------------------------
    Error Code: 0

After the update is finished, to view the staged components with the system and package versions, run the ``show_version`` command with the ``-s`` flag or the ``--staged`` option.

.. code-block::

    nvfwupd -t ip=192.0.2.10 user=<username> password=<password> show_version -p nvfw_GB200-P4972_0011_250404.1.0_prod-signed.fwpkg -s
    System Model: GB200 NVL
    Part number: $TRAY_PART_NUMBER
    Serial number: $TRAY_SERIAL_NUMBER
    Packages: ['GB200-P4972_0011_250404.1.0']
    Connection Status: Successful

    Firmware Devices:
    AP Name                  Sys Version         Staged Version  Pkg Version   Up-To-Date
    -------                  -----------         --------------  -----------   ----------
    FW_BMC_0                 GB200Nvl-25.01      GB200Nvl-25.02 GB200Nvl-25.02 No       
    FW_ERoT_BMC_0            01.04.0007_n04      01.04.0008_n04 01.04.0008_n04 No       
    Full_FW_Image_NIC_Slot_4 32.41.1300          N/A            N/A            No        
    Full_FW_Image_NIC_Slot_7 32.41.1300          N/A            N/A            No        
    UEFI                     buildbrain-gcid-397 N/A            N/A            No        
    HGX_FW_BMC_0             GB200Nvl-25.02-D    N/A            N/A            No        
    HGX_FW_CPLD_0            0.1D                N/A            N/A            No        
    HGX_FW_CPU_0             02.03.22            N/A            N/A            No        
    HGX_FW_CPU_1             02.03.22            N/A            N/A            No        
    HGX_FW_ERoT_BMC_0        01.04.0008.0000_n04 N/A            N/A            No        
    HGX_FW_ERoT_CPU_0        01.04.0008.0000_n04 N/A            N/A            No        
    HGX_FW_ERoT_CPU_1        01.04.0008.0000_n04 N/A            N/A            No        
    HGX_FW_ERoT_FPGA_0       01.04.0008.0000_n04 N/A            N/A            No        
    HGX_FW_ERoT_FPGA_1       01.04.0008.0000_n04 N/A            N/A            No        
    HGX_FW_FPGA_0            1.2E                N/A            N/A            No        
    HGX_FW_FPGA_1            1.2E                N/A            N/A            No        
    HGX_FW_GPU_0             97.00.16.00.00      N/A            N/A            No        
    HGX_FW_GPU_1             97.00.16.00.00      N/A            N/A            No        
    HGX_FW_GPU_2             97.00.16.00.00      N/A            N/A            No        
    HGX_FW_GPU_3             97.00.16.00.00      N/A            N/A            No        
    HGX_PCIeSwitchConfig_0   01170424            N/A            N/A            No        
    -------------------------------------------------------------------------------------
    Error Code: 0

Now that the component images have been staged, they can be quickly activated by using the same package and the ``-a`` flag or the ``--staged_activate_update`` option with the ``update_fw`` command.
Do not use the ``force_update`` option to use staged component images because the component images would be downloaded again.

.. code-block::

    nvfwupd -t ip=192.0.2.10 user=<username> password=<password> update_fw -p nvfw_GB200-P4972_0011_250404.1.0_prod-signed.fwpkg -a
    Updating ip address: ip=XXXX
    FW package: ['nvfw_GB200-P4972_0011_250404.1.0_prod-signed.fwpkg']
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
    Overall Time Taken: 0:01:15
    Refer to 'NVIDIA Firmware Update Document' on activation steps for new firmware to take effect.
    ------------------------------------------------------------------------------------
    Error Code: 0

When you use staged component images, the update takes less time. After the update is complete, to activate the images, complete the power cycle activation process.