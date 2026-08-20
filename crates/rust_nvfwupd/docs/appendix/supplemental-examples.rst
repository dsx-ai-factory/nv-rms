Supplemental Firmware Update Examples
===============================================

MGX and Other Grace Hopper Update Examples
------------------------------------------

This section provides update examples for MGX and Grace Hopper.

Updating the GPU ERoT Firmware
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To create the JSON file that shows the GPU's ERoT as the target, run the following command.

.. code-block::

    $ cat updparams.json
    {
        "HttpPushUriTargets" :["/redfish/v1/UpdateService/FirmwareInventory/ERoT_GPU_0"]
    }

To run a firmware update on the GPU ERoT on the Grace Hopper tray, run the following command.

.. code-block::

    $ nvfwupd -t ip=<BMC-IP> user=****\* password=****\* servertype=MGX update_fw -p nvfw_GH200-P5042_0004_230824.1.0_prod-signed.fwpkg -s updparams.json
    FW package: ['nvfw_GH200-P5042_0004_230824.1.0_prod-signed.fwpkg']
    Ok to proceed with firmware update? <Y/N>
    Y
    FW update started, Task Id: 7
    Wait for Firmware Update to Start...
    TaskState: Completed
    PercentComplete: 100
    TaskStatus: OK
    Firmware update successful!
    Overall Time Taken: 0:02:21
    Refer to 'NVIDIA Firmware Update Document' on activation steps for new firmware to take effect.

Updating the GPU Firmware
~~~~~~~~~~~~~~~~~~~~~~~~~

To create the JSON file that shows the GPU as the target, run the following command.

.. code-block::

    $ cat updparams.json
    {
        "HttpPushUriTargets" :["/redfish/v1/UpdateService/FirmwareInventory/FW_GPU_0"]
    }

To run a firmware update on the GPU on the Grace Hopper tray, run the following command:

.. code-block::

    $ nvfwupd -t ip=<BMC-IP> user=****\* password=****\* servertype=MGX update_fw -p nvfw_GH200-P5042_0004_230824.1.0_prod-signed.fwpkg -s updparams.json
    FW package: ['nvfw_GH200-P5042_0004_230824.1.0_prod-signed.fwpkg']
    Ok to proceed with firmware update? <Y/N>
    Y
    FW update started, Task Id: 0
    Wait for Firmware Update to Start...
    TaskState: Completed
    PercentComplete: 100
    TaskStatus: OK
    Firmware update successful!
    Overall Time Taken: 0:02:22
    Refer to 'NVIDIA Firmware Update Document' on activation steps for new firmware to take effect.

Updating the CPU ERoT Firmware
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To create the JSON file that shows the CPUs as the target, run the following command.

.. code-block::

    $ cat updparams.json
    {
        "HttpPushUriTargets" :["/redfish/v1/UpdateService/FirmwareInventory/ERoT_CPU_0"]
    }

To run a firmware update on the CPU's ERoT on the Grace Hopper tray, run the following command.

.. code-block::

    
    $ nvfwupd -t ip=<BMC-IP> user=*** password=*** servertype=MGX update_fw -p nvfw_GH200-P5042_0004_230824.1.0_prod-signed.fwpkg -s updparams.json
    FW package: ['nvfw_GH200-P5042_0004_230824.1.0_prod-signed.fwpkg']
    Ok to proceed with firmware update? <Y/N>
    Y
    FW update started, Task Id: 8
    Wait for Firmware Update to Start...
    TaskState: Completed
    PercentComplete: 100
    TaskStatus: OK
    Firmware update successful!
    Overall Time Taken: 0:02:21
    Refer to 'NVIDIA Firmware Update Document' on activation steps for new firmware to take effect.

Updating the SBIOS Firmware
~~~~~~~~~~~~~~~~~~~~~~~~~~~

To create the JSON file that shows the SBIOS as the target, run the following command.

.. code-block::

    $ cat updparams.json
    {
        "HttpPushUriTargets" :["/redfish/v1/UpdateService/FirmwareInventory/CPU_0"]
    }

To run a firmware update on the SBIOS of the Grace Hopper tray, run the following command.

.. code-block::

    $ nvfwupd -t ip=<BMC-IP> user=****\* password=****\* servertype=MGX update_fw -p nvfw_GH200-P5042_0004_230824.1.0_prod-signed.fwpkg -s updparams.json
    FW package: ['nvfw_GH200-P5042_0004_230824.1.0_prod-signed.fwpkg']
    Ok to proceed with firmware update? <Y/N>
    Y
    FW update started, Task Id: 9
    Wait for Firmware Update to Start...
    TaskState: Completed
    PercentComplete: 100
    TaskStatus: OK
    Firmware update successful!
    Overall Time Taken: 0:00:04
    Refer to 'NVIDIA Firmware Update Document' on activation steps for new firmware to take effect.

Updating the FPGA Firmware
~~~~~~~~~~~~~~~~~~~~~~~~~~

To create the JSON file that shows the FPGA as the target, run the following command.

.. code-block::

    
    $ cat updparams.json
    {
        "HttpPushUriTargets" :["/redfish/v1/UpdateService/FirmwareInventory/FW_FPGA_0"]
    }

To run a firmware update on the FPGA on the Grace Hopper tray, run the following command.

.. code-block::

    $ nvfwupd -t ip=<BMC-IP> user=****\* password=****\* servertype=MGX update_fw -p nvfw_GH200-P5042_0004_230824.1.0_prod-signed.fwpkg -s updparams.json
    FW package: ['nvfw_GH200-P5042_0004_230824.1.0_prod-signed.fwpkg']
    Ok to proceed with firmware update? <Y/N>
    Y
    FW update started, Task Id: 8
    Wait for Firmware Update to Start...
    TaskState: Completed
    PercentComplete: 100
    TaskStatus: OK
    Firmware update successful!
    Overall Time Taken: 0:00:04
    Refer to 'NVIDIA Firmware Update Document' on activation steps for new firmware to take effect.

GH200 Component Update Examples
-------------------------------

This section provides examples that perform component updates for multiple components and show you how to downgrade them using the force update option.

.. _updating-the-sbios-firmware-1:

Updating the SBIOS Firmware
~~~~~~~~~~~~~~~~~~~~~~~~~~~

The URIs listed in Targets in the following output can be replaced by URIs for the components that were selected from ``show_version`` AP name column.

.. code-block::

    $ cat sbios_only.json

    {
        "Targets": [
            "/redfish/v1/UpdateService/FirmwareInventory/HGX_FW_CPU_0",
            "/redfish/v1/UpdateService/FirmwareInventory/HGX_FW_CPU_1"
        ]
    }

    $ nvfwupd --target ip=<BMC IP> user=**\* password=**\* servertype=GH200 update_fw -s sbios_only.json -p nvfw_P4764_0001_240405.1.0_dbg-signed.fwpkg

Downgrading the SBIOS Firmware Using the Force Option
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block::

    $ cat force_sbios_only.json

    {
        "ForceUpdate": true,
        "Targets": [
        "/redfish/v1/UpdateService/FirmwareInventory/HGX_FW_CPU_0",
        "/redfish/v1/UpdateService/FirmwareInventory/HGX_FW_CPU_1"
        ]
    }

    $ nvfwupd --target ip=<BMC IP> user=**\* password=**\* servertype=GH200 update_fw -s force_sbios_only.json -p nvfw_P4764_0001_240405.1.0_dbg-signed.fwpkg

Downgrading the Complete HGX Tray Firmware Using the Force Option
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block::

    $ cat force_hgx_full.json

    {
        "ForceUpdate": true,
        "Targets": ["/redfish/v1/Chassis/HGX_Chassis_0"]
    }

    $ nvfwupd --target ip=<BMC IP> user=**\* password=**\* servertype=GH200 update_fw -s force_hgx_full.json -p nvfw_P4764_0001_240405.1.0_dbg-signed.fwpkg

Downgrading the BMC Tray Firmware Using the Force Option
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block::

    $ cat force_bmc_full.json

    {
        "ForceUpdate": true,
        "Targets": []
    }

    $ nvfwupd --target ip=<BMC IP> user=**\* password=**\* servertype=GH200 update_fw -s force_bmc_full.json -p nvfw_P3809_0001_240405.1.0_dbg-signed.fwpkg

GB200 NVL Component Update Examples
-----------------------------------

This section shows examples to perform component update for multiple selected components and how to downgrade them using the force update option.

Update the SBIOS Firmware
~~~~~~~~~~~~~~~~~~~~~~~~~

The URIs listed in Targets shown below can be replaced by URIs for any components chosen from ``show_version`` AP name column.

.. code-block::

    $ cat sbios_only.json

    {
        "Targets": [
        "/redfish/v1/UpdateService/FirmwareInventory/HGX_FW_CPU_0",
        "/redfish/v1/UpdateService/FirmwareInventory/HGX_FW_CPU_1"
        ]
    }

    $ nvfwupd --target ip=<BMC IP> user=**\* password=**\* servertype=GB200 update_fw -s sbios_only.json -p nvfw_GB200-P4975_0004_240717.1.0_custom_prod-signed.fwpkg

.. _downgrading-the-sbios-firmware-using-the-force-option-1:

Downgrading the SBIOS Firmware Using the Force Option
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block::

    $ cat force_sbios_only.json

    {
        "ForceUpdate": true,
        "Targets": [
        "/redfish/v1/UpdateService/FirmwareInventory/HGX_FW_CPU_0",
        "/redfish/v1/UpdateService/FirmwareInventory/HGX_FW_CPU_1"
        ]
    }

    $ nvfwupd --target ip=<BMC IP> user=**\* password=**\* servertype=GB200 update_fw -s force_sbios_only.json -p nvfw_GB200-P4975_0004_240717.1.0_custom_prod-signed.fwpkg

.. _downgrading-the-complete-hgx-tray-firmware-using-the-force-option-1:

Downgrading the Complete HGX Tray Firmware Using the Force Option
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block::
    
    $ cat force_hgx_full.json

    {
        "ForceUpdate": true,
        "Targets": ["/redfish/v1/Chassis/HGX_Chassis_0"]
    }

    $ nvfwupd --target ip=<BMC IP> user=**\* password=**\* servertype=GB200 update_fw -s force_hgx_full.json -p nvfw_GB200-P4975_0004_240717.1.0_custom_prod-signed.fwpkg
