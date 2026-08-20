MGX GH100 or Grace Hopper Update Examples
==============================================================

This chapter provides information about the updating firmware on MGX Grace Hopper with tool usage examples.

Displaying the Firmware Contents on the System and the Package
----------------------------------------------------------------------

This command completes the following tasks:

-  Displays the versions of the firmware installed in the system.

-  Displays the versions of the firmware packages included with the -p option.

-  Compares the versions installed in the system and the packages with the -p option.

Here is the output:

.. code-block::

    $ nvfwupd -t ip=<BMC IP> user=*** password=*** servertype=MGX show_version -p nvfw_Grace-CPU-P5041_0003_231109.1.4_prod-signed.fwpkg

    System Model: P3809
    Part number: $TRAY_PART_NUMBER
    Serial number: $TRAY_SERIAL_NUMBER
    Packages: ['Grace-CPU-P5041_0003_231109.1.4']
    Connection Status: Successful
    Firmware Devices:
    AP Name         Sys Version               Pkg Version           Up-To-Date
    -------         -----------               -----------           ----------

    BMC_Firmware    GraceBMC_23.09-3-rc2      23.09.03              Yes
    CPU_0           00010001                  01.00.01              Yes
    CPU_1           00010001                  01.00.01              Yes
    Cpld0           5                         N/A                   No
    ERoT_CPU_0      01.03.0114.0000_n01       01.03.0114.0000_n01   Yes
    ERoT_CPU_1      01.03.0114.0000_n01       01.03.0114.0000_n01   Yes
    FW_FPGA_0       0.88                      0.88                  Yes
    UEFI            buildbrain-gcid-34396453  N/A                   No

    ------------------------------------------------------------------------------------

    Error Code: 0

Updating the Firmware of the Entire Bundle (Recommended)
----------------------------------------------------------------------

To perform a firmware update of the entire server, run the following command using the appropriate fwpkg file for the target platform. The command in the following example is similar for all GH/MGX platforms. The exceptions are GH200 and GB200 NVL, and the exceptions are described in the relevant sections in this document. The same command can be used to downgrade MGX firmware.

.. note::

    The ``-s`` or ``–special`` option is not mandatory for MGX firmware update

.. code-block::

    $ nvfwupd -t ip=<BMC-IP> user=*** password=*** servertype=MGX update_fw -p nvfw_GH200-P5042_0004_230824.1.0_prod-signed.fwpkg
    Updating ip address: ip=<BMC-IP>
    FW package: ['nvfw_GH200-P5042_0004_230824.1.0_prod-signed.fwpkg']
    Ok to proceed with firmware update? <Y/N>
    Y
    FW update started, Task Id: 0
    Wait for Firmware Update to Start...
    TaskState: Completed
    PercentComplete: 100
    TaskStatus: OK
    Firmware update successful!
    Overall Time Taken: 0:14:19
    Refer to 'NVIDIA Firmware Update Document' on activation steps for new firmware to take
    effect. 
    -------------------------------------------------------------------------------------

    Error Code: 0

Activating the Firmware
----------------------------------------------------------------------

After performing firmware update of a component, or a full bundle, complete an AC power cycle to activate the new firmware. It can take up to five minutes for the BMC and the Redfish service to come up after the power cycle is complete. To check new system versions after the BMC Redfish service is back, run the show version command.

Updating the Firmware of Selected Components
----------------------------------------------------------------------

The example in this section shows you how to update a component from the system firmware inventory.


.. important::

    The examples are applicable **only** to NVIDIA MGX GH100.


To perform component updates for a Grace Hopper platform:

1. In the **AP Name** column, in the ``show_version`` output, select the AP to update and create a JSON file.

2. Replace ``<AP name from show_version output>`` with the name you selected:

.. code-block:: 

    $ cat updparams.json
    {
        "HttpPushUriTargets" :["/redfish/v1/UpdateService/FirmwareInventory/<AP name from show_version output>"]
    }

For example, to update only the BMC firmware on a target:

1. Get the component name for the BMC from the ``show_version`` output.

2. Create a JSON file like ``updparams.json`` (refer to the output).

3. Run the ``update_fw`` command with the ``-s`` option.

.. code-block::

    $ cat updparams.json
    {
        "HttpPushUriTargets": ["/redfish/v1/UpdateService/FirmwareInventory/BMC_Firmware"]
    }

    $ nvfwupd -t ip=<BMC-IP> user=*** password=*** servertype=MGX update_fw -p nvfw_GH200-P5042_0004_230824.1.0_prod-signed.fwpkg -s updparams.json
    FW package: ['nvfw_GH200-P5042_0004_230824.1.0_prod-signed.fwpkg']
    Ok to proceed with firmware update? <Y/N>
    Y
    FW update started, Task Id: 7
    Wait for Firmware Update to Start...
    TaskState: Completed
    PercentComplete: 100
    TaskStatus: OK
    Firmware update successful!
    Overall Time Taken: 0:00:04
    Refer to 'NVIDIA Firmware Update Document' on activation steps for new firmware totake effect.

    -------------------------------------------------------------------------------------

    Error Code: 0
