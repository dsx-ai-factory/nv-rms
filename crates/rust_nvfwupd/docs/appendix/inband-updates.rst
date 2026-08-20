.. _inband-updates:

Flint-Based Firmware Updates
=============================

NVFWUPD 2.0.9 introduces the ``flint_update`` command for flint-based firmware updates on supported devices. This feature enables direct firmware updates and version queries for devices like BlueField DPUs and ConnectX NICs.

Prerequisites
-------------

Before using flint-based updates, ensure the following requirements are met:

1. **Flint Tool Installation**: The flint utility must be installed on the host system. If flint is not found, NVFWUPD will return an error.
2. **Root/Administrator Access**: Flint updates require sudo permissions on the host.
3. **MST Driver**: Mellanox Software Tools (MST) driver must be installed; NVFWUPD will automatically start MST services.
4. **Image File**: The image file used for the update must be a valid firmware image for the target device.

Flint Tool Installation
^^^^^^^^^^^^^^^^^^^^^^^

The flint tool is included in Mellanox/NVIDIA Firmware Tools (MFT) package. See https://docs.nvidia.com/networking/display/mftv420 for more information on how to install and use the MFT package.

.. note::
    NVFWUPD will automatically start MST services when running flint_update commands. You do not need to manually start the MST service.


Using the flint_update Command
-------------------------------

The ``flint_update`` command enables firmware updates and version queries on supported devices.

Syntax
^^^^^^

.. code-block::

    $ nvfwupd [-o ip=<OS IP> user=<username> password=<password>] flint_update [options]

Parameters
^^^^^^^^^^

- ``-i`` or ``--image``: Image file for firmware flashing and version comparison (required for update)
- ``-d`` or ``--device_type``: Device type for firmware flashing and version query (for example, BlueField3, ConnectX)
- ``-j`` or ``--json``: Show output in JSON format
- ``-v`` or ``--version``: Query firmware version only (no update performed)
- ``-t`` or ``--timeout``: Query timeout in seconds for OS commands. Flash timeout is 20x the query timeout. (default 60 for queries, 1200 for flash operations)

.. note::
    For NVFWUPD 2.1.0 and later, a user can set a custom timeout for the query and flash operations. The default timeout is 60 seconds for queries and 1200 seconds for flash operations if not specified.

Global Options
^^^^^^^^^^^^^^

The command supports the ``-o`` or ``--os_target`` global option for specifying OS credentials when accessing remote systems:

.. code-block::

    $ nvfwupd -o ip=<OS IP> user=<username> password=<password> flint_update [options]

Querying Firmware Version
--------------------------

To query the current firmware version on a device without performing an update, use the ``-v`` or ``--version`` option. When combined with the ``-i`` option, it compares device versions against a target firmware image.

Example 1: Query and Compare ConnectX8 Devices on Remote Host
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

This example shows querying multiple ConnectX8 devices on a remote host and comparing their versions against a target firmware image:

.. code-block::

    $ nvfwupd -o ip=<HOST_IP> user=<username> password=<password> flint_update -d Connectx8 -i CX8Images/fw-ConnectX8-rel-40_45_3588-900-9X86E-00CX-SP0_Ax-UEFI-14.38.16-FlexBoot-3.7.500.signed.bin -v

    OS Connection Status: Successful
    Connected to: <username>@<HOST_IP>:22
    Starting MST service...
    Querying firmware version...
    Querying firmware version for Connectx8 devices...
    Querying firmware versions from image files...

    Version Comparison:
    ------------------------------------------------------------

    Image: CX8Images/fw-ConnectX8-rel-40_45_3588-900-9X86E-00CX-SP0_Ax-UEFI-14.38.16-FlexBoot-3.7.500.signed.bin (Version: 40.45.3588)
      ConnectX8(rev:0)     /dev/mst/mt4131_pciconf3.1 40.45.3582 - ✗ DIFFERENT
      ConnectX8(rev:0)     /dev/mst/mt4131_pciconf3  40.45.3582 - ✗ DIFFERENT
      ConnectX8(rev:0)     /dev/mst/mt4131_pciconf2.1 40.45.3582 - ✗ DIFFERENT
      ConnectX8(rev:0)     /dev/mst/mt4131_pciconf2  40.45.3582 - ✗ DIFFERENT
      ConnectX8(rev:0)     /dev/mst/mt4131_pciconf1.1 40.45.3582 - ✗ DIFFERENT
      ConnectX8(rev:0)     /dev/mst/mt4131_pciconf1  40.45.3582 - ✗ DIFFERENT
      ConnectX8(rev:0)     /dev/mst/mt4131_pciconf0.1 40.45.3582 - ✗ DIFFERENT
      ConnectX8(rev:0)     /dev/mst/mt4131_pciconf0  40.45.3582 - ✗ DIFFERENT

.. note::
    - NVFWUPD automatically starts MST services on the remote host
    - The `-v` option queries version only without performing an update
    - When an image file is provided with `-i`, versions are compared against the image
    - Status shows ✗ DIFFERENT when device version differs from image version (40.45.3582 vs 40.45.3588)
    - Device paths (for example, /dev/mst/mt4131_pciconf0) are automatically discovered
    - All devices show version 40.45.3582, while the target image is 40.45.3588, indicating updates are available

Example 2: Query All MST Devices on Remote Host
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

When the ``-d`` device type parameter is omitted, NVFWUPD queries all discoverable inband devices (for example, BlueField3, ConnectX8, GB100) on the system:

.. code-block::

    $ nvfwupd -o ip=<HOST_IP> user=<username> password=<password> flint_update -v

    OS Connection Status: Successful
    Connected to: <username>@<HOST_IP>:22
    Starting MST service...
    Querying firmware version...
    Querying firmware version for all MST devices...

    Device Firmware Versions:
    ------------------------------------------------------------
    GB100(rev:0)         /dev/mst/mt10561_pciconf3 Query failed

    GB100(rev:0)         /dev/mst/mt10561_pci_cr3  Query failed

    GB100(rev:0)         /dev/mst/mt10561_pciconf2 Query failed

    GB100(rev:0)         /dev/mst/mt10561_pci_cr2  Query failed

    GB100(rev:0)         /dev/mst/mt10561_pciconf1 Query failed

    GB100(rev:0)         /dev/mst/mt10561_pci_cr1  Query failed

    GB100(rev:0)         /dev/mst/mt10561_pciconf0 Query failed

    GB100(rev:0)         /dev/mst/mt10561_pci_cr0  Query failed

    BlueField3(rev:1)    /dev/mst/mt41692_pciconf1.1 32.44.1600

    BlueField3(rev:1)    /dev/mst/mt41692_pciconf1 32.44.1600

    BlueField3(rev:1)    /dev/mst/mt41692_pciconf0.1 32.44.1600

    BlueField3(rev:1)    /dev/mst/mt41692_pciconf0 32.44.1600

    ConnectX8(rev:0)     /dev/mst/mt4131_pciconf3.1 40.45.3582

    ConnectX8(rev:0)     /dev/mst/mt4131_pciconf3  40.45.3582

    ConnectX8(rev:0)     /dev/mst/mt4131_pciconf2.1 40.45.3582

    ConnectX8(rev:0)     /dev/mst/mt4131_pciconf2  40.45.3582

    ConnectX8(rev:0)     /dev/mst/mt4131_pciconf1.1 40.45.3582

    ConnectX8(rev:0)     /dev/mst/mt4131_pciconf1  40.45.3582

    ConnectX8(rev:0)     /dev/mst/mt4131_pciconf0.1 40.45.3582

    ConnectX8(rev:0)     /dev/mst/mt4131_pciconf0  40.45.3582

.. note::
    - Omitting the `-d` parameter queries all MST devices on the system
    - The output shows multiple device types: GB100 (GPUs), BlueField3 (DPUs), and ConnectX8 (NICs)
    - Some devices may show "Query failed" if they don't support firmware queries using flint, although they may still be inventoried by the system (for example, GB100 GPUs)
    - This is useful for getting a complete inventory of all MST devices and their running firmware versions

Updating Firmware
-----------------

To update firmware on a device, provide the image file using the ``-i`` option:

Example 3: Update ConnectX8 Devices on Remote Host
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

This example demonstrates updating multiple ConnectX8 devices on a remote host. NVFWUPD automatically transfers the firmware file, discovers all matching devices, and updates them sequentially:

.. code-block::

    $ nvfwupd -o ip=<HOST_IP> user=<username> password=<password> flint_update -d Connectx8 -i CX8Images/fw-ConnectX8-rel-40_45_3588-900-9X86E-00CX-SP0_Ax-UEFI-14.38.16-FlexBoot-3.7.500.signed.bin

    OS Connection Status: Successful
    Connected to: <username>@<HOST_IP>:22
    Starting MST service...
    Processing firmware image: CX8Images/fw-ConnectX8-rel-40_45_3588-900-9X86E-00CX-SP0_Ax-UEFI-14.38.16-FlexBoot-3.7.500.signed.bin
    Transferring firmware file CX8Images/fw-ConnectX8-rel-40_45_3588-900-9X86E-00CX-SP0_Ax-UEFI-14.38.16-FlexBoot-3.7.500.signed.bin to remote system...
    Firmware file transferred successfully: File CX8Images/fw-ConnectX8-rel-40_45_3588-900-9X86E-00CX-SP0_Ax-UEFI-14.38.16-FlexBoot-3.7.500.signed.bin transferred successfully to fw-ConnectX8-rel-40_45_3588-900-9X86E-00CX-SP0_Ax-UEFI-14.38.16-FlexBoot-3.7.500.signed.bin
    Firmware file verification on remote system: -rw-rw-r-- 1 nvidia nvidia 67108864 Oct 26 16:05 fw-ConnectX8-rel-40_45_3588-900-9X86E-00CX-SP0_Ax-UEFI-14.38.16-FlexBoot-3.7.500.signed.bin

    File permissions set successfully: 
    Device paths found: ['/dev/mst/mt4131_pciconf3', '/dev/mst/mt4131_pciconf2', '/dev/mst/mt4131_pciconf1', '/dev/mst/mt4131_pciconf0']
    Executing flash command: flint -d /dev/mst/mt4131_pciconf3 --yes -i fw-ConnectX8-rel-40_45_3588-900-9X86E-00CX-SP0_Ax-UEFI-14.38.16-FlexBoot-3.7.500.signed.bin b
    Flash successful for device /dev/mst/mt4131_pciconf3: 
        Current FW version on flash:  40.45.3582
        New FW version:               40.45.3588

    FSMST_INITIALIZE -   OK
    Writing Boot image component -   OK
    Restoring signature                     - OK
    -I- To load new FW run mlxfwreset or reboot machine.

    Executing flash command: flint -d /dev/mst/mt4131_pciconf2 --yes -i fw-ConnectX8-rel-40_45_3588-900-9X86E-00CX-SP0_Ax-UEFI-14.38.16-FlexBoot-3.7.500.signed.bin b
    Flash successful for device /dev/mst/mt4131_pciconf2: 
        Current FW version on flash:  40.45.3582
        New FW version:               40.45.3588

    FSMST_INITIALIZE -   OK
    Writing Boot image component -   OK
    Restoring signature                     - OK
    -I- To load new FW run mlxfwreset or reboot machine.

    Executing flash command: flint -d /dev/mst/mt4131_pciconf1 --yes -i fw-ConnectX8-rel-40_45_3588-900-9X86E-00CX-SP0_Ax-UEFI-14.38.16-FlexBoot-3.7.500.signed.bin b
    Flash successful for device /dev/mst/mt4131_pciconf1: 
        Current FW version on flash:  40.45.3582
        New FW version:               40.45.3588

    FSMST_INITIALIZE -   OK
    Writing Boot image component -   OK
    Restoring signature                     - OK
    -I- To load new FW run mlxfwreset or reboot machine.

    Executing flash command: flint -d /dev/mst/mt4131_pciconf0 --yes -i fw-ConnectX8-rel-40_45_3588-900-9X86E-00CX-SP0_Ax-UEFI-14.38.16-FlexBoot-3.7.500.signed.bin b
    Flash successful for device /dev/mst/mt4131_pciconf0: 
        Current FW version on flash:  40.45.3582
        New FW version:               40.45.3588

    FSMST_INITIALIZE -   OK
    Writing Boot image component -   OK
    Restoring signature                     - OK
    -I- To load new FW run mlxfwreset or reboot machine.

    Command execution completed:
    Command: flint_flash Connectx8 CX8Images/fw-ConnectX8-rel-40_45_3588-900-9X86E-00CX-SP0_Ax-UEFI-14.38.16-FlexBoot-3.7.500.signed.bin
    Status: Success
    Output: Firmware flashing completed successfully
    --------------------------------------------------

.. note::

    **Update Process:**
    
    1. Connect to remote system using SSH
    2. Start MST services
    3. Transfer firmware image
    4. Discover updatable device paths
    5. Flash each device sequentially
    6. Verify each flash operation
    7. Report completion status

Configuration File Support
---------------------------

Flint updates can be configured using a YAML configuration file with the ``OS_IP``, ``OS_USERNAME``, and ``OS_PASSWORD`` parameters:

.. code-block:: yaml

    # OS target for flint update
    OS_IP: "<HOST_IP>"
    OS_USERNAME: "<username>"
    OS_PASSWORD: "<password>"

    # Device type (replaces -d option in flint_update)
    FlintDeviceType: "Connectx"

    # Firmware image (replaces -i option in flint_update)
    FlintImageFilePath: "CX8Images/fw-ConnectX8-rel-40_45_3030-900-9X86E-00SX-SPA_Ax-UEFI-14.38.16-FlexBoot-3.7.500.bin"


Using the configuration file:

To acquire versions:

.. code-block::

    $ nvfwupd -c inband_config.yaml flint_update -v
    OS Connection Status: Successful
    Connected to: <username>@<HOST_IP>:22
    Starting MST service...
    Querying firmware version...
    Querying firmware version for Connectx devices...
    Querying firmware versions from image files...

    Version Comparison:
    ------------------------------------------------------------

    Image: CX8Images/fw-ConnectX8-rel-40_45_3030-900-9X86E-00SX-SPA_Ax-UEFI-14.38.16-FlexBoot-3.7.500.bin (Version: 40.45.3030)
    ConnectX8(rev:0)     /dev/mst/mt4131_pciconf0.1 40.46.3020 - ✗ DIFFERENT
    ConnectX8(rev:0)     /dev/mst/mt4131_pciconf0  40.46.3020 - ✗ DIFFERENT


To update firmware:

.. code-block::

    $ nvfwupd -c inband_config.yaml flint_update
    OS Connection Status: Successful
    Connected to: <username>@<HOST_IP>:22
    Starting MST service...
    Processing firmware image: CX8Images/fw-ConnectX8-rel-40_45_3030-900-9X86E-00SX-SPA_Ax-UEFI-14.38.16-FlexBoot-3.7.500.bin
    Transferring firmware file CX8Images/fw-ConnectX8-rel-40_45_3030-900-9X86E-00SX-SPA_Ax-UEFI-14.38.16-FlexBoot-3.7.500.bin to remote system...
    Firmware file transferred successfully: File CX8Images/fw-ConnectX8-rel-40_45_3030-900-9X86E-00SX-SPA_Ax-UEFI-14.38.16-FlexBoot-3.7.500.bin transferred successfully to fw-ConnectX8-rel-40_45_3030-900-9X86E-00SX-SPA_Ax-UEFI-14.38.16-FlexBoot-3.7.500.bin
    Firmware file verification on remote system: -rw-rw-r-- 1 nvidia nvidia 67108864 Nov  3 20:06 fw-ConnectX8-rel-40_45_3030-900-9X86E-00SX-SPA_Ax-UEFI-14.38.16-FlexBoot-3.7.500.bin
    ...

Activation After Update
------------------------

To activate the firmware after the update, use the activate_fw commands from the BMC to power cycle the system.


.. code-block::

    $ nvfwupd -t ip=<BMC IP> user=**** password=**** activate_fw -c RF_PWR_OFF

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


After powering down the system, use the Redfish Power On command to boot the system. This activates the new firmware updated through flint_update.

.. code-block::
    
    $ nvfwupd -t ip=<BMC IP> user=**** password=**** activate_fw -c RF_PWR_ON

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

Verifying the Update
---------------------

After activation, verify that the firmware version is updated by querying with the image file used for the update. The output should show ✓ MATCH when devices are at the target version:

.. code-block::

    $ nvfwupd -o ip=<HOST_IP> user=<username> password=<password> flint_update -d Connectx8 -i CX8Images/fw-ConnectX8-rel-40_45_3588-900-9X86E-00CX-SP0_Ax-UEFI-14.38.16-FlexBoot-3.7.500.signed.bin -v

    OS Connection Status: Successful
    Connected to: <username>@<HOST_IP>:22
    Starting MST service...
    Querying firmware version...
    Querying firmware version for Connectx8 devices...
    Querying firmware versions from image files...

    Version Comparison:
    ------------------------------------------------------------

    Image: CX8Images/fw-ConnectX8-rel-40_45_3588-900-9X86E-00CX-SP0_Ax-UEFI-14.38.16-FlexBoot-3.7.500.signed.bin (Version: 40.45.3588)
      ConnectX8(rev:0)     /dev/mst/mt4131_pciconf3.1 40.45.3588 - ✓ MATCH
      ConnectX8(rev:0)     /dev/mst/mt4131_pciconf3  40.45.3588 - ✓ MATCH
      ConnectX8(rev:0)     /dev/mst/mt4131_pciconf2.1 40.45.3588 - ✓ MATCH
      ConnectX8(rev:0)     /dev/mst/mt4131_pciconf2  40.45.3588 - ✓ MATCH
      ConnectX8(rev:0)     /dev/mst/mt4131_pciconf1.1 40.45.3588 - ✓ MATCH
      ConnectX8(rev:0)     /dev/mst/mt4131_pciconf1  40.45.3588 - ✓ MATCH
      ConnectX8(rev:0)     /dev/mst/mt4131_pciconf0.1 40.45.3588 - ✓ MATCH
      ConnectX8(rev:0)     /dev/mst/mt4131_pciconf0  40.45.3588 - ✓ MATCH

.. note::
    - All 8 devices now show version 40.45.3588, matching the target image
    - Status changed from ✗ DIFFERENT (before update) to ✓ MATCH (after update)
    - This confirms all ConnectX8 devices were successfully updated
    - Perform the verification after device reset or system reboot to ensure new firmware is active

JSON Output Format
------------------

When using the ``-j`` or ``--json`` option, the output is formatted as JSON for easier parsing:

.. code-block::

    $ nvfwupd -o ip=<HOST_IP> user=<username> password=<password> flint_update -v -j
    {
        "Error": [],
        "Error Code": 0,
        "Output": {
            "devices": [
            {
                "device": "/dev/mst/mt12738_pciconf0",
                "device_type": "GB100(rev:0)",
                "fw_version": "Query failed"
            },
            {
                "device": "/dev/mst/mt12738_pci_cr0",
                "device_type": "GB100(rev:0)",
                "fw_version": "Query failed"
            },
            {
                "device": "/dev/mst/mt4131_pciconf0.1",
                "device_type": "ConnectX8(rev:0)",
                "fw_version": "40.46.3020"
            },
            {
                "device": "/dev/mst/mt4131_pciconf0",
                "device_type": "ConnectX8(rev:0)",
                "fw_version": "40.46.3020"
            }
            ],
            "images": []
        }
    }

