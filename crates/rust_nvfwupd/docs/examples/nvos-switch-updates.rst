NVOS Switch Updates
-------------------

NVFWUPD supports NVIDIA Networking OS (NVOS) image updates for the following
switch targets:

- ``GB200Switch``
- ``GB300Switch``
- ``VRNVL72Switch``

NVOS image updates are supported in NVFWUPD 2.1.3 and later.

Use the switch management IP address with the ``-t`` or ``--target`` option.
NVOS updates use a single ``.bin`` image. Do not combine an NVOS ``.bin`` image
with PLDM ``.fwpkg`` files in the same command.

The `servertype `value is optional for switch targets. NVFWUPD can auto-detect supported switch targets. To select a platform explicitly, specify` servertype=gb200switch`, `servertype=gb300switch`, or `servertype=vrnvl72switch`.

Check the NVOS Version
~~~~~~~~~~~~~~~~~~~~~~

When you run `show_version` with an NVOS `.bin` image, NVFWUPD adds an NVOS row to the Firmware Devices table. The ``Sys Version``
column shows the running NVOS version on the switch, and the ``Pkg Version``
column shows the version parsed from the ``.bin`` file name.

.. code-block::

    $ nvfwupd -t ip=192.0.2.10 user=<username> password=<password> \
        show_version -p nvos-amd64-25.02.2579.bin

    System Model: N5400_LD
    Part number: <SWITCH_PART_NUMBER>
    Serial number: <SWITCH_SERIAL_NUMBER>
    Packages: ['nvos-25.02.2579']
    Connection Status: Successful

    Firmware Devices:
    AP Name        Sys Version         Pkg Version      Up-To-Date
    -------        -----------         -----------      ----------
    ASIC           35.2014.2184        N/A              No
    BIOS           0ACTV_00.01.022     N/A              No
    BMC            88.0002.1979        N/A              No
    CPLD1          CPLD000370_REV0600  N/A              No
    EROT           01.04.0049.0000_n04 N/A              No
    FPGA           0.24                N/A              No
    SSD            CE00A450            N/A              No
    transceiver    N/A                 N/A              No
    NVOS           nvos-25.02.2526     nvos-25.02.2579  No
    --------------------------------------------------------------
    Error Code: 0

Install an NVOS Image
~~~~~~~~~~~~~~~~~~~~~~~~

“NVFWUPD uploads the image to` /host/nos-images/` using SFTP, uses NVUE to install the image, and waits for the install action to reach a terminal state.

Before uploading the image, NVFWUPD sets the required NVUE values for switch ASIC firmware updates, including `auto-update` and `fw-source`.

NVFWUPD sets the NVOS install request to ``reboot=no``. After the image is
installed, use ``activate_fw -c NVUE_PWR_CYCLE`` when you are ready to reboot
the switch and activate the installed NVOS image.

.. caution::
    Activating the image power cycles the switch and interrupts service. Run
    this command only during a maintenance window.

.. code-block::

    $ nvfwupd -t ip=192.0.2.10 user=<username> password=<password> \
        update_fw -p nvos-amd64-25.02.2579.bin

    Updating ip address: ip=XXXX
    FW package: ["nvos-amd64-25.02.2579.bin"]
    Ok to proceed with firmware update? <Y/N>
    y
    Uploading NVOS image using SFTP to /host/nos-images/ (timeout: 60 minutes).
    This can take several minutes.
    Update file nvos-amd64-25.02.2579.bin was uploaded successfully
    Starting NVOS image update for: NVOS
    NVOS update task was created with ID 1
    Status for Job Id 1:
    {
        "detail": "Installing image: nvos-amd64-25.02.2579.bin",
        "http_status": 200,
        "issue": [],
        "percentage": "",
        "state": "running",
        "status": "Installing image: nvos-amd64-25.02.2579.bin",
        "timeout": 300,
        "type": "",
        "warnings": []
    }

    Status for Job Id 1:
    {
        "detail": "Reboot is recommended before using the new installation.",
        "http_status": 200,
        "issue": [],
        "percentage": "",
        "state": "action_success",
        "status": "Reboot is recommended before using the new installation.",
        "timeout": 300,
        "type": "",
        "warnings": []
    }

    -------------------------------------------------------------------
    Error Code: 0

.. note::

    The default NVOS SFTP upload timeout is 60 minutes. To use a different timeout for NVOS upload and NVUE install requests, specify `--timeout <seconds>` with `update_fw`.

Run NVOS Updates in JSON Background Mode
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

For automation, use ``-j -b`` with ``update_fw``. In JSON background mode,
NVFWUPD starts the NVOS install action and returns the NVUE task ID in the
``Output`` array.

.. code-block:: json

    {
        "Error": [],
        "Error Code": 0,
        "Output": [
            {
                "Id": "1"
            }
        ]
    }

Activate the Installed Image
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

After the NVOS install action completes, activate the installed image with the
``NVUE_PWR_CYCLE`` activation command.

.. caution::
    Activating the image power cycles the switch and interrupts service. Run
    this command only during a maintenance window.

.. code-block::

    $ nvfwupd -t ip=192.0.2.10 user=<username> password=<password> \
        activate_fw -c NVUE_PWR_CYCLE

    Power cycle task was created with ID 2
    Status for Job Id 2:
    {
        "detail": "",
        "http_status": 200,
        "issue": [],
        "percentage": "",
        "state": "running",
        "status": "",
        "timeout": 5,
        "type": "",
        "warnings": []
    }

Verification is a separate user task.

.. code-block::

    $ nvfwupd -t ip=192.0.2.10 user=<username> password=<password> \
        show_version -p nvos-amd64-25.02.2579.bin

    System Model: N5400_LD
    Part number: <SWITCH_PART_NUMBER>
    Serial number: <SWITCH_SERIAL_NUMBER>
    Packages: ['nvos-25.02.2579']
    Connection Status: Successful

    Firmware Devices:
    AP Name        Sys Version       Pkg Version       Up-To-Date
    -------        -----------       -----------       ----------
    NVOS           nvos-25.02.2579   nvos-25.02.2579   Yes
    -------------------------------------------------------------------
    Error Code: 0
