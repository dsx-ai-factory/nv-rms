Update Pre-Checks
----------------------------------------------------------------------------

NVFWUPD runs pre-checks before it starts a firmware update. These checks stop the update before the firmware package is uploaded if the target reports a state that is unsafe for flashing.

Pre-checks run as part of the ``update_fw`` command. If a pre-check fails,
NVFWUPD exits before creating a firmware update task.

Skip Update Pre-Checks
----------------------

The ``-k`` or ``--skip_pre_flight_checks`` option to skip update pre-checks.

.. code-block::

    $ nvfwupd -t ip=192.0.2.10 user=<username> password=<password> servertype=GB200 update_fw \
        -p nvfw_GB200_example.fwpkg \
        --skip_pre_flight_checks

Use this option only when the platform state is validated by
another process or when directed by platform-specific service guidance.

.. note::

    The expected inventory check is configured separately with `-e` or `--expected_inventory`. The `--skip_pre_flight_checks` option does not disable that check.

Write-Protection Checks
-----------------------

NVFWUPD checks global and per-component write protection state before firmware
updates on supported GB200, GB300, Vera Rubin NVL72, and LP30 Redfish targets.

NVFWUPD does not disable or clear write protection. If write protection is enabled,
the update is stopped and the user must disable write protection through the
appropriate platform service procedure before retrying the firmware update.

Global Hardware Write Protection
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

NVFWUPD queries the global chassis resource:

.. code-block::

    GET /redfish/v1/Chassis/Chassis_0

NVFWUPD then checks this Redfish property:

.. code-block::

    Oem.Nvidia.HardwareWriteProtectEnable

If the value is ``true``, NVFWUPD blocks the firmware update.

The following example shows a failure message:

.. code-block::

    Updating ip address: ip=XXXX
    FW package: ["nvfw_GB200_example.fwpkg"]
    Hardware write protect is enabled at /redfish/v1/Chassis/Chassis_0 Oem.Nvidia.HardwareWriteProtectEnable=true; firmware updates are not allowed until hardware write protect is disabled
    ------------------------------------------------------------------------------------------------------------------------
    Error Code: 1

If NVFWUPD cannot query` /redfish/v1/Chassis/Chassis_0`, or if the `Oem.Nvidia.HardwareWriteProtectEnable` field is missing, NVFWUPD logs a message and continues with the per-component write-protection checks.

Per-Component Firmware Write Protect
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

NVFWUPD queries the FirmwareInventory collection:

.. code-block::

    GET /redfish/v1/UpdateService/FirmwareInventory

NVFWUPD reads the `Members `property in the collection and queries each member URI to inspect the optional `WriteProtected` field.

The following example shows a component resource:

.. code-block:: json

    {
        "@odata.id": "/redfish/v1/UpdateService/FirmwareInventory/HGX_FW_GPU_0",
        "Id": "HGX_FW_GPU_0",
        "Updateable": true,
        "Version": "97.00.B9.00.99",
        "WriteProtected": false
    }

NVFWUPD evaluates only components that include a boolean `WriteProtected` field.

If any component reports ``WriteProtected=true``, NVFWUPD blocks the firmware
update and reports the protected component.

The following example shows a failure message:

.. code-block::

    Updating ip address: ip=XXXX
    FW package: ["nvfw_GB200_example.fwpkg"]
    Firmware component write protect is enabled for 1 component(s): /redfish/v1/UpdateService/FirmwareInventory/HGX_FW_GPU_0 WriteProtected=true; firmware updates are not allowed until all WriteProtected values are false
    ------------------------------------------------------------------------------------------------------------------------
    Error Code: 1

If NVFWUPD cannot query the FirmwareInventory collection, it retries before failing the update. If NVFWUPD cannot query an individual FirmwareInventory member, it logs the skipped component and continues checking the remaining members.

Successful Write-Protection Check
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If global write protect is disabled and no discovered component reports
``WriteProtected=true``, NVFWUPD continues with the normal firmware update
flow.

.. code-block::

    Updating ip address: ip=XXXX
    FW package: ["nvfw_GB200_example.fwpkg"]
    Ok to proceed with firmware update? <Y/N>

Write-Protection Failures in JSON Mode
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Write-protect pre-check failures are compatible with JSON mode. When you use the `-j` and `-b` options together, NVFWUPD returns the failure in the JSON error output and does not start the firmware update.

Background Copy Checks
----------------------

NVFWUPD checks BackgroundCopy state before firmware updates on supported GB200,
GB300, Vera Rubin NVL72, and LP30 Redfish targets. Firmware updates are not allowed
while a component background copy is still in progress.

This check is part of the normal ``update_fw`` pre-check flow and is skipped
when ``-k`` or ``--skip_pre_flight_checks`` is used.

Discovery Background Copy Status
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

NVFWUPD first queries the Chassis collection:

.. code-block::

    GET /redfish/v1/Chassis

It then queries each Chassis member and looks for:

.. code-block::

    Oem.Nvidia.BackgroundCopyStatus

NVFWUPD includes only chassis members that expose
``Oem.Nvidia.BackgroundCopyStatus``. If no chassis members expose the field,
the check passes.

Completed Status
~~~~~~~~~~~~~~~~

Each discovered component must report the following status value:

.. code-block::

    Oem.Nvidia.BackgroundCopyStatus: "Completed"

The status comparison is case-insensitive.

CLI Behavior
~~~~~~~~~~~~

For NVFWUPD CLI updates, the BackgroundCopy pre-check is a one-time check.
NVFWUPD discovers the components that expose ``BackgroundCopyStatus`` and reads
their current status once before allowing the update to proceed. Transient
query failures are retried briefly before the command fails.

If every discovered component reports ``Completed``, NVFWUPD continues with the
normal firmware update flow.

.. code-block::

    Updating ip address: ip=XXXX
    FW package: ["nvfw_GB200_example.fwpkg"]
    Ok to proceed with firmware update? <Y/N>

Failed Background Copy Check
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If any discovered component reports a status other than ``Completed``, NVFWUPD
stops before starting the firmware update. The error identifies each Chassis
resource that is blocking the update and shows its reported status.

.. code-block::

    Updating ip address: ip=XXXX
    FW package: ["nvfw_GB200_example.fwpkg"]
    BackgroundCopy is currently in progress for 1 component(s): /redfish/v1/Chassis/HGX_IRoT_GPU_0 status=InProgress; firmware updates are not allowed until all Oem.Nvidia.BackgroundCopyStatus values are Completed
    ------------------------------------------------------------------------------------------------------------------------
    Error Code: 1

If the Chassis collection or a Chassis member cannot be queried after retrying, NVFWUPD fails the pre-check and does not start the
firmware update.

Background Copy Failures in JSON Mode
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

BackgroundCopy pre-check failures are compatible with JSON mode. When you use the `-j` and `-b` options together, NVFWUPD returns the failure in the JSON error output and does not start the firmware update.
