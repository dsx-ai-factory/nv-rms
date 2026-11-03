Expected Inventory Checks
----------------------------------------------------------------------------

NVFWUPD includes an *optional* expected inventory check for the ``update_fw``
command. This check verifies that the target contains the required firmware
inventory AP names before NVFWUPD starts the update.

Use this option in automation workflows that require a specific set of
components. If the discovered firmware inventory does not match the expected
inventory, NVFWUPD stops before flashing firmware.

Create an Expected Inventory File
---------------------------------

Create a JSON file that lists the AP names that must be present in the target
firmware inventory. Use the values from the ``AP Name`` column in the
``show_version`` output.

.. code-block:: json

    [
        "FW_BMC_0",
        "FW_ERoT_BMC_0",
        "HGX_FW_BMC_0",
        "HGX_FW_GPU_0"
    ]

The following object form is also supported:

.. code-block:: json

    {
        "expected_inventory": [
            "FW_BMC_0",
            "FW_ERoT_BMC_0",
            "HGX_FW_BMC_0",
            "HGX_FW_GPU_0"
        ]
    }

The object key can be one of ``expected_inventory``, ``expectedInventory``,
``ap_names``, ``apNames``, or ``APNames``.

Rules
-----

- Expected inventory entries must use AP names, such as ``FW_BMC_0`` or
  ``HGX_FW_GPU_0``. Do not use full Redfish URIs in this file.
- Matching is case-insensitive.
- Duplicate AP names are ignored case-insensitively. The first spelling is
  preserved in error messages.
- NVFWUPD rejects empty or whitespace-only files.
- Non-string entries are rejected.
- If you do not provide an expected inventory file, NVFWUPD skips this check.
- The ``-k`` and ``--skip_pre_flight_checks`` options do not disable the
  expected inventory check.

Run an Update with Expected Inventory
-------------------------------------

Specify the expected inventory file with the ``-e`` or
``--expected_inventory`` option.

.. code-block::

    $ nvfwupd -t ip=192.0.2.10 user=<username> password=<password> servertype=GB200 update_fw \
        -p nvfw_GB200-P4972_0011_250404.1.0_prod-signed.fwpkg \
        -e expected_inventory.json

NVFWUPD validates the expected inventory before it prompts for update
confirmation or sends the firmware package to the target.

Successful Expected Inventory Check
-----------------------------------

If all expected AP names are present, the update continues normally.

.. code-block::

    Updating ip address: ip=XXXX
    FW package: ["nvfw_GB200-P4972_0011_250404.1.0_prod-signed.fwpkg"]
    Ok to proceed with firmware update? <Y/N>

Failed Check
------------

If one or more expected AP names are missing, NVFWUPD stops before the update starts and prints the missing AP names and the AP names found on the target.

.. code-block::

    Updating ip address: ip=XXXX
    FW package: ["nvfw_GB200-P4972_0011_250404.1.0_prod-signed.fwpkg"]
    pre-update expected inventory check: expected firmware inventory APs were not present: HGX_NOT_REAL_VALUE. Present APs: FW_BMC_0, FW_ERoT_BMC_0, HGX_FW_BMC_0, HGX_FW_GPU_0
    ------------------------------------------------------------------------------------------------------------------------
    Error Code: 1


Parallel Updates
----------------

You can use expected inventory with parallel updates that use a configuration file.

.. code-block::

    $ nvfwupd -c parallel_update_config.yaml update_fw -e expected_inventory.json

The same expected inventory list applies to every target in the parallel update. If target groups require different sets of expected AP names, run separate NVFWUPD commands with separate expected inventory files.

Redfish and Switch Behavior
---------------------------

For Redfish Baseboard Management Controller (BMC) targets, NVFWUPD reads the
``/redfish/v1/UpdateService/FirmwareInventory`` collection and checks the
``Members`` AP names.

For switch targets, NVFWUPD uses the switch firmware inventory NVIDIA User
Experience (NVUE) API and checks the reported AP names.

