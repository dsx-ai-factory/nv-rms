Background Copy for Targeted Components
---------------------------------------

NVFWUPD 2.0.6 and later supports the ``background_copy`` command, which can be used to make both component ERoT partitions the same for supported components. GB200 NVL, GB300 NVL and Vera Rubin NVL72 supports background copies.

1. Create an update file that contains the desired targets like in the following example:

.. code-block::

    cat BackgroundCopyCPU.json
    {
        "Targets": [
            "/redfish/v1/UpdateService/FirmwareInventory/HGX_FW_CPU_0",
            "/redfish/v1/UpdateService/FirmwareInventory/HGX_FW_CPU_1"
        ]
    }

2. Run the following command to complete a background copy for the component ERoT to the backup partition.

.. code-block::

    nvfwupd -t ip=192.0.2.10 user=<username> password=<password> background_copy -s BackgroundCopyCPU.json
    BMC Connection Status: Successful
    Background copy request successful
    Task State:
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


.. warning:: 
    Background copies can only be completed when a firmware update is not in progress. If you see the "Retry the operation once firmware update operation is complete." error, wait until the update has completed and try again.


Full Background Copy for All Supported Components
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To begin a background copy for all supported components: 

1. Create an update file with the following contents:

.. code-block::

    cat BackgroundCopyAll.json
    {
        "Targets": []
    }

2. After this update file is created, pass it to the NVFWUPD ``background_copy`` command:

.. code-block::

    nvfwupd -t ip=192.0.2.10 user=<username> password=<password> background_copy -s BackgroundCopyAll.json
    BMC Connection Status: Successful
    Background copy request successful
    Task State:
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

This begins a background copy to the backup partition for all associated ERoTs for supported components.


Background Copy Interactive Mode
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

NVFWUPD 2.1.0 and later supports the ``interactive`` or ``-i`` flag for the ``background_copy`` command. This mode can be used for foreground copies in Vera Rubin NVL72.

1. To target specific supported components, create an update file with the Redfish inventory URI of the component. For example, to target the BMC firmware slot:

.. code-block::

    cat BMC_Background_copy.json
    {
        "Targets": ["/redfish/v1/UpdateService/FirmwareInventory/FW_BMC_0"]
    }
    
2. Run the following command to complete a background copy for the component backup partition.

.. code-block::

    ./nvfwupd -t ip=192.0.2.10 user=<username> password=<password> background_copy -i -s BMC_Background_copy.json
    BMC Connection Status: Successful
    Background copy request successful
    Task State:
    ""
    Interactive mode: Polling firmware slots until versions match...
    Poll interval: 20 seconds
    Maximum wait time: 600 seconds

    [2025-12-12 16:19:27] Polling firmware slot versions...
    /redfish/v1/UpdateService/FirmwareInventory/FW_BMC_0: Active: VR-2510-03.87, Inactive:  (waiting...)
    Waiting 20 seconds before next poll...
    [2025-12-12 16:19:47] Polling firmware slot versions...
    /redfish/v1/UpdateService/FirmwareInventory/FW_BMC_0: Active: VR-2510-03.87, Inactive:  (waiting...)
    Waiting 20 seconds before next poll...
    [2025-12-12 16:20:07] Polling firmware slot versions...
    /redfish/v1/UpdateService/FirmwareInventory/FW_BMC_0: Active: VR-2510-03.87, Inactive:  (waiting...)
    Waiting 20 seconds before next poll...
    [2025-12-12 16:20:27] Polling firmware slot versions...
    /redfish/v1/UpdateService/FirmwareInventory/FW_BMC_0: Active: VR-2510-03.87, Inactive:  (waiting...)
    Waiting 20 seconds before next poll...
    [2025-12-12 16:20:48] Polling firmware slot versions...
    /redfish/v1/UpdateService/FirmwareInventory/FW_BMC_0: Active: VR-2510-03.87, Inactive:  (waiting...)
    Waiting 20 seconds before next poll...
    [2025-12-12 16:21:08] Polling firmware slot versions...
    /redfish/v1/UpdateService/FirmwareInventory/FW_BMC_0: Active: VR-2510-03.87, Inactive:  (waiting...)
    Waiting 20 seconds before next poll...
    [2025-12-12 16:21:28] Polling firmware slot versions...
    /redfish/v1/UpdateService/FirmwareInventory/FW_BMC_0: Active: VR-2510-03.87, Inactive:  (waiting...)
    Waiting 20 seconds before next poll...
    [2025-12-12 16:21:48] Polling firmware slot versions...
    /redfish/v1/UpdateService/FirmwareInventory/FW_BMC_0: Active: VR-2510-03.87, Inactive:  (waiting...)
    Waiting 20 seconds before next poll...
    [2025-12-12 16:22:09] Polling firmware slot versions...
    /redfish/v1/UpdateService/FirmwareInventory/FW_BMC_0: Active: VR-2510-03.87, Inactive:  (waiting...)
    Waiting 20 seconds before next poll...
    [2025-12-12 16:22:29] Polling firmware slot versions...
    /redfish/v1/UpdateService/FirmwareInventory/FW_BMC_0: Active: VR-2510-03.87, Inactive:  (waiting...)
    Waiting 20 seconds before next poll...
    [2025-12-12 16:22:49] Polling firmware slot versions...
    /redfish/v1/UpdateService/FirmwareInventory/FW_BMC_0: Active: VR-2510-03.87, Inactive:  (waiting...)
    Waiting 20 seconds before next poll...
    [2025-12-12 16:23:09] Polling firmware slot versions...
    /redfish/v1/UpdateService/FirmwareInventory/FW_BMC_0: Active: VR-2510-03.87, Inactive:  (waiting...)
    Waiting 20 seconds before next poll...
    [2025-12-12 16:23:30] Polling firmware slot versions...
    /redfish/v1/UpdateService/FirmwareInventory/FW_BMC_0: Active: VR-2510-03.87, Inactive:  (waiting...)
    Waiting 20 seconds before next poll...
    [2025-12-12 16:23:50] Polling firmware slot versions...
    /redfish/v1/UpdateService/FirmwareInventory/FW_BMC_0: Active: VR-2510-03.87, Inactive:  (waiting...)
    Waiting 20 seconds before next poll...
    [2025-12-12 16:24:10] Polling firmware slot versions...
    /redfish/v1/UpdateService/FirmwareInventory/FW_BMC_0: Active: VR-2510-03.87, Inactive:  (waiting...)
    Waiting 20 seconds before next poll...
    [2025-12-12 16:24:31] Polling firmware slot versions...
    /redfish/v1/UpdateService/FirmwareInventory/FW_BMC_0: Active: VR-2510-03.87, Inactive:  (waiting...)
    Waiting 20 seconds before next poll...
    [2025-12-12 16:24:51] Polling firmware slot versions...
    /redfish/v1/UpdateService/FirmwareInventory/FW_BMC_0: Active: VR-2510-03.87, Inactive:  (waiting...)
    Waiting 20 seconds before next poll...
    [2025-12-12 16:25:11] Polling firmware slot versions...
    /redfish/v1/UpdateService/FirmwareInventory/FW_BMC_0: Active: VR-2510-03.87, Inactive:  (waiting...)
    Waiting 20 seconds before next poll...
    [2025-12-12 16:25:31] Polling firmware slot versions...
    /redfish/v1/UpdateService/FirmwareInventory/FW_BMC_0: Active: VR-2510-03.87, Inactive:  (waiting...)
    Waiting 20 seconds before next poll...
    [2025-12-12 16:25:52] Polling firmware slot versions...
    /redfish/v1/UpdateService/FirmwareInventory/FW_BMC_0: Active: VR-2510-03.87, Inactive:  (waiting...)
    Waiting 20 seconds before next poll...
    [2025-12-12 16:26:12] Polling firmware slot versions...
    /redfish/v1/UpdateService/FirmwareInventory/FW_BMC_0: Active: VR-2510-03.87, Inactive:  (waiting...)
    Waiting 20 seconds before next poll...
    [2025-12-12 16:26:32] Polling firmware slot versions...
    /redfish/v1/UpdateService/FirmwareInventory/FW_BMC_0: Active: VR-2510-03.87, Inactive:  (waiting...)
    Waiting 20 seconds before next poll...
    [2025-12-12 16:26:52] Polling firmware slot versions...
    /redfish/v1/UpdateService/FirmwareInventory/FW_BMC_0: ✓ MATCHED - Version: VR-2510-03.87

    ====================================================================================
    SUCCESS: All firmware slots have matching versions!
    Final Status:
    /redfish/v1/UpdateService/FirmwareInventory/FW_BMC_0
    Version: VR-2510-03.87

.. note::
    The ``interactive`` mode can also be used to target all supported components by passing an update file with an empty targets list. { "Targets": [] }