JSON Output Options for Automation
----------------------------------------------------------------------------

nvfwupd v2.0.5 and later supports JSON output options for the ``show_version``, ``update_fw``, ``force_update``, and ``show_update_progress`` commands.

- The ``show_version`` command can output version details in json mode by appending the ``-j`` option.

.. code-block::

    $ nvfwupd -t ip=192.0.2.10 user='****' password='******' show_version -p nvfw_DGX_0003_241205.1.0_custom_prod-signed.fwpkg -j
    {
        "Connection Status": "Successful",
        "System Model": "DGXH100",
        "Part number": $TRAY_PART_NUMBER,
        "Serial number": $TRAY_SERIAL_NUMBER,
        "Packages": [
            "DGX_0003_241205.1.0_custom"
        ],
        "System IP": "192.0.2.10",
        "Firmware Devices": [
            {
                "AP Name": "CPLDMB_0",
                "Sys Version": "0.2.1.8",
                "Pkg Version": "N/A",
                "Up-To-Date": "No"
            },
            {
                "AP Name": "CPLDMID_0",
                "Sys Version": "0.2.1.1",
                "Pkg Version": "N/A",
                "Up-To-Date": "No"
            },
            {
                "AP Name": "CX7NIC_0",
                "Sys Version": "28.42.1000",
                "Pkg Version": "N/A",
                "Up-To-Date": "No"
            },
            {
                "AP Name": "CX7NIC_1",
                "Sys Version": "28.42.1000",
                "Pkg Version": "N/A",
                "Up-To-Date": "No"
            },
            {
                "AP Name": "CX7_0",
                "Sys Version": "28.42.1000",
                "Pkg Version": "N/A",
                "Up-To-Date": "No"
            },
            {
                "AP Name": "CX7_1",
                "Sys Version": "28.42.1000",
                "Pkg Version": "N/A",
                "Up-To-Date": "No"
            },
            {
                "AP Name": "CX7_2",
                "Sys Version": "28.42.1000",
                "Pkg Version": "N/A",
                "Up-To-Date": "No"
            },
            {
                "AP Name": "CX7_3",
                "Sys Version": "28.42.1000",
                "Pkg Version": "N/A",
                "Up-To-Date": "No"
            },
            {
                "AP Name": "CX7_4",
                "Sys Version": "28.42.1000",
                "Pkg Version": "N/A",
                "Up-To-Date": "No"
            },
            {
                "AP Name": "CX7_5",
                "Sys Version": "28.42.1000",
                "Pkg Version": "N/A",
                "Up-To-Date": "No"
            },
            {
                "AP Name": "CX7_6",
                "Sys Version": "28.42.1000",
                "Pkg Version": "N/A",
                "Up-To-Date": "No"
            },
            {
                "AP Name": "CX7_7",
                "Sys Version": "28.42.1000",
                "Pkg Version": "N/A",
                "Up-To-Date": "No"
            },
            {
                "AP Name": "EROT_BIOS_0",
                "Sys Version": "00.04.0052.0000_n00",
                "Pkg Version": "N/A",
                "Up-To-Date": "No"
            },
            {
                "AP Name": "EROT_BMC_0",
                "Sys Version": "00.04.0052.0000_n00",
                "Pkg Version": "N/A",
                "Up-To-Date": "No"
            },
            {
                "AP Name": "HGX_FW_BMC_0",
                "Sys Version": "HGX-22.10-1-rc77",
                "Pkg Version": "N/A",
                "Up-To-Date": "No"
            },
            {
                "AP Name": "HGX_FW_ERoT_BMC_0",
                "Sys Version": "00.02.0192.0000_n00",
                "Pkg Version": "N/A",
                "Up-To-Date": "No"
            },
            {
                "AP Name": "HGX_FW_ERoT_FPGA_0",
                "Sys Version": "00.02.0192.0000_n00",
                "Pkg Version": "N/A",
                "Up-To-Date": "No"
            },
            {
                "AP Name": "HGX_FW_ERoT_GPU_SXM_1",
                "Sys Version": "00.02.0192.0000_n00",
                "Pkg Version": "N/A",
                "Up-To-Date": "No"
            },
            {
                "AP Name": "HGX_FW_ERoT_GPU_SXM_2",
                "Sys Version": "00.02.0192.0000_n00",
                "Pkg Version": "N/A",
                "Up-To-Date": "No"
            },
            {
                "AP Name": "HGX_FW_ERoT_GPU_SXM_3",
                "Sys Version": "00.02.0192.0000_n00",
                "Pkg Version": "N/A",
                "Up-To-Date": "No"
            },
            {
                "AP Name": "HGX_FW_ERoT_GPU_SXM_4",
                "Sys Version": "00.02.0192.0000_n00",
                "Pkg Version": "N/A",
                "Up-To-Date": "No"
            },
            {
                "AP Name": "HGX_FW_ERoT_GPU_SXM_5",
                "Sys Version": "00.02.0192.0000_n00",
                "Pkg Version": "N/A",
                "Up-To-Date": "No"
            },
            {
                "AP Name": "HGX_FW_ERoT_GPU_SXM_6",
                "Sys Version": "00.02.0192.0000_n00",
                "Pkg Version": "N/A",
                "Up-To-Date": "No"
            },
            {
                "AP Name": "HGX_FW_ERoT_GPU_SXM_7",
                "Sys Version": "00.02.0192.0000_n00",
                "Pkg Version": "N/A",
                "Up-To-Date": "No"
            },
            {
                "AP Name": "HGX_FW_ERoT_GPU_SXM_8",
                "Sys Version": "00.02.0192.0000_n00",
                "Pkg Version": "N/A",
                "Up-To-Date": "No"
            },
            {
                "AP Name": "HGX_FW_ERoT_NVSwitch_0",
                "Sys Version": "00.02.0192.0000_n00",
                "Pkg Version": "N/A",
                "Up-To-Date": "No"
            },
            {
                "AP Name": "HGX_FW_ERoT_NVSwitch_1",
                "Sys Version": "00.02.0192.0000_n00",
                "Pkg Version": "N/A",
                "Up-To-Date": "No"
            },
            {
                "AP Name": "HGX_FW_ERoT_NVSwitch_2",
                "Sys Version": "00.02.0192.0000_n00",
                "Pkg Version": "N/A",
                "Up-To-Date": "No"
            },
            {
                "AP Name": "HGX_FW_ERoT_NVSwitch_3",
                "Sys Version": "00.02.0192.0000_n00",
                "Pkg Version": "N/A",
                "Up-To-Date": "No"
            },
            {
                "AP Name": "HGX_FW_ERoT_PCIeSwitch_0",
                "Sys Version": "00.02.0192.0000_n00",
                "Pkg Version": "N/A",
                "Up-To-Date": "No"
            },
            {
                "AP Name": "HGX_FW_FPGA_0",
                "Sys Version": "2.53",
                "Pkg Version": "N/A",
                "Up-To-Date": "No"
            },
            {
                "AP Name": "HGX_FW_GPU_SXM_1",
                "Sys Version": "96.00.BC.00.01",
                "Pkg Version": "N/A",
                "Up-To-Date": "No"
            },
            {
                "AP Name": "HGX_FW_GPU_SXM_2",
                "Sys Version": "96.00.BC.00.01",
                "Pkg Version": "N/A",
                "Up-To-Date": "No"
            },
            {
                "AP Name": "HGX_FW_GPU_SXM_3",
                "Sys Version": "96.00.BC.00.01",
                "Pkg Version": "N/A",
                "Up-To-Date": "No"
            },
            {
                "AP Name": "HGX_FW_GPU_SXM_4",
                "Sys Version": "96.00.BC.00.01",
                "Pkg Version": "N/A",
                "Up-To-Date": "No"
            },
            {
                "AP Name": "HGX_FW_GPU_SXM_5",
                "Sys Version": "96.00.BC.00.01",
                "Pkg Version": "N/A",
                "Up-To-Date": "No"
            },
            {
                "AP Name": "HGX_FW_GPU_SXM_6",
                "Sys Version": "96.00.BC.00.01",
                "Pkg Version": "N/A",
                "Up-To-Date": "No"
            },
            {
                "AP Name": "HGX_FW_GPU_SXM_7",
                "Sys Version": "96.00.BC.00.01",
                "Pkg Version": "N/A",
                "Up-To-Date": "No"
            },
            {
                "AP Name": "HGX_FW_GPU_SXM_8",
                "Sys Version": "96.00.BC.00.01",
                "Pkg Version": "N/A",
                "Up-To-Date": "No"
            },
            {
                "AP Name": "HGX_FW_NVSwitch_0",
                "Sys Version": "96.10.69.00.01",
                "Pkg Version": "N/A",
                "Up-To-Date": "No"
            },
            {
                "AP Name": "HGX_FW_NVSwitch_1",
                "Sys Version": "96.10.69.00.01",
                "Pkg Version": "N/A",
                "Up-To-Date": "No"
            },
            {
                "AP Name": "HGX_FW_NVSwitch_2",
                "Sys Version": "96.10.69.00.01",
                "Pkg Version": "N/A",
                "Up-To-Date": "No"
            },
            {
                "AP Name": "HGX_FW_NVSwitch_3",
                "Sys Version": "96.10.69.00.01",
                "Pkg Version": "N/A",
                "Up-To-Date": "No"
            },
            {
                "AP Name": "HGX_FW_PCIeRetimer_0",
                "Sys Version": "2.7.20",
                "Pkg Version": "N/A",
                "Up-To-Date": "No"
            },
            {
                "AP Name": "HGX_FW_PCIeRetimer_1",
                "Sys Version": "2.7.20",
                "Pkg Version": "N/A",
                "Up-To-Date": "No"
            },
            {
                "AP Name": "HGX_FW_PCIeRetimer_2",
                "Sys Version": "2.7.20",
                "Pkg Version": "N/A",
                "Up-To-Date": "No"
            },
            {
                "AP Name": "HGX_FW_PCIeRetimer_3",
                "Sys Version": "2.7.20",
                "Pkg Version": "N/A",
                "Up-To-Date": "No"
            },
            {
                "AP Name": "HGX_FW_PCIeRetimer_4",
                "Sys Version": "2.7.20",
                "Pkg Version": "N/A",
                "Up-To-Date": "No"
            },
            {
                "AP Name": "HGX_FW_PCIeRetimer_5",
                "Sys Version": "2.7.20",
                "Pkg Version": "N/A",
                "Up-To-Date": "No"
            },
            {
                "AP Name": "HGX_FW_PCIeRetimer_6",
                "Sys Version": "2.7.20",
                "Pkg Version": "N/A",
                "Up-To-Date": "No"
            },
            {
                "AP Name": "HGX_FW_PCIeRetimer_7",
                "Sys Version": "2.7.20",
                "Pkg Version": "N/A",
                "Up-To-Date": "No"
            },
            {
                "AP Name": "HGX_FW_PCIeSwitch_0",
                "Sys Version": "1.9.5F",
                "Pkg Version": "N/A",
                "Up-To-Date": "No"
            },
            {
                "AP Name": "HGX_InfoROM_GPU_SXM_1",
                "Sys Version": "G520.0200.00.05",
                "Pkg Version": "N/A",
                "Up-To-Date": "No"
            },
            {
                "AP Name": "HGX_InfoROM_GPU_SXM_2",
                "Sys Version": "G520.0200.00.05",
                "Pkg Version": "N/A",
                "Up-To-Date": "No"
            },
            {
                "AP Name": "HGX_InfoROM_GPU_SXM_3",
                "Sys Version": "G520.0200.00.05",
                "Pkg Version": "N/A",
                "Up-To-Date": "No"
            },
            {
                "AP Name": "HGX_InfoROM_GPU_SXM_4",
                "Sys Version": "G520.0200.00.05",
                "Pkg Version": "N/A",
                "Up-To-Date": "No"
            },
            {
                "AP Name": "HGX_InfoROM_GPU_SXM_5",
                "Sys Version": "G520.0200.00.05",
                "Pkg Version": "N/A",
                "Up-To-Date": "No"
            },
            {
                "AP Name": "HGX_InfoROM_GPU_SXM_6",
                "Sys Version": "G520.0200.00.05",
                "Pkg Version": "N/A",
                "Up-To-Date": "No"
            },
            {
                "AP Name": "HGX_InfoROM_GPU_SXM_7",
                "Sys Version": "G520.0200.00.05",
                "Pkg Version": "N/A",
                "Up-To-Date": "No"
            },
            {
                "AP Name": "HGX_InfoROM_GPU_SXM_8",
                "Sys Version": "G520.0200.00.05",
                "Pkg Version": "N/A",
                "Up-To-Date": "No"
            },
            {
                "AP Name": "HGX_InfoROM_NVSwitch_0",
                "Sys Version": "5612.0002.00.01",
                "Pkg Version": "N/A",
                "Up-To-Date": "No"
            },
            {
                "AP Name": "HGX_InfoROM_NVSwitch_1",
                "Sys Version": "5612.0002.00.01",
                "Pkg Version": "N/A",
                "Up-To-Date": "No"
            },
            {
                "AP Name": "HGX_InfoROM_NVSwitch_2",
                "Sys Version": "5612.0002.00.01",
                "Pkg Version": "N/A",
                "Up-To-Date": "No"
            },
            {
                "AP Name": "HGX_InfoROM_NVSwitch_3",
                "Sys Version": "5612.0002.00.01",
                "Pkg Version": "N/A",
                "Up-To-Date": "No"
            },
            {
                "AP Name": "HostBIOS_0",
                "Sys Version": "01.05.03",
                "Pkg Version": "N/A",
                "Up-To-Date": "No"
            },
            {
                "AP Name": "HostBMC_0",
                "Sys Version": "24.09.17",
                "Pkg Version": "N/A",
                "Up-To-Date": "No"
            },
            {
                "AP Name": "PCIeRetimer_0",
                "Sys Version": "2.07.19",
                "Pkg Version": "2.07.19",
                "Up-To-Date": "Yes"
            },
            {
                "AP Name": "PCIeRetimer_1",
                "Sys Version": "2.07.19",
                "Pkg Version": "2.07.19",
                "Up-To-Date": "Yes"
            },
            {
                "AP Name": "PCIeSwitch_0",
                "Sys Version": "0.0.7",
                "Pkg Version": "N/A",
                "Up-To-Date": "No"
            },
            {
                "AP Name": "PCIeSwitch_1",
                "Sys Version": "1.0.7",
                "Pkg Version": "N/A",
                "Up-To-Date": "No"
            },
            {
                "AP Name": "PSU_0",
                "Sys Version": "0204.0201.0204",
                "Pkg Version": "0204.0201.0204",
                "Up-To-Date": "No"
            },
            {
                "AP Name": "PSU_1",
                "Sys Version": "0204.0201.0204",
                "Pkg Version": "0204.0201.0204",
                "Up-To-Date": "No"
            },
            {
                "AP Name": "PSU_2",
                "Sys Version": "0204.0201.0204",
                "Pkg Version": "0204.0201.0204",
                "Up-To-Date": "No"
            },
            {
                "AP Name": "PSU_3",
                "Sys Version": "0204.0201.0204",
                "Pkg Version": "0204.0201.0204",
                "Up-To-Date": "No"
            },
            {
                "AP Name": "PSU_4",
                "Sys Version": "0204.0201.0204",
                "Pkg Version": "0204.0201.0204",
                "Up-To-Date": "No"
            },
            {
                "AP Name": "PSU_5",
                "Sys Version": "0204.0201.0204",
                "Pkg Version": "0204.0201.0204",
                "Up-To-Date": "No"
            }
        ],
        "Error Code": 0
    }


- The ``force_update`` command can be used to change the force update status of servers that support it and print out in json mode with the ``-j`` option. To enable or disable the ``force_update`` status in a state change, the BMC will return {}.

.. code-block::

    $ nvfwupd -t ip=192.0.2.10 user='****' password='******' force_update status -j
    {
        "Error": [],
        "Error Code": 0,
        "Output": [
            {
                "@odata.context": "/redfish/v1/$metadata#UpdateService.UpdateService",
                "@odata.etag": "\"1736811147\"",
                "@odata.id": "/redfish/v1/UpdateService",
                "@odata.type": "#UpdateService.v1_11_0.UpdateService",
                "Actions": {
                    "Oem": {
                        "#NvidiaUpdateService.ClearNVRAM": {
                            "@Redfish.ActionInfo": "/redfish/v1/UpdateService/Oem/Nvidia/ClearNVRAMActionInfo",
                            "target": "/redfish/v1/UpdateService/Actions/Oem/NvidiaUpdateService.ClearNVRAM"
                        },
                        "#NvidiaUpdateService.CommitImage": {
                            "@Redfish.ActionInfo": "/redfish/v1/UpdateService/Oem/Nvidia/CommitImageActionInfo",
                            "target": "/redfish/v1/UpdateService/Actions/Oem/NvidiaUpdateService.CommitImage"
                        },
                        "#UpdateService.UploadCABundle": {
                            "@Redfish.ActionInfo": "/redfish/v1/UpdateService/UploadCABundleActionInfo",
                            "target": "/redfish/v1/UpdateService/Actions/Oem/UpdateService.UploadCABundle"
                        }
                    }
                },
                "Description": "Redfish Update Service",
                "FirmwareInventory": {
                    "@odata.id": "/redfish/v1/UpdateService/FirmwareInventory"
                },
                "HttpPushUriOptions": {
                    "ForceUpdate": true,
                    "HttpPushUriApplyTime": {
                        "ApplyTime": "Immediate"
                    }
                },
                "HttpPushUriOptionsBusy": false,
                "HttpPushUriTargetsBusy": false,
                "Id": "UpdateService",
                "MaxImageSizeBytes": 430198784,
                "MultipartHttpPushUri": "/redfish/v1/UpdateService/upload",
                "Name": "Update Service",
                "Oem": {
                    "Ami": {
                        "@odata.type": "#AMIUpdateService.v1_0_0.AMIUpdateService",
                        "FlashPercentage": null,
                        "HttpPushUriOptions": {
                            "ForceUpdateClearConfig": false
                        },
                        "UpdateStatus": null,
                        "UpdateTarget": null
                    },
                    "BMC": {
                        "@odata.type": "#AMIUpdateService.v1_0_0.BMC"
                    }
                },
                "ServiceEnabled": true,
                "SoftwareInventory": {
                    "@odata.id": "/redfish/v1/UpdateService/SoftwareInventory"
                },
                "Status": {
                    "Health": "OK",
                    "State": "Enabled"
                }
            }
        ]
    }

    $ nvfwupd -t ip=192.0.2.10 user='****' password='******' force_update disable -j
    {
        "Error": [],
        "Error Code": 0,
        "Output": [
            {}
        ]
    }

    $ nvfwupd -t ip=192.0.2.10 user='****' password='******' force_update enable -j
    {
        "Error": [],
        "Error Code": 0,
        "Output": [
            {}
        ]
    }

- Firmware updates can be started in JSON mode using the ``update_fw`` command with the ``-j`` option. The JSON mode for firmware updates is only supported in background mode when you add the ``-b`` option.

.. code-block::

    $ nvfwupd -t ip=192.0.2.10 user='****' password='******' update_fw -p nvfw_DGX_0003_241205.1.0_custom_prod-signed.fwpkg -j -b -s DGX_Full.json
    {
        "Error": [],
        "Error Code": 0,
        "Output": [
            {
                "@odata.type": "#UpdateService.v1_11_0.UpdateService",
                "Messages": [
                    {
                        "@odata.type": "#Message.v1_0_8.Message",
                        "Message": "A new task /redfish/v1/TaskService/Tasks/2 was created.",
                        "MessageArgs": [
                            "/redfish/v1/TaskService/Tasks/2"
                        ],
                        "MessageId": "Task.1.0.New",
                        "Resolution": "None",
                        "Severity": "OK"
                    },
                    {
                        "@odata.type": "#Message.v1_0_8.Message",
                        "Message": "The action UpdateService.MultipartPush was submitted to do firmware update.",
                        "MessageArgs": [
                            "UpdateService.MultipartPush"
                        ],
                        "MessageId": "UpdateService.1.0.StartFirmwareUpdate",
                        "Resolution": "None",
                        "Severity": "OK"
                    }
                ]
            }
        ]
    }

- The progress of an ongoing update can also be viewed in json format by passing the ``-j`` option with a task id that includes the ``-i`` option to the ``show_update_progress`` command.

.. code-block::

    $ nvfwupd -t ip=192.0.2.10 user='****' password='******' show_update_progress -j -i 2
    {
        "Error": [],
        "Error Code": 0,
        "Output": [
            {
                "@odata.context": "/redfish/v1/$metadata#Task.Task",
                "@odata.etag": "\"1736812864\"",
                "@odata.id": "/redfish/v1/TaskService/Tasks/2",
                "@odata.type": "#Task.v1_4_2.Task",
                "Description": "Task for Update Service Task",
                "EndTime": "2025-01-14T00:02:39+00:00",
                "HidePayload": false,
                "Id": "2",
                "Messages": [
                    {
                        "@odata.type": "#Message.v1_0_8.Message",
                        "Message": "Image '/var/tmp/bundles/nvfw_DGX_0003_241205.1.0_custom_prod-signed.fwpkg' is being transferred to 'HostBMC_0'.",
                        "MessageArgs": [
                            "/var/tmp/bundles/nvfw_DGX_0003_241205.1.0_custom_prod-signed.fwpkg",
                            "HostBMC_0"
                        ],
                        "MessageId": "Update.1.0.TransferringToComponent",
                        "Resolution": "None.",
                        "Severity": "OK"
                    },
                    {
                        "@odata.type": "#Message.v1_0_8.Message",
                        "Message": "ResourceErrorsDetected",
                        "MessageArgs": [
                            "ResourceErrorsDetected"
                        ],
                        "MessageId": "UpdateManager.1.0.MessageNil",
                        "Resolution": "None.",
                        "Severity": "Warning"
                    },
                    {
                        "@odata.type": "#Message.v1_0_8.Message",
                        "Message": "The target device 'PCIeRetimer_1' will be updated with image '2.07.19'.",
                        "MessageArgs": [
                            "PCIeRetimer_1",
                            "2.07.19"
                        ],
                        "MessageId": "Update.1.0.TargetDetermined",
                        "Resolution": "None.",
                        "Severity": "OK"
                    },
                    {
                        "@odata.type": "#Message.v1_0_8.Message",
                        "Message": "Image '2.07.19' is being transferred to 'PCIeRetimer_1'.",
                        "MessageArgs": [
                            "2.07.19",
                            "PCIeRetimer_1"
                        ],
                        "MessageId": "Update.1.0.TransferringToComponent",
                        "Resolution": "None.",
                        "Severity": "OK"
                    },
                    {
                        "@odata.type": "#Message.v1_0_8.Message",
                        "Message": "Image '2.07.19' is being installed on 'PCIeRetimer_1'.",
                        "MessageArgs": [
                            "2.07.19",
                            "PCIeRetimer_1"
                        ],
                        "MessageId": "Update.1.0.InstallingOnComponent",
                        "Resolution": "None.",
                        "Severity": "OK"
                    },
                    {
                        "@odata.type": "#Message.v1_0_8.Message",
                        "Message": "PCIeRetimer_1 firmware update to version 2.07.19 started.",
                        "MessageArgs": [
                            "PCIeRetimer_1",
                            "2.07.19"
                        ],
                        "MessageId": "UpdateManager.1.0.FirmwareUpdateStarted",
                        "Resolution": "None.",
                        "Severity": "OK"
                    },
                    {
                        "@odata.type": "#Message.v1_0_8.Message",
                        "Message": "PCIeRetimer_1 firmware update to version 2.07.19 completed successfully.",
                        "MessageArgs": [
                            "PCIeRetimer_1",
                            "2.07.19"
                        ],
                        "MessageId": "UpdateManager.1.0.FirmwareUpdateCompleted",
                        "Resolution": "None.",
                        "Severity": "OK"
                    },
                    {
                        "@odata.type": "#Message.v1_0_8.Message",
                        "Message": "Image 'PCIeRetimer_1' is being verified at '2.07.19'.",
                        "MessageArgs": [
                            "PCIeRetimer_1",
                            "2.07.19"
                        ],
                        "MessageId": "Update.1.0.VerifyingAtComponent",
                        "Resolution": "None.",
                        "Severity": "OK"
                    },
                    {
                        "@odata.type": "#Message.v1_0_8.Message",
                        "Message": "2.07.19 firmware version succeeded: PCIeRetimer_1.",
                        "MessageArgs": [
                            "2.07.19",
                            "PCIeRetimer_1"
                        ],
                        "MessageId": "UpdateManager.1.0.FirmwareVerificationSuccess",
                        "Resolution": "None.",
                        "Severity": "OK"
                    },
                    {
                        "@odata.type": "#Message.v1_0_8.Message",
                        "Message": "Device 'PCIeRetimer_1' successfully updated with image '2.07.19'.",
                        "MessageArgs": [
                            "PCIeRetimer_1",
                            "2.07.19"
                        ],
                        "MessageId": "Update.1.0.UpdateSuccessful",
                        "Resolution": "None.",
                        "Severity": "OK"
                    },
                    {
                        "@odata.type": "#Message.v1_0_8.Message",
                        "Message": "Task /redfish/v1/UpdateService/upload has completed.",
                        "MessageArgs": [
                            "/redfish/v1/UpdateService/upload"
                        ],
                        "MessageId": "Task.1.0.Completed",
                        "Resolution": "None",
                        "Severity": "OK"
                    }
                ],
                "Name": "Update Service Task",
                "PercentComplete": 100,
                "StartTime": "2025-01-14T00:01:06+00:00",
                "TaskState": "Completed",
                "TaskStatus": "OK"
            }
        ]
    }
