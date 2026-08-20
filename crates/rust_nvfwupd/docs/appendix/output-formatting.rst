NVFWUPD Table Output Formatting
===============================

Overview
--------
The nvfwupd tool provides detailed table output format when using the -d flag with the update_fw command, displaying comprehensive debugging information and thorough firmware update progress reporting.
Command:
::

    ./nvfwupd -t ip=192.0.2.10 user=**** password=**** servertype=GB200 -v update_fw -d -s compute_full.json -p ../nvfw_GB200-P4975_0004_250325.1.0_prod-signed.fwpkg

Output:
::

    Updating ip address: ip=XXXX
    FW package: ['../nvfw_GB200-P4975_0004_250325.1.0_prod-signed.fwpkg']
    Ok to proceed with firmware update? <Y/N>
    Y

    {"@odata.id": "/redfish/v1/TaskService/Tasks/HGX_9", "@odata.type": "#Task.v1_4_3.Task", "Id": "HGX_9", "TaskState": "Running", "TaskStatus": "OK"}
    FW update started, Task Id: HGX_9
    Wait for Firmware Update to Start...

    +---------------------------+--------------------------------------------------------+
    | MessageId                 | Message                                                |
    +===========================+========================================================+
    | TaskEvent.1.0.3.TaskStart | The task with Id '9' has started.                      |
    | ed                        |                                                        |
    +---------------------------+--------------------------------------------------------+
    | Update.1.0.TargetDetermin | The target device 'HGX_FW_ERoT_CPU_0' will be          |
    | ed                        | updated with image '01.04.0008.0000_n04'.              |
    +---------------------------+--------------------------------------------------------+
    | Update.1.0.TargetDetermin | The target device 'HGX_FW_CPU_0' will be               |
    | ed                        | updated with image '02.03.16'.                         |
    +---------------------------+--------------------------------------------------------+
    | Update.1.0.TargetDetermin | The target device 'HGX_FW_ERoT_CPU_1' will be          |
    | ed                        | updated with image '01.04.0008.0000_n04'.              |
    +---------------------------+--------------------------------------------------------+
    | Update.1.0.TargetDetermin | The target device 'HGX_FW_CPU_1' will be               |
    | ed                        | updated with image '02.03.16'.                         |
    +---------------------------+--------------------------------------------------------+
    | Update.1.0.TargetDetermin | The target device 'HGX_FW_ERoT_BMC_0' will be          |
    | ed                        | updated with image '01.04.0008.0000_n04'.              |
    +---------------------------+--------------------------------------------------------+
    | Update.1.0.TargetDetermin | The target device 'HGX_FW_BMC_0' will be               |
    | ed                        | updated with image 'GB200Nvl-25.01-7'.                 |
    +---------------------------+--------------------------------------------------------+
    | Update.1.0.TransferringTo | Image ' ' is being transferred to                      |
    | Component                 | 'HGX_PCIeSwitchConfig_0'.                              |
    +---------------------------+--------------------------------------------------------+
    | NvidiaUpdate.1.0.Componen | The update operation for the component                 |
    | tUpdateSkipped            | 'HGX_FW_ERoT_BMC_0' is skipped because                 |
    |                           | 'Component image is identical'.                        |
    +---------------------------+--------------------------------------------------------+
    | NvidiaUpdate.1.0.Componen | The update operation for the component                 |
    | tUpdateSkipped            | 'HGX_FW_ERoT_CPU_1' is skipped because                 |
    |                           | 'Component image is identical'.                        |
    +---------------------------+--------------------------------------------------------+
    | NvidiaUpdate.1.0.Componen | The update operation for the component                 |
    | tUpdateSkipped            | 'HGX_FW_ERoT_CPU_0' is skipped because                 |
    |                           | 'Component image is identical'.                        |
    +---------------------------+--------------------------------------------------------+
    | Update.1.0.TransferringTo | Image 'GB200Nvl-25.01-7' is being transferred to       |
    | Component                 | 'HGX_FW_BMC_0'.                                        |
    +---------------------------+--------------------------------------------------------+
    | Update.1.0.TransferringTo | Image '02.03.16' is being transferred to               |
    | Component                 | 'HGX_FW_CPU_1'.                                        |
    +---------------------------+--------------------------------------------------------+
    | Update.1.0.TransferringTo | Image '02.03.16' is being transferred to               |
    | Component                 | 'HGX_FW_CPU_0'.                                        |
    +---------------------------+--------------------------------------------------------+
    | Update.1.0.TransferringTo | Image '0.1C' is being transferred to 'MAX10 CPLD'.     |
    | Component                 |                                                        |
    +---------------------------+--------------------------------------------------------+
    | Update.1.0.UpdateSuccessf | Device 'HGX_PCIeSwitchConfig_0' successfully updated   |
    | ul                        | with image ''.                                         |
    +---------------------------+--------------------------------------------------------+
    | TaskEvent.1.0.3.TaskProgr | The task with Id '9' has changed to progress 20 percent|
    | essChanged                | complete.                                              |
    +---------------------------+--------------------------------------------------------+
    | Update.1.0.UpdateSuccessf | Device 'MAX10 CPLD' successfully updated with image    |
    | ul                        | '0.1C'.                                                |
    +---------------------------+--------------------------------------------------------+
    | Update.1.0.UpdateSuccessf | Device 'HGX_FW_CPU_0' successfully updated with image  |
    | ul                        | '02.03.16'.                                            |
    +---------------------------+--------------------------------------------------------+
    | Update.1.0.AwaitToActivat | Awaiting for an action to proceed with activating image|
    | e                         | '02.03.16' on 'HGX_FW_CPU_0'.                          |
    +---------------------------+--------------------------------------------------------+
    | Update.1.0.UpdateSuccessf | Device 'HGX_FW_CPU_1' successfully updated with image  |
    | ul                        | '02.03.16'.                                            |
    +---------------------------+--------------------------------------------------------+
    | Update.1.0.AwaitToActivat | Awaiting for an action to proceed with activating image|
    | e                         | '02.03.16' on 'HGX_FW_CPU_1'.                          |
    +---------------------------+--------------------------------------------------------+
    | TaskEvent.1.0.3.TaskProgr | The task with Id '9' has changed to progress 40 percent|
    | essChanged                | complete.                                              |
    +---------------------------+--------------------------------------------------------+
    | Update.1.0.UpdateSuccessf | Device 'HGX_FW_BMC_0' successfully updated with image  |
    | ul                        | 'GB200Nvl-25.01-7'.                                    |
    +---------------------------+--------------------------------------------------------+
    | Update.1.0.AwaitToActivat | Awaiting for an action to proceed with activating image|
    | e                         | 'GB200Nvl-25.01-7' on 'HGX_FW_BMC_0'.                  |
    +---------------------------+--------------------------------------------------------+
    | TaskEvent.1.0.3.TaskProgr | The task with Id '9' has changed to progress 100       |
    | essChanged                | percent complete.                                      |
    +---------------------------+--------------------------------------------------------+
    | TaskEvent.1.0.3.TaskCompl | The task with Id '9' has completed.                    |
    | etedOK                    |                                                        |
    +---------------------------+--------------------------------------------------------+

The output will conclude with:
::

    Firmware update successful!
    Overall Time Taken: 0:10:58
    Refer to 'NVIDIA Firmware Update Document' on activation steps for new firmware to take effect.
