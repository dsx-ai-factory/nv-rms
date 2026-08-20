.. _platform-agnostic-updates:

Platform-Agnostic Firmware Updates with nvfwupd Using the Config File
===============================================================================

nvfwupd v2.0.0 and later supports input and platform-agnostic firmware updates through the YAML configuration file, which allows users to customize update methods and the Redfish URIs that used for the update.

Config File Parameters
----------------------

The tool supports various configuration parameters and ``BMC_IP``, ``RF_USERNAME`` and ``RF_PASSWORD`` are always mandatory. Here is a sample YAML file that explains each parameter:

.. code-block:: yaml

   # Define the target platform as one of HGX/DGX/GH/NVOS/GH200/GB200/GB300/HGXB100/HGXB300/GB200Switch/GB300Switch.
   TargetPlatform: 'GH'
   # Disable Sanitize Log, disabling Sanitize Log leads to print system IP and user credential to the logs and screen
   SANITIZE_LOG: False

   # Provide full path of firmware file(s) to be used for firmware update. Value is a list
   FWUpdateFilePath: 
   - "../packages/ nvfw_Grace-CPU-P5041_0003_240124.1.0_prod-signed.fwpkg"

   # Define URI for MultipartHttpPushUri (Optional override). 
   # Default value is taken from UpdateService
   MultipartHttpPushUri: '/redfish/v1/UpdateService/update-multipart'

   # HttpPushUri for PLDM Firmware Update (Optional override). 
   # Default value "/redfish/v1/UpdateService"
   HttpPushUri: '/redfish/v1/UpdateService'

   # Change if TaskServiceUri is different from default value below
   # Task id will always be taken from update response in update_fw or
   # from –i input in show_update_progress 
   TaskServiceUri: '/redfish/v1/TaskService/Tasks/'

   # Define differnt update methods. 
   # Valid values {'MultipartHttpPushUri', 'HttpPushUri', 'NVOSRest'}
   FwUpdateMethod: "MultipartHttpPushUri"

   # Optional Parameter used with MultipartHttpPushUri update method
   # used to define dict of parameters for multipart FW update
   MultipartOptions:
   - ForceUpdate: True

   # Optional Parameter that can be used to set ApplyTime for powershelf updates
   SpecialUpdateParameters: { "ApplyTime” : “OnReset” }

   # Target IP address. BMC IP/NVOS Rest service IP/localhost for port forwarding
   BMC_IP: "192.0.2.10"
   RF_USERNAME: "user"
   RF_PASSWORD: "<password>"
   # Target port config if port forwarding is used.
   TUNNEL_TCP_PORT: "14443"

   # List of update targets. replaces -s/--special option input file. Value is list of target URIs
   # Use UpdateParametersTargets: {} for DGX empty JSON value used for full DGX update
   # Use UpdateParametersTargets: [] for Oberon empty list used for BMC update
   # Remove this parameter for GH full update
   UpdateParametersTargets:
   - "/redfish/v1/UpdateService/FirmwareInventory/CPLDMB_0"

   # Config for reset BMC parameters. Value is a dict.
   # Use ResetType: 'ResetAll' for DGX
   # Use ResetToDefaultsType: 'ResetAll' for MGX or HGX
   BMCResetParameters:
      ResetType: 'ResetAll'

   # Multi target input. Value is list of dicts.
   Targets:
   - BMC_IP: "192.0.2.10"
     RF_USERNAME: "user"
     RF_PASSWORD: "<password>"
     TUNNEL_TCP_PORT: "14443"
   - BMC_IP: "198.51.100.10"
     RF_USERNAME: "user"
     RF_PASSWORD: "<password>"
     TUNNEL_TCP_PORT: "14444"


Running nvfwupd Commands Using the Config File
----------------------------------------------

To use the tool and update a platform that supports MultipartHttpPushUri or HttpPushUri, but is not automatically identified by the tool or provide a platform that is not a supported error, a configuration file can be used to provide the input and customize the behavior.

Here is some additional information:

-  Support for ``show_version`` on an unknown platform is limited.

   If the TargetPlatform parameter is not in the config file, ``show_version`` will not match the firmware inventory to PLDM package contents. The **Pkg Version** and **Up-to-date** columns will be ``N/A`` and ``No`` respectively.

-  The make_upd_targets command is not supported with config file because the resulting JSON files cannot be used with the config file.

-  The config file takes update targets as a config file parameter, and because the tool is supposed to be used with a platform that is not known to the tool, the target list must be identified and verified by users **before** providing it as input.

-  While updating a GraceHopper platform, we strongly recommend that you set the ``TargetPlatform`` parameter as ``GraceHopper``.

Setting this parameter to another type can lead to unwanted issues on the platform.

.. code-block::

   $ cat config.yaml
   
   TargetPlatform: 'GH'
   FWUpdateFilePath:
   - "../../packages/nvfw_P4764_0000_240103.1.2_dbg-signed.fwpkg"
   # MultipartHttpPushUri: '/redfish/v1/UpdateService/update-multipart'
   FwUpdateMethod: "MultipartHttpPushUri"
   BMC_IP: "192.0.2.10"
   RF_USERNAME: "****"
   RF_PASSWORD: "******"
   UpdateParametersTargets:
   - "/redfish/v1/Chassis/5B247A_Baseboard_0"
   BMCResetParameters:
   ResetToDefaultsType: 'ResetAll'
   
.. code-block::

   $ nvfwupd -c config.yaml update_fw
   Updating ip address: ip=192.0.2.10
   FW package: ['../../packages/nvfw_P4764_0000_240103.1.2_dbg-signed.fwpkg']
   Ok to proceed with firmware update? <Y/N>
   y
   {
      "@odata.id": "/redfish/v1/TaskService/Tasks/5B247A_5",
      "@odata.type": "#Task.v1_4_3.Task",
      "Id": "5B247A_5",
      "TaskState": "Running",
      "TaskStatus": "OK"
   }
   FW update started, Task Id: 5B247A_5
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
   Overall Time Taken: 0:08:15
   Update successful. Perform activation steps for new firmware to take effect.
   -------------------------------------------------------------------------------------
   Error Code: 0

.. code-block::

   $ nvfwupd -c oberon_config.yaml perform_factory_reset
   BMC IP: ip=192.0.2.10
   Factory Reset request successful
   Task State:
   {
      "@Message.ExtendedInfo": [
         {
            "@odata.type": "#Message.v1_1_1.Message",
            "Message": "The request completed successfully.",
            "MessageArgs": [],
            "MessageId": "Base.1.15.0.Success",
            "MessageSeverity": "OK",
            "Resolution": "None"
         }
      ]
   }
   -------------------------------------------------------------------------------------

.. code-block::

   nvfwupd -c config.yaml show_update_progress -i 0 
   Task Info for Id: 0 
   StartTime: 2024-01-20T02:46:15+00:00 
   TaskState: Completed 
   PercentComplete: 100 
   TaskStatus: OK 
   EndTime: 2024-01-20T02:46:17+00:00 
   Overall Time Taken: 0:00:02 
   Overall Task Status: { 
      "@odata.id": "/redfish/v1/TaskService/Tasks/0", 
      "@odata.type": "#Task.v1_4_3.Task", 
      "EndTime": "2024-01-20T02:46:17+00:00", 
      "Id": "0", 
      "Messages": [ 
         { 
               "@odata.type": "#Message.v1_0_0.Message", 
               "Message": "The task with id 0 has started.", 
               "MessageArgs": [ 
                  "0" 
               ], 
               "MessageId": "TaskEvent.1.0.1.TaskStarted", 
               "Resolution": "None.", 
               "Severity": "OK" 
         }, 
         { 
               "@odata.type": "#Message.v1_0_0.Message", 
               "Message": "The task with id 0 has changed to progress 100 percent complete.", 
               "MessageArgs": [ 
                  "0", 
                  "100" 
               ], 
               "MessageId": "TaskEvent.1.0.1.TaskProgressChanged", 
               "Resolution": "None.", 
               "Severity": "OK" 
         }, 
         { 
               "@odata.type": "#Message.v1_0_0.Message", 
               "Message": "The task with id 0 has Completed.", 
               "MessageArgs": [ 
                  "0" 
               ], 
               "MessageId": "TaskEvent.1.0.1.TaskCompletedOK", 
               "Resolution": "None.", 
               "Severity": "OK" 
         } 
      ], 
      "Name": "Task 0", 
      "Payload": { 
         "HttpHeaders": [ 
               "Host: 192.0.2.10", 
               "User-Agent: python-requests/2.28.2", 
               "Accept-Encoding: gzip, deflate", 
               "Accept: */*", 
               "Connection: keep-alive", 
               "Content-Length: 109023143" 
         ], 
         "HttpOperation": "POST", 
         "JsonBody": "null", 
         "TargetUri": "/redfish/v1/UpdateService/update-multipart" 
      }, 
      "PercentComplete": 100, 
      "StartTime": "2024-01-20T02:46:15+00:00", 
      "TaskMonitor": "/redfish/v1/TaskService/Tasks/0/Monitor", 
      "TaskState": "Completed", 
      "TaskStatus": "OK" 
   } 
   Update is successful. 
   -------------------------------------------------------------------------------------
