
Introduction
============

An NVIDIA® Grace™ Hopper™, Grace Blackwell or Vera Rubin server comprises Grace or Vera CPUs, optional GPUs, and a few other components (refer to the table below). Application Programming (AP) components can be updated using Out-Of-Band (OOB) with Redfish APIs or using in-band with vendor-provided tools.

:ref:`The table below <grace-hopper-ap-components>` lists the available Grace Hopper or Grace Blackwell AP components, the update methods, and where to find the instructions to update these components.

.. _grace-hopper-ap-components:

.. list-table:: Grace Hopper or Grace Blackwell Components
    :widths: auto
    :header-rows: 1

    * - Component
      - OOB Bundle
      - In Band Update
    * - Entire Bundle
      - Y
      - N
    * - BMC ERoT
      - Y
      - N
    * - BMC
      - Y
      - N
    * - CPU ERoT
      - Y
      - N
    * - GPU
      - Y
      - N
    * - FPGA
      - Y
      - N

Supported Platforms
--------------------

The following platforms are supported in NVFWUPD:

-  NVIDIA Grace P4352, NVIDIA GH200 P4351, and NVIDIA Grace Hopper x4

-  NVIDIA GH200, NVIDIA MGX C2 Grace SuperChip, and NVIDIA MGX GH200

-  NVIDIA GB200 NVL, NVIDIA GB300 NVL

-  NVIDIA Vera Rubin NVL72, NVIDIA MGX Vera C1, NVIDIA MGX Vera C2

-  Delta PowerShelf, LiteOn PowerShelf, and Megmeet PowerShelf


Updating Grace Hopper, Grace Blackwell, or Vera Rubin Firmware
--------------------------------------------------------------

This section provides information about how to update the firmware using the nvfwupd tool.

Tool and Firmware Availability
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To determine the tool and firmware availability, complete the following steps:

1. To get a PLDM firmware package that is signed by NVIDIA, contact the Application Engineering (AE) Team.

2. Enter the following information:

   - BMC-IP address or hostname

   - User ID

   - password

3. Additional Tools Needed:

   - IPMITool must additionally be installed for full firmware activation command support

.. important::

   -  The package only runs in a Linux environment, and ``nvfwupd`` is an ELF binary.
                                                                              
   -  The package has been tested on arm64 and x86_64 Ubuntu.

   -  Any Linux distribution that supports the listed dynamic libraries and additional tools should be able to run ``nvfwupd``.

Command Syntax
--------------

.. code-block::

  Usage: nvfwupd [ global options ] <command>

  Global options:
      -t --target ip=<BMC/NVOS IP> user=<login id> password=<password> [target options...]
            BMC/NVOS target. Use NVOS IP for switch targets.

      -o --os_target ip=<OS IP> user=<OS login id> password=<OS password> [OS target options...]
            Host OS SSH target for in-band workflows.

      -c --config Path for config file (optional).
            Configure tool behavior

      -v --verbose Chosen path for logfile (optional). Default path is current working directory.
            Increase verbosity

  Target option notes:
      Target options: port=<port num for port forwarding>, servertype=<Type of server>.

      OS target options: port=<SSH port>, servertype=<Type of server>.

      Supported servertype values: DGX, DGXRUBIN, HGX, MGX, GH200, HGXB100, HGXB300, HGXRUBIN, GB200,
      GB300, VRNVL72, GB200Switch, GB300Switch, VRNVL72Switch, Powershelf.

  Commands:
      help       Show tool help.                         

      version    Show tool version.                      

      show_pkg_content [ options... ]
          -p  --package        PLDM firmware package                                       

      unpack [ options... ]
          -p  --package        PLDM firmware package                                       
          -o  --outdir         Directory path to save unpacked firmware files (optional). Default path is current working directory of tool.

      <Global options...> show_version [ options... ]
          -p  --package                PLDM firmware package                                       
          -j  --json                   show output in JSON                                         
          -s  --staged                 Show staged firmware versions                               
          -i  --inventory              Show software inventory such as IST Vector Versions         

      <Global options...> update_fw [ options... ]
          -p  --package                PLDM firmware package                                       
          -y  --yes                    Bypass firmware update confirmation prompt                  
          -b  --background             Exit without waiting for the update process to finish       
          -t  --timeout                API request timeout value in seconds                        
          -s  --special                Special Update json file                                    
          -o  --oem_parameters         Oem Parameters json file                                    
          -e  --expected_inventory     JSON file listing FirmwareInventory AP names that must be present before update
          -d  --details                Show update progress in table format                        
          -j  --json                   show output in JSON. Must be paired with the -b background option, and always bypasses update confirmation prompt.
          -u  --staged_update          SPI Staged Update                                           
          -a  --staged_activate_update SPI and Activate Staged Update                              
          -k  --skip_pre_flight_checks Skip pre-flight checks before firmware update               

      <Global options...> activate_fw [ options... ]
          -c  --cmd                    Activation command name. List of supported commands ['PWR_STATUS', 'PWR_OFF', 'PWR_ON', 'PWR_CYCLE', 'RESET_COLD', 'RESET_WARM', 'NVUE_PWR_CYCLE', 'RF_AUX_PWR_CYCLE', 'RF_PWR_ON', 'RF_PWR_OFF', 'RF_PWR_CYCLE', 'RF_PWR_STATUS', 'RF_PWRSHELF_RESET', 'RF_PWRSHELF_RESET_FORCE']

      <Global options...> background_copy [ options... ]
          -s  --special                JSON File for selecting background copy target              
          -i  --interactive            Interactive mode - poll until Active and Inactive firmware slots match
          -t  --timeout                Maximum wait time in seconds for interactive mode polling (default 600)
          -p  --poll_interval          Poll interval in seconds for interactive mode (default 20)  

      <Global options...> force_update [ options... ]
          enable|disable|status        enable, disable or check current force update value on target
          -j  --json                   show output in JSON                                         

      <Global options...> show_update_progress [ options... ]
          -i  --id                     List of Task IDs delimited by space                         
          -j  --json                   show output in JSON                                         

      <Global options...> perform_factory_reset

      <Global options...> make_upd_targets [ options... ]
          -o  --outdir                 Directory path to create update target files (optional). Default path is current working directory of tool.

      <Global options...> flint_update [ options... ]
          -i  --image                  Image file for firmware flashing and version comparison     
          -d  --device_type            Device type for firmware flashing and version query (e.g., BlueField3, ConnectX)
          -j  --json                   show output in JSON.                                        
          -v  --version                Query firmware version only                                 
          -t  --timeout                Query timeout in seconds for OS commands. Flash timeout is 20x the query timeout. (default 60 for queries, 1200 for flash operations)
