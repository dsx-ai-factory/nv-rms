
Document History 
=================

* **2.1.2** - Jul 6, 2026

  - Added support for Megmeet PowerShelf platforms.
  - Changed the default multipart update order to send ``UpdateParameters`` before ``UpdateFile``.

* **2.1.1** - Apr 27, 2026

  - Added support for Vera Rubin NVL72 NVMe drive versioning and updates
  - Added support for optional Redfish SoftwareInventory display in show_version output
  - Added support for IST Vector uploads in Vera Rubin NVL72
  - Increased default thread count for parallel show_version and update_fw operations from 3 to 10
  - Added support for user customizable thread count for parallel show_version and update_fw operations

* **2.1.0** - Feb 13, 2026

  - Added support for Vera Rubin NVL72 Compute, Switch Trays and Racks.
  - Added support for OnReset workflows for GB200/GB300/Vera Rubin NVL72 powershelves.
  - Added new activation commands "RF_PWRSHELF_RESET" and "RF_PWRSHELF_RESET_FORCE" for powershelf reset.
  - Added a new update_fw option to skip all prechecks and push an update regardless of precheck failures.
  - Added a new background_copy "interactive" mode to monitor foreground copy progress.
  - Added a new task checking feature to prevent firmware updates if an existing update task is already running on the system for GB200/GB300/Vera Rubin NVL72.
  - Added user provided timeout value for flint_update command to query on slow networks.

* **2.0.9** - Nov 6, 2025

  - Added support for GB200/GB300 NVL Delta and LiteOn PowerShelf platforms including firmware updates using Redfish APIs.
  - Added OEM parameters support for firmware update operations using the --oem_parameters option.
  - Added UPDATE_DELAY feature for parallel updates to prevent BMC memory exhaustion for BMC/HMC parallel updates.
  - Added flint_update command for flint-based firmware updates on the host system.
  - Added new activation command "RF_PWR_STATUS" for power status queries.

* **2.0.8** - Aug 15, 2025

  - Added support for HGXB300.
  - Added support for displaying multiple packages in parallel for the show_version command.

* **2.0.7** - Apr 25, 2025

  - Fixed issue in GB300 NVL firmware package not showing proper CPLD firmware version

* **2.0.6** - Apr 22, 2025

  - Added support for background copy for GB200 NVL.
  - Added support for GB300 NVL.
  - New Redfish-based firmware activation options.
  - Added support for SPI staged updates.
  - If no special update file is provided for updates, added default update parameters for GB200 NVL.

* **2.0.5** - Jan 16, 2025

  - Added support for parallel server updates through the config file.
  - Added json options for update_fw, show_update_progress, and force_update.
  - Added IPv6 support
  - Deprecated "targets" sub-option for multi-target input. The config.yaml input shall be used for this.
  - New PDF formatting 

* **2.0.4** - Nov 22, 2024

  - Enhanced automatic server type detection for DGX platforms.

* **2.0.3** - Oct 22, 2024
  
  -  Added the activate_fw command for firmware activation utilities.
  -  Added full support to update the GB200 NVL Switch Tray.             

* **2.0.2** - Aug 13, 2024
    
  - Added support for GB200 NVL platforms and minor bug fixes.

* **2.0.1** - May 10, 2024
    
  - Added support for a force update for NVIDIA® GH200.                                                                      
  - Log Sanitization: IP and creds are masked by default in the tool output.                                                 
  - Usage now supports --target and –package override from the command-line interface (CLI) over a config file.              
  - The --targets option now has the servertype sub-option to help with the usage when an unidentified platform error occurs.

* **2.0.0** - Mar 13, 2024
    
  - Added support to the config file for platform agnostic use.   

* **1.1.3** - Dec 6, 2023
    
  - Added the show_pkg_content and unpack commands.                                 
  - Provided an exit with error code 1 on update and tool failures.                
  - Enhanced the show_update_progress output to provide the complete Redfish status.                                                                         
  - Added custom log file path support.                                               

* **1.1.1** - Sept 26, 2023
    
  - Removed the BMC_ERoT section.  

* **1.1.1** - Aug 29, 2023

  - Added a man page with BaseOS information and the make_upd_targets command.

* **1.0.1** - May 26, 2023

  - Added Grace Hopper Support     

* **1.0.0** - Apr 26, 2023

  - Initial nvfwupd
