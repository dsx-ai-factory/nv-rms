Using the Tool When an Unknown Platform Error Occurs
==============================================================

When an unknown platform error occurs on an MGX or a GH200 system, to resolve the error, in the ``--target`` option, use the servertype suboption.

.. note::
    
    The suboption can be used with **all** commands that use ``--target`` option (see the sample output). 
    
    For MGX platforms, use ``servertype=MGX`` and for GB200 NVL platforms use ``servertype=GB200``.                        

.. code-block::

    
    $ nvfwupd --target ip=192.0.2.10 user=*** password=*** servertype=GH200 show_version –p nvfw_P4764_0001_240405.1.0_dbg-signed.fwpkg
    System Model: P3809
    Part number: $TRAY_PART_NUMBER
    Serial number: $TRAY_SERIAL_NUMBER
    Packages: ['P4764_0001_240405.1.0']
    System IP: XXXX
    Firmware Devices:
    
    AP Name            Sys Version         Pkg Version         Up-To-Date 
    -------            -----------         -----------         
    Cpld0              0.00                N/A                 No         
    FW_BMC_0           GH-24.01-6          N/A                 No         
    FW_ERoT_BMC_0      01.03.0131.0000_n01 N/A                 No         
    HGX_CPU_0          01.02.00            01.02.04            No         
    HGX_CPU_1          01.02.00            01.02.04            No         
    HGX_Cpld0          0.13                0.17                No         
    HGX_ERoT_CPU_0     01.03.0131.0000_n01 01.03.0136.0000_n01 No         
    HGX_ERoT_CPU_1     01.03.0131.0000_n01 01.03.0136.0000_n01 No         
    HGX_ERoT_FPGA_0    01.03.0131.0000_n01 01.03.0136.0000_n01 No         
    HGX_ERoT_GPU_0     01.03.0131.0000_n01 01.03.0136.0000_n01 No         
    HGX_ERoT_GPU_1     01.03.0131.0000_n01 01.03.0136.0000_n01 No         
    HGX_FW_BMC_0       GH-24.01-6          24.03.C             No         
    HGX_FW_FPGA_0      0.32                0.3A                No         
    HGX_HMC_ERoT_HMC_0 01.03.0131.0000_n01 01.03.0136.0000_n01 No         

    -------------------------------------------------------------------------------------
    Error Code: 0
