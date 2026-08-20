MGX C2 Grace SuperChip and MGX GH200
==============================================

The following table provides a list of the firmware components that are supported through the Redfish (PLDM) firmware update on MGX C2 Grace SuperChip and MGX GH200.

..  table:: Firmware Components Supported in a Redfish Firmware Update
    :name: mgx_grace_firmware_table
    :widths: auto

    +----------------+---------------+------------+----------------------------+-------------------------------------+
    | **Component**  | **MGX GH200** | **MGX C2** | **Firmware                 | **Firmware Update Path**            |
    |                |               |            | Update                     |                                     |
    |                |               |            | Time**                     |                                     |
    +----------------+---------------+------------+----------------------------+-------------------------------------+
    | VBIOS          | Yes           | N/A        | Approximately one minute   | BMC > FPGA > GPU EROT using PLDM T5 |
    +----------------+---------------+------------+----------------------------+-------------------------------------+
    | SBIOS          | Yes           | Yes        | Approximately five minutes | BMC > FPGA > CPU EROT using PLDM T5 |
    +----------------+---------------+------------+----------------------------+-------------------------------------+
    | FPGA           | Yes           | Yes        | Approximately two minutes  | BMC > SPI > FPGA                    |
    +----------------+---------------+------------+----------------------------+-------------------------------------+
    | EROT           | Yes           | Yes        | Approximately two minutes  | BMC > EROT or BMC > FPGA > EROT     |
    +----------------+---------------+------------+----------------------------+-------------------------------------+
    | BMC            | Yes           | Yes        | Approximately 10 Minutes   | BMC > SPI > EROT                    |
    +----------------+---------------+------------+----------------------------+-------------------------------------+
