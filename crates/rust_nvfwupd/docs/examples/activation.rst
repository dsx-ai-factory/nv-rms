
Activation Command
------------------

The tool supports the ``activate_fw`` command from v2.0.3 and later, and a variety of activation operations are supported.

.. note::
    Firmware activation commands do not wait for the BMC to come back online, and return immediately after issuing the command.

Here is an example of the command:

.. code-block:: 

    $ nvfwupd -t ip=<BMC-IP> user=**** password=**** servertype=<> activate_fw –c <Supported Command from table below>

:ref:`The supported commands table <supported-commands>` provides information about the supported commands and what they do.

.. _supported-commands:

..  table:: Supported Activation Commands Per Platform
    :name: activation_table
    :widths: auto

    +---------------------------+-------------------------------------+---------------------------------------------------------+
    | **Supported Command**     | **Operation**                       | **Supported platform**                                  |
    +---------------------------+-------------------------------------+---------------------------------------------------------+
    | PWR_STATUS                | IPMI Tool check chassis power status| MGX, GH200, GB200 NVL, GB300 NVL, and Vera Rubin NVL72  |
    +---------------------------+-------------------------------------+---------------------------------------------------------+
    | PWR_OFF                   | IPMI Tool chassis power off         | MGX, GH200, GB200 NVL, GB300 NVL, and Vera Rubin NVL72  |
    +---------------------------+-------------------------------------+---------------------------------------------------------+
    | PWR_ON                    | IPMI Tool chassis power on          | MGX, GH200, GB200 NVL, GB300 NVL, and Vera Rubin NVL72  |
    +---------------------------+-------------------------------------+---------------------------------------------------------+
    | PWR_CYCLE                 | IPMI Tool chassis power cycle       | MGX, GH200, GB200 NVL, GB300 NVL, and Vera Rubin NVL72  |
    +---------------------------+-------------------------------------+---------------------------------------------------------+
    | RESET_COLD                | IPMI Tool cold reset                | MGX, GH200, GB200 NVL, GB300 NVL, and Vera Rubin NVL72  |
    +---------------------------+-------------------------------------+---------------------------------------------------------+
    | RESET_WARM                | IPMI Tool warm reset                | MGX, GH200                                              |
    +---------------------------+-------------------------------------+---------------------------------------------------------+
    | NVUE_PWR_CYCLE            | Power cycle GB200 NVL Switch NVOS   | GB200Switch, GB300Switch, and Vera Rubin NVL72 Switch   |
    +---------------------------+-------------------------------------+---------------------------------------------------------+
    | RF_AUX_PWR_CYCLE          | AC Cycle the BMC using Redfish      | GH200, GB200 NVL, GB300 NVL, and Vera Rubin NVL72       |
    +---------------------------+-------------------------------------+---------------------------------------------------------+
    | RF_PWR_ON                 | Redfish chassis power on            | GH200, GB200 NVL, GB300 NVL, and Vera Rubin NVL72       |
    +---------------------------+-------------------------------------+---------------------------------------------------------+
    | RF_PWR_OFF                | Redfish chassis power off           | GH200, GB200 NVL, GB300 NVL, and Vera Rubin NVL72       |
    +---------------------------+-------------------------------------+---------------------------------------------------------+
    | RF_PWR_CYCLE              | Redfish chassis power cycle         | GH200, GB200 NVL, GB300 NVL, and Vera Rubin NVL72       |
    +---------------------------+-------------------------------------+---------------------------------------------------------+
    | RF_PWR_STATUS             | Redfish chassis power status query  | GH200, GB200 NVL, GB300 NVL, and Vera Rubin NVL72       |
    +---------------------------+-------------------------------------+---------------------------------------------------------+
    | RF_PWRSHELF_RESET         | Redfish PowerShelf graceful restart | Delta, LiteOn, and Megmeet PowerShelves                 |
    +---------------------------+-------------------------------------+---------------------------------------------------------+
    | RF_PWRSHELF_RESET_FORCE   | Redfish PowerShelf force restart    | Delta, LiteOn, and Megmeet PowerShelves                 |
    +---------------------------+-------------------------------------+---------------------------------------------------------+

