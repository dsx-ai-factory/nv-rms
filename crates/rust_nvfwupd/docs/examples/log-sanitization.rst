Disabling Log Sanitization
--------------------------

Starting with nvfwupd v2.0.1, the tool will sanitize logs by using the default IP address and the username, and the password will be masked in the logs/command-line interface (CLI). To disable log sanitization, create a configuration file with the ``SANITIZE_LOG`` parameter and run the tool with the ``–c`` option to get logs and/or output without IP masking.

.. code-block::

    $ cat log_config.yaml

    SANITIZE_LOG: False

    $ nvfwupd –t ip=<BMC IP> user=*** password=*** -c log_config.yaml <command name> <command sub-options>
