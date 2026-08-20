Getting the Verbose Logs
------------------------

To get verbose tool logs in a file, use the ``–v`` or the ``--verbose`` option with the file path. If file path is not provided, the logs will be created in the ``nvfwupd_log.txt`` file in the current working directory.

Here is an example:

.. code-block::

    $ nvfwupd –v ./mypath/mylogfile.log –t ip=<BMC IP> user=*** password=*** show_version -p nvfw_Grace-CPU-P5041_0003_231109.1.4_prod-signed.fwpkg
