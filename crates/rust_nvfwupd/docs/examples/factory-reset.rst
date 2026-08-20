
A Factory Reset of the Target System
------------------------------------

To complete a factory reset the target system, run the following command.

.. code-block::

    $ nvfwupd -t ip=<BMC-IP> user=***** password=***** servertype=MGX perform_factory_reset

    Factory Reset request successful

    Task State:
    {
        "@Message.ExtendedInfo": [
            {
                "@odata.type": "#Message.v1_1_1.Message",
                "Message": "The request completed successfully.",
                "MessageArgs": [],
                "MessageId": "Base.1.13.0.Success",
                "MessageSeverity": "OK",
                "Resolution": "None"
            }
        ]
    }

.. warning:: 

    This command deletes the overlay partition, so all changes to the BMC will be deleted.