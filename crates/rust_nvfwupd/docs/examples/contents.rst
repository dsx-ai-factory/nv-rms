Displaying the Contents of a Package
------------------------------------

To view the PLDM metadata of a firmware package file, use the ``show_pkg_content`` option.

.. code-block::
    
    $ nvfwupd show_pkg_content -p nvfw_Grace-CPU-P5041_0003_231109.1.4_prod-signed.fwpkg

    {
        "PackageHeaderInformation": {
            "PackageHeaderIdentifier": "f018878c-cb7d-4943-9800-a02f059aca02",
            "PackageHeaderFormatRevision": "1",
            "PackageReleaseDateTime": "2023-11-9 12:35:48:0 +0",
            "PackageVersionString": "Grace-CPU-P5041_0003_231109.1.4",
            "PackageSHA256": "891731860c988190eb74409969cfa09db7ae465c9fbc741d051c83c44336f1e6"
        },
        "FirmwareDeviceRecords": [
            {
                "ComponentImageSetVersionString": "BMC::",
                "DeviceDescriptors": [
                    {
                        "InitialDescriptorType": "IANA Enterprise ID",
                        "InitialDescriptorData": "0x00001647"
                    },
                    {
                        "AdditionalDescriptorType": "UUID",
                        "AdditionalDescriptorData": "0xa5a6bcbdf1fe4b0cb57b2a4a71c48116"
                    },
                    {
                        "AdditionalDescriptorType": "Vendor Defined",
                        "VendorDefinedDescriptorTitleString": "GLACIERDSD",
                        "VendorDefinedDescriptorData": "0x1b"
                    }
                ],
                "Components": [
                    {
                        "ComponentIdentifier": "0x0",
                        "ComponentVersionString": "23.09.03",
                        "APSKUID": "N/A"
                    }
                ]
            },
            {
                "ComponentImageSetVersionString": "ERoT,SBIOS:SKU_895:",
                "DeviceDescriptors": [
                    {
                        "InitialDescriptorType": "IANA Enterprise ID",
                        "InitialDescriptorData": "0x00001647"
                    },
                    {
                        "AdditionalDescriptorType": "UUID",
                        "AdditionalDescriptorData": "0x162023c93ec5411595f448701d49d675"
                    },
                    {
                        "AdditionalDescriptorType": "Vendor Defined",
                        "VendorDefinedDescriptorTitleString": "GLACIERDSD",
                        "VendorDefinedDescriptorData": "0x38"
                    },
                    {
                        "AdditionalDescriptorType": "Vendor Defined",
                        "VendorDefinedDescriptorTitleString": "APSKU",
                        "VendorDefinedDescriptorData": "0x01000038"
                    },
                    {
                        "AdditionalDescriptorType": "Vendor Defined",
                        "VendorDefinedDescriptorTitleString": "ECSKU",
                        "VendorDefinedDescriptorData": "0x4a353681"
                    }
                ],
                "Components": [
                    {
                        "ComponentIdentifier": "0xff00",
                        "ComponentVersionString": "01.03.0114.0000_n01",
                        "ECSKUID": "0x4a353681"
                    },
                    {
                        "ComponentIdentifier": "0x38",
                        "ComponentVersionString": "01.00.01",
                        "APSKUID": "0x01000038"
                    }
                ]
            },
            {
                "ComponentImageSetVersionString": "SMR::",
                "DeviceDescriptors": [
                    {
                        "InitialDescriptorType": "IANA Enterprise ID",
                        "InitialDescriptorData": "0x00001647"
                    },
                    {
                        "AdditionalDescriptorType": "UUID",
                        "AdditionalDescriptorData": "0x8d83a0929f33481f9a12ec3768f7d0b2"
                    }
                ],
                "Components": [
                    {
                        "ComponentIdentifier": "0x0",
                        "ComponentVersionString": "0.88",
                        "APSKUID": "N/A"
                    }
                ]
            }
        ]
    }
    
    -------------------------------------------------------------------------------------
