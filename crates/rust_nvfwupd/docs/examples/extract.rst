Extracting the Firmware Files and the PLDM Metadata
---------------------------------------------------

To view PLDM metadata of a firmware package file, and extract the firmware binaries from the package, use the ``unpack`` option.

.. code-block::

    $ nvfwupd unpack -o results/ -p nvfw_Grace-CPU-P5041_0003_231109.1.4_prod-signed.fwpkg

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
                        "FWImage": "./results/BMC_23.09.03_image.bin",
                        "FWImageSHA256": "58f0a104434780acd52c7ce9e69452c8fbe720eb0a06558eb1a4707e08e43790",
                        "SignatureType": "N/A",
                        "FWImageSize": 67108864
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
                        "VendorDefinedDescriptorData": "0x000001"
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
                        "FWImage": "./results/ERoT_01.03.0114.0000_n01_image.bin",
                        "FWImageSHA256": "03fbe345723cf79ae83d057526aa4f469b7ddbd99afeb9b911e74fbd723398fc",
                        "SignatureType": "N/A",
                        "FWImageSize": 203008
                    },
                    {
                        "ComponentIdentifier": "0x38",
                        "ComponentVersionString": "01.00.01",
                        "FWImage": "./results/SBIOS_SKU_895_01.00.01_image.bin",
                        "FWImageSHA256": "67284f8b1fe9fe9574a746953c78d8693324cc7c14a96def93b47dc2f1e78cc5",
                        "SignatureType": "N/A",
                        "FWImageSize": 17051648,
                        "AP_SKU_ID": "0x000001"
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
                        "FWImage": "./results/SMR_0.88_image.bin",
                        "FWImageSHA256": "f58ddc9d6d2944c664efd2597ec0ece5b148abbc85ee7513005b409bf2757a44",
                        "SignatureType": "N/A",
                        "FWImageSize": 11472896
                    }
                ]
            }
        ]
    }
    
    -------------------------------------------------------------------------------------
