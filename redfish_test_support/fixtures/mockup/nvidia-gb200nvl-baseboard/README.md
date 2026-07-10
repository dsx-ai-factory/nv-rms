# GB200NVL HMC/BMC 1.0.00 QA Drop Mockup 25.02-C

This mockups reflects GB200NVL BMC and HMC QS release version 25.02-C (1.0.00).

## Notes on Terminology

* **"Compute Tray"** - An indvidual rack mounted compute node
* **"Chassis"** - a entire rack of Compute Trays, Switches, Power Equipement, etc.
* Redfish uses `Chassis` in it's traditional form of a "container" (e.g. a rack server's sheet metal, a board, or a virtual zone that includes other chassis) - not to be confused with the entire rack or the entire Compute Tray.

## Notes on the Redfish Identity of the GB200NVL Compute Tray

* PDB (the Power Distribution Board) `/redfish/v1/Chassis/PDB_0` FRU EEPROM is the single Compute Tray point of identity. It is accessible only to the BMC.
* HMC does not present a single global Compute Tray identity, but exposes individual board FRU info at the board level.
* HMC's ComputerSystem `/redfish/v1/Systems/HGX_Baseboard_0` is not a complete `ComputerSystem` resource, but is a host for Processor, Memory, and Log information.  It does not control or track system power statue, boot order, BIOS configuration, Boot Progress Code Logging, or other BMC-managed devices such as CX7 or drives. This is left to the BMC.
* HMC's topmost container Chassis `/redfish/v1/HGX_Chassis_0` is a virtual "Zone" ChassisType and is not physical.  It serves as a "top-most" Chassis resource for all components managed by the HMC.  Therefore, it also has no hardware identity such as Model, SKU, SerialNumber, nor does it host physical sensors.
* CBCs (Cable Cartridges, 4 per compute tray, /redfish/v1/Chassis/CBC_x in BMC model) contain FRU EEPROMs describing the identity and parameters of the Chassis (the entire rack) - these FRU EEPROMs are scannable by the BMC like any other FRU EEPROM.  There is no "primary" CBC FRU EEPROM - there are 4 peers.

## HMC Known Issues

## BMC Known Issues

`/redfish/v1/UpdateService` contains two OEM Actions with incorrect JSON structure.  They are enclosed within an "Nvidia" object.  These will not be corrected for GB200.

```json
    "Actions": {
        "Oem": {
            "#NvidiaUpdateService.CommitImage": {
                "@Redfish.ActionInfo": "/redfish/v1/UpdateService/Oem/Nvidia/CommitImageActionInfo",
                "target": "/redfish/v1/UpdateService/Actions/Oem/NvidiaUpdateService.CommitImage"
            },
            "Nvidia": {
                "#NvidiaUpdateService.PublicKeyExchange": {
                    "target": "/redfish/v1/UpdateService/Actions/Oem/NvidiaUpdateService.PublicKeyExchange"
                },
                "#NvidiaUpdateService.RevokeAllRemoteServerPublicKeys": {
                    "target": "/redfish/v1/UpdateService/Actions/Oem/NvidiaUpdateService.RevokeAllRemoteServerPublicKeys"
                }
            }
        }
    },
```
