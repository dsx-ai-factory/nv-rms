/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: LicenseRef-NvidiaProprietary
 *
 * NVIDIA CORPORATION, its affiliates and licensors retain all intellectual
 * property and proprietary rights in and to this material, related
 * documentation and any modifications thereto. Any use, reproduction,
 * disclosure or distribution of this material and related documentation
 * without an express license agreement from NVIDIA CORPORATION or
 * its affiliates is strictly prohibited.
 */

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use clap::{Parser, Subcommand};
use librms::protos::rack_manager as rm;
use librms::protos::rack_manager_v2 as rm_v2;
use rm::rack_manager_client::RackManagerClient;
use rm_v2::rack_manager_v2_client::RackManagerV2Client;
use tonic::Request;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity};

type ClientResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

struct Client {
    rack_manager: RackManagerClient<Channel>,
    rack_manager_v2: RackManagerV2Client<Channel>,
}

impl std::ops::Deref for Client {
    type Target = RackManagerClient<Channel>;

    fn deref(&self) -> &Self::Target {
        &self.rack_manager
    }
}

impl std::ops::DerefMut for Client {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.rack_manager
    }
}

/// Standalone Rack Management Service gRPC client.
#[derive(Debug, Parser)]
#[command(
    name = "rack_manager_client",
    version,
    arg_required_else_help = true,
    after_help = "Examples:
  rack_manager_client localhost 8801 switch app_status /tmp/nmxc_targets.csv
  rack_manager_client localhost 8801 switch cluster_state /tmp/sw01.csv
  rack_manager_client localhost 8801 switch cluster_state_set /tmp/switches.csv 0
  rack_manager_client localhost 8801 switch gnmi_service /tmp/sw01.csv 1
  rack_manager_client localhost 8801 switch configure_certificate /tmp/switches.csv --service nvue-api --service scale-up-fabric-manager --domain site-wide
  rack_manager_client localhost 8801 switch configure_certificate_status <job_id>
  rack_manager_client localhost 8801 firmware_object add gb200 /tmp/firmware-manifest.json $ARTIFACTORY_TOKEN --set-default
  rack_manager_client localhost 8801 firmware_object apply rack-1 /tmp/nodes.csv prod gb200 --component-filter 2:BMC
  rack_manager_client localhost 8801 firmware_object apply_stored_switch_system_image rack-1 /tmp/switches.csv prod gb200
  rack_manager_client localhost 8801 switch device_info rack-1 c-01
  rack_manager_client localhost 8801 switch device_info_by_type rack-1 ComputeGb200Nvidia
  rack_manager_client localhost 8801 switch device_info_batch /tmp/switches.csv"
)]
struct Cli {
    /// Rack Manager gRPC server host or IP address
    host: String,

    /// Rack Manager gRPC server port
    port: u16,

    /// Path to client certificate file for mutual TLS
    #[arg(long = "cert", requires = "key")]
    cert: Option<PathBuf>,

    /// Path to client private key file for mutual TLS
    #[arg(long = "key", requires = "cert")]
    key: Option<PathBuf>,

    /// Path to CA certificate file for server verification
    #[arg(long = "ca")]
    ca: Option<PathBuf>,

    /// Force an insecure HTTP/2 connection, even if certificates are provided
    #[arg(long)]
    insecure: bool,

    /// TLS server name override for certificate verification
    #[arg(long = "tls-domain")]
    tls_domain: Option<String>,

    /// Connection timeout in seconds
    #[arg(long, default_value_t = 5)]
    connect_timeout_seconds: u64,

    /// Per-RPC request timeout in seconds
    #[arg(long, default_value_t = 1800)]
    request_timeout_seconds: u64,

    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Check connectivity with the Rack Manager GetVersion RPC
    Version,

    /// Set power state for a node: 1=off, 2=on, 3=reset
    Power {
        rack_id: String,
        node_id: String,
        operation: i32,
    },

    /// Get power state for a node
    Status { rack_id: String, node_id: String },

    /// Register caller-supplied devices in RMS inventory.
    /// CSV: node_id,rack_id,ip,port,username,password,mac,type[,endpoint_role][,host_name][,bmc_ip,bmc_port,bmc_username,bmc_password[,bmc_mac]]
    #[command(name = "add_nodes")]
    AddNodes { devices_csv: PathBuf },

    /// List the complete node inventory across all racks
    #[command(name = "list_node_inventory")]
    ListNodeInventory,

    /// List all rack identifiers known to the service, including empty racks
    #[command(name = "list_racks")]
    ListRacks,

    /// Get the configured rack power-on order
    #[command(name = "get_power_on_order")]
    GetPowerOnOrder { rack_id: String },

    /// Set rack power-on order as node [timeout_seconds] pairs
    #[command(name = "set_power_on_order")]
    SetPowerOnOrder {
        rack_id: String,
        #[arg(required = true)]
        items: Vec<String>,
    },

    /// Set power state for caller-supplied nodes from a CSV.
    /// CSV: node_id,rack_id,ip,port,username,password,mac,type[,endpoint_role][,host_name][,bmc_ip,bmc_port,bmc_username,bmc_password[,bmc_mac]]
    #[command(name = "batch_set_power_state")]
    BatchSetPowerState {
        nodes_csv: PathBuf,

        /// 1=Off, 2=On, 3=Reset, 4=ForceOff, 5=ForceOn, 6=GracefulShutdown, 7=GracefulRestart, 8=ForceRestart
        operation: i32,
    },

    /// Get power state for caller-supplied nodes from a CSV.
    /// CSV: node_id,rack_id,ip,port,username,password,mac,type[,endpoint_role][,host_name][,bmc_ip,bmc_port,bmc_username,bmc_password[,bmc_mac]]
    #[command(name = "batch_get_power_state")]
    BatchGetPowerState { nodes_csv: PathBuf },

    /// Power on all nodes in a rack using the configured sequence
    #[command(name = "rack_power_on")]
    RackPowerOn { rack_id: String },

    /// Power off all nodes in a rack
    #[command(name = "rack_power_off")]
    RackPowerOff { rack_id: String },

    /// Power cycle all nodes in a rack using the configured sequence
    #[command(name = "rack_power_cycle")]
    RackPowerCycle { rack_id: String },

    /// Start async firmware update for one inventory node
    #[command(name = "update_firmware")]
    UpdateFirmware {
        rack_id: String,
        node_id: String,
        #[arg(required = true, value_name = "target:filename")]
        targets: Vec<String>,
        #[arg(long)]
        activate: bool,
        #[arg(long)]
        force: bool,
    },

    /// Start async firmware update for all inventory nodes of one type
    #[command(name = "batch_update_firmware_by_node_type")]
    BatchUpdateFirmwareByNodeType {
        rack_id: String,
        node_type: i32,
        #[arg(required = true, value_name = "target:filename")]
        targets: Vec<String>,
        #[arg(long)]
        activate: bool,
        #[arg(long)]
        force: bool,
    },

    /// Start async firmware update for caller-supplied nodes
    #[command(name = "batch_update_firmware")]
    BatchUpdateFirmware {
        nodes_csv: PathBuf,
        targets_csv: PathBuf,
        #[arg(long)]
        activate: bool,
        #[arg(long)]
        force: bool,
    },

    /// Get async firmware job status
    #[command(name = "firmware_job_status")]
    FirmwareJobStatus { job_id: String },

    /// Get generic async job status
    #[command(name = "job_status")]
    JobStatus {
        job_id: String,
        #[arg(long)]
        include_child_job_states: bool,
    },

    /// Get firmware inventory for one inventory node
    #[command(name = "firmware_inventory_node")]
    FirmwareInventoryNode { rack_id: String, node_id: String },

    /// Get firmware inventory for all nodes in a rack
    #[command(name = "firmware_inventory_rack")]
    FirmwareInventoryRack { rack_id: String },

    /// Start async switch system image update for caller-supplied switches
    #[command(name = "update_switch_system_image")]
    UpdateSwitchSystemImage {
        targets_csv: PathBuf,
        image_filename: String,
        local_file_path: String,
    },

    /// Get async switch system image job status
    #[command(name = "switch_system_image_job_status")]
    SwitchSystemImageJobStatus { job_id: String },

    /// Update an NVOS user's password on caller-supplied switches
    #[command(name = "update_switch_system_password")]
    UpdateSwitchSystemPassword {
        targets_csv: PathBuf,
        username: String,
        password: String,
    },

    /// Firmware manifest management commands
    #[command(name = "firmware_object")]
    FirmwareObject {
        #[command(subcommand)]
        command: FirmwareObjectCommand,
    },

    /// Switch-specific commands
    Switch {
        #[command(subcommand)]
        command: SwitchCommand,
    },
}

#[derive(Debug, Subcommand)]
enum FirmwareObjectCommand {
    /// Add a firmware object from a JSON firmware manifest
    Add {
        hardware_type: String,
        config_json: PathBuf,
        access_token: String,
        #[arg(long)]
        set_default: bool,
    },

    /// Get one firmware object by object ID
    Get { object_id: String },

    /// List firmware objects
    List {
        #[arg(long)]
        hardware_type: Option<String>,
        #[arg(long)]
        only_available: bool,
    },

    /// Delete one firmware object by object ID
    Delete { object_id: String },

    /// Set the default firmware object for its hardware type
    #[command(name = "set_default")]
    SetDefault { object_id: String },

    /// Apply stored firmware object targets to caller-supplied nodes
    #[command(name = "apply_stored_firmware_object")]
    ApplyStored {
        rack_id: String,
        nodes_csv: PathBuf,
        firmware_type: String,
        hardware_type: String,
        #[arg(long)]
        object_id: Option<String>,
        #[arg(long)]
        force: bool,
        #[arg(long = "component")]
        components: Vec<String>,
        #[arg(long = "component-filter", value_name = "NODE_TYPE:COMPONENT")]
        component_filters: Vec<String>,
        #[arg(long = "target-filter", value_name = "NODE_TYPE:TARGET")]
        target_filters: Vec<String>,
    },

    /// Parse/download/apply firmware object targets from a JSON firmware manifest without persisting it
    #[command(name = "apply")]
    Apply {
        rack_id: String,
        nodes_csv: PathBuf,
        config_json: PathBuf,
        access_token: String,
        firmware_type: String,
        hardware_type: String,
        #[arg(long)]
        force: bool,
        #[arg(long = "component-filter", value_name = "NODE_TYPE:COMPONENT")]
        component_filters: Vec<String>,
        #[arg(long = "target-filter", value_name = "NODE_TYPE:TARGET")]
        target_filters: Vec<String>,
    },

    /// Apply a firmware-object switch system image to caller-supplied switches
    #[command(name = "apply_stored_switch_system_image")]
    ApplyStoredSwitchSystemImage {
        rack_id: String,
        switches_csv: PathBuf,
        software_type: String,
        hardware_type: String,
        #[arg(long)]
        object_id: Option<String>,
    },

    /// Parse/download/apply a switch system image from a JSON firmware manifest without persisting it
    #[command(name = "apply_switch_system_image")]
    ApplySwitchSystemImage {
        rack_id: String,
        switches_csv: PathBuf,
        config_json: PathBuf,
        access_token: String,
        software_type: String,
        hardware_type: String,
    },

    /// List firmware-object apply history
    History {
        #[arg(long)]
        object_id: Option<String>,
        #[arg(long = "rack-id")]
        rack_ids: Vec<String>,
    },
}

#[derive(Debug, Subcommand)]
enum SwitchCommand {
    /// List switch system images through inventory credentials
    #[command(name = "sysimg_list")]
    SysimgList { rack_id: String, node_id: String },

    /// List switch firmware for a component type
    #[command(name = "list_firmware")]
    ListFirmware {
        rack_id: String,
        node_id: String,
        component_type: i32,
    },

    /// Fetch ScaleUpFabric cluster state from one direct-switch CSV target
    #[command(name = "cluster_state")]
    ClusterState { target_csv: PathBuf },

    /// Set ScaleUpFabric cluster state on one or more direct-switch CSV targets
    #[command(name = "cluster_state_set")]
    ClusterStateSet { targets_csv: PathBuf, enabled: i32 },

    /// Fetch nmx-controller application status from one or more direct-switch CSV targets
    #[command(name = "app_status")]
    AppStatus { targets_csv: PathBuf },

    /// Enable or disable gNMI service for telemetry
    #[command(name = "gnmi_service")]
    GnmiService { target_csv: PathBuf, enabled: i32 },

    /// Configure NMX controller on one direct-switch CSV target
    #[command(name = "nmxconfigure")]
    Nmxconfigure {
        target_csv: PathBuf,
        topology_type: String,

        /// TLS domain for switch client material lookup (optional; server default applies)
        #[arg(long)]
        domain: Option<String>,
    },

    /// Reconcile the scale-up fabric across direct-switch CSV targets (async; returns a job_id)
    #[command(name = "nvlink_configure")]
    NvlinkConfigure {
        targets_csv: PathBuf,
        topology_type: String,

        /// node_id of the switch that should run NMX Controller (optional; server picks a default)
        #[arg(long)]
        primary_switch_node_id: Option<String>,

        /// TLS domain for switch client material lookup (optional; server default applies)
        #[arg(long)]
        domain: Option<String>,
    },

    /// Read observed scale-up fabric status from direct-switch CSV targets (no changes)
    #[command(name = "fabric_status")]
    FabricStatus {
        targets_csv: PathBuf,

        /// TLS domain for switch client material lookup (optional; server default applies)
        #[arg(long)]
        domain: Option<String>,
    },

    /// Install RMS-configured mTLS certificates on caller-supplied switches
    #[command(name = "configure_certificate")]
    ConfigureCertificate {
        targets_csv: PathBuf,
        /// Switch service to protect (repeatable): nvue-api, scale-up-fabric-telemetry,
        /// scale-up-fabric-manager, scale-up-fabric-telemetry-interface
        #[arg(long = "service", required = true)]
        services: Vec<String>,
        /// TLS domain for switch certificate material lookup (optional; server default applies)
        #[arg(long)]
        domain: Option<String>,
        /// Run optional NVUE/NMX Hello connectivity tests after certificate installation
        #[arg(long)]
        test_hello: bool,
    },

    /// Get async switch certificate configuration job status
    #[command(
        name = "configure_certificate_status",
        visible_alias = "certificate_job_status"
    )]
    ConfigureCertificateStatus { job_id: String },

    /// Get tray/chassis location (chassis SN, slot, tray index) for one inventory node
    #[command(name = "device_info")]
    DeviceInfo { rack_id: String, node_id: String },

    /// List tray/chassis locations for all inventory nodes of one type in a rack
    #[command(name = "device_info_by_type")]
    DeviceInfoByType {
        rack_id: String,
        /// NodeType number or name (e.g. 1 or ComputeGb200Nvidia)
        node_type: String,
    },

    /// Get tray/chassis locations for caller-supplied switches from a CSV.
    /// CSV: node_id,rack_id,ip,port,username,password[,mac_address][,host_name][,node_type]
    #[command(name = "device_info_batch")]
    DeviceInfoBatch { targets_csv: PathBuf },
}

#[derive(Clone, Debug)]
struct SwitchTarget {
    node_id: String,
    rack_id: String,
    ip_address: String,
    port: u32,
    username: String,
    password: String,
    mac_address: String,
    /// TLS hostname for switch host_endpoint; defaults to `ip_address` when omitted in CSV.
    host_name: String,
    /// Switch NodeType; defaults to SwitchGb200Nvidia for backward compatibility.
    node_type: i32,
}

#[derive(Clone, Debug)]
struct NodeListBmcEndpoint {
    ip_address: String,
    port: u32,
    username: String,
    password: String,
    mac_address: String,
}

#[derive(Clone, Debug)]
struct NodeListEntry {
    node_id: String,
    rack_id: String,
    ip_address: String,
    port: u32,
    username: String,
    password: String,
    mac_address: String,
    node_type: i32,
    endpoint_role: Option<NodeListEndpointRole>,
    /// TLS hostname for switch host_endpoint; defaults to `ip_address` when empty.
    host_name: String,
    bmc_endpoint: Option<NodeListBmcEndpoint>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum NodeListEndpointMode {
    InventoryRegistration,
    DirectOperation,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum NodeListEndpointRole {
    Bmc,
    Host,
}

impl Cli {
    fn uses_tls(&self) -> bool {
        !self.insecure && (self.cert.is_some() || self.key.is_some() || self.ca.is_some())
    }

    fn endpoint_uri(&self) -> String {
        let scheme = if self.uses_tls() { "https" } else { "http" };
        format!("{scheme}://{}:{}", self.host, self.port)
    }
}

async fn build_client(cli: &Cli) -> ClientResult<Client> {
    let connect_timeout = Duration::from_secs(cli.connect_timeout_seconds);
    let request_timeout = Duration::from_secs(cli.request_timeout_seconds);
    let endpoint = Endpoint::from_shared(cli.endpoint_uri())?
        .connect_timeout(connect_timeout)
        .timeout(request_timeout);

    let endpoint = if cli.uses_tls() {
        endpoint.tls_config(build_tls_config(cli)?)?
    } else {
        endpoint
    };

    let channel = endpoint.connect().await?;

    Ok(Client {
        rack_manager: RackManagerClient::new(channel.clone()),
        rack_manager_v2: RackManagerV2Client::new(channel),
    })
}

fn build_tls_config(cli: &Cli) -> ClientResult<ClientTlsConfig> {
    let mut config = ClientTlsConfig::new().domain_name(
        cli.tls_domain
            .as_deref()
            .unwrap_or(cli.host.as_str())
            .to_owned(),
    );

    if let Some(ca_path) = &cli.ca {
        let ca_pem = std::fs::read(ca_path)?;
        config = config.ca_certificate(Certificate::from_pem(ca_pem));
    }

    if let (Some(cert_path), Some(key_path)) = (&cli.cert, &cli.key) {
        let cert_pem = std::fs::read(cert_path)?;
        let key_pem = std::fs::read(key_path)?;
        config = config.identity(Identity::from_pem(cert_pem, key_pem));
    }

    Ok(config)
}

async fn run(cli: Cli) -> ClientResult<()> {
    let mut client = build_client(&cli).await?;

    match cli.command {
        Command::Version => {
            let response = client
                .get_version(Request::new(rm::GetVersionRequest {}))
                .await?
                .into_inner();
            println!("Rack Manager version: {}", response.version);
        }
        Command::Power {
            rack_id,
            node_id,
            operation,
        } => power(&mut client, rack_id, node_id, operation).await?,
        Command::Status { rack_id, node_id } => status(&mut client, rack_id, node_id).await?,
        Command::AddNodes { devices_csv } => add_nodes(&mut client, &devices_csv).await?,
        Command::ListNodeInventory => list_node_inventory(&mut client).await?,
        Command::ListRacks => list_racks(&mut client).await?,
        Command::GetPowerOnOrder { rack_id } => get_power_on_order(&mut client, rack_id).await?,
        Command::SetPowerOnOrder { rack_id, items } => {
            set_power_on_order(&mut client, rack_id, items).await?
        }
        Command::BatchSetPowerState {
            nodes_csv,
            operation,
        } => batch_set_power_state(&mut client, &nodes_csv, operation).await?,
        Command::BatchGetPowerState { nodes_csv } => {
            batch_get_power_state(&mut client, &nodes_csv).await?
        }
        Command::RackPowerOn { rack_id } => {
            rack_power(&mut client, rack_id, rm::RackPowerOperation::On).await?
        }
        Command::RackPowerOff { rack_id } => {
            rack_power(&mut client, rack_id, rm::RackPowerOperation::Off).await?
        }
        Command::RackPowerCycle { rack_id } => {
            rack_power(&mut client, rack_id, rm::RackPowerOperation::Cycle).await?
        }
        Command::UpdateFirmware {
            rack_id,
            node_id,
            targets,
            activate,
            force,
        } => update_firmware(&mut client, rack_id, node_id, targets, activate, force).await?,
        Command::BatchUpdateFirmwareByNodeType {
            rack_id,
            node_type,
            targets,
            activate,
            force,
        } => {
            batch_update_firmware_by_node_type(
                &mut client,
                rack_id,
                node_type,
                targets,
                activate,
                force,
            )
            .await?
        }
        Command::BatchUpdateFirmware {
            nodes_csv,
            targets_csv,
            activate,
            force,
        } => batch_update_firmware(&mut client, &nodes_csv, &targets_csv, activate, force).await?,
        Command::FirmwareJobStatus { job_id } => firmware_job_status(&mut client, job_id).await?,
        Command::JobStatus {
            job_id,
            include_child_job_states,
        } => job_status(&mut client, job_id, include_child_job_states).await?,
        Command::FirmwareInventoryNode { rack_id, node_id } => {
            firmware_inventory_node(&mut client, rack_id, node_id).await?
        }
        Command::FirmwareInventoryRack { rack_id } => {
            firmware_inventory_rack(&mut client, rack_id).await?
        }
        Command::UpdateSwitchSystemImage {
            targets_csv,
            image_filename,
            local_file_path,
        } => {
            update_switch_system_image(&mut client, &targets_csv, image_filename, local_file_path)
                .await?
        }
        Command::SwitchSystemImageJobStatus { job_id } => {
            switch_system_image_job_status(&mut client, job_id).await?
        }
        Command::UpdateSwitchSystemPassword {
            targets_csv,
            username,
            password,
        } => update_switch_system_password(&mut client, &targets_csv, username, password).await?,
        Command::FirmwareObject { command } => {
            firmware_object_command(&mut client, command).await?
        }
        Command::Switch { command } => switch_command(&mut client, command).await?,
    }

    Ok(())
}

async fn firmware_object_command(
    client: &mut Client,
    command: FirmwareObjectCommand,
) -> ClientResult<()> {
    match command {
        FirmwareObjectCommand::Add {
            hardware_type,
            config_json,
            access_token,
            set_default,
        } => {
            add_firmware_object(
                client,
                hardware_type,
                &config_json,
                access_token,
                set_default,
            )
            .await
        }
        FirmwareObjectCommand::Get { object_id } => get_firmware_object(client, object_id).await,
        FirmwareObjectCommand::List {
            hardware_type,
            only_available,
        } => list_firmware_objects(client, hardware_type, only_available).await,
        FirmwareObjectCommand::Delete { object_id } => {
            delete_firmware_object(client, object_id).await
        }
        FirmwareObjectCommand::SetDefault { object_id } => {
            set_default_firmware_object(client, object_id).await
        }
        FirmwareObjectCommand::ApplyStored {
            rack_id,
            nodes_csv,
            firmware_type,
            hardware_type,
            object_id,
            force,
            components,
            component_filters,
            target_filters,
        } => {
            apply_stored_firmware_object(
                client,
                rack_id,
                &nodes_csv,
                firmware_type,
                hardware_type,
                object_id,
                force,
                components,
                component_filters,
                target_filters,
            )
            .await
        }
        FirmwareObjectCommand::Apply {
            rack_id,
            nodes_csv,
            config_json,
            access_token,
            firmware_type,
            hardware_type,
            force,
            component_filters,
            target_filters,
        } => {
            apply_firmware_object(
                client,
                rack_id,
                &nodes_csv,
                &config_json,
                access_token,
                firmware_type,
                hardware_type,
                force,
                component_filters,
                target_filters,
            )
            .await
        }
        FirmwareObjectCommand::ApplyStoredSwitchSystemImage {
            rack_id,
            switches_csv,
            software_type,
            hardware_type,
            object_id,
        } => {
            apply_stored_switch_system_image(
                client,
                rack_id,
                &switches_csv,
                software_type,
                hardware_type,
                object_id,
            )
            .await
        }
        FirmwareObjectCommand::ApplySwitchSystemImage {
            rack_id,
            switches_csv,
            config_json,
            access_token,
            software_type,
            hardware_type,
        } => {
            apply_switch_system_image(
                client,
                ApplySwitchSystemImageInput {
                    rack_id,
                    switches_csv,
                    config_json_path: config_json,
                    access_token,
                    software_type,
                    hardware_type,
                },
            )
            .await
        }
        FirmwareObjectCommand::History {
            object_id,
            rack_ids,
        } => firmware_object_history(client, object_id, rack_ids).await,
    }
}

async fn switch_command(client: &mut Client, command: SwitchCommand) -> ClientResult<()> {
    match command {
        SwitchCommand::SysimgList { rack_id, node_id } => {
            sysimg_list(client, rack_id, node_id).await
        }
        SwitchCommand::ListFirmware {
            rack_id,
            node_id,
            component_type,
        } => list_firmware(client, rack_id, node_id, component_type).await,
        SwitchCommand::ClusterState { target_csv } => cluster_state(client, &target_csv).await,
        SwitchCommand::ClusterStateSet {
            targets_csv,
            enabled,
        } => cluster_state_set(client, &targets_csv, enabled).await,
        SwitchCommand::AppStatus { targets_csv } => app_status(client, &targets_csv).await,
        SwitchCommand::GnmiService {
            target_csv,
            enabled,
        } => gnmi_service(client, &target_csv, enabled).await,
        SwitchCommand::Nmxconfigure {
            target_csv,
            topology_type,
            domain,
        } => nmxconfigure(client, &target_csv, topology_type, domain).await,
        SwitchCommand::NvlinkConfigure {
            targets_csv,
            topology_type,
            primary_switch_node_id,
            domain,
        } => {
            nvlink_configure(
                client,
                &targets_csv,
                topology_type,
                primary_switch_node_id,
                domain,
            )
            .await
        }
        SwitchCommand::FabricStatus {
            targets_csv,
            domain,
        } => fabric_status(client, &targets_csv, domain).await,
        SwitchCommand::ConfigureCertificate {
            targets_csv,
            services,
            domain,
            test_hello,
        } => configure_switch_certificate(client, &targets_csv, services, domain, test_hello).await,
        SwitchCommand::ConfigureCertificateStatus { job_id } => {
            configure_certificate_status(client, job_id).await
        }
        SwitchCommand::DeviceInfo { rack_id, node_id } => {
            get_node_device_info(client, rack_id, node_id).await
        }
        SwitchCommand::DeviceInfoByType { rack_id, node_type } => {
            list_node_device_info_by_node_type(client, rack_id, &node_type).await
        }
        SwitchCommand::DeviceInfoBatch { targets_csv } => {
            batch_get_node_device_info(client, &targets_csv).await
        }
    }
}

async fn power(
    client: &mut Client,
    rack_id: String,
    node_id: String,
    operation: i32,
) -> ClientResult<()> {
    validate_power_operation(operation)?;
    let response = client
        .set_power_state(rm::SetPowerStateRequest {
            rack_id,
            node_id,
            operation,
        })
        .await?
        .into_inner();

    print_status(response.status);
    ensure_success(response.status, "power command failed")
}

async fn status(client: &mut Client, rack_id: String, node_id: String) -> ClientResult<()> {
    let response = client
        .get_power_state(rm::GetPowerStateRequest { rack_id, node_id })
        .await?
        .into_inner();

    print_status(response.status);
    println!(
        "Rack ID: {}, Node ID: {}",
        response.rack_id, response.node_id
    );
    println!("Power State: {}", response.pstate);
    ensure_success(response.status, "status command failed")
}

async fn add_nodes(client: &mut Client, devices_csv: &Path) -> ClientResult<()> {
    let nodes = parse_node_list_csv(devices_csv)?
        .into_iter()
        .map(|node| node_list_entry_to_proto(node, NodeListEndpointMode::InventoryRegistration))
        .collect();
    let response = client
        .create_nodes(rm::CreateNodesRequest {
            nodes: Some(rm::NodeSet { nodes }),
        })
        .await?
        .into_inner();

    let common = response
        .response
        .ok_or("add_nodes response missing response")?;
    print_status(common.status);
    println!("Message: {}", common.message);
    ensure_success(common.status, "add_nodes failed")
}

async fn list_node_inventory(client: &mut Client) -> ClientResult<()> {
    let response = client
        .list_node_inventory(rm::ListNodeInventoryRequest {})
        .await?
        .into_inner();

    if response.nodes.is_empty() {
        println!("No nodes found in inventory.");
        return Ok(());
    }

    println!("Inventory contains {} node(s):", response.nodes.len());
    for node in &response.nodes {
        print_node_inventory(node);
    }
    Ok(())
}

async fn list_racks(client: &mut Client) -> ClientResult<()> {
    let response = client
        .list_racks(rm::ListRacksRequest {})
        .await?
        .into_inner();

    if response.rack_ids.is_empty() {
        println!("No racks found.");
        return Ok(());
    }

    println!("Found {} rack(s):", response.rack_ids.len());
    for rack_id in &response.rack_ids {
        println!("  {rack_id}");
    }
    Ok(())
}

async fn get_node_device_info(
    client: &mut Client,
    rack_id: String,
    node_id: String,
) -> ClientResult<()> {
    let response = client
        .get_node_device_info(rm::GetNodeDeviceInfoRequest { rack_id, node_id })
        .await?
        .into_inner();

    print_status(response.status);
    println!("Message: {}", response.message);
    if let Some(info) = response.device_info.as_ref() {
        print_node_device_info(info);
    }
    ensure_success(response.status, "get_node_device_info failed")
}

async fn list_node_device_info_by_node_type(
    client: &mut Client,
    rack_id: String,
    node_type: &str,
) -> ClientResult<()> {
    let node_type = parse_node_type_field(node_type, "node_type")?;
    let response = client
        .list_node_device_info_by_node_type(rm::ListNodeDeviceInfoByNodeTypeRequest {
            rack_id,
            node_type,
            node_descriptor: None,
        })
        .await?
        .into_inner();

    print_status(response.status);
    println!("Message: {}", response.message);
    print_node_operation_stats(&response.stats);
    print_node_device_info_table(&response.node_device_details);
    ensure_success(response.status, "list_node_device_info_by_node_type failed")
}

async fn batch_get_node_device_info(client: &mut Client, targets_csv: &Path) -> ClientResult<()> {
    let nodes = parse_switch_targets_csv(targets_csv)?
        .into_iter()
        .map(switch_target_to_node)
        .collect();
    let response = client
        .batch_get_node_device_info(rm::BatchGetNodeDeviceInfoRequest {
            nodes: Some(rm::NodeSet { nodes }),
        })
        .await?
        .into_inner();

    print_status(response.status);
    println!("Message: {}", response.message);
    print_node_operation_stats(&response.stats);
    print_node_device_info_table(&response.node_device_details);
    ensure_success(response.status, "batch_get_node_device_info failed")
}

async fn get_power_on_order(client: &mut Client, rack_id: String) -> ClientResult<()> {
    let response = client
        .get_rack_power_on_sequence(rm::GetRackPowerOnSequenceRequest { rack_id })
        .await?
        .into_inner();

    print_status(response.status);
    println!(
        "Power on order is {}",
        if response.is_valid {
            "VALID"
        } else {
            "INVALID"
        }
    );
    println!(
        "Power on order contains {} items:",
        response.power_on_order.len()
    );
    for (index, item) in response.power_on_order.iter().enumerate() {
        let check = item.completion_check.as_ref();
        let enabled = check.map(|c| c.enabled).unwrap_or(false);
        let timeout = check.map(|c| c.timeout_seconds).unwrap_or_default();
        println!(
            "  {}. Node: {}, Completion Check: {}, Timeout: {}s",
            index + 1,
            item.node_id,
            if enabled { "enabled" } else { "disabled" },
            timeout
        );
    }
    ensure_success(response.status, "get_power_on_order failed")
}

async fn set_power_on_order(
    client: &mut Client,
    rack_id: String,
    items: Vec<String>,
) -> ClientResult<()> {
    let power_on_order = parse_power_on_order(items)?;
    let response = client
        .set_rack_power_on_sequence(rm::SetRackPowerOnSequenceRequest {
            rack_id,
            power_on_order,
        })
        .await?
        .into_inner();

    let common = response
        .response
        .ok_or("set_power_on_order response missing common response")?;
    print_status(common.status);
    println!("Message: {}", common.message);
    ensure_success(common.status, "set_power_on_order failed")
}

async fn batch_set_power_state(
    client: &mut Client,
    nodes_csv: &Path,
    operation: i32,
) -> ClientResult<()> {
    validate_power_operation(operation)?;
    let nodes = parse_node_list_csv(nodes_csv)?
        .into_iter()
        .map(|node| node_list_entry_to_proto(node, NodeListEndpointMode::DirectOperation))
        .collect();
    let response = client
        .batch_set_power_state(rm::BatchSetPowerStateRequest {
            nodes: Some(rm::NodeSet { nodes }),
            operation,
        })
        .await?
        .into_inner();

    let common = response
        .response
        .ok_or("batch_set_power_state response missing common response")?;
    print_status(common.status);
    println!("Message: {}", common.message);
    print_node_operation_stats(&common.stats);
    print_common_node_results(&common.node_results);
    ensure_success(common.status, "batch_set_power_state failed")
}

async fn batch_get_power_state(client: &mut Client, nodes_csv: &Path) -> ClientResult<()> {
    let nodes = parse_node_list_csv(nodes_csv)?
        .into_iter()
        .map(|node| node_list_entry_to_proto(node, NodeListEndpointMode::DirectOperation))
        .collect();
    let response = client
        .batch_get_power_state(rm::BatchGetPowerStateRequest {
            nodes: Some(rm::NodeSet { nodes }),
        })
        .await?
        .into_inner();

    let rm::BatchGetPowerStateResponse {
        response,
        node_power_states,
    } = response;
    let common = response.ok_or("batch_get_power_state response missing common response")?;
    print_status(common.status);
    println!("Message: {}", common.message);
    print_node_operation_stats(&common.stats);
    print_common_node_results(&common.node_results);
    print_node_power_states(&node_power_states);
    ensure_success(common.status, "batch_get_power_state failed")
}

async fn rack_power(
    client: &mut Client,
    rack_id: String,
    operation: rm::RackPowerOperation,
) -> ClientResult<()> {
    let response = client
        .sequence_rack_power(rm::SequenceRackPowerRequest {
            rack_id,
            operation: operation as i32,
        })
        .await?
        .into_inner();

    print_status(response.status);
    println!("Message: {}", response.message);
    ensure_success(response.status, "rack power command failed")
}

async fn update_firmware(
    client: &mut Client,
    rack_id: String,
    node_id: String,
    target_args: Vec<String>,
    activate: bool,
    force_update: bool,
) -> ClientResult<()> {
    let targets = parse_firmware_targets(&target_args)?;
    let response = client
        .update_firmware(rm::UpdateFirmwareRequest {
            rack_id,
            node_id,
            filename: String::new(),
            target: String::new(),
            activate,
            firmware_targets: targets,
            force_update,
        })
        .await?
        .into_inner();

    print_status(response.status);
    println!("Message: {}", response.message);
    println!("Job ID: {}", response.job_id);
    println!("Error code: {}", firmware_error_name(response.error_code));
    ensure_success(response.status, "update_firmware failed")
}

async fn batch_update_firmware_by_node_type(
    client: &mut Client,
    rack_id: String,
    node_type: i32,
    target_args: Vec<String>,
    activate: bool,
    force_update: bool,
) -> ClientResult<()> {
    rm::NodeType::try_from(node_type)
        .map_err(|_| "node_type must be a valid NodeType enum value")?;
    let targets = parse_firmware_targets(&target_args)?;
    let response = client
        .batch_update_firmware_by_node_type(rm::BatchUpdateFirmwareByNodeTypeRequest {
            rack_id,
            node_type,
            filename: String::new(),
            target: String::new(),
            firmware_targets: targets,
            activate,
            force_update,
            node_descriptor: None,
        })
        .await?
        .into_inner();

    let rm::BatchUpdateFirmwareByNodeTypeResponse { response, jobs } = response;
    let common =
        response.ok_or("batch_update_firmware_by_node_type response missing common response")?;
    print_status(common.status);
    println!("Message: {}", common.message);
    println!("Parent Job ID: {}", common.job_id);
    print_node_operation_stats(&common.stats);
    print_firmware_node_jobs(&jobs);
    print_common_node_results(&common.node_results);
    ensure_success(common.status, "batch_update_firmware_by_node_type failed")
}

async fn batch_update_firmware(
    client: &mut Client,
    nodes_csv: &Path,
    targets_csv: &Path,
    activate: bool,
    force_update: bool,
) -> ClientResult<()> {
    let nodes = parse_node_list_csv(nodes_csv)?
        .into_iter()
        .map(|node| node_list_entry_to_proto(node, NodeListEndpointMode::DirectOperation))
        .collect();
    let firmware_targets = parse_targets_by_type_csv(targets_csv)?;

    let response = client
        .batch_update_firmware(rm::BatchUpdateFirmwareRequest {
            nodes: Some(rm::NodeSet { nodes }),
            firmware_targets,
            activate,
            force_update,
            node_descriptor_firmware_targets: Vec::new(),
        })
        .await?
        .into_inner();

    let rm::BatchUpdateFirmwareResponse { response, jobs } = response;
    let common = response.ok_or("batch_update_firmware response missing common response")?;
    print_status(common.status);
    println!("Message: {}", common.message);
    println!("Parent Job ID: {}", common.job_id);
    print_node_operation_stats(&common.stats);
    print_firmware_node_jobs(&jobs);
    print_common_node_results(&common.node_results);
    ensure_success(common.status, "batch_update_firmware failed")
}

async fn firmware_job_status(client: &mut Client, job_id: String) -> ClientResult<()> {
    let response = client
        .get_firmware_job_status(rm::GetFirmwareJobStatusRequest { job_id })
        .await?
        .into_inner();

    print_status(response.status);
    println!("Job ID: {}", response.job_id);
    println!("State: {}", firmware_job_state_name(response.job_state));
    println!("Description: {}", response.state_description);
    if !response.rack_id.is_empty() {
        println!("Rack: {}", response.rack_id);
    }
    if !response.node_id.is_empty() {
        println!("Node: {}", response.node_id);
    }
    if !response.error_message.is_empty() {
        println!("Error: {}", response.error_message);
    }
    if !response.result_json.is_empty() {
        println!("Result: {}", response.result_json);
    }
    ensure_success(response.status, "firmware_job_status failed")?;
    if response.job_state == rm::FirmwareJobState::Failed as i32 {
        let message = if response.error_message.is_empty() {
            "firmware job failed".to_owned()
        } else {
            format!("firmware job failed: {}", response.error_message)
        };
        return Err(message.into());
    }
    Ok(())
}

async fn job_status(
    client: &mut Client,
    job_id: String,
    include_child_job_states: bool,
) -> ClientResult<()> {
    let response = client
        .get_job_status(rm::GetJobStatusRequest {
            job_id,
            include_child_job_states,
        })
        .await?
        .into_inner();

    if response.job_states.is_empty() {
        return Err("job_status returned no job states".into());
    }

    for (index, job) in response.job_states.iter().enumerate() {
        if index > 0 {
            println!();
        }

        println!("Job ID: {}", job.job_id);

        if let Some(parent_job_id) = &job.parent_job_id {
            println!("Parent Job ID: {parent_job_id}");
        }

        if !job.child_job_ids.is_empty() {
            println!("Child Jobs: {}", job.child_job_ids.join(", "));
        }

        println!("State: {}", job_execution_state_name(job.execution_state));
        println!("Description: {}", job.state_description);

        if let Some(rack_id) = &job.rack_id {
            println!("Rack: {rack_id}");
        }

        if let Some(node_id) = &job.node_id {
            println!("Node: {node_id}");
        }

        if job.error_code != rm::JobError::Unspecified as i32 {
            println!("Error Code: {}", job_error_name(job.error_code));
        }

        if !job.error_message.is_empty() {
            println!("Error: {}", job.error_message);
        }

        if !job.result_json.is_empty() {
            println!("Result: {}", job.result_json);
        }
    }

    if let Some(message) = failed_job_status_message(&response.job_states) {
        return Err(message.into());
    }

    Ok(())
}

fn failed_job_status_message(job_states: &[rm::JobStatus]) -> Option<String> {
    let failed = job_states
        .iter()
        .find(|job| job.execution_state == rm::JobExecutionState::Failed as i32)?;

    if failed.error_message.is_empty() {
        Some(format!("job {} failed", failed.job_id))
    } else {
        Some(format!(
            "job {} failed: {}",
            failed.job_id, failed.error_message
        ))
    }
}

async fn firmware_inventory_node(
    client: &mut Client,
    rack_id: String,
    node_id: String,
) -> ClientResult<()> {
    let response = client
        .get_node_firmware_inventory(rm::GetNodeFirmwareInventoryRequest { node_id, rack_id })
        .await?
        .into_inner();

    print_status(response.status);
    print_firmware_inventory(&response.firmware_list);
    ensure_success(response.status, "firmware_inventory_node failed")
}

async fn firmware_inventory_rack(client: &mut Client, rack_id: String) -> ClientResult<()> {
    let response = client
        .get_rack_firmware_inventory(rm::GetRackFirmwareInventoryRequest { rack_id })
        .await?
        .into_inner();

    print_status(response.status);
    if response.nodes.is_empty() {
        println!("No firmware inventory returned.");
    }
    for node in &response.nodes {
        println!("Node: {}", node.node_id);
        print_firmware_inventory(&node.firmware_list);
    }
    ensure_success(response.status, "firmware_inventory_rack failed")
}

async fn update_switch_system_image(
    client: &mut Client,
    targets_csv: &Path,
    image_filename: String,
    local_file_path: String,
) -> ClientResult<()> {
    let nodes = parse_switch_targets_csv(targets_csv)?
        .into_iter()
        .map(switch_target_to_node)
        .collect();
    let response = client
        .update_switch_system_image(rm::UpdateSwitchSystemImageRequest {
            nodes: Some(rm::NodeSet { nodes }),
            image_filename,
            local_file_path,
        })
        .await?
        .into_inner();

    let rm::UpdateSwitchSystemImageResponse { response, jobs } = response;
    let common = response.ok_or("update_switch_system_image response missing common response")?;
    print_status(common.status);
    println!("Message: {}", common.message);
    println!("Parent Job ID: {}", common.job_id);
    print_node_operation_stats(&common.stats);
    for job in &jobs {
        println!("  Node: {} -> Job: {}", job.node_id, job.job_id);
    }
    print_common_node_results(&common.node_results);
    ensure_success(common.status, "update_switch_system_image failed")
}

async fn switch_system_image_job_status(client: &mut Client, job_id: String) -> ClientResult<()> {
    let response = client
        .get_switch_system_image_job_status(rm::GetSwitchSystemImageJobStatusRequest { job_id })
        .await?
        .into_inner();

    print_status(response.status);
    println!("Job ID: {}", response.job_id);
    println!("State: {}", response.state);
    println!("Message: {}", response.message);
    if !response.rack_id.is_empty() {
        println!("Rack: {}", response.rack_id);
    }
    if !response.node_id.is_empty() {
        println!("Node: {}", response.node_id);
    }
    if !response.error_message.is_empty() {
        println!("Error: {}", response.error_message);
    }
    if !response.result_json.is_empty() {
        println!("Result: {}", response.result_json);
    }
    ensure_success(response.status, "switch_system_image_job_status failed")
}

async fn update_switch_system_password(
    client: &mut Client,
    targets_csv: &Path,
    username: String,
    password: String,
) -> ClientResult<()> {
    let targets = parse_switch_targets_csv(targets_csv)?;
    let nodes = targets.iter().cloned().map(switch_target_to_node).collect();

    let response = client
        .update_switch_system_password(rm::UpdateSwitchSystemPasswordRequest {
            nodes: Some(rm::NodeSet { nodes }),
            username,
            password,
        })
        .await?
        .into_inner();

    let rm::UpdateSwitchSystemPasswordResponse { response } = response;
    let common =
        response.ok_or("update_switch_system_password response missing common response")?;

    print_status(common.status);
    println!("Message: {}", common.message);
    println!("Parent Job ID: {}", common.job_id);
    print_node_operation_stats(&common.stats);

    print_common_node_results(&common.node_results);

    ensure_success(common.status, "update_switch_system_password failed")
}

async fn add_firmware_object(
    client: &mut Client,
    hardware_type: String,
    config_json_path: &Path,
    access_token: String,
    set_default: bool,
) -> ClientResult<()> {
    let config_json = std::fs::read_to_string(config_json_path)?;
    let response = client
        .add_firmware_object(rm::AddFirmwareObjectRequest {
            config_json,
            access_token: Some(access_token),
            hardware_type,
            set_default,
        })
        .await?
        .into_inner();
    let object = response
        .object
        .ok_or("add_firmware_object response missing object")?;

    println!(
        "{}",
        serde_json::to_string_pretty(&firmware_object_json(&object))?
    );
    Ok(())
}

async fn get_firmware_object(client: &mut Client, object_id: String) -> ClientResult<()> {
    let response = client
        .get_firmware_object(rm::GetFirmwareObjectRequest { id: object_id })
        .await?
        .into_inner();
    let object = response
        .object
        .ok_or("get_firmware_object response missing object")?;

    print_firmware_object_detail(&object);
    Ok(())
}

async fn list_firmware_objects(
    client: &mut Client,
    hardware_type: Option<String>,
    only_available: bool,
) -> ClientResult<()> {
    let response = client
        .list_firmware_objects(rm::ListFirmwareObjectsRequest {
            only_available,
            hardware_type: hardware_type.unwrap_or_default(),
        })
        .await?
        .into_inner();

    print_firmware_object_list(&response.objects);
    Ok(())
}

async fn delete_firmware_object(client: &mut Client, object_id: String) -> ClientResult<()> {
    let response = client
        .delete_firmware_object(rm::DeleteFirmwareObjectRequest { id: object_id })
        .await?
        .into_inner();
    let response = response
        .response
        .ok_or("delete_firmware_object response missing common response")?;

    print_status(response.status);
    println!("Message: {}", response.message);
    ensure_success(response.status, "delete_firmware_object failed")
}

async fn set_default_firmware_object(client: &mut Client, object_id: String) -> ClientResult<()> {
    let response = client
        .set_default_firmware_object(rm::SetDefaultFirmwareObjectRequest { object_id })
        .await?
        .into_inner();
    let object = response
        .object
        .ok_or("set_default_firmware_object response missing object")?;

    println!(
        "{}",
        serde_json::to_string_pretty(&firmware_object_json(&object))?
    );
    Ok(())
}

// Keep sending deprecated global component filters when the legacy CLI flag is used.
#[allow(clippy::too_many_arguments, deprecated)]
async fn apply_stored_firmware_object(
    client: &mut Client,
    rack_id: String,
    nodes_csv: &Path,
    firmware_type: String,
    hardware_type: String,
    object_id: Option<String>,
    force_update: bool,
    components: Vec<String>,
    component_filter_args: Vec<String>,
    target_filter_args: Vec<String>,
) -> ClientResult<()> {
    let nodes = parse_node_list_csv(nodes_csv)?
        .into_iter()
        .map(|node| node_list_entry_to_proto(node, NodeListEndpointMode::DirectOperation))
        .collect();
    let component_filters = parse_component_filters(&component_filter_args, &target_filter_args)?;
    let response = client
        .apply_stored_firmware_object(rm::ApplyStoredFirmwareObjectRequest {
            rack_id,
            object_id: object_id.unwrap_or_default(),
            firmware_type,
            hardware_type,
            components,
            nodes: Some(rm::NodeSet { nodes }),
            force_update,
            component_filters,
            node_descriptor_component_filters: Vec::new(),
        })
        .await?
        .into_inner();

    println!("Object ID: {}", response.object_id);
    let common = response
        .response
        .ok_or("apply_stored_firmware_object response missing common response")?;
    print_status(common.status);
    println!("Message: {}", common.message);
    println!("Parent Job ID: {}", common.job_id);
    print_node_operation_stats(&common.stats);
    print_firmware_node_jobs(&response.jobs);
    print_common_node_results(&common.node_results);
    ensure_success(common.status, "apply_stored_firmware_object failed")
}

#[allow(clippy::too_many_arguments)]
async fn apply_firmware_object(
    client: &mut Client,
    rack_id: String,
    nodes_csv: &Path,
    config_json_path: &Path,
    access_token: String,
    firmware_type: String,
    hardware_type: String,
    force_update: bool,
    component_filter_args: Vec<String>,
    target_filter_args: Vec<String>,
) -> ClientResult<()> {
    let config_json = std::fs::read_to_string(config_json_path)?;
    let nodes = parse_node_list_csv(nodes_csv)?
        .into_iter()
        .map(|node| node_list_entry_to_proto(node, NodeListEndpointMode::DirectOperation))
        .collect();
    let component_filters = parse_component_filters(&component_filter_args, &target_filter_args)?;
    let response = client
        .apply_firmware_object(rm::ApplyFirmwareObjectRequest {
            rack_id,
            config_json,
            access_token: Some(access_token),
            firmware_type,
            hardware_type,
            nodes: Some(rm::NodeSet { nodes }),
            force_update,
            component_filters,
            node_descriptor_component_filters: Vec::new(),
        })
        .await?
        .into_inner();

    println!("Object ID: {}", response.object_id);
    let common = response
        .response
        .ok_or("apply_firmware_object response missing common response")?;
    print_status(common.status);
    println!("Message: {}", common.message);
    println!("Parent Job ID: {}", common.job_id);
    print_node_operation_stats(&common.stats);
    print_firmware_node_jobs(&response.jobs);
    print_common_node_results(&common.node_results);
    ensure_success(common.status, "apply_firmware_object failed")
}

async fn apply_stored_switch_system_image(
    client: &mut Client,
    rack_id: String,
    switches_csv: &Path,
    software_type: String,
    hardware_type: String,
    object_id: Option<String>,
) -> ClientResult<()> {
    let nodes = parse_switch_targets_csv(switches_csv)?
        .into_iter()
        .map(switch_target_to_node)
        .collect();
    let response = client
        .apply_stored_switch_system_image(rm::ApplyStoredSwitchSystemImageRequest {
            rack_id,
            object_id: object_id.unwrap_or_default(),
            software_type,
            hardware_type,
            nodes: Some(rm::NodeSet { nodes }),
        })
        .await?
        .into_inner();

    println!("Object ID: {}", response.object_id);
    println!("Image filename: {}", response.image_filename);
    let common = response
        .response
        .ok_or("apply_stored_switch_system_image response missing common response")?;
    print_status(common.status);
    println!("Message: {}", common.message);
    println!("Parent Job ID: {}", common.job_id);
    print_node_operation_stats(&common.stats);
    for job in &response.jobs {
        println!("  Node: {} -> Job: {}", job.node_id, job.job_id);
    }
    print_common_node_results(&common.node_results);
    ensure_success(common.status, "apply_stored_switch_system_image failed")
}

struct ApplySwitchSystemImageInput {
    rack_id: String,
    switches_csv: PathBuf,
    config_json_path: PathBuf,
    access_token: String,
    software_type: String,
    hardware_type: String,
}

async fn apply_switch_system_image(
    client: &mut Client,
    input: ApplySwitchSystemImageInput,
) -> ClientResult<()> {
    let ApplySwitchSystemImageInput {
        rack_id,
        switches_csv,
        config_json_path,
        access_token,
        software_type,
        hardware_type,
    } = input;

    let config_json = std::fs::read_to_string(config_json_path)?;
    let nodes = parse_switch_targets_csv(&switches_csv)?
        .into_iter()
        .map(switch_target_to_node)
        .collect();
    let response = client
        .apply_switch_system_image(rm::ApplySwitchSystemImageRequest {
            rack_id,
            config_json,
            access_token: Some(access_token),
            software_type,
            hardware_type,
            nodes: Some(rm::NodeSet { nodes }),
        })
        .await?
        .into_inner();

    println!("Object ID: {}", response.object_id);
    println!("Image filename: {}", response.image_filename);
    let common = response
        .response
        .ok_or("apply_switch_system_image response missing common response")?;
    print_status(common.status);
    println!("Message: {}", common.message);
    println!("Parent Job ID: {}", common.job_id);
    print_node_operation_stats(&common.stats);
    for job in &response.jobs {
        println!("  Node: {} -> Job: {}", job.node_id, job.job_id);
    }
    print_common_node_results(&common.node_results);
    ensure_success(common.status, "apply_switch_system_image failed")
}

async fn firmware_object_history(
    client: &mut Client,
    object_id: Option<String>,
    rack_ids: Vec<String>,
) -> ClientResult<()> {
    let response = client
        .get_firmware_object_history(rm::GetFirmwareObjectHistoryRequest {
            object_id: object_id.unwrap_or_default(),
            rack_ids,
        })
        .await?
        .into_inner();

    let records: Vec<_> = response
        .records
        .iter()
        .map(firmware_object_history_json)
        .collect();
    println!("{}", serde_json::to_string_pretty(&records)?);
    Ok(())
}

async fn sysimg_list(client: &mut Client, rack_id: String, node_id: String) -> ClientResult<()> {
    let response = client
        .list_switch_system_images(rm::ListSwitchSystemImagesRequest { rack_id, node_id })
        .await?
        .into_inner();

    print_status(response.status);
    if !response.images_json.is_empty() {
        println!("System images: {}", response.images_json);
    }
    if !response.error_message.is_empty() {
        println!("Error: {}", response.error_message);
    }
    ensure_success(response.status, "switch sysimg_list failed")
}

async fn list_firmware(
    client: &mut Client,
    rack_id: String,
    node_id: String,
    component_type: i32,
) -> ClientResult<()> {
    validate_component_type(component_type)?;
    let response = client
        .list_switch_firmware(rm::ListSwitchFirmwareRequest {
            component_type,
            rack_id,
            node_id,
        })
        .await?
        .into_inner();

    print_status(response.status);
    if !response.result_json.is_empty() {
        println!("Firmware information: {}", response.result_json);
    }
    if !response.error_message.is_empty() {
        println!("Error: {}", response.error_message);
    }
    ensure_success(response.status, "switch list_firmware failed")
}

async fn cluster_state(client: &mut Client, target_csv: &Path) -> ClientResult<()> {
    let target = parse_single_switch_target_csv(target_csv)?;
    let response = client
        .get_scale_up_fabric_state(rm::GetScaleUpFabricStateRequest {
            node: Some(switch_target_to_node(target)),
        })
        .await?
        .into_inner();

    print_status(response.status);
    if !response.state_json.is_empty() {
        println!("Cluster state: {}", response.state_json);
    }
    if !response.error_message.is_empty() {
        println!("Error: {}", response.error_message);
    }
    ensure_success(response.status, "switch cluster_state failed")
}

async fn cluster_state_set(
    client: &mut Client,
    targets_csv: &Path,
    enabled: i32,
) -> ClientResult<()> {
    let enabled = parse_enabled_flag(enabled)?;
    let targets = parse_switch_targets_csv(targets_csv)?;
    let nodes = targets.into_iter().map(switch_target_to_node).collect();

    let response = client
        .batch_set_scale_up_fabric_state(rm::BatchSetScaleUpFabricStateRequest {
            nodes: Some(rm::NodeSet { nodes }),
            enabled,
        })
        .await?
        .into_inner();

    let common = response
        .response
        .ok_or("batch_set_scale_up_fabric_state response missing common response")?;
    print_status(common.status);
    println!("Message: {}", common.message);
    print_node_operation_stats(&common.stats);
    print_common_node_results(&common.node_results);
    ensure_success(common.status, "switch cluster_state_set failed")
}

async fn app_status(client: &mut Client, targets_csv: &Path) -> ClientResult<()> {
    let targets = parse_switch_targets_csv(targets_csv)?;
    let nodes = targets.iter().cloned().map(switch_target_to_node).collect();

    let response = client
        .batch_get_scale_up_fabric_service_status(rm::BatchGetScaleUpFabricServiceStatusRequest {
            nodes: Some(rm::NodeSet { nodes }),
        })
        .await?
        .into_inner();

    print_status(response.status);
    for target in &targets {
        println!("Service status:");
        println!("  node_id: {}", target.node_id);
        match response.service_statuses.get(&target.node_id) {
            Some(entry) => {
                println!("  status_json: {}", entry.status_json);
                println!("  error_message: {}", entry.error_message);
            }
            None => {
                println!("  status_json:");
                println!("  error_message: missing response entry");
            }
        }
    }
    ensure_success(response.status, "switch app_status failed")
}

async fn gnmi_service(client: &mut Client, target_csv: &Path, enabled: i32) -> ClientResult<()> {
    let enabled = parse_enabled_flag(enabled)?;
    let target = parse_single_switch_target_csv(target_csv)?;
    let response = client
        .set_scale_up_fabric_telemetry_interface_state(
            rm::SetScaleUpFabricTelemetryInterfaceStateRequest {
                node: Some(switch_target_to_node(target)),
                enable: enabled,
            },
        )
        .await?
        .into_inner();

    print_status(response.status);
    if !response.result_json.is_empty() {
        println!("Result: {}", response.result_json);
    }
    if !response.error_message.is_empty() {
        println!("Error: {}", response.error_message);
    }
    ensure_success(response.status, "switch gnmi_service failed")
}

async fn nmxconfigure(
    client: &mut Client,
    target_csv: &Path,
    topology_type: String,
    domain: Option<String>,
) -> ClientResult<()> {
    let target = parse_single_switch_target_csv(target_csv)?;

    let response = client
        .configure_scale_up_fabric_manager(rm::ConfigureScaleUpFabricManagerRequest {
            node: Some(switch_target_to_node(target)),
            topology_type,
            domain,
        })
        .await?
        .into_inner();

    print_status(response.status);
    println!("Configuration message: {}", response.message);
    println!("Topology used: {}", response.topology_used);

    println!(
        "ScaleUpFabricState enabled: {}",
        response.scale_up_fabric_state_enabled
    );

    println!("gRPC enabled: {}", response.grpc_enabled);
    ensure_success(response.status, "switch nmxconfigure failed")
}

async fn nvlink_configure(
    client: &mut Client,
    targets_csv: &Path,
    topology_type: String,
    primary_switch_node_id: Option<String>,
    domain: Option<String>,
) -> ClientResult<()> {
    let targets = parse_switch_targets_csv(targets_csv)?;
    let nodes = targets.into_iter().map(switch_target_to_node).collect();

    let response = client
        .rack_manager_v2
        .configure_scale_up_fabric_manager(rm_v2::ConfigureScaleUpFabricManagerRequest {
            nodes: Some(rm::NodeSet { nodes }),
            primary_switch_node_id,
            domain,
            config: Some(rm_v2::ScaleUpFabricConfig {
                topology_type,
                extra_static_configs: Vec::new(),
            }),
        })
        .await?
        .into_inner();

    println!(
        "RackManagerV2.ConfigureScaleUpFabricManager job_id: {}",
        response.job_id
    );

    println!(
        "Poll it with: rack_manager_client job_status {}",
        response.job_id
    );

    Ok(())
}

async fn fabric_status(
    client: &mut Client,
    targets_csv: &Path,
    domain: Option<String>,
) -> ClientResult<()> {
    let targets = parse_switch_targets_csv(targets_csv)?;
    let nodes = targets.into_iter().map(switch_target_to_node).collect();

    let response = client
        .get_scale_up_fabric_status(rm::GetScaleUpFabricStatusRequest {
            nodes: Some(rm::NodeSet { nodes }),
            domain,
        })
        .await?
        .into_inner();

    print_status(response.status);

    if let Some(status) = response.fabric_status {
        println!("Topology: {}", status.topology_type);

        for entry in status.extra_static_configs {
            println!(
                "Static config: {}/{} = {}",
                entry.config_file_name, entry.key, entry.value
            );
        }

        for sw in status.switches {
            let fabric_manager_status = if sw.fabric_manager_status.is_empty() {
                "-".to_string()
            } else {
                sw.fabric_manager_status
            };

            let err = if sw.error_message.is_empty() {
                String::new()
            } else {
                format!(" error={}", sw.error_message)
            };

            println!(
                "Switch {}: enabled={} fabric_manager_status={}{}",
                sw.node_id, sw.enabled, fabric_manager_status, err
            );
        }
    }

    if !response.error_message.is_empty() {
        println!("Error: {}", response.error_message);
    }

    ensure_success(response.status, "switch fabric_status failed")
}

async fn configure_switch_certificate(
    client: &mut Client,
    targets_csv: &Path,
    service_names: Vec<String>,
    domain: Option<String>,
    test_hello: bool,
) -> ClientResult<()> {
    let services = parse_switch_service_names(&service_names)?;
    let nodes = parse_switch_targets_csv(targets_csv)?
        .into_iter()
        .map(switch_target_to_node)
        .collect();
    let response = client
        .configure_switch_certificate(rm::ConfigureSwitchCertificateRequest {
            nodes: Some(rm::NodeSet { nodes }),
            services,
            test_hello,
            domain,
        })
        .await?
        .into_inner();

    let rm::ConfigureSwitchCertificateResponse { response, jobs } = response;
    let common = response.ok_or("configure_switch_certificate response missing common response")?;
    print_status(common.status);
    println!("Message: {}", common.message);
    println!("Parent Job ID: {}", common.job_id);
    print_node_operation_stats(&common.stats);
    for job in &jobs {
        println!("  Node: {} -> Job: {}", job.node_id, job.job_id);
    }
    print_common_node_results(&common.node_results);
    ensure_success(common.status, "switch configure_certificate failed")
}

async fn configure_certificate_status(client: &mut Client, job_id: String) -> ClientResult<()> {
    let response = client
        .get_configure_switch_certificate_job_status(
            rm::GetConfigureSwitchCertificateJobStatusRequest { job_id },
        )
        .await?
        .into_inner();

    print_status(response.status);
    println!("Job ID: {}", response.job_id);
    println!("State: {}", response.state);
    println!("Message: {}", response.message);
    if !response.rack_id.is_empty() {
        println!("Rack: {}", response.rack_id);
    }
    if !response.node_id.is_empty() {
        println!("Node: {}", response.node_id);
    }
    if !response.error_message.is_empty() {
        println!("Error: {}", response.error_message);
    }
    if !response.result_json.is_empty() {
        println!("Result: {}", response.result_json);
    }
    ensure_success(
        response.status,
        "switch configure_certificate_status failed",
    )?;
    if response.state == "failed" {
        let message = if response.error_message.is_empty() {
            "switch certificate job failed".to_owned()
        } else {
            format!("switch certificate job failed: {}", response.error_message)
        };
        return Err(message.into());
    }
    Ok(())
}

fn parse_switch_service_names(names: &[String]) -> ClientResult<Vec<i32>> {
    if names.is_empty() {
        return Err("at least one --service is required".into());
    }

    let mut services = Vec::with_capacity(names.len());
    for name in names {
        let service = parse_switch_service_name(name)?;
        if services.contains(&service) {
            continue;
        }
        services.push(service);
    }
    Ok(services)
}

fn parse_switch_service_name(name: &str) -> ClientResult<i32> {
    let service = match name.trim().to_ascii_lowercase().as_str() {
        "nvue-api" | "nvue_api" | "1" => rm::SwitchService::NvueApi as i32,
        "scale-up-fabric-telemetry" | "scale_up_fabric_telemetry" | "2" => {
            rm::SwitchService::ScaleUpFabricTelemetry as i32
        }
        "scale-up-fabric-manager" | "scale_up_fabric_manager" | "3" => {
            rm::SwitchService::ScaleUpFabricManager as i32
        }
        "scale-up-fabric-telemetry-interface" | "scale_up_fabric_telemetry_interface" | "4" => {
            rm::SwitchService::ScaleUpFabricTelemetryInterface as i32
        }
        _ => {
            return Err(format!(
                "invalid switch service '{name}'. Expected nvue-api, scale-up-fabric-telemetry, scale-up-fabric-manager, or scale-up-fabric-telemetry-interface"
            )
            .into());
        }
    };
    Ok(service)
}

fn parse_power_on_order(items: Vec<String>) -> ClientResult<Vec<rm::PowerOnOrderItem>> {
    let mut parsed = Vec::new();
    let mut index = 0;
    while index < items.len() {
        let node_id = items[index].clone();
        let mut timeout_seconds = 5;
        if let Some(next) = items.get(index + 1)
            && let Ok(timeout) = next.parse::<u32>()
            && timeout > 0
        {
            timeout_seconds = timeout;
            index += 1;
        }
        parsed.push(rm::PowerOnOrderItem {
            node_id,
            completion_check: Some(rm::CompletionCheck {
                enabled: true,
                timeout_seconds,
            }),
        });
        index += 1;
    }
    Ok(parsed)
}

fn parse_firmware_targets(args: &[String]) -> ClientResult<Vec<rm::FirmwareTarget>> {
    let mut targets = Vec::new();
    for arg in args {
        let Some((target, filename)) = arg.split_once(':') else {
            return Err(format!("invalid target format '{arg}', expected target:filename").into());
        };
        if target.is_empty() || filename.is_empty() {
            return Err(format!("invalid target format '{arg}', expected target:filename").into());
        }
        targets.push(rm::FirmwareTarget {
            target: target.to_owned(),
            filename: filename.to_owned(),
        });
    }
    Ok(targets)
}

fn parse_switch_targets_csv(path: &Path) -> ClientResult<Vec<SwitchTarget>> {
    let mut targets = Vec::new();
    for (line_number, fields) in csv_records(path)?.into_iter().enumerate() {
        if fields.len() < 6 {
            return Err(format!(
                "invalid CSV format on line {}. Expected node_id,rack_id,ip,port,username,password[,mac_address][,host_name][,node_type]",
                line_number + 1
            )
            .into());
        }

        let port = if fields[3].is_empty() {
            22
        } else {
            fields[3]
                .parse::<u32>()
                .map_err(|_| format!("invalid port on line {}", line_number + 1))?
        };
        let ip_address = fields[2].clone();
        let host_name = fields
            .get(7)
            .filter(|name| !name.is_empty())
            .cloned()
            .unwrap_or_else(|| ip_address.clone());
        let node_type = fields
            .get(8)
            .filter(|node_type| !node_type.is_empty())
            .map(|node_type| parse_node_type_field(node_type, &format!("line {}", line_number + 1)))
            .transpose()?
            .unwrap_or(rm::NodeType::SwitchGb200Nvidia as i32);
        if !is_switch_node_type(node_type) {
            return Err(format!(
                "invalid switch node type '{}' on line {}",
                fields.get(8).map(String::as_str).unwrap_or_default(),
                line_number + 1
            )
            .into());
        }
        targets.push(SwitchTarget {
            node_id: fields[0].clone(),
            rack_id: fields[1].clone(),
            ip_address,
            port,
            username: fields[4].clone(),
            password: fields[5].clone(),
            mac_address: fields.get(6).cloned().unwrap_or_default(),
            host_name,
            node_type,
        });
    }
    if targets.is_empty() {
        return Err(format!("no switch targets parsed from {}", path.display()).into());
    }
    Ok(targets)
}

fn parse_single_switch_target_csv(path: &Path) -> ClientResult<SwitchTarget> {
    let targets = parse_switch_targets_csv(path)?;
    if targets.len() != 1 {
        return Err(format!(
            "expected exactly one switch target in {}, found {}",
            path.display(),
            targets.len()
        )
        .into());
    }
    Ok(targets.into_iter().next().expect("one target checked"))
}

fn is_switch_node_type(node_type: i32) -> bool {
    matches!(
        rm::NodeType::try_from(node_type),
        Ok(rm::NodeType::SwitchGb200Nvidia
            | rm::NodeType::SwitchGb300Nvidia
            | rm::NodeType::SwitchVrnvl72Nvidia)
    )
}

fn looks_like_ipv4(value: &str) -> bool {
    value.parse::<std::net::Ipv4Addr>().is_ok()
}

fn is_bmc_block_start(fields: &[String], start: usize) -> bool {
    fields.get(start).is_some_and(|s| !s.is_empty())
        && fields
            .get(start + 1)
            .and_then(|s| s.parse::<u32>().ok())
            .is_some()
        && fields.get(start + 2).is_some_and(|s| !s.is_empty())
        && fields.get(start + 3).is_some()
}

fn looks_like_partial_bmc_block(fields: &[String], start: usize) -> bool {
    fields
        .get(start)
        .is_some_and(|ip| !ip.is_empty() && looks_like_ipv4(ip))
        && !is_bmc_block_start(fields, start)
}

fn parse_optional_bmc_endpoint(
    fields: &[String],
    start: usize,
    line_number: usize,
) -> ClientResult<Option<NodeListBmcEndpoint>> {
    match fields.get(start).map(|s| s.as_str()) {
        None | Some("") => Ok(None),
        Some(bmc_ip) => {
            let missing = |col: &str| {
                format!(
                    "missing {col} for BMC endpoint on line {}; all four BMC columns (bmc_ip,bmc_port,bmc_username,bmc_password) must be provided together",
                    line_number + 1
                )
            };
            let bmc_port_str = fields
                .get(start + 1)
                .filter(|s| !s.is_empty())
                .ok_or_else(|| missing("bmc_port"))?;
            let bmc_username = fields
                .get(start + 2)
                .filter(|s| !s.is_empty())
                .ok_or_else(|| missing("bmc_username"))?;
            let bmc_password = fields
                .get(start + 3)
                .ok_or_else(|| missing("bmc_password"))?;
            let bmc_port = bmc_port_str
                .parse::<u32>()
                .map_err(|_| format!("invalid bmc_port on line {}", line_number + 1))?;
            let bmc_mac = fields
                .get(start + 4)
                .map(|s| s.as_str())
                .unwrap_or("")
                .to_owned();
            Ok(Some(NodeListBmcEndpoint {
                ip_address: bmc_ip.to_owned(),
                port: bmc_port,
                username: bmc_username.clone(),
                password: bmc_password.clone(),
                mac_address: bmc_mac,
            }))
        }
    }
}

fn parse_node_list_csv(path: &Path) -> ClientResult<Vec<NodeListEntry>> {
    let mut nodes = Vec::new();
    for (line_number, fields) in csv_records(path)?.into_iter().enumerate() {
        if fields.len() < 8 {
            return Err(format!(
                "invalid CSV format on line {}. Expected node_id,rack_id,ip,port,username,password,mac,type[,endpoint_role][,host_name][,bmc_ip,bmc_port,bmc_username,bmc_password[,bmc_mac]]",
                line_number + 1
            )
            .into());
        }
        let port = fields[3]
            .parse::<u32>()
            .map_err(|_| format!("invalid port on line {}", line_number + 1))?;
        let node_type = parse_node_type_field(&fields[7], &format!("line {}", line_number + 1))?;
        let endpoint_role = match fields.get(8).map(|field| field.to_ascii_lowercase()) {
            None => None,
            Some(role) if role.is_empty() || role == "default" => None,
            Some(role) if role == "bmc" => Some(NodeListEndpointRole::Bmc),
            Some(role) if role == "host" => Some(NodeListEndpointRole::Host),
            Some(role) => {
                return Err(format!(
                    "invalid endpoint role '{role}' on line {}. Expected bmc, host, or default",
                    line_number + 1
                )
                .into());
            }
        };

        let mut tail_start = 9;
        let mut host_name = String::new();
        if is_switch_node_type(node_type) {
            if looks_like_partial_bmc_block(&fields, tail_start) {
                return Err(format!(
                    "missing bmc_port for BMC endpoint on line {}; all four BMC columns (bmc_ip,bmc_port,bmc_username,bmc_password) must be provided together",
                    line_number + 1
                )
                .into());
            }
            if is_bmc_block_start(&fields, tail_start) {
                // Legacy layout: BMC columns immediately follow endpoint_role.
            } else if fields.get(tail_start).is_some_and(|name| !name.is_empty()) {
                host_name = fields[tail_start].clone();
                tail_start += 1;
            }
        }

        let bmc_endpoint = parse_optional_bmc_endpoint(&fields, tail_start, line_number)?;

        nodes.push(NodeListEntry {
            node_id: fields[0].clone(),
            rack_id: fields[1].clone(),
            ip_address: fields[2].clone(),
            port,
            username: fields[4].clone(),
            password: fields[5].clone(),
            mac_address: fields[6].clone(),
            node_type,
            endpoint_role,
            host_name,
            bmc_endpoint,
        });
    }
    if nodes.is_empty() {
        return Err(format!("no nodes parsed from {}", path.display()).into());
    }
    Ok(nodes)
}

fn parse_targets_by_type_csv(path: &Path) -> ClientResult<HashMap<i32, rm::FirmwareTargetList>> {
    let mut targets_by_type: HashMap<i32, Vec<rm::FirmwareTarget>> = HashMap::new();
    for (line_number, fields) in csv_records(path)?.into_iter().enumerate() {
        if fields.len() < 3 {
            return Err(format!(
                "invalid CSV format on line {}. Expected node_type,target,filename",
                line_number + 1
            )
            .into());
        }
        let node_type = parse_node_type_field(&fields[0], &format!("line {}", line_number + 1))?;
        targets_by_type
            .entry(node_type)
            .or_default()
            .push(rm::FirmwareTarget {
                target: fields[1].clone(),
                filename: fields[2].clone(),
            });
    }
    if targets_by_type.is_empty() {
        return Err(format!("no firmware targets parsed from {}", path.display()).into());
    }

    let targets = targets_by_type
        .into_iter()
        .map(|(node_type, targets)| (node_type, rm::FirmwareTargetList { targets }))
        .collect();
    Ok(targets)
}

fn parse_component_filters(
    component_args: &[String],
    target_args: &[String],
) -> ClientResult<HashMap<i32, rm::FirmwareObjectComponentFilter>> {
    let mut component_filters: HashMap<i32, Vec<String>> = HashMap::new();
    let mut wildcards: HashMap<i32, bool> = HashMap::new();
    for arg in component_args {
        let Some((node_type, component)) = arg.split_once(':') else {
            return Err(
                format!("invalid component filter '{arg}', expected NODE_TYPE:COMPONENT").into(),
            );
        };
        if component.is_empty() {
            return Err(format!(
                "invalid component filter '{arg}', component must not be empty; use '*' for all components"
            )
            .into());
        }
        let node_type = node_type
            .parse::<i32>()
            .map_err(|_| format!("invalid node type in component filter '{arg}'"))?;
        rm::NodeType::try_from(node_type)
            .map_err(|_| format!("invalid node type in component filter '{arg}'"))?;
        let filter = component_filters.entry(node_type).or_default();
        if component == "*" {
            if !filter.is_empty() {
                return Err(format!(
                    "invalid component filter '{arg}', wildcard cannot be combined with explicit components for node type {node_type}"
                )
                .into());
            }
            wildcards.insert(node_type, true);
        } else {
            if wildcards.get(&node_type).copied().unwrap_or(false) {
                return Err(format!(
                    "invalid component filter '{arg}', explicit components cannot be combined with wildcard for node type {node_type}"
                )
                .into());
            }
            filter.push(component.to_owned());
        }
    }

    let mut target_filters: HashMap<i32, Vec<String>> = HashMap::new();
    let mut target_wildcards: HashMap<i32, bool> = HashMap::new();
    for arg in target_args {
        let Some((node_type, target)) = arg.split_once(':') else {
            return Err(format!("invalid target filter '{arg}', expected NODE_TYPE:TARGET").into());
        };
        if target.is_empty() {
            return Err(format!(
                "invalid target filter '{arg}', target must not be empty; use '*' for all targets"
            )
            .into());
        }
        let node_type = node_type
            .parse::<i32>()
            .map_err(|_| format!("invalid node type in target filter '{arg}'"))?;
        rm::NodeType::try_from(node_type)
            .map_err(|_| format!("invalid node type in target filter '{arg}'"))?;
        if component_filters.contains_key(&node_type) {
            return Err(format!(
                "target filter '{arg}' cannot be combined with component filters for node type {node_type}"
            )
            .into());
        }
        let filter = target_filters.entry(node_type).or_default();
        if target == "*" {
            if !filter.is_empty() {
                return Err(format!(
                    "invalid target filter '{arg}', wildcard cannot be combined with explicit targets for node type {node_type}"
                )
                .into());
            }
            target_wildcards.insert(node_type, true);
        } else {
            if target_wildcards.get(&node_type).copied().unwrap_or(false) {
                return Err(format!(
                    "invalid target filter '{arg}', explicit targets cannot be combined with wildcard for node type {node_type}"
                )
                .into());
            }
            filter.push(parse_firmware_object_component_target(node_type, target)?);
        }
    }

    let mut filters = HashMap::new();
    filters.extend(
        component_filters
            .into_iter()
            .map(|(node_type, components)| {
                (node_type, rm::FirmwareObjectComponentFilter { components })
            }),
    );
    filters.extend(target_filters.into_iter().map(|(node_type, components)| {
        (node_type, rm::FirmwareObjectComponentFilter { components })
    }));
    Ok(filters)
}

fn parse_firmware_object_component_target(node_type: i32, value: &str) -> ClientResult<String> {
    let normalized = value.trim().replace('-', "_").to_ascii_uppercase();
    let node_type = rm::NodeType::try_from(node_type)
        .map_err(|_| format!("invalid node type in target filter '{node_type}:{value}'"))?;
    let target = match node_type {
        rm::NodeType::ComputeGb200Nvidia | rm::NodeType::ComputeGb300Nvidia => {
            match normalized.as_str() {
                "BMC" => "BMC",
                "HMC" => "HMC",
                "CPU_0" => "CPU_0",
                "CPU_1" => "CPU_1",
                "GPU_0" => "GPU_0",
                "GPU_1" => "GPU_1",
                "GPU_2" => "GPU_2",
                "GPU_3" => "GPU_3",
                "HGX_BMC_0" => "HGX_BMC_0",
                "FPGA_0" => "FPGA_0",
                "FPGA_1" => "FPGA_1",
                "CPLD_0" => "CPLD_0",
                "EROT_BMC_0" => "EROT_BMC_0",
                "EROT_CPU_0" => "EROT_CPU_0",
                "EROT_CPU_1" => "EROT_CPU_1",
                "EROT_FPGA_0" => "EROT_FPGA_0",
                "EROT_FPGA_1" => "EROT_FPGA_1",
                "INFOROM_GPU_0" => "INFOROM_GPU_0",
                "INFOROM_GPU_1" => "INFOROM_GPU_1",
                "INFOROM_GPU_2" => "INFOROM_GPU_2",
                "INFOROM_GPU_3" => "INFOROM_GPU_3",
                "PCIE_SWITCH_CONFIG_0" => "PCIE_SWITCH_CONFIG_0",
                _ => {
                    return Err(format!(
                        "unknown firmware object target '{value}' for node type {node_type:?}"
                    )
                    .into());
                }
            }
        }
        rm::NodeType::ComputeVrnvl72Nvidia => match normalized.as_str() {
            "BMC" => "BMC",
            "HMC" => "HMC",
            _ => {
                return Err(format!(
                    "unknown firmware object target '{value}' for node type {node_type:?}"
                )
                .into());
            }
        },
        rm::NodeType::ComputeGb300Lenovo => match normalized.as_str() {
            "BMC" => "BMC",
            "HMC" => "HMC",
            _ => {
                return Err(format!(
                    "unknown firmware object target '{value}' for node type {node_type:?}"
                )
                .into());
            }
        },
        rm::NodeType::SwitchGb200Nvidia | rm::NodeType::SwitchGb300Nvidia => {
            match normalized.as_str() {
                "BMC" => "BMC",
                "FPGA" => "FPGA",
                "EROT" => "EROT",
                "CPLD" => "CPLD",
                "BIOS" => "BIOS",
                _ => {
                    return Err(format!(
                        "unknown firmware object target '{value}' for node type {node_type:?}"
                    )
                    .into());
                }
            }
        }
        rm::NodeType::PowershelfGb200Delta | rm::NodeType::PowershelfGb300Delta => {
            match normalized.as_str() {
                "DELTA_PMC" | "DELTAPMC" => "DeltaPMC",
                "DELTA_PSU" | "DELTAPSU" => "DeltaPSU",
                _ => {
                    return Err(format!(
                        "unknown firmware object target '{value}' for node type {node_type:?}"
                    )
                    .into());
                }
            }
        }
        rm::NodeType::PowershelfGb200Liteon | rm::NodeType::PowershelfGb300Liteon => {
            match normalized.as_str() {
                "LITEON_PMC" | "LITEONPMC" => "LiteOnPMC",
                "LITEON_PSU" | "LITEONPSU" => "LiteOnPSU",
                _ => {
                    return Err(format!(
                        "unknown firmware object target '{value}' for node type {node_type:?}"
                    )
                    .into());
                }
            }
        }
        rm::NodeType::SwitchVrnvl72Nvidia => match normalized.as_str() {
            "BMC" => "BMC",
            "BIOS" | "SBIOS" => "BIOS",
            "SMA" => "SMA",
            "EROT" => "EROT",
            "CPLD" | "CPLD1" | "CPLD_1" => "CPLD",
            _ => {
                return Err(format!(
                    "unknown firmware object target '{value}' for node type {node_type:?}"
                )
                .into());
            }
        },
        rm::NodeType::Unspecified => {
            return Err(format!("invalid node type in target filter '{value}'").into());
        }
    };

    Ok(target.to_owned())
}

fn csv_records(path: &Path) -> ClientResult<Vec<Vec<String>>> {
    let content = std::fs::read_to_string(path)?;
    Ok(content
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .map(|line| {
            line.split(',')
                .map(|field| field.trim().to_owned())
                .collect()
        })
        .collect())
}

fn switch_target_to_node(target: SwitchTarget) -> rm::NodeInfo {
    rm::NodeInfo {
        node_id: target.node_id,
        rack_id: target.rack_id,
        r#type: Some(target.node_type),
        bmc_endpoint: None,
        host_endpoint: Some(rm::Endpoint {
            interface: Some(rm::NetworkInterface {
                ip_address: target.ip_address,
                mac_address: target.mac_address,
                host_name: Some(target.host_name),
            }),
            port: target.port,
            credentials: Some(user_pass(target.username, target.password)),
        }),
        node_descriptor: None,
    }
}

fn node_list_entry_to_proto(node: NodeListEntry, mode: NodeListEndpointMode) -> rm::NodeInfo {
    let switch_host_name = if node.host_name.is_empty() {
        node.ip_address.clone()
    } else {
        node.host_name.clone()
    };
    let endpoint = rm::NetworkInterface {
        ip_address: node.ip_address.clone(),
        mac_address: node.mac_address.clone(),
        host_name: None,
    };
    let endpoint_proto = rm::Endpoint {
        interface: Some(endpoint),
        port: node.port,
        credentials: Some(user_pass(node.username.clone(), node.password.clone())),
    };
    if is_switch_node_type(node.node_type) {
        let host_endpoint_proto = rm::Endpoint {
            interface: Some(rm::NetworkInterface {
                ip_address: node.ip_address.clone(),
                mac_address: node.mac_address.clone(),
                host_name: Some(switch_host_name),
            }),
            port: node.port,
            credentials: Some(user_pass(node.username.clone(), node.password.clone())),
        };
        // When explicit BMC columns are present, always populate both endpoints:
        // main fields → host_endpoint, BMC columns → bmc_endpoint.
        // endpoint_role and mode are ignored in this case.
        if let Some(bmc) = node.bmc_endpoint {
            let bmc_endpoint_proto = rm::Endpoint {
                interface: Some(rm::NetworkInterface {
                    ip_address: bmc.ip_address,
                    mac_address: bmc.mac_address,
                    host_name: None,
                }),
                port: bmc.port,
                credentials: Some(user_pass(bmc.username, bmc.password)),
            };
            return rm::NodeInfo {
                node_id: node.node_id,
                rack_id: node.rack_id,
                r#type: Some(node.node_type),
                bmc_endpoint: Some(bmc_endpoint_proto),
                host_endpoint: Some(host_endpoint_proto),
                node_descriptor: None,
            };
        }

        let (bmc_endpoint, host_endpoint) = match (mode, node.endpoint_role) {
            (_, Some(NodeListEndpointRole::Bmc)) => (Some(endpoint_proto), None),
            (_, Some(NodeListEndpointRole::Host)) => (None, Some(host_endpoint_proto)),
            (NodeListEndpointMode::InventoryRegistration, None) => (
                Some(endpoint_proto.clone()),
                Some(host_endpoint_proto.clone()),
            ),
            (NodeListEndpointMode::DirectOperation, None) => (None, Some(host_endpoint_proto)),
        };

        rm::NodeInfo {
            node_id: node.node_id,
            rack_id: node.rack_id,
            r#type: Some(node.node_type),
            bmc_endpoint,
            host_endpoint,
            node_descriptor: None,
        }
    } else {
        let (bmc_endpoint, host_endpoint) = match node.endpoint_role {
            Some(NodeListEndpointRole::Host) => (None, Some(endpoint_proto)),
            Some(NodeListEndpointRole::Bmc) | None => (Some(endpoint_proto), None),
        };

        rm::NodeInfo {
            node_id: node.node_id,
            rack_id: node.rack_id,
            r#type: Some(node.node_type),
            bmc_endpoint,
            host_endpoint,
            node_descriptor: None,
        }
    }
}

fn user_pass(username: String, password: String) -> rm::Credentials {
    rm::Credentials {
        auth: Some(rm::credentials::Auth::UserPass(rm::UsernamePassword {
            username,
            password,
        })),
    }
}

fn parse_enabled_flag(enabled: i32) -> ClientResult<bool> {
    match enabled {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err("enabled must be 1 or 0".into()),
    }
}

fn validate_power_operation(operation: i32) -> ClientResult<()> {
    if matches!(
        rm::PowerOperation::try_from(operation),
        Ok(rm::PowerOperation::Off
            | rm::PowerOperation::On
            | rm::PowerOperation::Reset
            | rm::PowerOperation::ForceOff
            | rm::PowerOperation::ForceOn
            | rm::PowerOperation::GracefulShutdown
            | rm::PowerOperation::GracefulRestart
            | rm::PowerOperation::ForceRestart)
    ) {
        Ok(())
    } else {
        Err("operation must be 1 (Off), 2 (On), 3 (Reset), 4 (ForceOff), 5 (ForceOn), 6 (GracefulShutdown), 7 (GracefulRestart), or 8 (ForceRestart)".into())
    }
}

fn parse_node_type_field(value: &str, context: &str) -> ClientResult<i32> {
    let value = value.trim();
    if value.is_empty() {
        return Err(format!("{context}: node type is required").into());
    }

    if let Ok(node_type) = value.parse::<i32>() {
        rm::NodeType::try_from(node_type).map_err(|_| {
            format!("{context}: invalid node type '{value}'; use a NodeType number or enum name")
        })?;
        return Ok(node_type);
    }

    if let Some(node_type) = rm::NodeType::from_str_name(&value.to_ascii_uppercase()) {
        return Ok(node_type as i32);
    }

    let normalized = value.to_ascii_lowercase().replace(['-', ' '], "_");
    for node_type in 1i32..=rm::NodeType::SwitchVrnvl72Nvidia as i32 {
        let Ok(variant) = rm::NodeType::try_from(node_type) else {
            continue;
        };
        if normalized == format!("{variant:?}").to_ascii_lowercase() {
            return Ok(node_type);
        }
    }

    Err(format!(
        "{context}: invalid node type '{value}'; use a NodeType number \
         (1=ComputeGb200Nvidia, 3=SwitchGb200Nvidia, ...) or enum name"
    )
    .into())
}

fn validate_component_type(component_type: i32) -> ClientResult<()> {
    if matches!(
        rm::SwitchFirmwareComponentType::try_from(component_type),
        Ok(rm::SwitchFirmwareComponentType::Bmc
            | rm::SwitchFirmwareComponentType::Fpga
            | rm::SwitchFirmwareComponentType::Erot
            | rm::SwitchFirmwareComponentType::Cpld
            | rm::SwitchFirmwareComponentType::Bios
            | rm::SwitchFirmwareComponentType::Transceiver)
    ) {
        Ok(())
    } else {
        Err("component_type must be 2 (BMC), 3 (FPGA), 4 (EROT), 5 (CPLD), 6 (BIOS), or 7 (TRANSCEIVER)".into())
    }
}

fn print_status(status: i32) {
    println!("Response status: {} ({})", status, return_code_name(status));
}

fn ensure_success(status: i32, message: &str) -> ClientResult<()> {
    if status == rm::ReturnCode::Success as i32 {
        Ok(())
    } else {
        Err(message.into())
    }
}

fn print_firmware_node_jobs(jobs: &[rm::NodeFirmwareJobInfo]) {
    if jobs.is_empty() {
        return;
    }
    println!("Node jobs:");
    for job in jobs {
        println!("  {}: {}", job.node_id, job.job_id);
    }
}

fn print_node_operation_stats(stats: &Option<rm::NodeOperationStats>) {
    let Some(stats) = stats.as_ref() else {
        return;
    };

    println!(
        "Total: {}, Succeeded: {}, Failed: {}",
        stats.total_nodes, stats.successful_nodes, stats.failed_nodes
    );
}

fn print_common_node_results(results: &[rm::NodeOperationResult]) {
    if results.is_empty() {
        return;
    }
    println!("Node results:");
    for result in results {
        print!("  {}: {}", result.node_id, return_code_name(result.status));
        if !result.error_message.is_empty() {
            print!(" - {}", result.error_message);
        }
        println!();
    }
}

fn print_node_power_states(states: &[rm::NodePowerState]) {
    if states.is_empty() {
        return;
    }
    println!("Node power states:");
    for state in states {
        println!("  {}: {}", state.node_id, state.pstate);
    }
}

fn optional_u32_display(value: Option<u32>) -> String {
    value.map(|v| v.to_string()).unwrap_or_else(|| "-".into())
}

fn optional_i64_display(value: Option<i64>) -> String {
    value.map(|v| v.to_string()).unwrap_or_else(|| "-".into())
}

fn print_node_device_info(info: &rm::NodeDeviceInfo) {
    println!("Node: {}", info.node_id);
    println!("  Chassis SN: {}", optional_i64_display(info.chassis_sn));
    println!(
        "  Tray slot number: {}",
        optional_u32_display(info.slot_number)
    );
    println!("  Tray index: {}", optional_u32_display(info.tray_index));
}

fn print_node_device_info_table(details: &[rm::NodeDeviceInfo]) {
    if details.is_empty() {
        println!("No device info returned.");
        return;
    }

    let rows = details
        .iter()
        .map(|info| {
            vec![
                info.node_id.clone(),
                optional_i64_display(info.chassis_sn),
                optional_u32_display(info.slot_number),
                optional_u32_display(info.tray_index),
            ]
        })
        .collect::<Vec<_>>();
    print_table(&["Node", "Chassis SN", "Slot", "Tray Index"], &rows);
}

fn print_firmware_inventory(firmware_list: &[rm::FirmwareInventoryInfo]) {
    if firmware_list.is_empty() {
        println!("  No firmware inventory returned.");
        return;
    }

    println!("Firmware inventory:");
    for firmware in firmware_list {
        println!(
            "  {}: version={} target={} updateable={} health={} type={}",
            firmware.name,
            firmware.version,
            firmware.target,
            firmware.updateable,
            firmware.health,
            firmware.firmware_type
        );
    }
}

fn print_node_inventory(node: &rm::NodeInventoryInfo) {
    println!("\nNode: {}", node.node_id);
    println!("  Rack: {}", display_value(&node.rack_id));
    println!("  Type: {}", node_type_name(node.r#type));
    println!("  Product: {}", display_value(&node.product));
    println!("  Health: {}", display_value(&node.health));
    println!("  BMC: {}:{}", display_value(&node.ip_address), node.port);
    println!("  MAC: {}", display_value(&node.mac_address));
    if !node.host_ip_addresses.is_empty() {
        println!("  Host IPs: {}", node.host_ip_addresses.join(", "));
    }
    if !node.host_mac_addresses.is_empty() {
        println!("  Host MACs: {}", node.host_mac_addresses.join(", "));
    }

    if node.components.is_empty() {
        println!("  Components: none");
        return;
    }

    println!("  Components:");
    let rows = node
        .components
        .iter()
        .map(|component| {
            vec![
                display_string(&component.component_id),
                component.r#type.to_string(),
                display_string(&component.hardware_type),
                display_string(&component.model),
                display_string(&component.serial_number),
                display_string(&component.state),
                display_string(&component.health),
            ]
        })
        .collect::<Vec<_>>();
    print_indented_table(
        &[
            "Component",
            "Type",
            "Hardware",
            "Model",
            "Serial",
            "State",
            "Health",
        ],
        &rows,
        "  ",
    );
}

fn node_type_name(node_type: i32) -> String {
    match rm::NodeType::try_from(node_type) {
        Ok(node_type) => format!("{node_type:?}"),
        Err(_) => format!("UNKNOWN ({node_type})"),
    }
}

fn print_firmware_object_list(objects: &[rm::FirmwareObject]) {
    if objects.is_empty() {
        println!("No firmware objects found.");
        return;
    }

    let rows = objects
        .iter()
        .map(|object| {
            vec![
                object.id.clone(),
                display_string(&object.hardware_type),
                if object.is_default {
                    "*".to_owned()
                } else {
                    String::new()
                },
                object.available.to_string(),
                timestamp_display(object.created.as_ref()),
                timestamp_display(object.updated.as_ref()),
            ]
        })
        .collect::<Vec<_>>();
    print_table(
        &[
            "ID",
            "Hardware Type",
            "Default",
            "Available",
            "Created",
            "Updated",
        ],
        &rows,
    );
}

fn print_firmware_object_detail(object: &rm::FirmwareObject) {
    let rows = vec![
        vec!["ID".to_owned(), object.id.clone()],
        vec![
            "Hardware Type".to_owned(),
            display_string(&object.hardware_type),
        ],
        vec!["Default".to_owned(), object.is_default.to_string()],
        vec!["Available".to_owned(), object.available.to_string()],
        vec![
            "Created".to_owned(),
            timestamp_display(object.created.as_ref()),
        ],
        vec![
            "Updated".to_owned(),
            timestamp_display(object.updated.as_ref()),
        ],
    ];
    print_table(&["Field", "Value"], &rows);

    let Some(metadata) = object.metadata.as_ref() else {
        println!("\nFirmware Components: (not yet downloaded)");
        return;
    };

    let has_components = !metadata.device_components.is_empty();
    let has_images = !metadata.switch_system_images.is_empty();
    if !has_components && !has_images {
        println!("\nFirmware Components: (not yet downloaded)");
        return;
    }

    print_firmware_object_components(metadata);
}

fn print_firmware_object_components(metadata: &rm::FirmwareObjectMetadata) {
    for device in &metadata.device_components {
        println!("\n[{}]", display_value(&device.device_type));
        let rows = device
            .components
            .iter()
            .flat_map(|component| {
                if component.artifacts.is_empty() {
                    return vec![vec![
                        display_string(&component.name),
                        "-".to_owned(),
                        display_string(&component.bundle),
                        "-".to_owned(),
                        display_string(&component.version),
                    ]];
                }

                component
                    .artifacts
                    .iter()
                    .map(|artifact| {
                        vec![
                            display_string(&component.name),
                            display_string(&artifact.firmware_type.to_uppercase()),
                            display_string(&artifact.bundle),
                            display_string(&artifact.target),
                            display_string(&component.version),
                        ]
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        print_table(&["Component", "Type", "Bundle", "Target", "Version"], &rows);

        for component in &device.components {
            if component.subcomponents.is_empty() {
                continue;
            }
            println!("\n  {} Subcomponents:", display_value(&component.name));
            let rows = component
                .subcomponents
                .iter()
                .map(|subcomponent| {
                    vec![
                        display_string(&subcomponent.name),
                        display_string(&subcomponent.version),
                        display_string(&subcomponent.sku_id),
                    ]
                })
                .collect::<Vec<_>>();
            print_indented_table(&["Component", "Version", "SKUID"], &rows, "  ");
        }
    }

    if !metadata.switch_system_images.is_empty() {
        println!("\n[Switch System Images]");
        let rows = metadata
            .switch_system_images
            .iter()
            .map(|image| {
                vec![
                    display_string(&image.software_type.to_uppercase()),
                    display_string(&image.package_name),
                    display_string(&image.image_filename),
                    display_string(&image.version),
                    image.required.to_string(),
                ]
            })
            .collect::<Vec<_>>();
        print_table(
            &["Type", "Package", "Image Filename", "Version", "Required"],
            &rows,
        );
    }
}

fn print_table(headers: &[&str], rows: &[Vec<String>]) {
    let lines = table_lines(headers, rows);
    for line in lines {
        println!("{line}");
    }
}

fn print_indented_table(headers: &[&str], rows: &[Vec<String>], indent: &str) {
    let lines = table_lines(headers, rows);
    for line in lines {
        println!("{indent}{line}");
    }
}

fn table_lines(headers: &[&str], rows: &[Vec<String>]) -> Vec<String> {
    let mut widths = headers
        .iter()
        .map(|header| header.len())
        .collect::<Vec<_>>();
    for row in rows {
        for (idx, value) in row.iter().enumerate() {
            if let Some(width) = widths.get_mut(idx) {
                *width = (*width).max(value.len());
            }
        }
    }

    let border = format!(
        "+{}+",
        widths
            .iter()
            .map(|width| "-".repeat(width + 2))
            .collect::<Vec<_>>()
            .join("+")
    );
    let mut lines = Vec::with_capacity(rows.len() + 4);
    lines.push(border.clone());
    lines.push(table_row(
        &headers
            .iter()
            .map(|header| (*header).to_owned())
            .collect::<Vec<_>>(),
        &widths,
    ));
    lines.push(border.clone());
    for row in rows {
        lines.push(table_row(row, &widths));
    }
    lines.push(border);
    lines
}

fn table_row(values: &[String], widths: &[usize]) -> String {
    let cells = values
        .iter()
        .enumerate()
        .map(|(idx, value)| format!(" {:width$} ", value, width = widths[idx]))
        .collect::<Vec<_>>()
        .join("|");
    format!("|{cells}|")
}

fn display_string(value: &str) -> String {
    display_value(value).to_owned()
}

fn display_value(value: &str) -> &str {
    if value.is_empty() { "-" } else { value }
}

fn timestamp_display(timestamp: Option<&prost_types::Timestamp>) -> String {
    let Some(timestamp) = timestamp else {
        return "-".to_owned();
    };
    let nanos = match u32::try_from(timestamp.nanos) {
        Ok(nanos) => nanos,
        Err(_) => return format!("{}.{:09}", timestamp.seconds, timestamp.nanos),
    };
    chrono::DateTime::<chrono::Utc>::from_timestamp(timestamp.seconds, nanos)
        .map(|datetime| datetime.to_rfc3339())
        .unwrap_or_else(|| format!("{}.{nanos:09}", timestamp.seconds))
}

fn firmware_object_json(firmware: &rm::FirmwareObject) -> serde_json::Value {
    serde_json::json!({
        "object_id": firmware.id,
        "available": firmware.available,
        "created": timestamp_json(firmware.created.as_ref()),
        "updated": timestamp_json(firmware.updated.as_ref()),
        "hardware_type": firmware.hardware_type,
        "is_default": firmware.is_default,
        "metadata": firmware.metadata.as_ref().map(firmware_object_metadata_json),
    })
}

fn firmware_object_metadata_json(metadata: &rm::FirmwareObjectMetadata) -> serde_json::Value {
    let device_components: Vec<_> = metadata
        .device_components
        .iter()
        .map(|device| {
            serde_json::json!({
                "node_type": device.node_type,
                "device_type": device.device_type,
                "components": device.components.iter().map(|component| {
                    serde_json::json!({
                        "name": component.name,
                        "version": component.version,
                        "bundle": component.bundle,
                        "source_component": component.source_component,
                        "subcomponents": component.subcomponents.iter().map(|subcomponent| {
                            serde_json::json!({
                                "name": subcomponent.name,
                                "version": subcomponent.version,
                                "sku_id": subcomponent.sku_id,
                            })
                        }).collect::<Vec<_>>(),
                        "artifacts": component.artifacts.iter().map(|artifact| {
                            serde_json::json!({
                                "firmware_type": artifact.firmware_type,
                                "filename": artifact.filename,
                                "target": artifact.target,
                                "bundle": artifact.bundle,
                            })
                        }).collect::<Vec<_>>(),
                    })
                }).collect::<Vec<_>>(),
            })
        })
        .collect();
    let switch_system_images: Vec<_> = metadata
        .switch_system_images
        .iter()
        .map(|image| {
            serde_json::json!({
                "software_type": image.software_type,
                "version": image.version,
                "package_name": image.package_name,
                "image_filename": image.image_filename,
                "required": image.required,
            })
        })
        .collect();
    serde_json::json!({
        "device_components": device_components,
        "switch_system_images": switch_system_images,
    })
}

fn firmware_object_history_json(record: &rm::FirmwareObjectHistoryRecord) -> serde_json::Value {
    serde_json::json!({
        "object_id": record.object_id,
        "rack_id": record.rack_id,
        "firmware_type": record.firmware_type,
        "applied_at": timestamp_json(record.applied_at.as_ref()),
        "firmware_available": record.firmware_available,
        "hardware_type": record.hardware_type,
        "node_ids": record.node_ids,
    })
}

fn timestamp_json(timestamp: Option<&prost_types::Timestamp>) -> serde_json::Value {
    match timestamp {
        Some(timestamp) => serde_json::json!({
            "seconds": timestamp.seconds,
            "nanos": timestamp.nanos,
        }),
        None => serde_json::Value::Null,
    }
}

fn return_code_name(status: i32) -> &'static str {
    match rm::ReturnCode::try_from(status) {
        Ok(rm::ReturnCode::Unspecified) => "RETURN_CODE_UNSPECIFIED",
        Ok(rm::ReturnCode::Success) => "RETURN_CODE_SUCCESS",
        Ok(rm::ReturnCode::Failure) => "RETURN_CODE_FAILURE",
        Err(_) => "UNKNOWN",
    }
}

fn firmware_job_state_name(state: i32) -> &'static str {
    match rm::FirmwareJobState::try_from(state) {
        Ok(rm::FirmwareJobState::Unspecified) => "FIRMWARE_JOB_STATE_UNSPECIFIED",
        Ok(rm::FirmwareJobState::Queued) => "FIRMWARE_JOB_STATE_QUEUED",
        Ok(rm::FirmwareJobState::Running) => "FIRMWARE_JOB_STATE_RUNNING",
        Ok(rm::FirmwareJobState::Completed) => "FIRMWARE_JOB_STATE_COMPLETED",
        Ok(rm::FirmwareJobState::Failed) => "FIRMWARE_JOB_STATE_FAILED",
        Err(_) => "UNKNOWN",
    }
}

fn job_execution_state_name(state: i32) -> &'static str {
    rm::JobExecutionState::try_from(state).map_or("UNKNOWN", |state| state.as_str_name())
}

fn job_error_name(error: i32) -> &'static str {
    rm::JobError::try_from(error).map_or("UNKNOWN", |error| error.as_str_name())
}

fn firmware_error_name(error: i32) -> &'static str {
    match rm::FirmwareUpdateError::try_from(error) {
        Ok(rm::FirmwareUpdateError::Unspecified) => "FIRMWARE_UPDATE_ERROR_UNSPECIFIED",
        Ok(rm::FirmwareUpdateError::Success) => "FIRMWARE_UPDATE_ERROR_SUCCESS",
        Ok(rm::FirmwareUpdateError::FileNotFound) => "FIRMWARE_UPDATE_ERROR_FILE_NOT_FOUND",
        Ok(rm::FirmwareUpdateError::ClientFailure) => "FIRMWARE_UPDATE_ERROR_CLIENT_FAILURE",
        Ok(rm::FirmwareUpdateError::TargetNotFound) => "FIRMWARE_UPDATE_ERROR_TARGET_NOT_FOUND",
        Ok(rm::FirmwareUpdateError::ServerError) => "FIRMWARE_UPDATE_ERROR_SERVER_ERROR",
        Ok(rm::FirmwareUpdateError::TaskFailed) => "FIRMWARE_UPDATE_ERROR_TASK_FAILED",
        Ok(rm::FirmwareUpdateError::NoTaskInfo) => "FIRMWARE_UPDATE_ERROR_NO_TASK_INFO",
        Ok(rm::FirmwareUpdateError::Exception) => "FIRMWARE_UPDATE_ERROR_EXCEPTION",
        Ok(rm::FirmwareUpdateError::InvalidResponse) => "FIRMWARE_UPDATE_ERROR_INVALID_RESPONSE",
        Ok(rm::FirmwareUpdateError::MonitoringTimeout) => {
            "FIRMWARE_UPDATE_ERROR_MONITORING_TIMEOUT"
        }
        Ok(rm::FirmwareUpdateError::TaskException) => "FIRMWARE_UPDATE_ERROR_TASK_EXCEPTION",
        Ok(rm::FirmwareUpdateError::Unknown) => "FIRMWARE_UPDATE_ERROR_UNKNOWN",
        Ok(rm::FirmwareUpdateError::UpdateInProgress) => "FIRMWARE_UPDATE_ERROR_UPDATE_IN_PROGRESS",
        Err(_) => "UNKNOWN",
    }
}

#[tokio::main]
async fn main() {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

    let cli = Cli::parse();
    if let Err(e) = run(cli).await {
        eprintln!("rack_manager_client: {e}");
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn switch_node_entry() -> NodeListEntry {
        NodeListEntry {
            node_id: "sw-01".to_owned(),
            rack_id: "rack-01".to_owned(),
            ip_address: "10.0.1.1".to_owned(),
            port: 8443,
            username: "admin".to_owned(),
            password: "secret".to_owned(),
            mac_address: "aa:bb:cc:dd:ee:ff".to_owned(),
            node_type: rm::NodeType::SwitchGb200Nvidia as i32,
            endpoint_role: None,
            host_name: String::new(),
            bmc_endpoint: None,
        }
    }

    #[test]
    fn inventory_registration_switch_populates_bmc_and_host_endpoints() {
        let node = node_list_entry_to_proto(
            switch_node_entry(),
            NodeListEndpointMode::InventoryRegistration,
        );

        let Some(bmc) = node.bmc_endpoint.as_ref() else {
            panic!("expected BMC endpoint");
        };
        let Some(host) = node.host_endpoint.as_ref() else {
            panic!("expected host endpoint");
        };
        let Some(bmc_interface) = bmc.interface.as_ref() else {
            panic!("expected BMC interface");
        };
        let Some(host_interface) = host.interface.as_ref() else {
            panic!("expected host interface");
        };

        assert_eq!(bmc_interface.ip_address, "10.0.1.1");
        assert_eq!(host_interface.ip_address, "10.0.1.1");
        assert_eq!(bmc.port, 8443);
        assert_eq!(host.port, 8443);
        assert_eq!(bmc.credentials.as_ref(), host.credentials.as_ref());
    }

    #[test]
    fn direct_operation_switch_uses_host_endpoint_only() {
        let node =
            node_list_entry_to_proto(switch_node_entry(), NodeListEndpointMode::DirectOperation);

        assert!(node.bmc_endpoint.is_none());
        assert!(node.host_endpoint.is_some());
    }

    #[test]
    fn direct_operation_switch_bmc_role_uses_bmc_endpoint_only() {
        let mut entry = switch_node_entry();
        entry.endpoint_role = Some(NodeListEndpointRole::Bmc);

        let node = node_list_entry_to_proto(entry, NodeListEndpointMode::DirectOperation);

        assert!(node.bmc_endpoint.is_some());
        assert!(node.host_endpoint.is_none());
    }

    #[test]
    fn inventory_registration_switch_bmc_role_uses_bmc_endpoint_only() {
        let mut entry = switch_node_entry();
        entry.endpoint_role = Some(NodeListEndpointRole::Bmc);

        let node = node_list_entry_to_proto(entry, NodeListEndpointMode::InventoryRegistration);

        assert!(node.bmc_endpoint.is_some());
        assert!(node.host_endpoint.is_none());
    }

    #[test]
    fn node_type_name_renders_known_and_unknown_values() {
        assert_eq!(
            node_type_name(rm::NodeType::ComputeGb200Nvidia as i32),
            "ComputeGb200Nvidia"
        );
        assert_eq!(
            node_type_name(rm::NodeType::ComputeVrnvl72Nvidia as i32),
            "ComputeVrnvl72Nvidia"
        );
        assert_eq!(
            node_type_name(rm::NodeType::SwitchVrnvl72Nvidia as i32),
            "SwitchVrnvl72Nvidia"
        );
        assert_eq!(node_type_name(-1), "UNKNOWN (-1)");
    }

    #[test]
    fn failed_job_status_message_reports_failed_child() {
        let job_states = [
            rm::JobStatus {
                job_id: "parent-job".to_owned(),
                execution_state: rm::JobExecutionState::Running as i32,
                ..Default::default()
            },
            rm::JobStatus {
                job_id: "child-job".to_owned(),
                execution_state: rm::JobExecutionState::Failed as i32,
                error_message: "credentials rejected".to_owned(),
                ..Default::default()
            },
        ];

        assert_eq!(
            failed_job_status_message(&job_states).as_deref(),
            Some("job child-job failed: credentials rejected")
        );
    }

    #[test]
    fn device_info_display_helpers_format_compute_tray_fields() {
        assert_eq!(
            optional_i64_display(Some(1_784_124_020_070)),
            "1784124020070"
        );
        assert_eq!(optional_u32_display(Some(69)), "69");
        assert_eq!(optional_u32_display(Some(103)), "103");
        assert_eq!(optional_u32_display(None), "-");
    }

    #[test]
    fn parse_node_type_field_accepts_numeric_and_named_values() -> ClientResult<()> {
        assert_eq!(
            parse_node_type_field("1", "test")?,
            rm::NodeType::ComputeGb200Nvidia as i32
        );
        assert_eq!(
            parse_node_type_field("ComputeGb200Nvidia", "test")?,
            rm::NodeType::ComputeGb200Nvidia as i32
        );
        assert_eq!(
            parse_node_type_field("COMPUTE_GB200_NVIDIA", "test")?,
            rm::NodeType::ComputeGb200Nvidia as i32
        );
        assert_eq!(
            parse_node_type_field("10", "test")?,
            rm::NodeType::ComputeVrnvl72Nvidia as i32
        );
        assert_eq!(
            parse_node_type_field("ComputeVrnvl72Nvidia", "test")?,
            rm::NodeType::ComputeVrnvl72Nvidia as i32
        );
        assert_eq!(
            parse_node_type_field("SwitchVrnvl72Nvidia", "test")?,
            rm::NodeType::SwitchVrnvl72Nvidia as i32
        );
        Ok(())
    }

    #[test]
    fn parse_node_type_field_rejects_unknown_numeric_value() {
        let error = parse_node_type_field("999", "test").expect_err("unknown value should fail");

        assert!(error.to_string().contains("invalid node type '999'"));
    }

    #[test]
    fn parse_node_list_accepts_named_node_type() -> ClientResult<()> {
        let csv = tempfile::NamedTempFile::new()?;
        std::fs::write(
            csv.path(),
            "c-01,rack-01,10.0.0.1,443,root,secret,aa:bb:cc:dd:ee:ff,ComputeGb200Nvidia\n",
        )?;

        let nodes = parse_node_list_csv(csv.path())?;

        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0].node_type, rm::NodeType::ComputeGb200Nvidia as i32);
        Ok(())
    }

    #[test]
    fn parse_target_filters_are_node_type_specific() -> ClientResult<()> {
        let filters = parse_component_filters(
            &[],
            &[
                format!("{}:CPU_0", rm::NodeType::ComputeGb200Nvidia as i32),
                format!("{}:HMC", rm::NodeType::ComputeVrnvl72Nvidia as i32),
                format!("{}:BIOS", rm::NodeType::SwitchGb200Nvidia as i32),
            ],
        )?;

        assert_eq!(
            filters[&(rm::NodeType::ComputeGb200Nvidia as i32)].components,
            vec!["CPU_0"]
        );
        assert_eq!(
            filters[&(rm::NodeType::ComputeVrnvl72Nvidia as i32)].components,
            vec!["HMC"]
        );
        assert_eq!(
            filters[&(rm::NodeType::SwitchGb200Nvidia as i32)].components,
            vec!["BIOS"]
        );

        let err = parse_component_filters(
            &[],
            &[format!("{}:CPU_0", rm::NodeType::SwitchGb200Nvidia as i32)],
        )
        .expect_err("switch target filter should reject compute target");
        assert!(err.to_string().contains("SwitchGb200Nvidia"));

        let err = parse_component_filters(
            &[],
            &[format!(
                "{}:CPU_0",
                rm::NodeType::ComputeVrnvl72Nvidia as i32
            )],
        )
        .expect_err("VRNVL72 target filter should reject unconfirmed GB target");
        assert!(err.to_string().contains("ComputeVrnvl72Nvidia"));

        assert_eq!(
            parse_firmware_object_component_target(
                rm::NodeType::SwitchVrnvl72Nvidia as i32,
                "CPLD1"
            )?,
            "CPLD"
        );

        Ok(())
    }

    #[test]
    fn parse_target_filter_wildcard_keeps_explicit_empty_components() -> ClientResult<()> {
        let filters = parse_component_filters(
            &[],
            &[format!("{}:*", rm::NodeType::ComputeGb200Nvidia as i32)],
        )?;

        assert_eq!(
            filters[&(rm::NodeType::ComputeGb200Nvidia as i32)].components,
            Vec::<String>::new()
        );

        Ok(())
    }

    #[test]
    fn parse_switch_targets_accepts_missing_mac_address() -> ClientResult<()> {
        let csv = tempfile::NamedTempFile::new()?;
        std::fs::write(csv.path(), "sw-01,rack-01,10.0.1.1,,admin,secret\n")?;

        let targets = parse_switch_targets_csv(csv.path())?;

        assert_eq!(targets.len(), 1);
        assert_eq!(targets[0].node_id, "sw-01");
        assert_eq!(targets[0].port, 22);
        assert_eq!(targets[0].mac_address, "");
        assert_eq!(targets[0].host_name, "10.0.1.1");
        assert_eq!(targets[0].node_type, rm::NodeType::SwitchGb200Nvidia as i32);

        Ok(())
    }

    #[test]
    fn parse_switch_targets_accepts_explicit_host_name() -> ClientResult<()> {
        let csv = tempfile::NamedTempFile::new()?;
        std::fs::write(
            csv.path(),
            "sw-01,rack-01,10.0.1.1,443,admin,secret,aa:bb:cc:dd:ee:ff,sw-01.switch.example.com\n",
        )?;

        let targets = parse_switch_targets_csv(csv.path())?;

        assert_eq!(targets.len(), 1);
        assert_eq!(targets[0].host_name, "sw-01.switch.example.com");

        Ok(())
    }

    #[test]
    fn parse_switch_targets_accepts_vrnvl72_node_type() -> ClientResult<()> {
        let csv = tempfile::NamedTempFile::new()?;
        std::fs::write(
            csv.path(),
            "sw-01,rack-01,10.0.1.1,443,admin,secret,,vr-switch.example.com,SwitchVrnvl72Nvidia\n",
        )?;

        let targets = parse_switch_targets_csv(csv.path())?;

        assert_eq!(
            targets[0].node_type,
            rm::NodeType::SwitchVrnvl72Nvidia as i32
        );
        assert_eq!(targets[0].host_name, "vr-switch.example.com");

        Ok(())
    }

    #[test]
    fn switch_target_to_node_populates_host_name() {
        let node = switch_target_to_node(SwitchTarget {
            node_id: "sw-01".into(),
            rack_id: "rack-01".into(),
            ip_address: "10.0.1.1".into(),
            port: 443,
            username: "admin".into(),
            password: "secret".into(),
            mac_address: String::new(),
            host_name: "sw-01.switch.example.com".into(),
            node_type: rm::NodeType::SwitchVrnvl72Nvidia as i32,
        });

        assert_eq!(node.r#type, Some(rm::NodeType::SwitchVrnvl72Nvidia as i32));

        let host_name = node
            .host_endpoint
            .and_then(|endpoint| endpoint.interface)
            .and_then(|iface| iface.host_name)
            .expect("expected host_name on switch host endpoint");
        assert_eq!(host_name, "sw-01.switch.example.com");
    }

    #[test]
    fn parse_node_list_accepts_optional_endpoint_role() -> ClientResult<()> {
        let csv = tempfile::NamedTempFile::new()?;
        std::fs::write(
            csv.path(),
            "sw-01,rack-01,10.0.1.1,443,root,secret,aa:bb:cc:dd:ee:ff,6,bmc\n",
        )?;

        let nodes = parse_node_list_csv(csv.path())?;

        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0].endpoint_role, Some(NodeListEndpointRole::Bmc));

        Ok(())
    }

    #[test]
    fn parse_node_list_rejects_unknown_endpoint_role() -> ClientResult<()> {
        let csv = tempfile::NamedTempFile::new()?;
        std::fs::write(
            csv.path(),
            "sw-01,rack-01,10.0.1.1,443,root,secret,aa:bb:cc:dd:ee:ff,6,mgmt\n",
        )?;

        let err = parse_node_list_csv(csv.path()).expect_err("expected invalid endpoint role");

        assert!(err.to_string().contains("invalid endpoint role"));

        Ok(())
    }

    #[test]
    fn parse_node_list_accepts_switch_host_name() -> ClientResult<()> {
        let csv = tempfile::NamedTempFile::new()?;
        std::fs::write(
            csv.path(),
            "sw-01,rack-01,10.0.1.1,443,root,secret,aa:bb:cc:dd:ee:ff,SwitchGb200Nvidia,,sw-01.switch.example.com\n",
        )?;

        let nodes = parse_node_list_csv(csv.path())?;

        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0].host_name, "sw-01.switch.example.com");
        Ok(())
    }

    #[test]
    fn parse_node_list_accepts_switch_host_name_with_bmc_columns() -> ClientResult<()> {
        let csv = tempfile::NamedTempFile::new()?;
        std::fs::write(
            csv.path(),
            "sw-01,rack-01,10.0.0.1,443,nvue-user,nvue-pass,aa:bb:cc:dd:ee:ff,SwitchVrnvl72Nvidia,,sw-01.switch.example.com,192.168.1.100,8443,bmc-user,bmc-pass\n",
        )?;

        let nodes = parse_node_list_csv(csv.path())?;

        assert_eq!(nodes[0].host_name, "sw-01.switch.example.com");
        assert_eq!(nodes[0].node_type, rm::NodeType::SwitchVrnvl72Nvidia as i32);
        let bmc = nodes[0]
            .bmc_endpoint
            .as_ref()
            .expect("expected BMC endpoint");
        assert_eq!(bmc.ip_address, "192.168.1.100");
        Ok(())
    }

    #[test]
    fn node_list_switch_host_name_populates_host_endpoint() {
        let entry = NodeListEntry {
            node_id: "sw-01".to_owned(),
            rack_id: "rack-01".to_owned(),
            ip_address: "10.0.1.1".to_owned(),
            port: 443,
            username: "root".to_owned(),
            password: "secret".to_owned(),
            mac_address: "aa:bb:cc:dd:ee:ff".to_owned(),
            node_type: rm::NodeType::SwitchGb200Nvidia as i32,
            endpoint_role: None,
            host_name: "sw-01.switch.example.com".into(),
            bmc_endpoint: None,
        };

        let node = node_list_entry_to_proto(entry, NodeListEndpointMode::DirectOperation);

        let host_name = node
            .host_endpoint
            .as_ref()
            .and_then(|endpoint| endpoint.interface.as_ref())
            .and_then(|iface| iface.host_name.as_deref())
            .expect("expected host_name on switch host endpoint");
        assert_eq!(host_name, "sw-01.switch.example.com");
    }

    #[test]
    fn parse_node_list_accepts_bmc_columns_with_mac() -> ClientResult<()> {
        let csv = tempfile::NamedTempFile::new()?;
        std::fs::write(
            csv.path(),
            "sw-01,rack-01,10.0.0.1,443,nvue-user,nvue-pass,aa:bb:cc:dd:ee:ff,6,,192.168.1.100,8443,bmc-user,bmc-pass,11:22:33:44:55:66\n",
        )?;

        let nodes = parse_node_list_csv(csv.path())?;

        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0].ip_address, "10.0.0.1");
        assert_eq!(nodes[0].port, 443);
        assert_eq!(nodes[0].username, "nvue-user");
        let bmc = nodes[0]
            .bmc_endpoint
            .as_ref()
            .expect("expected BMC endpoint");
        assert_eq!(bmc.ip_address, "192.168.1.100");
        assert_eq!(bmc.port, 8443);
        assert_eq!(bmc.username, "bmc-user");
        assert_eq!(bmc.password, "bmc-pass");
        assert_eq!(bmc.mac_address, "11:22:33:44:55:66");

        Ok(())
    }

    #[test]
    fn parse_node_list_accepts_bmc_columns_without_mac() -> ClientResult<()> {
        let csv = tempfile::NamedTempFile::new()?;
        std::fs::write(
            csv.path(),
            "sw-01,rack-01,10.0.0.1,443,nvue-user,nvue-pass,aa:bb:cc:dd:ee:ff,6,,192.168.1.100,8443,bmc-user,bmc-pass\n",
        )?;

        let nodes = parse_node_list_csv(csv.path())?;

        let bmc = nodes[0]
            .bmc_endpoint
            .as_ref()
            .expect("expected BMC endpoint");
        assert_eq!(bmc.ip_address, "192.168.1.100");
        assert_eq!(
            bmc.mac_address, "",
            "bmc_mac should default to empty string"
        );

        Ok(())
    }

    #[test]
    fn parse_node_list_rejects_partial_bmc_columns() -> ClientResult<()> {
        let csv = tempfile::NamedTempFile::new()?;
        // bmc_ip present but bmc_port missing
        std::fs::write(
            csv.path(),
            "sw-01,rack-01,10.0.0.1,443,nvue-user,nvue-pass,aa:bb:cc:dd:ee:ff,6,,192.168.1.100\n",
        )?;

        let err =
            parse_node_list_csv(csv.path()).expect_err("expected error for partial BMC columns");

        assert!(
            err.to_string().contains("bmc_port"),
            "expected error to mention bmc_port; got: {err}"
        );

        Ok(())
    }

    #[test]
    fn switch_with_explicit_bmc_endpoint_populates_both_endpoints() {
        let entry = NodeListEntry {
            node_id: "sw-01".to_owned(),
            rack_id: "rack-01".to_owned(),
            ip_address: "10.0.0.1".to_owned(),
            port: 443,
            username: "nvue-user".to_owned(),
            password: "nvue-pass".to_owned(),
            mac_address: "aa:bb:cc:dd:ee:ff".to_owned(),
            node_type: rm::NodeType::SwitchVrnvl72Nvidia as i32,
            endpoint_role: None,
            host_name: String::new(),
            bmc_endpoint: Some(NodeListBmcEndpoint {
                ip_address: "192.168.1.100".to_owned(),
                port: 8443,
                username: "bmc-user".to_owned(),
                password: "bmc-pass".to_owned(),
                mac_address: "11:22:33:44:55:66".to_owned(),
            }),
        };

        let node = node_list_entry_to_proto(entry, NodeListEndpointMode::DirectOperation);

        let host = node.host_endpoint.as_ref().expect("expected host endpoint");
        let bmc = node.bmc_endpoint.as_ref().expect("expected BMC endpoint");

        assert_eq!(node.r#type, Some(rm::NodeType::SwitchVrnvl72Nvidia as i32));

        assert_eq!(host.interface.as_ref().unwrap().ip_address, "10.0.0.1");
        assert_eq!(
            host.interface.as_ref().unwrap().host_name.as_deref(),
            Some("10.0.0.1")
        );
        assert_eq!(host.port, 443);

        assert_eq!(bmc.interface.as_ref().unwrap().ip_address, "192.168.1.100");
        assert_eq!(
            bmc.interface.as_ref().unwrap().mac_address,
            "11:22:33:44:55:66"
        );
        assert_eq!(bmc.port, 8443);
    }

    #[test]
    fn switch_with_explicit_bmc_endpoint_ignores_mode_and_role() {
        // Even in InventoryRegistration mode with a Bmc role, explicit BMC columns win.
        let entry = NodeListEntry {
            node_id: "sw-01".to_owned(),
            rack_id: "rack-01".to_owned(),
            ip_address: "10.0.0.1".to_owned(),
            port: 443,
            username: "nvue-user".to_owned(),
            password: "nvue-pass".to_owned(),
            mac_address: "aa:bb:cc:dd:ee:ff".to_owned(),
            node_type: rm::NodeType::SwitchGb200Nvidia as i32,
            endpoint_role: Some(NodeListEndpointRole::Bmc),
            host_name: String::new(),
            bmc_endpoint: Some(NodeListBmcEndpoint {
                ip_address: "192.168.1.100".to_owned(),
                port: 8443,
                username: "bmc-user".to_owned(),
                password: "bmc-pass".to_owned(),
                mac_address: "11:22:33:44:55:66".to_owned(),
            }),
        };

        let node = node_list_entry_to_proto(entry, NodeListEndpointMode::InventoryRegistration);

        assert!(
            node.host_endpoint.is_some(),
            "host_endpoint should be set from main columns"
        );
        assert!(
            node.bmc_endpoint.is_some(),
            "bmc_endpoint should be set from BMC columns"
        );
        assert_eq!(
            node.host_endpoint
                .as_ref()
                .unwrap()
                .interface
                .as_ref()
                .unwrap()
                .ip_address,
            "10.0.0.1"
        );
        assert_eq!(
            node.bmc_endpoint
                .as_ref()
                .unwrap()
                .interface
                .as_ref()
                .unwrap()
                .ip_address,
            "192.168.1.100"
        );
    }

    #[test]
    fn configure_certificate_status_command_parses() {
        let cli = Cli::try_parse_from([
            "rack_manager_client",
            "localhost",
            "8801",
            "switch",
            "configure_certificate_status",
            "job-123",
        ])
        .expect("expected configure_certificate_status to parse");

        let Command::Switch {
            command: SwitchCommand::ConfigureCertificateStatus { job_id },
        } = cli.command
        else {
            panic!("expected switch configure_certificate_status command");
        };
        assert_eq!(job_id, "job-123");
    }
}
