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

use std::net::{IpAddr, Ipv4Addr};
use std::ops::Range;
use std::path::PathBuf;

use crate::fixtures;
use crate::redfish_mockup_server::MockRedfishServer;

const DEFAULT_TASK_DELAY_SECONDS: f32 = 1.0;
const DEFAULT_FIRMWARE_FAILURE_RATE: f32 = 0.0;

/// Bundled fixture sets that can back a simulator endpoint.
#[derive(Debug, Clone)]
pub enum RedfishFixture {
    /// GB200 compute BMC plus HMC fixture archive.
    Gb200Compute,
    /// GB200 HMC-only fixture archive.
    Gb200Hmc,
    /// Power shelf fixture archive.
    Powershelf,
    /// Caller-provided tar.gz fixture archive.
    Custom(PathBuf),
}

impl RedfishFixture {
    fn path(&self) -> String {
        match self {
            Self::Gb200Compute => fixtures::path_string(fixtures::gb200_compute_archive()),
            Self::Gb200Hmc => fixtures::path_string(fixtures::gb200_hmc_archive()),
            Self::Powershelf => fixtures::path_string(fixtures::powershelf_archive()),
            Self::Custom(path) => fixtures::path_string(path.clone()),
        }
    }
}

/// Running in-process Redfish simulator.
pub struct RedfishSimulator {
    server: MockRedfishServer,
    ports: Vec<u16>,
}

impl RedfishSimulator {
    /// Start configuring a simulator instance.
    pub fn builder() -> RedfishSimulatorBuilder {
        RedfishSimulatorBuilder::default()
    }

    /// HTTPS ports for configuring RMS node BMC endpoints, in logical endpoint order.
    pub fn ports(&self) -> &[u16] {
        &self.ports
    }

    /// Stop simulator listeners.
    pub fn stop(&self) {
        self.server.stop();
    }
}

/// Builder for in-process Redfish simulator instances.
pub struct RedfishSimulatorBuilder {
    endpoints: Vec<(u16, RedfishFixture)>,
    port_ranges: Vec<(Range<u16>, RedfishFixture)>,
    auto_ports: Vec<(usize, RedfishFixture)>,
    bind_ip: IpAddr,
    task_delay_seconds: f32,
    firmware_failure_rate: f32,
}

impl Default for RedfishSimulatorBuilder {
    fn default() -> Self {
        Self {
            endpoints: Vec::new(),
            port_ranges: Vec::new(),
            auto_ports: Vec::new(),
            bind_ip: IpAddr::V4(Ipv4Addr::LOCALHOST),
            task_delay_seconds: DEFAULT_TASK_DELAY_SECONDS,
            firmware_failure_rate: DEFAULT_FIRMWARE_FAILURE_RATE,
        }
    }
}

impl RedfishSimulatorBuilder {
    /// Set generated firmware task completion delay in seconds.
    ///
    /// # Panics
    ///
    /// Panics if `seconds` is negative or not finite.
    pub fn with_task_delay(mut self, seconds: f32) -> Self {
        assert_valid_task_delay(seconds);
        self.task_delay_seconds = seconds;
        self
    }

    /// Set generated firmware task failure probability from 0.0 to 1.0.
    ///
    /// # Panics
    ///
    /// Panics if `rate` is not finite or is outside the range 0.0 to 1.0.
    pub fn with_firmware_failure_rate(mut self, rate: f32) -> Self {
        assert_valid_failure_rate(rate);
        self.firmware_failure_rate = rate;
        self
    }

    /// Bind simulator listeners to the given IP address.
    pub fn bind_ip(mut self, bind_ip: IpAddr) -> Self {
        self.bind_ip = bind_ip;
        self
    }

    /// Configure generated firmware tasks to always fail.
    pub fn always_fail_firmware_updates(mut self) -> Self {
        self.firmware_failure_rate = 1.0;
        self
    }

    /// Configure generated firmware tasks to never fail.
    pub fn never_fail_firmware_updates(mut self) -> Self {
        self.firmware_failure_rate = 0.0;
        self
    }

    /// Add an endpoint on a fixed port. Use port 0 for an OS-assigned port.
    pub fn add_endpoint(mut self, port: u16, fixture: RedfishFixture) -> Self {
        self.endpoints.push((port, fixture));
        self
    }

    /// Add a GB200 compute endpoint. Use port 0 for an OS-assigned port.
    pub fn add_gb200_compute(mut self, port: u16) -> Self {
        self.endpoints.push((port, RedfishFixture::Gb200Compute));
        self
    }

    /// Add a power shelf endpoint. Use port 0 for an OS-assigned port.
    pub fn add_powershelf(mut self, port: u16) -> Self {
        self.endpoints.push((port, RedfishFixture::Powershelf));
        self
    }

    /// Add a fixture-backed endpoint for every port in a range.
    pub fn add_port_range(mut self, range: Range<u16>, fixture: RedfishFixture) -> Self {
        self.port_ranges.push((range, fixture));
        self
    }

    /// Add N logical OS-assigned ports backed by the same fixture.
    ///
    /// Every logical node is bound to its own dedicated listener (one simulated
    /// BMC per node), so concurrent firmware updates never collide on a shared
    /// `TaskService`.
    pub fn add_auto_ports(mut self, count: usize, fixture: RedfishFixture) -> Self {
        self.auto_ports.push((count, fixture));
        self
    }

    /// Start the simulator and bind all configured HTTPS endpoints.
    pub async fn start(self) -> RedfishSimulator {
        let mut builder = MockRedfishServer::builder().bind_ip(self.bind_ip);

        for (port, fixture) in self.endpoints {
            builder = builder.add_endpoint(port, &fixture.path());
        }

        for (range, fixture) in self.port_ranges {
            builder = builder.add_port_range(range, &fixture.path());
        }

        for (count, fixture) in self.auto_ports {
            builder = builder.add_auto_ports(count, &fixture.path());
        }

        let server = builder.start().await;
        server.set_delay(self.task_delay_seconds);
        server.set_failure_rate(self.firmware_failure_rate);
        let ports = server.ports().to_vec();

        RedfishSimulator { server, ports }
    }
}

fn assert_valid_task_delay(seconds: f32) {
    assert!(
        seconds.is_finite() && seconds >= 0.0,
        "task delay must be non-negative and finite"
    );
}

fn assert_valid_failure_rate(rate: f32) {
    assert!(
        rate.is_finite() && (0.0..=1.0).contains(&rate),
        "firmware failure rate must be in [0.0, 1.0]"
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[should_panic(expected = "task delay must be non-negative and finite")]
    fn with_task_delay_rejects_negative() {
        let _ = RedfishSimulator::builder().with_task_delay(-0.1);
    }

    #[test]
    #[should_panic(expected = "firmware failure rate must be in [0.0, 1.0]")]
    fn with_firmware_failure_rate_rejects_out_of_range_value() {
        let _ = RedfishSimulator::builder().with_firmware_failure_rate(1.1);
    }
}
