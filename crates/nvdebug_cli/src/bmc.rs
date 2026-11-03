/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
//! How nvdebug reaches one device.

use std::fmt;
use std::net::IpAddr;
use std::str::FromStr;

use rustls_pki_types::DnsName;
use secrecy::{ExposeSecret, SecretString};

use crate::Error;

/// The key under which nvdebug's DUT file keeps per-file defaults. A target
/// of this name would be read as defaults, not as a device.
pub const DUT_DEFAULTS_KEY: &str = "DUT_Defaults";

/// Where a BMC is: an IP address or a DNS name. nvdebug resolves a name
/// itself when it connects.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BmcHost {
    Ip(IpAddr),
    Name(DnsName<'static>),
}

impl BmcHost {
    /// An IP address, or else a DNS name: dot-separated labels of letters,
    /// digits, `-` and `_`, the last label not all digits. Brackets, a port,
    /// a user and a path are refused, and so is an IPv6 zone id: a zone
    /// names an interface of the caller's own host, and RMS reaches BMCs
    /// over routable addresses.
    pub fn parse(text: &str) -> Result<Self, Error> {
        if let Ok(ip) = text.parse::<IpAddr>() {
            return Ok(Self::Ip(ip));
        }

        DnsName::try_from(text.to_owned())
            .map(Self::Name)
            .map_err(Error::invalid(
                "bmc host",
                "expected an IP address or a DNS name",
            ))
    }
}

impl FromStr for BmcHost {
    type Err = Error;

    fn from_str(text: &str) -> Result<Self, Error> {
        Self::parse(text)
    }
}

impl fmt::Display for BmcHost {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Ip(ip) => ip.fmt(f),
            Self::Name(name) => f.write_str(name.as_ref()),
        }
    }
}

/// How nvdebug reaches one device: the BMC's address and a Redfish login.
#[derive(Debug, Clone)]
pub struct Bmc {
    host: BmcHost,
    user: String,
    password: SecretString,
}

impl Bmc {
    pub fn new(host: BmcHost, user: String, password: SecretString) -> Result<Self, Error> {
        if user.is_empty() {
            return Err(Error::new_invalid(
                "bmc user",
                "expected a non-empty username",
            ));
        }
        if password.expose_secret().is_empty() {
            return Err(Error::new_invalid(
                "bmc password",
                "expected a non-empty password",
            ));
        }

        Ok(Self {
            host,
            user,
            password,
        })
    }

    pub(crate) fn host(&self) -> &BmcHost {
        &self.host
    }

    pub(crate) fn user(&self) -> &str {
        &self.user
    }

    pub(crate) fn password(&self) -> &SecretString {
        &self.password
    }
}
