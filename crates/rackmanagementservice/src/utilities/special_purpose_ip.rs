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

//! IANA special-purpose address blocks used to reject SSRF-prone targets.
//!
//! One table covers artifact downloads and switch-target validation. Callers
//! name whether RFC 1918 / unique-local addresses are allowed.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

/// Whether RFC 1918 IPv4 and unique-local IPv6 (`fc00::/7`) may be used.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PrivateUse {
    /// In-rack switches may use private address space.
    Allowed,
    /// Artifact downloads with `allow_private_targets = false`.
    Disallowed,
}

#[derive(Clone, Copy)]
struct V4Block {
    network: Ipv4Addr,
    prefix: u8,
    private_use: bool,
}

#[derive(Clone, Copy)]
struct V6Block {
    network: Ipv6Addr,
    prefix: u8,
    private_use: bool,
}

/// IANA IPv4 Special-Purpose Address Registry covering prefixes.
const V4_BLOCKS: &[V4Block] = &[
    V4Block {
        network: Ipv4Addr::new(0, 0, 0, 0),
        prefix: 8,
        private_use: false,
    },
    V4Block {
        network: Ipv4Addr::new(10, 0, 0, 0),
        prefix: 8,
        private_use: true,
    },
    V4Block {
        network: Ipv4Addr::new(100, 64, 0, 0),
        prefix: 10,
        private_use: false,
    },
    V4Block {
        network: Ipv4Addr::new(127, 0, 0, 0),
        prefix: 8,
        private_use: false,
    },
    V4Block {
        network: Ipv4Addr::new(169, 254, 0, 0),
        prefix: 16,
        private_use: false,
    },
    V4Block {
        network: Ipv4Addr::new(172, 16, 0, 0),
        prefix: 12,
        private_use: true,
    },
    V4Block {
        network: Ipv4Addr::new(192, 0, 0, 0),
        prefix: 24,
        private_use: false,
    },
    V4Block {
        network: Ipv4Addr::new(192, 0, 2, 0),
        prefix: 24,
        private_use: false,
    },
    V4Block {
        network: Ipv4Addr::new(192, 31, 196, 0),
        prefix: 24,
        private_use: false,
    },
    V4Block {
        network: Ipv4Addr::new(192, 52, 193, 0),
        prefix: 24,
        private_use: false,
    },
    V4Block {
        network: Ipv4Addr::new(192, 88, 99, 0),
        prefix: 24,
        private_use: false,
    },
    V4Block {
        network: Ipv4Addr::new(192, 168, 0, 0),
        prefix: 16,
        private_use: true,
    },
    V4Block {
        network: Ipv4Addr::new(192, 175, 48, 0),
        prefix: 24,
        private_use: false,
    },
    V4Block {
        network: Ipv4Addr::new(198, 18, 0, 0),
        prefix: 15,
        private_use: false,
    },
    V4Block {
        network: Ipv4Addr::new(198, 51, 100, 0),
        prefix: 24,
        private_use: false,
    },
    V4Block {
        network: Ipv4Addr::new(203, 0, 113, 0),
        prefix: 24,
        private_use: false,
    },
    V4Block {
        network: Ipv4Addr::new(224, 0, 0, 0),
        prefix: 4,
        private_use: false,
    },
    V4Block {
        network: Ipv4Addr::new(240, 0, 0, 0),
        prefix: 4,
        private_use: false,
    },
];

/// IANA IPv6 Special-Purpose Address Registry covering prefixes.
///
/// IPv4-mapped addresses (`::ffff:0:0/96`) are unwrapped before matching.
const V6_BLOCKS: &[V6Block] = &[
    V6Block {
        network: Ipv6Addr::UNSPECIFIED,
        prefix: 128,
        private_use: false,
    },
    V6Block {
        network: Ipv6Addr::LOCALHOST,
        prefix: 128,
        private_use: false,
    },
    V6Block {
        network: Ipv6Addr::new(0x64, 0xff9b, 0, 0, 0, 0, 0, 0),
        prefix: 96,
        private_use: false,
    },
    V6Block {
        network: Ipv6Addr::new(0x64, 0xff9b, 1, 0, 0, 0, 0, 0),
        prefix: 48,
        private_use: false,
    },
    V6Block {
        network: Ipv6Addr::new(0x0100, 0, 0, 0, 0, 0, 0, 0),
        prefix: 64,
        private_use: false,
    },
    V6Block {
        network: Ipv6Addr::new(0x2001, 0, 0, 0, 0, 0, 0, 0),
        prefix: 23,
        private_use: false,
    },
    V6Block {
        network: Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 0),
        prefix: 32,
        private_use: false,
    },
    V6Block {
        network: Ipv6Addr::new(0x2002, 0, 0, 0, 0, 0, 0, 0),
        prefix: 16,
        private_use: false,
    },
    V6Block {
        network: Ipv6Addr::new(0x3fff, 0, 0, 0, 0, 0, 0, 0),
        prefix: 20,
        private_use: false,
    },
    V6Block {
        network: Ipv6Addr::new(0xfc00, 0, 0, 0, 0, 0, 0, 0),
        prefix: 7,
        private_use: true,
    },
    V6Block {
        network: Ipv6Addr::new(0xfe80, 0, 0, 0, 0, 0, 0, 0),
        prefix: 10,
        private_use: false,
    },
    V6Block {
        network: Ipv6Addr::new(0xff00, 0, 0, 0, 0, 0, 0, 0),
        prefix: 8,
        private_use: false,
    },
];

/// Returns true when `ip` is in the IANA special-purpose table.
///
/// IPv4-mapped IPv6 addresses are classified as their embedded IPv4 address.
/// [`PrivateUse::Allowed`] skips RFC 1918 and unique-local blocks only.
pub fn is_special_purpose_ip(ip: IpAddr, private_use: PrivateUse) -> bool {
    match unwrap_ipv4_mapped(ip) {
        IpAddr::V4(ip) => v4_blocks(private_use).any(|block| ipv4_contains(ip, block)),
        IpAddr::V6(ip) => v6_blocks(private_use).any(|block| ipv6_contains(ip, block)),
    }
}

fn unwrap_ipv4_mapped(ip: IpAddr) -> IpAddr {
    match ip {
        IpAddr::V6(ip) => ip
            .to_ipv4_mapped()
            .map(IpAddr::V4)
            .unwrap_or(IpAddr::V6(ip)),
        IpAddr::V4(ip) => IpAddr::V4(ip),
    }
}

fn v4_blocks(private_use: PrivateUse) -> impl Iterator<Item = &'static V4Block> {
    V4_BLOCKS
        .iter()
        .filter(move |block| private_use == PrivateUse::Disallowed || !block.private_use)
}

fn v6_blocks(private_use: PrivateUse) -> impl Iterator<Item = &'static V6Block> {
    V6_BLOCKS
        .iter()
        .filter(move |block| private_use == PrivateUse::Disallowed || !block.private_use)
}

fn ipv4_contains(ip: Ipv4Addr, block: &V4Block) -> bool {
    let shift = 32 - u32::from(block.prefix);
    let mask = if block.prefix == 0 {
        0
    } else {
        u32::MAX << shift
    };
    (u32::from(ip) & mask) == (u32::from(block.network) & mask)
}

fn ipv6_contains(ip: Ipv6Addr, block: &V6Block) -> bool {
    let shift = 128 - u32::from(block.prefix);
    let mask = if block.prefix == 0 {
        0
    } else {
        u128::MAX << shift
    };
    (u128::from(ip) & mask) == (u128::from(block.network) & mask)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ip(value: &str) -> IpAddr {
        value.parse().expect("ip")
    }

    #[test]
    fn disallowed_private_use_rejects_cgnat_this_network_reserved_and_nat64() {
        for value in ["100.64.0.1", "0.0.0.1", "240.0.0.1", "64:ff9b::a00:1"] {
            assert!(
                is_special_purpose_ip(ip(value), PrivateUse::Disallowed),
                "{value}"
            );
        }
        assert!(is_special_purpose_ip(
            ip("::ffff:10.0.0.1"),
            PrivateUse::Disallowed
        ));
    }

    #[test]
    fn allowed_private_use_keeps_rfc1918_and_ula_and_rejects_the_rest() {
        assert!(!is_special_purpose_ip(ip("10.0.0.1"), PrivateUse::Allowed));
        assert!(!is_special_purpose_ip(
            ip("192.168.1.1"),
            PrivateUse::Allowed
        ));
        assert!(!is_special_purpose_ip(ip("fc00::1"), PrivateUse::Allowed));
        assert!(is_special_purpose_ip(ip("100.64.0.1"), PrivateUse::Allowed));
        assert!(is_special_purpose_ip(ip("127.0.0.1"), PrivateUse::Allowed));
        assert!(is_special_purpose_ip(
            ip("169.254.1.1"),
            PrivateUse::Allowed
        ));
        assert!(is_special_purpose_ip(
            ip("64:ff9b::a00:1"),
            PrivateUse::Allowed
        ));
    }

    #[test]
    fn public_unicast_is_not_special_purpose() {
        for value in ["8.8.8.8", "1.1.1.1", "2001:4860:4860::8888"] {
            assert!(
                !is_special_purpose_ip(ip(value), PrivateUse::Disallowed),
                "{value}"
            );
        }
    }
}
