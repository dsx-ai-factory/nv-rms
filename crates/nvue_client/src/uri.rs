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

const HEX: &[u8; 16] = b"0123456789ABCDEF";

pub(crate) fn path_segment(value: &str) -> String {
    percent_encode(value)
}

pub(crate) fn query_value(value: &str) -> String {
    percent_encode(value)
}

fn percent_encode(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len());

    for byte in value.bytes() {
        if is_unreserved(byte) {
            encoded.push(byte as char);
        } else {
            encoded.push('%');
            encoded.push(HEX[(byte >> 4) as usize] as char);
            encoded.push(HEX[(byte & 0x0f) as usize] as char);
        }
    }

    encoded
}

fn is_unreserved(byte: u8) -> bool {
    matches!(
        byte,
        b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~'
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn path_segments_escape_reserved_and_non_ascii_bytes() {
        assert_eq!(path_segment("BMC/slot 1#ä"), "BMC%2Fslot%201%23%C3%A4");
    }

    #[test]
    fn query_values_escape_reserved_bytes() {
        assert_eq!(
            query_value("pending&base=applied"),
            "pending%26base%3Dapplied"
        );
    }
}
