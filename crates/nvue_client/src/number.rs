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

use std::fmt;

use serde::de::{self, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// JSON number accepted from recursive NVUE compatibility fields.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JsonNumber(String);

impl JsonNumber {
    /// Return the number representation captured at deserialization.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Serialize for JsonNumber {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        if let Ok(value) = self.0.parse::<i64>() {
            return serializer.serialize_i64(value);
        }

        if let Ok(value) = self.0.parse::<u64>() {
            return serializer.serialize_u64(value);
        }

        if let Ok(value) = self.0.parse::<f64>() {
            return serializer.serialize_f64(value);
        }

        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for JsonNumber {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(JsonNumberVisitor)
    }
}

struct JsonNumberVisitor;

impl Visitor<'_> for JsonNumberVisitor {
    type Value = JsonNumber;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a JSON number")
    }

    fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(JsonNumber(value.to_string()))
    }

    fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(JsonNumber(value.to_string()))
    }

    fn visit_f64<E>(self, value: f64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(JsonNumber(value.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_json_number_shapes() {
        let signed: JsonNumber = serde_json::from_str("-1").unwrap();
        let unsigned: JsonNumber = serde_json::from_str("18446744073709551615").unwrap();
        let float: JsonNumber = serde_json::from_str("1.5").unwrap();

        assert_eq!(signed.as_str(), "-1");
        assert_eq!(unsigned.as_str(), "18446744073709551615");
        assert_eq!(float.as_str(), "1.5");
    }
}
