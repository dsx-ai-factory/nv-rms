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

//! Firmware package parsing module for nvfwupd.
//!
//! Provides the [`FirmwarePkg`] trait and two implementations:
//! - [`PLDM`]: parses PLDM firmware packages using an external unpack tool
//! - [`TarPkg`]: parses tar-based firmware packages (e.g. PowerShelf)
//!
//! The [`get_pkg_parser`] factory function inspects the file and returns the
//! appropriate parser.

use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Component, Path, PathBuf};

use async_trait::async_trait;
use indexmap::IndexMap;
use serde::Serialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tokio::io::AsyncReadExt;

// ---------------------------------------------------------------------------
// FirmwarePkg trait
// ---------------------------------------------------------------------------

/// Trait defining firmware package parsing operations.
///
/// Each implementation must be able to validate a firmware package, extract
/// AP name to version mappings, and clean up any temporary files.
#[async_trait]
pub trait FirmwarePkg: Send + Sync {
    /// Parse a firmware package file.
    ///
    /// Returns `(success, error_message)`. On success the implementation
    /// populates its internal AP-name-to-version dictionary.
    async fn parse_pkg(
        &mut self,
        package_name: &str,
        json_dict: Option<&mut Value>,
    ) -> (bool, String);

    /// Remove any temporary files created during parsing or unpacking.
    async fn remove_files(&mut self);

    /// Return a reference to the AP-name to version mapping extracted
    /// from the parsed package.
    fn apname_version_dict(&self) -> &HashMap<String, IndexMap<String, Vec<String>>>;

    /// Print the parsed package contents (formatted JSON for PLDM, summary for Tar).
    fn print_package_content(&self, package_name: &str);

    /// Unpack the package and populate the internal file-to-AP mapping.
    async fn prepare_unpack_file_dict(&mut self, _pkg_name: &str) {}

    /// Return the AP-name to [version, file_path] mapping from the last unpack.
    fn unpack_file_ap_dict(&self) -> &HashMap<String, Vec<String>> {
        static EMPTY: std::sync::OnceLock<HashMap<String, Vec<String>>> =
            std::sync::OnceLock::new();
        EMPTY.get_or_init(HashMap::new)
    }

    /// Return the raw PLDM dict (`m_pldm_dict`) as a JSON `Value`.
    /// Only meaningful for PLDM packages; TarPkg returns an empty object.
    fn pldm_raw_dict(&self) -> Value {
        Value::Object(serde_json::Map::new())
    }
}

// ---------------------------------------------------------------------------
// Factory function
// ---------------------------------------------------------------------------

/// Inspect `file_path` and return the appropriate [`FirmwarePkg`] parser.
///
/// If the file looks like a tar archive (starts with a valid tar header),
/// a [`TarPkg`] is returned. Otherwise, a [`PLDM`] parser is returned.
pub async fn get_pkg_parser(
    file_path: &str,
    verbose: bool,
    _parallel_operation: bool,
    _json_output: Option<&Value>,
) -> Box<dyn FirmwarePkg> {
    if is_tar_file(file_path).await {
        let mut pkg = TarPkg::new();
        pkg.verbose = verbose;
        Box::new(pkg)
    } else {
        let mut pkg = PLDM::new();
        pkg.verbose = verbose;
        Box::new(pkg)
    }
}

/// Check whether a file appears to be a tar archive.
///
/// Checks for POSIX "ustar\0" magic, GNU "ustar " magic, and old V7
/// tar format (valid checksum in header), matching the scope of Python's
/// `tarfile.is_tarfile()`.
pub async fn is_tar_file(path: &str) -> bool {
    let Ok(mut f) = tokio::fs::File::open(path).await else {
        return false;
    };
    let mut buf = [0u8; 512];
    if f.read(&mut buf).await.unwrap_or(0) < 512 {
        return false;
    }
    // POSIX: "ustar\0" at offset 257
    if &buf[257..263] == b"ustar\0" {
        return true;
    }
    // GNU: "ustar " (with trailing space + NUL) at offset 257
    if buf[257..262] == *b"ustar" && (buf[262] == b' ' || buf[262] == 0) {
        return true;
    }
    // V7: no magic, but verify the octal checksum field at 148..156
    let chksum_str = std::str::from_utf8(&buf[148..156]).unwrap_or("");
    if let Ok(expected) = u32::from_str_radix(chksum_str.trim().trim_end_matches('\0'), 8) {
        if expected > 0 {
            let mut sum: u32 = 0;
            for (i, &b) in buf.iter().enumerate() {
                sum += if (148..156).contains(&i) {
                    b' ' as u32
                } else {
                    b as u32
                };
            }
            if sum == expected {
                return true;
            }
        }
    }
    false
}

// ---------------------------------------------------------------------------
// TarPkg
// ---------------------------------------------------------------------------

fn powershelf_component_type_from_purpose(purpose: &str) -> &'static str {
    let tokens: Vec<&str> = purpose
        .split(|ch: char| !ch.is_ascii_alphanumeric())
        .filter(|token| !token.is_empty())
        .collect();

    for token in tokens.iter().rev() {
        if token.eq_ignore_ascii_case("PSU") {
            return "psu";
        }
        if token.eq_ignore_ascii_case("MCU") || token.eq_ignore_ascii_case("PLDM") {
            return "mcu";
        }
        if token.eq_ignore_ascii_case("BMC") {
            return "bmc";
        }
    }

    if tokens
        .iter()
        .any(|token| token.eq_ignore_ascii_case("OPENBMC"))
    {
        return "bmc";
    }

    "unknown"
}

/// Parses tar-based firmware packages.
///
/// Optionally reads a `MANIFEST` file inside the tar for component metadata.
/// If no MANIFEST is present, default values are used (e.g. for PowerShelf).
pub struct TarPkg {
    /// AP name -> version mapping.
    pub apname_version_dict: HashMap<String, IndexMap<String, Vec<String>>>,
    /// Verbose output flag.
    pub verbose: bool,
    /// Path to the extracted tar contents (for cleanup).
    untar_file_path: String,
}

impl TarPkg {
    /// Create a new, empty `TarPkg` parser.
    pub fn new() -> Self {
        Self {
            apname_version_dict: HashMap::new(),
            verbose: false,
            untar_file_path: String::new(),
        }
    }

    /// Parse a MANIFEST file and populate `apname_version_dict`.
    ///
    /// If `skip_parsing` is true or `manifest_path` is `None`, default
    /// component metadata is used (component_type=psu, version=N/A).
    #[cfg(test)]
    async fn parse_manifest_file(
        &mut self,
        manifest_path: Option<&str>,
        package_name: &str,
        skip_parsing: bool,
    ) -> (bool, String) {
        let manifest_content = if skip_parsing {
            None
        } else if let Some(manifest_path) = manifest_path {
            match tokio::fs::read_to_string(manifest_path).await {
                Ok(content) => Some(content),
                Err(e) => return (false, format!("Error parsing MANIFEST file: {}", e)),
            }
        } else {
            None
        };

        self.parse_manifest_content(manifest_content.as_deref(), package_name, skip_parsing)
    }

    fn parse_manifest_content(
        &mut self,
        manifest_content: Option<&str>,
        package_name: &str,
        skip_parsing: bool,
    ) -> (bool, String) {
        if skip_parsing || manifest_content.is_none() {
            let component_type = "psu";
            let version = "N/A".to_string();
            let model = "PowerShelf".to_string();
            let component_name = format!("{}_firmware", component_type);
            let mut components = IndexMap::new();
            components.insert(
                component_name,
                vec![version, model, component_type.to_string()],
            );
            self.apname_version_dict
                .insert(package_name.to_string(), components);
            return (true, String::new());
        }

        let Some(manifest_content) = manifest_content else {
            return (
                false,
                "Invalid MANIFEST file. Missing purpose or version field.".to_string(),
            );
        };
        let mut component_type = "unknown".to_string();
        let mut version = "unknown".to_string();
        let mut model = "PowerShelf".to_string();

        for line in manifest_content.lines() {
            let line = line.trim();

            if let Some((key, value)) = line.split_once('=') {
                match key {
                    "purpose" => {
                        component_type = powershelf_component_type_from_purpose(value).to_string();
                    }
                    "version" => {
                        version = value.to_string();
                    }
                    "model" => {
                        model = value.to_string();
                    }
                    _ => {}
                }
            }
        }

        if component_type != "unknown" && version != "unknown" {
            let component_name = format!("{}_firmware", component_type);
            let mut components = IndexMap::new();
            components.insert(component_name, vec![version, model, component_type]);
            self.apname_version_dict
                .insert(package_name.to_string(), components);
            (true, String::new())
        } else {
            (
                false,
                "Invalid MANIFEST file. Missing purpose or version field.".to_string(),
            )
        }
    }

    fn unpack_tar_blocking(package_name: String) -> Result<(String, Option<String>), String> {
        let dir_path =
            tempfile::tempdir().map_err(|e| format!("Failed to create temp dir: {}", e))?;
        let dir_str = dir_path.path().to_string_lossy().to_string();

        validate_tar_entries(&package_name)?;

        let tar_file =
            File::open(&package_name).map_err(|e| format!("Error extracting tar file: {}", e))?;
        let mut archive = tar::Archive::new(tar_file);
        archive
            .unpack(dir_path.path())
            .map_err(|e| format!("Error extracting tar file: {}", e))?;

        let manifest_content = find_manifest_in_dir_blocking(dir_path.path())
            .map(|manifest_path| {
                fs::read_to_string(&manifest_path)
                    .map_err(|e| format!("Error parsing MANIFEST file: {}", e))
            })
            .transpose()?;

        let _ = dir_path.keep();
        Ok((dir_str, manifest_content))
    }
}

#[async_trait]
impl FirmwarePkg for TarPkg {
    async fn parse_pkg(
        &mut self,
        package_name: &str,
        _json_dict: Option<&mut Value>,
    ) -> (bool, String) {
        let package_name_owned = package_name.to_string();
        let (untar_file_path, manifest_content) = match tokio::task::spawn_blocking(move || {
            Self::unpack_tar_blocking(package_name_owned)
        })
        .await
        {
            Ok(Ok(result)) => result,
            Ok(Err(e)) => return (false, e),
            Err(e) => return (false, format!("Error extracting tar file: {}", e)),
        };

        self.untar_file_path = untar_file_path;
        self.parse_manifest_content(
            manifest_content.as_deref(),
            package_name,
            manifest_content.is_none(),
        )
    }

    async fn remove_files(&mut self) {
        if !self.untar_file_path.is_empty() {
            let _ = tokio::fs::remove_dir_all(&self.untar_file_path).await;
            self.untar_file_path.clear();
        }
    }

    fn apname_version_dict(&self) -> &HashMap<String, IndexMap<String, Vec<String>>> {
        &self.apname_version_dict
    }

    fn print_package_content(&self, package_name: &str) {
        if let Some(components) = self.apname_version_dict.get(package_name) {
            println!("Package: {}", package_name);
            for (ap_name, version_info) in components {
                println!("  {}: {:?}", ap_name, version_info);
            }
        }
    }
}

fn validate_tar_entries(package_name: &str) -> Result<(), String> {
    let tar_file =
        File::open(package_name).map_err(|e| format!("Error reading tar file: {}", e))?;
    let mut archive = tar::Archive::new(tar_file);
    let entries = archive
        .entries()
        .map_err(|e| format!("Error reading tar file: {}", e))?;

    for entry in entries {
        let entry = entry.map_err(|e| format!("Error reading tar file: {}", e))?;
        let entry_type = entry.header().entry_type();
        if entry_type.is_symlink() || entry_type.is_hard_link() {
            return Err(format!(
                "Security error: tar contains link entry: {}",
                entry
                    .path()
                    .ok()
                    .map(|p| p.display().to_string())
                    .unwrap_or_default()
            ));
        }
        let path = entry
            .path()
            .map_err(|e| format!("Error reading tar file: {}", e))?;
        if path.is_absolute() {
            return Err(format!(
                "Security error: tar contains absolute path: {}",
                path.display()
            ));
        }
        if path
            .components()
            .any(|component| matches!(component, Component::ParentDir))
        {
            return Err(format!(
                "Security error: tar contains path traversal: {}",
                path.display()
            ));
        }
    }

    Ok(())
}

/// Recursively search for a file named `MANIFEST` under `dir`.
pub async fn find_manifest_in_dir(dir: &Path) -> Option<PathBuf> {
    let dir = dir.to_path_buf();
    tokio::task::spawn_blocking(move || find_manifest_in_dir_blocking(&dir))
        .await
        .ok()
        .flatten()
}

fn find_manifest_in_dir_blocking(dir: &Path) -> Option<PathBuf> {
    if let Ok(entries) = fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_file() {
                if let Some(name) = path.file_name() {
                    if name == "MANIFEST" {
                        return Some(path);
                    }
                }
            } else if path.is_dir() {
                if let Some(found) = find_manifest_in_dir_blocking(&path) {
                    return Some(found);
                }
            }
        }
    }
    None
}

// ---------------------------------------------------------------------------
// PLDM native binary parser
// ---------------------------------------------------------------------------

const PLDM_VALID_UUIDS: [([u8; 16], &str); 4] = [
    (
        [
            0xF0, 0x18, 0x87, 0x8C, 0xCB, 0x7D, 0x49, 0x43, 0x98, 0x00, 0xA0, 0x2F, 0x05, 0x9A,
            0xCA, 0x02,
        ],
        "1",
    ),
    (
        [
            0x12, 0x44, 0xD2, 0x64, 0x8D, 0x7D, 0x47, 0x18, 0xA0, 0x30, 0xFC, 0x8A, 0x56, 0x58,
            0x7D, 0x5A,
        ],
        "2",
    ),
    (
        [
            0x31, 0x19, 0xCE, 0x2F, 0xE8, 0x0A, 0x4A, 0x99, 0xAF, 0x6D, 0x46, 0xF8, 0xB1, 0x21,
            0xF6, 0xBF,
        ],
        "3",
    ),
    (
        [
            0x7B, 0x29, 0x1C, 0x99, 0x6D, 0xB6, 0x42, 0x08, 0x80, 0x1B, 0x02, 0x02, 0x6E, 0x46,
            0x3C, 0x78,
        ],
        "4",
    ),
];

fn descriptor_type_name(dt: u16) -> String {
    match dt {
        0x0000 => "PCI Vendor ID".into(),
        0x0001 => "IANA Enterprise ID".into(),
        0x0002 => "UUID".into(),
        0x0003 => "PnP Vendor ID".into(),
        0x0004 => "ACPI Vendor ID".into(),
        0x0005 => "IEEE Assigned Company ID".into(),
        0x0006 => "SCSI Vendor ID".into(),
        0x0100 => "PCI Device ID".into(),
        0x0101 => "PCI Subsystem Vendor ID".into(),
        0x0102 => "PCI Subsystem ID".into(),
        0x0103 => "PCI Revision ID".into(),
        0x0104 => "PnP Product Identifier".into(),
        0x0105 => "ACPI Product Identifier".into(),
        0x0106 => "ASCII Model Number".into(),
        0x0107 => "ASCII Model Number".into(),
        0x0108 => "SCSI Product ID".into(),
        0x0109 => "UBM Controller Device Code".into(),
        0xFFFF => "Vendor Defined".into(),
        _ => format!("{:#x}", dt),
    }
}

fn is_little_endian_descriptor(name: &str) -> bool {
    matches!(
        name,
        "IANA Enterprise ID"
            | "PCI Vendor ID"
            | "PCI Device ID"
            | "PCI Subsystem Vendor ID"
            | "PCI Subsystem ID"
    )
}

fn format_uuid_bytes(bytes: &[u8]) -> String {
    if bytes.len() < 16 {
        return String::new();
    }
    format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        bytes[0], bytes[1], bytes[2], bytes[3],
        bytes[4], bytes[5], bytes[6], bytes[7],
        bytes[8], bytes[9],
        bytes[10], bytes[11], bytes[12], bytes[13], bytes[14], bytes[15],
    )
}

fn padded_hex_le(bytes: &[u8]) -> String {
    if bytes.is_empty() {
        return "0x".into();
    }
    let val = bytes
        .iter()
        .rev()
        .fold(0u128, |acc, &b| (acc << 8) | b as u128);
    let hex = format!("{:x}", val);
    let padded = format!("{:0>width$}", hex, width = bytes.len() * 2);
    format!("0x{}", padded)
}

fn raw_hex(bytes: &[u8]) -> String {
    let hex: String = bytes.iter().map(|b| format!("{:02x}", b)).collect();
    format!("0x{}", hex)
}

fn decode_descriptor_data(type_name: &str, data: &[u8]) -> String {
    if is_little_endian_descriptor(type_name) {
        padded_hex_le(data)
    } else {
        raw_hex(data)
    }
}

fn parse_timestamp(ts: &[u8]) -> String {
    if ts.len() < 13 {
        return String::new();
    }
    let year = (ts[11] as u16) << 8 | ts[10] as u16;
    let month = ts[9];
    let day = ts[8];
    let hour = ts[7];
    let minute = ts[6];
    let second = ts[5];
    let micro = (ts[4] as u32) << 16 | (ts[3] as u32) << 8 | ts[2] as u32;
    let utc_raw = (ts[1] as i16) << 8 | ts[0] as i16;
    let sign = if utc_raw < 0 { "-" } else { "+" };
    let utc_abs = utc_raw.unsigned_abs();
    format!(
        "{}-{}-{} {}:{}:{}:{} {}{}",
        year, month, day, hour, minute, second, micro, sign, utc_abs
    )
}

fn read_u8(f: &mut File) -> io::Result<u8> {
    let mut buf = [0u8; 1];
    f.read_exact(&mut buf)?;
    Ok(buf[0])
}

fn read_u16_le(f: &mut File) -> io::Result<u16> {
    let mut buf = [0u8; 2];
    f.read_exact(&mut buf)?;
    Ok(u16::from_le_bytes(buf))
}

fn read_u32_le(f: &mut File) -> io::Result<u32> {
    let mut buf = [0u8; 4];
    f.read_exact(&mut buf)?;
    Ok(u32::from_le_bytes(buf))
}

fn read_bytes(f: &mut File, n: usize) -> io::Result<Vec<u8>> {
    let mut buf = vec![0u8; n];
    f.read_exact(&mut buf)?;
    Ok(buf)
}

fn skip_bytes(f: &mut File, n: u64) -> io::Result<()> {
    let current = f.stream_position()?;
    let target = current
        .checked_add(n)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "skip offset overflow"))?;
    let file_len = f.metadata()?.len();
    if target > file_len {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            format!("skip target {target} exceeds file length {file_len}"),
        ));
    }

    f.seek(SeekFrom::Start(target))?;
    Ok(())
}

fn read_string(f: &mut File, n: usize) -> io::Result<String> {
    let buf = read_bytes(f, n)?;
    let s = buf.split(|&b| b == 0).next().unwrap_or(&buf);
    Ok(String::from_utf8_lossy(s).into_owned())
}

fn sha256_file(path: &str) -> String {
    let Ok(mut f) = File::open(path) else {
        return String::new();
    };
    let mut hasher = Sha256::new();
    let mut buf = [0u8; 8192];
    loop {
        match f.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => hasher.update(&buf[..n]),
            Err(_) => return String::new(),
        }
    }
    format!("{:x}", hasher.finalize())
}

// Internal parsed representations

struct PldmHeader {
    identifier: String,
    format_revision: u8,
    release_datetime: String,
    component_bitmap_bit_length: u16,
    version_string: String,
}

#[derive(Clone)]
struct RawDescriptor {
    is_initial: bool,
    descriptor_type: u16,
    data: Vec<u8>,
    vendor_title: Option<String>,
    vendor_data: Option<Vec<u8>>,
}

struct DeviceIdRecord {
    component_image_set_version_string: String,
    applicable_components: u64,
    descriptors: Vec<RawDescriptor>,
}

struct ComponentImageInfo {
    identifier: u16,
    location_offset: u32,
    size: u32,
    version_string: String,
    /// Final on-disk path of the extracted image. `Some` only when the
    /// package was parsed with `do_unpack=true` (unpack command). Its
    /// presence is the signal that unpack-mode display fields
    /// (FWImageSHA256, FWImageSize, SignatureType, AP_SKU_ID, APSKU VDD
    /// transform) should be emitted.
    fw_image_name: Option<String>,
    /// SHA256 hex of the extracted image bytes (populated during unpack).
    sha256: Option<String>,
    /// Byte size of the extracted image (populated during unpack).
    image_size: Option<u64>,
}

struct PldmParsedData {
    header: PldmHeader,
    device_records: Vec<DeviceIdRecord>,
    component_images: Vec<ComponentImageInfo>,
}

impl PldmParsedData {
    fn parse(path: &str, do_unpack: bool, out_dir: &str) -> Result<Self, String> {
        let mut f = File::open(path).map_err(|e| format!("Cannot open {}: {}", path, e))?;

        let header = Self::parse_header(&mut f)?;
        let device_records = Self::parse_device_records(&mut f, &header)?;
        if header.format_revision > 1 {
            Self::parse_downstream_area(&mut f)?;
        }
        let mut component_images = Self::parse_component_images(&mut f, &header)?;
        // Skip header checksum (4 bytes) and payload checksum if present
        let _ = read_u32_le(&mut f); // header checksum
        if header.format_revision >= 4 {
            let _ = read_u32_le(&mut f); // payload checksum
        }

        if do_unpack {
            Self::extract_files(
                &mut f,
                &mut component_images,
                &device_records,
                path,
                out_dir,
            )?;
        }

        Ok(PldmParsedData {
            header,
            device_records,
            component_images,
        })
    }

    fn parse_header(f: &mut File) -> Result<PldmHeader, String> {
        let uuid_bytes = read_bytes(f, 16).map_err(|e| format!("Read error: {}", e))?;
        let identifier = format_uuid_bytes(&uuid_bytes);

        let mut valid_revision = None;
        for (known_uuid, _ver) in &PLDM_VALID_UUIDS {
            if uuid_bytes == *known_uuid {
                valid_revision = Some(_ver);
                break;
            }
        }
        if valid_revision.is_none() {
            return Err(format!("Not a valid PLDM package: UUID {}", identifier));
        }

        let format_revision = read_u8(f).map_err(|e| format!("Read error: {}", e))?;
        let _header_size = read_u16_le(f).map_err(|e| format!("Read error: {}", e))?;
        let ts_bytes = read_bytes(f, 13).map_err(|e| format!("Read error: {}", e))?;
        let release_datetime = parse_timestamp(&ts_bytes);
        let component_bitmap_bit_length =
            read_u16_le(f).map_err(|e| format!("Read error: {}", e))?;
        let _ver_string_type = read_u8(f).map_err(|e| format!("Read error: {}", e))?;
        let ver_string_len = read_u8(f).map_err(|e| format!("Read error: {}", e))?;
        let version_string =
            read_string(f, ver_string_len as usize).map_err(|e| format!("Read error: {}", e))?;

        Ok(PldmHeader {
            identifier,
            format_revision,
            release_datetime,
            component_bitmap_bit_length,
            version_string,
        })
    }

    fn parse_device_records(
        f: &mut File,
        header: &PldmHeader,
    ) -> Result<Vec<DeviceIdRecord>, String> {
        let count = read_u8(f).map_err(|e| format!("Read error: {}", e))?;
        let component_bitmap_bit_length = usize::from(header.component_bitmap_bit_length);
        if component_bitmap_bit_length > u64::BITS as usize {
            return Err(format!(
                "Unsupported component bitmap bit length: {component_bitmap_bit_length}"
            ));
        }
        let bitmap_bytes = component_bitmap_bit_length.div_ceil(8);
        let mut records = Vec::new();

        for _ in 0..count {
            let _record_length = read_u16_le(f).map_err(|e| format!("Read error: {}", e))?;
            let descriptor_count = read_u8(f).map_err(|e| format!("Read error: {}", e))?;
            let _device_update_flags = read_u32_le(f).map_err(|e| format!("Read error: {}", e))?;
            let _cisv_string_type = read_u8(f).map_err(|e| format!("Read error: {}", e))?;
            let cisv_string_len = read_u8(f).map_err(|e| format!("Read error: {}", e))?;
            let fw_pkg_data_len = read_u16_le(f).map_err(|e| format!("Read error: {}", e))?;

            if header.format_revision >= 4 {
                let ref_manifest_len = read_u32_le(f).map_err(|e| format!("Read error: {}", e))?;
                if ref_manifest_len > 0 {
                    skip_bytes(f, ref_manifest_len as u64)
                        .map_err(|e| format!("Skip reference manifest error: {}", e))?;
                }
            }

            let ac_bytes = read_bytes(f, bitmap_bytes).map_err(|e| format!("Read error: {}", e))?;
            let mut applicable_components: u64 = 0;
            for (i, &b) in ac_bytes.iter().enumerate() {
                applicable_components |= (b as u64) << (i * 8);
            }

            let component_image_set_version_string = read_string(f, cisv_string_len as usize)
                .map_err(|e| format!("Read error: {}", e))?;

            let mut descriptors = Vec::new();
            for j in 0..descriptor_count {
                let desc_type = read_u16_le(f).map_err(|e| format!("Read error: {}", e))?;
                let desc_len = read_u16_le(f).map_err(|e| format!("Read error: {}", e))?;

                if j == 0 {
                    let data = read_bytes(f, desc_len as usize)
                        .map_err(|e| format!("Read error: {}", e))?;
                    descriptors.push(RawDescriptor {
                        is_initial: true,
                        descriptor_type: desc_type,
                        data,
                        vendor_title: None,
                        vendor_data: None,
                    });
                } else if desc_type == 0xFFFF {
                    let desc_len = desc_len as usize;
                    if desc_len < 2 {
                        return Err(format!(
                            "Malformed vendor descriptor: length {desc_len} is shorter than the title metadata"
                        ));
                    }

                    let _title_type = read_u8(f).map_err(|e| format!("Read error: {}", e))?;
                    let title_len = read_u8(f).map_err(|e| format!("Read error: {}", e))?;
                    let title = read_string(f, title_len as usize)
                        .map_err(|e| format!("Read error: {}", e))?;

                    let vendor_header_len = 2 + title_len as usize;
                    if desc_len < vendor_header_len {
                        return Err(format!(
                            "Malformed vendor descriptor: length {desc_len} is shorter than title metadata plus title ({vendor_header_len} bytes)"
                        ));
                    }

                    let vd_data_len = desc_len - vendor_header_len;
                    let vd_data =
                        read_bytes(f, vd_data_len).map_err(|e| format!("Read error: {}", e))?;
                    descriptors.push(RawDescriptor {
                        is_initial: false,
                        descriptor_type: desc_type,
                        data: Vec::new(),
                        vendor_title: Some(title),
                        vendor_data: Some(vd_data),
                    });
                } else {
                    let data = read_bytes(f, desc_len as usize)
                        .map_err(|e| format!("Read error: {}", e))?;
                    descriptors.push(RawDescriptor {
                        is_initial: false,
                        descriptor_type: desc_type,
                        data,
                        vendor_title: None,
                        vendor_data: None,
                    });
                }
            }

            let _fw_pkg_data = read_bytes(f, fw_pkg_data_len as usize)
                .map_err(|e| format!("Read error: {}", e))?;

            records.push(DeviceIdRecord {
                component_image_set_version_string,
                applicable_components,
                descriptors,
            });
        }
        Ok(records)
    }

    fn parse_downstream_area(f: &mut File) -> Result<(), String> {
        let _count = read_u8(f).map_err(|e| format!("Read error: {}", e))?;
        Ok(())
    }

    fn parse_component_images(
        f: &mut File,
        header: &PldmHeader,
    ) -> Result<Vec<ComponentImageInfo>, String> {
        let count = read_u16_le(f).map_err(|e| format!("Read error: {}", e))?;
        let component_bitmap_bit_length = usize::from(header.component_bitmap_bit_length);
        if component_bitmap_bit_length > u64::BITS as usize {
            return Err(format!(
                "Unsupported component bitmap bit length: {component_bitmap_bit_length}"
            ));
        }
        if usize::from(count) > component_bitmap_bit_length {
            return Err(format!(
                "Unsupported component image count: {count} exceeds component bitmap bit length {component_bitmap_bit_length}"
            ));
        }
        let mut images = Vec::new();

        for _ in 0..count {
            let _classification = read_u16_le(f).map_err(|e| format!("Read error: {}", e))?;
            let identifier = read_u16_le(f).map_err(|e| format!("Read error: {}", e))?;
            let _comparison_stamp = read_bytes(f, 4).map_err(|e| format!("Read error: {}", e))?;
            let _options = read_u16_le(f).map_err(|e| format!("Read error: {}", e))?;
            let _activation_method = read_u16_le(f).map_err(|e| format!("Read error: {}", e))?;
            let location_offset = read_u32_le(f).map_err(|e| format!("Read error: {}", e))?;
            let size = read_u32_le(f).map_err(|e| format!("Read error: {}", e))?;
            let _ver_string_type = read_u8(f).map_err(|e| format!("Read error: {}", e))?;
            let ver_string_len = read_u8(f).map_err(|e| format!("Read error: {}", e))?;
            let version_string = read_string(f, ver_string_len as usize)
                .map_err(|e| format!("Read error: {}", e))?;

            if header.format_revision >= 3 {
                let opaque_len = read_u32_le(f).map_err(|e| format!("Read error: {}", e))?;
                if opaque_len > 0 {
                    skip_bytes(f, opaque_len as u64)
                        .map_err(|e| format!("Skip opaque data error: {}", e))?;
                }
            }

            images.push(ComponentImageInfo {
                identifier,
                location_offset,
                size,
                version_string,
                fw_image_name: None,
                sha256: None,
                image_size: None,
            });
        }
        Ok(images)
    }

    fn extract_files(
        f: &mut File,
        images: &mut [ComponentImageInfo],
        records: &[DeviceIdRecord],
        package_path: &str,
        out_dir: &str,
    ) -> Result<(), String> {
        let pkg_size = fs::metadata(package_path).map(|m| m.len()).unwrap_or(0);
        let out_path = Path::new(out_dir);
        if !out_path.exists() {
            fs::create_dir_all(out_path).map_err(|e| format!("Cannot create dir: {}", e))?;
        }

        for (index, img) in images.iter_mut().enumerate() {
            if (img.location_offset as u64) + (img.size as u64) > pkg_size {
                return Err(format!(
                    "Component offset {} + size {} exceeds package size {}",
                    img.location_offset, img.size, pkg_size
                ));
            }

            let base_name = Self::get_image_file_name(records, index, &img.version_string);
            if base_name.is_empty() {
                continue;
            }

            // Write the image to a temporary path, hashing as we go. After the
            // write completes, rename to include the first 8 hex chars of the
            // SHA256 before `_image.bin` (Python parity — see
            // mcuapp/fw_parser/fwpkg_unpack.py).
            let tmp_path = Self::extracted_file_path(out_path, &base_name)?;
            f.seek(SeekFrom::Start(img.location_offset as u64))
                .map_err(|e| format!("Seek error: {}", e))?;

            let mut out_file = File::create(&tmp_path)
                .map_err(|e| format!("Cannot create {}: {}", tmp_path.display(), e))?;
            let mut hasher = Sha256::new();
            let mut remaining = img.size as usize;
            let mut buf = [0u8; 8192];
            while remaining > 0 {
                let to_read = remaining.min(buf.len());
                f.read_exact(&mut buf[..to_read])
                    .map_err(|e| format!("Read error: {}", e))?;
                out_file
                    .write_all(&buf[..to_read])
                    .map_err(|e| format!("Write error: {}", e))?;
                hasher.update(&buf[..to_read]);
                remaining -= to_read;
            }
            // Close the file before rename (Windows-safe; harmless on Linux).
            drop(out_file);

            let sha_hex = format!("{:x}", hasher.finalize());
            let prefix = sha_hex.chars().take(8).collect::<String>();

            // Insert the 8-char hash between the existing suffix and the
            // trailing "_image.bin" (or ".fwpkg"). Matches Python's naming
            // convention: "<name>_<version>_<8chars>_image.bin".
            let final_name = if let Some(stripped) = base_name.strip_suffix("_image.bin") {
                format!("{}_{}_image.bin", stripped, prefix)
            } else if let Some(stripped) = base_name.strip_suffix(".fwpkg") {
                format!("{}_{}.fwpkg", stripped, prefix)
            } else {
                format!("{}_{}", base_name, prefix)
            };
            let final_path = Self::extracted_file_path(out_path, &final_name)?;
            if final_path != tmp_path {
                fs::rename(&tmp_path, &final_path).map_err(|e| {
                    format!(
                        "Cannot rename {} -> {}: {}",
                        tmp_path.display(),
                        final_path.display(),
                        e
                    )
                })?;
            }

            img.fw_image_name = Some(final_path.to_string_lossy().into_owned());
            img.sha256 = Some(sha_hex);
            img.image_size = Some(img.size as u64);
        }
        Ok(())
    }

    fn get_image_file_name(records: &[DeviceIdRecord], comp_index: usize, version: &str) -> String {
        let mask = 1u64 << comp_index;
        for rec in records {
            if rec.applicable_components & mask != mask {
                continue;
            }
            let name = &rec.component_image_set_version_string;
            let base = if name.contains(',') {
                let parts: Vec<&str> = name.split(',').collect();
                let mut count = 0usize;
                let mut ac = rec.applicable_components;
                for _ in 0..=comp_index {
                    if ac & 1 == 1 {
                        count += 1;
                    }
                    ac >>= 1;
                }
                parts
                    .get(count.saturating_sub(1))
                    .unwrap_or(&"")
                    .to_string()
            } else {
                name.clone()
            };
            if base.is_empty() {
                continue;
            }
            let mut fname = base.replace(':', "_").replace("_N/A", "");
            fname = format!("{}_{}", fname, version);
            fname = Self::sanitize_file_name_component(&fname);
            if fname.starts_with("FW-Package") {
                fname.push_str(".fwpkg");
            } else {
                fname.push_str("_image.bin");
            }
            let re = regex::Regex::new("_+").unwrap();
            return re.replace_all(&fname, "_").into_owned();
        }
        String::new()
    }

    fn sanitize_file_name_component(file_name: &str) -> String {
        file_name
            .chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() || c == '.' || c == '_' || c == '-' {
                    c
                } else {
                    '_'
                }
            })
            .collect()
    }

    fn extracted_file_path(out_dir: &Path, file_name: &str) -> Result<PathBuf, String> {
        if file_name.contains('/') || file_name.contains('\\') {
            return Err(format!(
                "Unsafe PLDM output file name contains path separators: {file_name}"
            ));
        }

        let mut components = Path::new(file_name).components();
        match (components.next(), components.next()) {
            (Some(Component::Normal(_)), None) => Ok(out_dir.join(file_name)),
            _ => Err(format!(
                "Unsafe PLDM output file name contains path components: {file_name}"
            )),
        }
    }

    fn get_component_name_for_index(
        records: &[DeviceIdRecord],
        comp_index: usize,
    ) -> Option<(String, &[RawDescriptor])> {
        let mask = 1u64 << comp_index;
        for rec in records {
            if rec.applicable_components & mask != mask {
                continue;
            }
            let name = &rec.component_image_set_version_string;
            let base = if name.contains(',') {
                let parts: Vec<&str> = name.split(',').collect();
                let mut count = 0usize;
                let mut ac = rec.applicable_components;
                for _ in 0..=comp_index {
                    if ac & 1 == 1 {
                        count += 1;
                    }
                    ac >>= 1;
                }
                parts
                    .get(count.saturating_sub(1))
                    .unwrap_or(&"")
                    .to_string()
            } else {
                name.clone()
            };
            if !base.is_empty() {
                return Some((base, &rec.descriptors));
            }
        }
        None
    }

    /// Build the display JSON matching Python's `prepare_records_json` output.
    fn to_display_json(&self, package_path: &str) -> Value {
        let sha = sha256_file(package_path);
        let header_json = json!({
            "PackageHeaderIdentifier": self.header.identifier,
            "PackageHeaderFormatRevision": format!("{}", self.header.format_revision),
            "PackageReleaseDateTime": self.header.release_datetime,
            "PackageVersionString": self.header.version_string,
            "PackageSHA256": sha,
        });

        let mut fw_records = Vec::new();
        for record in &self.device_records {
            let (components, descriptors) = self.build_record_output(record);
            fw_records.push(json!({
                "ComponentImageSetVersionString": record.component_image_set_version_string,
                "DeviceDescriptors": descriptors,
                "Components": components,
            }));
        }

        json!({
            "PackageHeaderInformation": header_json,
            "FirmwareDeviceRecords": fw_records,
        })
    }

    fn build_record_output(&self, record: &DeviceIdRecord) -> (Vec<Value>, Vec<Value>) {
        let indices =
            Self::get_applicable_indices(record.applicable_components, self.component_images.len());
        let device_name = &record.component_image_set_version_string;
        // Collect SKU info from vendor defined descriptors
        let mut ap_sku = "N/A".to_string();
        let mut ec_sku = "N/A".to_string();
        let mut ax_sku = "N/A".to_string();
        for desc in &record.descriptors {
            if desc.is_initial {
                continue;
            }
            if let (Some(title), Some(data)) = (&desc.vendor_title, &desc.vendor_data) {
                let hex_val = format!(
                    "0x{}",
                    data.iter()
                        .map(|b| format!("{:02x}", b))
                        .collect::<String>()
                );
                match title.as_str() {
                    "APSKU" => ap_sku = hex_val,
                    "ECSKU" => ec_sku = hex_val,
                    "AXSKU" => ax_sku = hex_val,
                    _ => {}
                }
            }
        }

        // Detect whether we're in unpack mode — i.e. at least one image in
        // this record was extracted. Unpack mode adds FWImageSHA256,
        // FWImageSize, SignatureType; renames APSKUID -> AP_SKU_ID; and
        // applies the APSKU VDD reverse-and-strip transform from
        // mcuapp/fw_parser/fwpkg_unpack.py (Python parity).
        let is_unpack = indices
            .iter()
            .any(|&i| self.component_images[i].fw_image_name.is_some());

        // Within a record, the APSKU transform applies only when there's at
        // least one non-Retimer component with an extracted FWImage — Python
        // keys off `components[-1]["FWImage"]` not containing "PCIeRetimer".
        let apply_apsku_transform = is_unpack
            && indices.iter().any(|&i| {
                self.component_images[i]
                    .fw_image_name
                    .as_deref()
                    .map(|n| !n.contains("PCIeRetimer"))
                    .unwrap_or(false)
            });

        // Pre-compute the transformed APSKU value ("reversed + last byte
        // stripped") so we can reuse it for both the AP_SKU_ID component
        // field and the overridden VendorDefinedDescriptorData below.
        let transformed_apsku = if apply_apsku_transform {
            record.descriptors.iter().find_map(|d| {
                if d.is_initial {
                    return None;
                }
                let (title, data) = match (&d.vendor_title, &d.vendor_data) {
                    (Some(t), Some(v)) if t == "APSKU" => (t, v),
                    _ => return None,
                };
                if data.len() < 2 {
                    return None;
                }
                // Strip trailing 1 byte, reverse remaining bytes, hex-encode.
                let mut stripped = data[..data.len() - 1].to_vec();
                stripped.reverse();
                let hex: String = stripped.iter().map(|b| format!("{:02x}", b)).collect();
                Some(format!("0x{}", hex))
            })
        } else {
            None
        };

        let mut components = Vec::new();
        let mut first_non_erot_assigned = false;
        for &img_idx in &indices {
            let img = &self.component_images[img_idx];
            let mut comp = json!({
                "ComponentIdentifier": format!("{:#x}", img.identifier),
                "ComponentVersionString": img.version_string,
            });
            if let Some(ref fw_name) = img.fw_image_name {
                comp["FWImage"] = json!(fw_name);
                // Python emits the trio below in the exact order:
                // FWImage -> FWImageSHA256 -> SignatureType -> FWImageSize.
                if let Some(ref sha) = img.sha256 {
                    comp["FWImageSHA256"] = json!(sha);
                }
                comp["SignatureType"] = json!("N/A");
                if let Some(sz) = img.image_size {
                    comp["FWImageSize"] = json!(sz);
                }
            }

            if img.identifier == 0xff00 {
                // In unpack mode Python omits ECSKUID entirely when no
                // ECSKU descriptor is present, and the component loop
                // doesn't write an ECSKUID field at all for the ERoT
                // variant (it's only populated by the extract-mode
                // formatter when the transform fires). Match that: only
                // emit ECSKUID in show_pkg_content mode, where it's
                // always set (possibly to "N/A").
                if !is_unpack {
                    comp["ECSKUID"] = json!(ec_sku);
                }
            } else if ax_sku != "N/A" && first_non_erot_assigned {
                comp["AXSKUID"] = json!(ax_sku);
            } else {
                // Unpack-mode rename: `APSKUID` -> `AP_SKU_ID` (Python naming)
                // with the reverse-and-strip-transformed value. Retimer
                // components keep the raw-hex APSKUID (their SKU is a vendor
                // id, not a real SKU — Python skips the transform for them).
                //
                // Python further omits the SKU field entirely in unpack
                // mode when there is no APSKU descriptor for this record
                // (i.e. `ap_sku == "N/A"`). Match that.
                let is_retimer_comp = img
                    .fw_image_name
                    .as_deref()
                    .map(|n| n.contains("PCIeRetimer"))
                    .unwrap_or(false);
                if is_unpack && !is_retimer_comp {
                    if let Some(ref ap_val) = transformed_apsku {
                        comp["AP_SKU_ID"] = json!(ap_val);
                    }
                    // No APSKU descriptor -> omit the field entirely.
                } else if is_unpack && is_retimer_comp {
                    // Retimer in unpack mode: omit SKU field.
                } else {
                    comp["APSKUID"] = json!(ap_sku);
                }
                first_non_erot_assigned = true;
            }
            components.push(comp);
        }

        // Build formatted descriptors
        let mut descriptors = Vec::new();
        for desc in &record.descriptors {
            let type_name = descriptor_type_name(desc.descriptor_type);
            if desc.is_initial {
                descriptors.push(json!({
                    "InitialDescriptorType": type_name,
                    "InitialDescriptorData": decode_descriptor_data(&type_name, &desc.data),
                }));
            } else if let (Some(title), Some(vd_data)) = (&desc.vendor_title, &desc.vendor_data) {
                // For APSKU descriptors on non-Retimer records during unpack,
                // override the VDD value with the transformed form to match
                // Python's output. All other descriptors use the raw-hex form.
                let hex_val = if title == "APSKU" && apply_apsku_transform {
                    transformed_apsku.clone().unwrap_or_else(|| {
                        format!(
                            "0x{}",
                            vd_data
                                .iter()
                                .map(|b| format!("{:02x}", b))
                                .collect::<String>()
                        )
                    })
                } else {
                    format!(
                        "0x{}",
                        vd_data
                            .iter()
                            .map(|b| format!("{:02x}", b))
                            .collect::<String>()
                    )
                };
                descriptors.push(json!({
                    "AdditionalDescriptorType": type_name,
                    "VendorDefinedDescriptorTitleString": title,
                    "VendorDefinedDescriptorData": hex_val,
                }));
            } else {
                descriptors.push(json!({
                    "AdditionalDescriptorType": type_name,
                    "AdditionalDescriptorData": decode_descriptor_data(&type_name, &desc.data),
                }));
            }
        }

        (components, descriptors)
    }

    fn get_applicable_indices(bitmap: u64, max_bits: usize) -> Vec<usize> {
        (0..max_bits)
            .filter(|&i| bitmap & (1u64 << i) != 0)
            .collect()
    }
}

// ---------------------------------------------------------------------------
// PLDM struct
// ---------------------------------------------------------------------------

/// Parses PLDM firmware packages using a native binary parser.
pub struct PLDM {
    pub m_pldm_dict: HashMap<String, Value>,
    pub apname_version_dict: HashMap<String, IndexMap<String, Vec<String>>>,
    pub unpack_file_ap_dict: HashMap<String, Vec<String>>,
    pub unpack_dirpath: String,
    pub verbose: bool,
    /// Cached parsed data from the most recent parse operation.
    last_parsed: Option<PldmParsedData>,
}

impl PLDM {
    pub fn new() -> Self {
        Self {
            m_pldm_dict: HashMap::new(),
            apname_version_dict: HashMap::new(),
            unpack_file_ap_dict: HashMap::new(),
            unpack_dirpath: String::new(),
            verbose: false,
            last_parsed: None,
        }
    }

    /// Parse (and optionally unpack) a PLDM firmware package using the
    /// native binary parser.
    ///
    /// On success the display JSON is stored in `m_pldm_dict` and the
    /// parsed data is cached in `last_parsed`.
    pub async fn unpack_pkg(
        &mut self,
        package_name: &str,
        out_dir: &str,
        unpack: bool,
    ) -> (bool, Option<Value>, String) {
        let package_name_owned = package_name.to_string();
        let out_dir_owned = out_dir.to_string();
        match tokio::task::spawn_blocking(move || {
            let parsed = PldmParsedData::parse(&package_name_owned, unpack, &out_dir_owned)?;
            let display_json = parsed.to_display_json(&package_name_owned);
            Ok::<_, String>((parsed, display_json))
        })
        .await
        {
            Ok(Ok((parsed, display_json))) => {
                self.m_pldm_dict
                    .insert(package_name.to_string(), display_json.clone());
                self.last_parsed = Some(parsed);
                (true, Some(display_json), String::new())
            }
            Ok(Err(e)) => (false, None, e),
            Err(e) => (false, None, format!("PLDM parser task failed: {}", e)),
        }
    }

    /// Build the AP-name to version mapping from the most recently parsed data.
    fn build_apname_version_from_parsed(&mut self) {
        let parsed = match self.last_parsed {
            Some(ref p) => p,
            None => return,
        };

        let mut ver_dict: IndexMap<String, Vec<String>> = IndexMap::new();

        for (index, img) in parsed.component_images.iter().enumerate() {
            if let Some((name, descriptors)) =
                PldmParsedData::get_component_name_for_index(&parsed.device_records, index)
            {
                let sku_id = Self::get_ap_sku_from_descriptors(descriptors, &name);
                let ap_name = format!("{},{}", name, sku_id);
                ver_dict.insert(
                    ap_name,
                    vec![img.version_string.clone(), sku_id.to_lowercase()],
                );
            }
        }

        self.apname_version_dict
            .insert(parsed.header.version_string.clone(), ver_dict);
    }

    fn get_ap_sku_from_descriptors(descriptors: &[RawDescriptor], ap_name: &str) -> String {
        let target = if ap_name.to_lowercase() == "erot" {
            "ECSKU"
        } else {
            "APSKU"
        };
        for desc in descriptors {
            if let (Some(title), Some(data)) = (&desc.vendor_title, &desc.vendor_data) {
                if title == target {
                    return format!(
                        "0x{}",
                        data.iter()
                            .map(|b| format!("{:02x}", b))
                            .collect::<String>()
                    );
                }
            }
        }
        String::new()
    }

    /// Build AP-name to [version, file_path] dict for unpacked images.
    pub async fn get_unpack_file_dict(&mut self, pkg_name: &str) {
        let tmp_dir =
            match tokio::task::spawn_blocking(|| tempfile::tempdir().map(|d| d.keep())).await {
                Ok(Ok(path)) => path,
                _ => return,
            };
        let out_dir = tmp_dir.to_string_lossy().to_string();
        self.unpack_dirpath = out_dir.clone();

        let (ok, _, _) = self.unpack_pkg(pkg_name, &out_dir, true).await;
        if !ok {
            return;
        }

        let parsed = match self.last_parsed {
            Some(ref p) => p,
            None => return,
        };

        let mut file_dict: HashMap<String, Vec<String>> = HashMap::new();
        for (index, img) in parsed.component_images.iter().enumerate() {
            if let Some((name, _)) =
                PldmParsedData::get_component_name_for_index(&parsed.device_records, index)
            {
                let fw_file = img.fw_image_name.clone().unwrap_or_default();
                file_dict.insert(name, vec![img.version_string.clone(), fw_file]);
            }
        }
        self.unpack_file_ap_dict = file_dict;
    }

    pub fn print_package(&self, package_name: &str) {
        if let Some(pkg_data) = self.m_pldm_dict.get(package_name) {
            let buf = Vec::new();
            let formatter = serde_json::ser::PrettyFormatter::with_indent(b"    ");
            let mut ser = serde_json::Serializer::with_formatter(buf, formatter);
            if pkg_data.serialize(&mut ser).is_ok() {
                if let Ok(pretty) = String::from_utf8(ser.into_inner()) {
                    println!("{}", pretty);
                }
            }
        }
    }
}

#[async_trait]
impl FirmwarePkg for PLDM {
    async fn parse_pkg(
        &mut self,
        package_name: &str,
        _json_dict: Option<&mut Value>,
    ) -> (bool, String) {
        let (status, _, err) = self.unpack_pkg(package_name, "./", false).await;
        if !status {
            let msg = format!(
                "Given input file {} is not a valid PLDM fwpkg",
                package_name
            );
            tracing::warn!("{}", msg);
            return (false, err);
        }

        let og_pkg_name = self
            .m_pldm_dict
            .get(package_name)
            .and_then(|d| d.get("PackageHeaderInformation"))
            .and_then(|h| h.get("PackageVersionString"))
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        if og_pkg_name.contains("HGX") && og_pkg_name.contains("DGX") {
            self.m_pldm_dict.remove(package_name);
            self.last_parsed = None;

            let tmp_dir =
                match tokio::task::spawn_blocking(|| tempfile::tempdir().map(|d| d.keep())).await {
                    Ok(Ok(path)) => path,
                    _ => {
                        let msg = "Failed to create temp dir for PLDM unpack".to_string();
                        tracing::warn!("{}", msg);
                        return (false, msg);
                    }
                };
            let outdir = tmp_dir.to_string_lossy().to_string();

            let (status, _, _) = self.unpack_pkg(package_name, &outdir, true).await;
            if !status {
                let _ = tokio::fs::remove_dir_all(&outdir).await;
                let msg = format!(
                    "Given input file {} is not a valid PLDM fwpkg",
                    package_name
                );
                tracing::warn!("{}", msg);
                return (false, msg);
            }

            let mut hgx_pkg_name = String::new();
            if let Some(pkg_dict) = self.m_pldm_dict.get(package_name) {
                if let Some(fw_records) = pkg_dict
                    .get("FirmwareDeviceRecords")
                    .and_then(|v| v.as_array())
                {
                    'outer: for fw_record in fw_records {
                        if let Some(components) =
                            fw_record.get("Components").and_then(|v| v.as_array())
                        {
                            for fw_comp in components {
                                let ver_str = fw_comp
                                    .get("ComponentVersionString")
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("");
                                if ver_str.contains("HGX") {
                                    hgx_pkg_name = fw_comp
                                        .get("FWImage")
                                        .and_then(|v| v.as_str())
                                        .unwrap_or("")
                                        .to_string();
                                    break 'outer;
                                }
                            }
                        }
                    }
                }
            }

            self.m_pldm_dict.remove(package_name);
            self.last_parsed = None;

            if !hgx_pkg_name.is_empty() {
                let (status, _, _) = self.unpack_pkg(&hgx_pkg_name, &outdir, false).await;
                let _ = tokio::fs::remove_dir_all(&outdir).await;
                if !status {
                    let msg = format!(
                        "Given input file {} is not a valid PLDM fwpkg",
                        hgx_pkg_name
                    );
                    tracing::warn!("{}", msg);
                    return (false, msg);
                }
                self.build_apname_version_from_parsed();
            } else {
                let _ = tokio::fs::remove_dir_all(&outdir).await;
            }
        } else {
            self.build_apname_version_from_parsed();
        }

        (true, String::new())
    }

    async fn remove_files(&mut self) {
        if !self.unpack_dirpath.is_empty() {
            let _ = tokio::fs::remove_dir_all(&self.unpack_dirpath).await;
            self.unpack_dirpath.clear();
        }
    }

    fn apname_version_dict(&self) -> &HashMap<String, IndexMap<String, Vec<String>>> {
        &self.apname_version_dict
    }

    fn print_package_content(&self, package_name: &str) {
        self.print_package(package_name);
    }

    async fn prepare_unpack_file_dict(&mut self, pkg_name: &str) {
        self.get_unpack_file_dict(pkg_name).await;
    }

    fn unpack_file_ap_dict(&self) -> &HashMap<String, Vec<String>> {
        &self.unpack_file_ap_dict
    }

    fn pldm_raw_dict(&self) -> Value {
        serde_json::json!(self.m_pldm_dict)
    }
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Cursor, Seek, SeekFrom, Write};

    fn write_test_tar(tar_path: &Path, entry_path: &str, content: &[u8]) {
        let tar_file = File::create(tar_path).expect("create test tar");
        let mut builder = tar::Builder::new(tar_file);
        let mut header = tar::Header::new_gnu();
        header.set_size(content.len() as u64);
        header.set_mode(0o644);
        header.set_cksum();
        builder
            .append_data(&mut header, entry_path, Cursor::new(content))
            .expect("append tar entry");
        builder.finish().expect("finish test tar");
    }

    fn write_test_tar_symlink(tar_path: &Path, entry_path: &str, link_name: &str) {
        let tar_file = File::create(tar_path).expect("create test tar");
        let mut builder = tar::Builder::new(tar_file);
        let mut header = tar::Header::new_gnu();
        header.set_entry_type(tar::EntryType::Symlink);
        header.set_size(0);
        header.set_mode(0o777);
        header.set_link_name(link_name).expect("set symlink target");
        header.set_cksum();
        builder
            .append_data(&mut header, entry_path, Cursor::new(Vec::<u8>::new()))
            .expect("append tar symlink");
        builder.finish().expect("finish test tar");
    }

    fn rewrite_first_tar_entry_name(tar_path: &Path, entry_path: &[u8]) {
        let mut tar_file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(tar_path)
            .expect("open test tar for rewrite");
        let mut header = [0u8; 512];
        tar_file
            .read_exact(&mut header)
            .expect("read first tar header");

        header[..100].fill(0);
        header[..entry_path.len()].copy_from_slice(entry_path);
        header[148..156].fill(b' ');
        let checksum: u32 = header.iter().map(|byte| *byte as u32).sum();
        let checksum_field = format!("{:06o}\0 ", checksum);
        header[148..156].copy_from_slice(checksum_field.as_bytes());

        tar_file
            .seek(SeekFrom::Start(0))
            .expect("seek first tar header");
        tar_file.write_all(&header).expect("rewrite tar header");
    }

    #[tokio::test]
    async fn test_tar_detection_non_tar() {
        assert!(!is_tar_file("/nonexistent/file.tar").await);
    }

    #[test]
    fn test_pldm_new() {
        let pldm = PLDM::new();
        assert!(pldm.m_pldm_dict.is_empty());
        assert!(pldm.apname_version_dict.is_empty());
    }

    #[tokio::test]
    async fn test_tarpkg_default_manifest() {
        let mut tar = TarPkg::new();
        let (ok, err) = tar.parse_manifest_file(None, "test.tar", true).await;
        assert!(ok);
        assert!(err.is_empty());
        assert!(tar.apname_version_dict.contains_key("test.tar"));
    }

    #[tokio::test]
    async fn test_tarpkg_parse_tar_manifest_async() {
        let temp = tempfile::tempdir().expect("create temp dir");
        let tar_path = temp.path().join("firmware.tar");
        write_test_tar(
            &tar_path,
            "MANIFEST",
            b"purpose=PSU\nversion=1.2.3\nmodel=LiteOn\n",
        );

        let package_name = tar_path.to_string_lossy().to_string();
        let mut tar = TarPkg::new();
        let (ok, err) = tar.parse_pkg(&package_name, None).await;

        assert!(ok, "{err}");
        let components = tar
            .apname_version_dict()
            .get(&package_name)
            .expect("package version dict");
        assert_eq!(
            components
                .get("psu_firmware")
                .expect("psu firmware metadata"),
            &vec!["1.2.3".to_string(), "LiteOn".to_string(), "psu".to_string()]
        );

        let unpacked_path = tar.untar_file_path.clone();
        assert!(!unpacked_path.is_empty());
        assert!(Path::new(&unpacked_path).exists());
        tar.remove_files().await;
        assert!(!Path::new(&unpacked_path).exists());
    }

    #[test]
    fn test_tarpkg_openbmc_versionpurpose_psu_is_psu() {
        let mut tar = TarPkg::new();
        let manifest = "purpose=xyz.openbmc_project.Software.Version.VersionPurpose.PSU\n\
                        version=V01\n\
                        extended_version=model=IPS495500-AIT-N1BA,manufacturer=MEGMEET\n";
        let (ok, err) = tar.parse_manifest_content(Some(manifest), "fw.tar", false);

        assert!(ok, "{err}");
        let components = tar
            .apname_version_dict()
            .get("fw.tar")
            .expect("package version dict");
        assert!(components.contains_key("psu_firmware"));
        assert!(!components.contains_key("bmc_firmware"));
        assert_eq!(
            components
                .get("psu_firmware")
                .expect("psu firmware metadata"),
            &vec![
                "V01".to_string(),
                "PowerShelf".to_string(),
                "psu".to_string()
            ]
        );
    }

    #[test]
    fn test_tarpkg_mcu_and_pldm_purpose_create_mcu_firmware() {
        for purpose in [
            "PLDM image",
            "MCU Firmware",
            "xyz.openbmc_project.Software.Version.VersionPurpose.MCU",
        ] {
            let mut tar = TarPkg::new();
            let manifest = format!("purpose={purpose}\nversion=V02B01\n");
            let (ok, err) = tar.parse_manifest_content(Some(&manifest), "fw.tar", false);

            assert!(ok, "{err}");
            let components = tar
                .apname_version_dict()
                .get("fw.tar")
                .expect("package version dict");
            assert!(components.contains_key("mcu_firmware"), "{purpose}");
            assert!(!components.contains_key("bmc_firmware"), "{purpose}");
            assert_eq!(
                components
                    .get("mcu_firmware")
                    .expect("mcu firmware metadata")[0],
                "V02B01"
            );
        }
    }

    #[test]
    fn test_tarpkg_final_component_token_wins() {
        let mut tar = TarPkg::new();
        let manifest = "purpose=xyz.PSU.Software.Version.VersionPurpose.MCU\nversion=V02B01\n";
        let (ok, err) = tar.parse_manifest_content(Some(manifest), "fw.tar", false);

        assert!(ok, "{err}");
        let components = tar
            .apname_version_dict()
            .get("fw.tar")
            .expect("package version dict");
        assert!(components.contains_key("mcu_firmware"));
        assert!(!components.contains_key("psu_firmware"));
    }

    #[tokio::test]
    async fn test_tarpkg_rejects_path_traversal() {
        let temp = tempfile::tempdir().expect("create temp dir");
        let tar_path = temp.path().join("evil.tar");
        write_test_tar(&tar_path, "safe/MANIFEST", b"purpose=PSU\nversion=1.2.3\n");
        rewrite_first_tar_entry_name(&tar_path, b"../MANIFEST");

        let package_name = tar_path.to_string_lossy().to_string();
        let mut tar = TarPkg::new();
        let (ok, err) = tar.parse_pkg(&package_name, None).await;

        assert!(!ok);
        assert!(err.contains("path traversal"), "{err}");
        assert!(tar.untar_file_path.is_empty());
    }

    #[tokio::test]
    async fn test_tarpkg_rejects_link_entries() {
        let temp = tempfile::tempdir().expect("create temp dir");
        let tar_path = temp.path().join("link.tar");
        write_test_tar_symlink(&tar_path, "MANIFEST", "/tmp/manifest");

        let package_name = tar_path.to_string_lossy().to_string();
        let mut tar = TarPkg::new();
        let (ok, err) = tar.parse_pkg(&package_name, None).await;

        assert!(!ok);
        assert!(err.contains("link entry"), "{err}");
        assert!(tar.untar_file_path.is_empty());
    }

    #[test]
    fn test_format_uuid() {
        let bytes = [
            0xF0, 0x18, 0x87, 0x8C, 0xCB, 0x7D, 0x49, 0x43, 0x98, 0x00, 0xA0, 0x2F, 0x05, 0x9A,
            0xCA, 0x02,
        ];
        assert_eq!(
            format_uuid_bytes(&bytes),
            "f018878c-cb7d-4943-9800-a02f059aca02"
        );
    }

    fn pldm_header(format_revision: u8) -> PldmHeader {
        pldm_header_with_bitmap_bits(format_revision, 8)
    }

    fn pldm_header_with_bitmap_bits(
        format_revision: u8,
        component_bitmap_bit_length: u16,
    ) -> PldmHeader {
        PldmHeader {
            identifier: "test".to_string(),
            format_revision,
            release_datetime: String::new(),
            component_bitmap_bit_length,
            version_string: String::new(),
        }
    }

    fn parse_device_record_bytes(bytes: &[u8]) -> Result<Vec<DeviceIdRecord>, String> {
        parse_device_record_bytes_with_revision(1, bytes)
    }

    fn parse_device_record_bytes_with_revision(
        format_revision: u8,
        bytes: &[u8],
    ) -> Result<Vec<DeviceIdRecord>, String> {
        parse_device_record_bytes_with_bitmap_bits(format_revision, 8, bytes)
    }

    fn parse_device_record_bytes_with_bitmap_bits(
        format_revision: u8,
        component_bitmap_bit_length: u16,
        bytes: &[u8],
    ) -> Result<Vec<DeviceIdRecord>, String> {
        let header = pldm_header_with_bitmap_bits(format_revision, component_bitmap_bit_length);
        let mut file = tempfile::tempfile().expect("create temp PLDM data");
        file.write_all(bytes).expect("write temp PLDM data");
        file.seek(SeekFrom::Start(0))
            .expect("rewind temp PLDM data");

        PldmParsedData::parse_device_records(&mut file, &header)
    }

    fn parse_component_image_bytes(
        format_revision: u8,
        bytes: &[u8],
    ) -> Result<Vec<ComponentImageInfo>, String> {
        parse_component_image_bytes_with_bitmap_bits(format_revision, 8, bytes)
    }

    fn parse_component_image_bytes_with_bitmap_bits(
        format_revision: u8,
        component_bitmap_bit_length: u16,
        bytes: &[u8],
    ) -> Result<Vec<ComponentImageInfo>, String> {
        let header = pldm_header_with_bitmap_bits(format_revision, component_bitmap_bit_length);
        let mut file = tempfile::tempfile().expect("create temp PLDM component image data");
        file.write_all(bytes)
            .expect("write temp PLDM component image data");
        file.seek(SeekFrom::Start(0))
            .expect("rewind temp PLDM component image data");

        PldmParsedData::parse_component_images(&mut file, &header)
    }

    fn device_record_with_vendor_descriptor(desc_len: u16, vendor_payload: &[u8]) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.push(1); // device record count
        bytes.extend_from_slice(&0u16.to_le_bytes()); // record length, unused
        bytes.push(2); // descriptor count
        bytes.extend_from_slice(&0u32.to_le_bytes()); // device update flags
        bytes.push(0); // component image set version string type
        bytes.push(0); // component image set version string length
        bytes.extend_from_slice(&0u16.to_le_bytes()); // firmware package data length
        bytes.push(1); // applicable component bitmap
        bytes.extend_from_slice(&0u16.to_le_bytes()); // initial descriptor type
        bytes.extend_from_slice(&0u16.to_le_bytes()); // initial descriptor length
        bytes.extend_from_slice(&0xFFFFu16.to_le_bytes()); // vendor-defined descriptor type
        bytes.extend_from_slice(&desc_len.to_le_bytes());
        bytes.extend_from_slice(vendor_payload);
        bytes
    }

    fn device_record_with_ref_manifest(ref_manifest_len: u32, ref_manifest: &[u8]) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.push(1); // device record count
        bytes.extend_from_slice(&0u16.to_le_bytes()); // record length, unused
        bytes.push(0); // descriptor count
        bytes.extend_from_slice(&0u32.to_le_bytes()); // device update flags
        bytes.push(0); // component image set version string type
        bytes.push(3); // component image set version string length
        bytes.extend_from_slice(&0u16.to_le_bytes()); // firmware package data length
        bytes.extend_from_slice(&ref_manifest_len.to_le_bytes());
        bytes.extend_from_slice(ref_manifest);
        bytes.push(0x05); // applicable component bitmap
        bytes.extend_from_slice(b"1.0");
        bytes
    }

    fn device_record_without_bitmap_payload() -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.push(1); // device record count
        bytes.extend_from_slice(&0u16.to_le_bytes()); // record length, unused
        bytes.push(0); // descriptor count
        bytes.extend_from_slice(&0u32.to_le_bytes()); // device update flags
        bytes.push(0); // component image set version string type
        bytes.push(3); // component image set version string length
        bytes.extend_from_slice(&0u16.to_le_bytes()); // firmware package data length
        bytes.extend_from_slice(b"1.0");
        bytes
    }

    fn component_image_with_opaque_data(opaque_len: u32, opaque_data: &[u8]) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&1u16.to_le_bytes()); // component image count
        bytes.extend_from_slice(&0u16.to_le_bytes()); // classification
        bytes.extend_from_slice(&0x1234u16.to_le_bytes()); // identifier
        bytes.extend_from_slice(&0u32.to_le_bytes()); // comparison stamp
        bytes.extend_from_slice(&0u16.to_le_bytes()); // options
        bytes.extend_from_slice(&0u16.to_le_bytes()); // activation method
        bytes.extend_from_slice(&0u32.to_le_bytes()); // location offset
        bytes.extend_from_slice(&0u32.to_le_bytes()); // size
        bytes.push(0); // version string type
        bytes.push(3); // version string length
        bytes.extend_from_slice(b"1.0");
        bytes.extend_from_slice(&opaque_len.to_le_bytes());
        bytes.extend_from_slice(opaque_data);
        bytes
    }

    fn component_image_count_only(count: u16) -> Vec<u8> {
        count.to_le_bytes().to_vec()
    }

    #[test]
    fn vendor_descriptor_rejects_length_shorter_than_title_metadata() {
        let bytes = device_record_with_vendor_descriptor(0, &[]);
        match parse_device_record_bytes(&bytes) {
            Err(err) => assert!(err.contains("Malformed vendor descriptor"), "{err}"),
            Ok(_) => panic!("malformed vendor descriptor should fail parsing"),
        }
    }

    #[test]
    fn vendor_descriptor_rejects_length_shorter_than_title() {
        let bytes = device_record_with_vendor_descriptor(2, &[0, 1, b'X']);
        match parse_device_record_bytes(&bytes) {
            Err(err) => assert!(err.contains("Malformed vendor descriptor"), "{err}"),
            Ok(_) => panic!("malformed vendor descriptor should fail parsing"),
        }
    }

    #[test]
    fn device_record_skips_rev4_reference_manifest() {
        let bytes = device_record_with_ref_manifest(4, b"skip");
        let records =
            parse_device_record_bytes_with_revision(4, &bytes).expect("parse device record");

        assert_eq!(records.len(), 1);
        assert_eq!(records[0].applicable_components, 0x05);
        assert_eq!(records[0].component_image_set_version_string, "1.0");
    }

    #[test]
    fn device_record_rejects_oversized_reference_manifest_without_misparse() {
        let bytes = device_record_with_ref_manifest(u32::MAX, &[]);

        match parse_device_record_bytes_with_revision(4, &bytes) {
            Err(err) => assert!(err.contains("Skip reference manifest error"), "{err}"),
            Ok(_) => panic!("oversized reference manifest should fail parsing"),
        }
    }

    #[test]
    fn device_record_rejects_bitmap_length_that_would_wrap_u16_add() {
        let bytes = device_record_without_bitmap_payload();

        match parse_device_record_bytes_with_bitmap_bits(1, u16::MAX, &bytes) {
            Err(err) => assert!(
                err.contains("Unsupported component bitmap bit length"),
                "{err}"
            ),
            Ok(_) => panic!("oversized bitmap length should not wrap to a zero-byte bitmap"),
        }
    }

    #[test]
    fn component_image_rejects_oversized_opaque_data_without_allocation() {
        let bytes = component_image_with_opaque_data(u32::MAX, &[]);

        match parse_component_image_bytes(3, &bytes) {
            Err(err) => assert!(err.contains("Skip opaque data error"), "{err}"),
            Ok(_) => panic!("oversized opaque data should fail parsing"),
        }
    }

    #[test]
    fn component_image_rejects_count_that_exceeds_header_bitmap_width() {
        let bytes = component_image_count_only(33);

        match parse_component_image_bytes_with_bitmap_bits(1, 32, &bytes) {
            Err(err) => assert!(
                err.contains("exceeds component bitmap bit length 32"),
                "{err}"
            ),
            Ok(_) => panic!("oversized component image count should fail parsing"),
        }
    }

    #[test]
    fn component_image_rejects_count_that_exceeds_u64_bitmap_width() {
        let bytes = component_image_count_only(65);

        match parse_component_image_bytes_with_bitmap_bits(1, 64, &bytes) {
            Err(err) => assert!(
                err.contains("exceeds component bitmap bit length 64"),
                "{err}"
            ),
            Ok(_) => panic!("component image count beyond u64 bitmap width should fail parsing"),
        }
    }

    #[test]
    fn component_image_skips_rev3_opaque_data() {
        let bytes = component_image_with_opaque_data(4, b"skip");
        let images = parse_component_image_bytes(3, &bytes).expect("parse component image");

        assert_eq!(images.len(), 1);
        assert_eq!(images[0].identifier, 0x1234);
        assert_eq!(images[0].version_string, "1.0");
    }

    fn device_record_for_image_name(name: &str) -> DeviceIdRecord {
        DeviceIdRecord {
            component_image_set_version_string: name.to_string(),
            applicable_components: 1,
            descriptors: Vec::new(),
        }
    }

    #[test]
    fn pldm_image_file_name_sanitizes_path_separators() {
        let records = vec![device_record_for_image_name("../../tmp/owned")];
        let name = PldmParsedData::get_image_file_name(&records, 0, "../v1\\bad");

        assert!(!name.contains('/'), "{name}");
        assert!(!name.contains('\\'), "{name}");
        assert!(
            Path::new(&name)
                .components()
                .all(|component| matches!(component, Component::Normal(_))),
            "{name}"
        );
        assert!(name.ends_with("_image.bin"), "{name}");
    }

    #[test]
    fn pldm_image_file_name_preserves_safe_package_name() {
        let records = vec![device_record_for_image_name("FW-Package:HGX_N/A")];
        let name = PldmParsedData::get_image_file_name(&records, 0, "1.2-rc_3");

        assert_eq!(name, "FW-Package_HGX_1.2-rc_3.fwpkg");
    }

    #[test]
    fn pldm_extracted_file_path_rejects_path_components() {
        let out_dir = Path::new("/tmp/pldm-out");

        for file_name in ["../owned.bin", "nested/owned.bin", "nested\\owned.bin", "."] {
            assert!(
                PldmParsedData::extracted_file_path(out_dir, file_name).is_err(),
                "{file_name}"
            );
        }

        assert_eq!(
            PldmParsedData::extracted_file_path(out_dir, "safe_name-1.2.bin").unwrap(),
            out_dir.join("safe_name-1.2.bin")
        );
    }

    #[test]
    fn test_padded_hex_le() {
        assert_eq!(padded_hex_le(&[0x47, 0x16, 0x00, 0x00]), "0x00001647");
        assert_eq!(padded_hex_le(&[0x03, 0x1a]), "0x1a03");
    }

    #[test]
    fn test_timestamp() {
        // year=2042 (0x07FA): ts[10]=0xFA (low), ts[11]=0x07 (high)
        let ts = [0u8, 0, 0, 0, 0, 11, 48, 18, 24, 3, 0xFA, 0x07, 0x00];
        let s = parse_timestamp(&ts);
        assert!(s.contains("2042"));
    }

    // ---------------------------------------------------------------
    // descriptor_type_name
    // ---------------------------------------------------------------

    #[test]
    fn test_descriptor_type_name_known() {
        assert_eq!(descriptor_type_name(0x0000), "PCI Vendor ID");
        assert_eq!(descriptor_type_name(0x0002), "UUID");
        assert_eq!(descriptor_type_name(0x0100), "PCI Device ID");
        assert_eq!(descriptor_type_name(0xFFFF), "Vendor Defined");
    }

    #[test]
    fn test_descriptor_type_name_unknown() {
        let name = descriptor_type_name(0x9999);
        assert!(name.contains("0x9999"));
    }

    // ---------------------------------------------------------------
    // is_little_endian_descriptor
    // ---------------------------------------------------------------

    #[test]
    fn test_le_descriptor() {
        assert!(is_little_endian_descriptor("PCI Vendor ID"));
        assert!(is_little_endian_descriptor("IANA Enterprise ID"));
        assert!(!is_little_endian_descriptor("UUID"));
        assert!(!is_little_endian_descriptor("Vendor Defined"));
    }

    // ---------------------------------------------------------------
    // raw_hex
    // ---------------------------------------------------------------

    #[test]
    fn test_raw_hex() {
        assert_eq!(raw_hex(&[0xDE, 0xAD, 0xBE, 0xEF]), "0xdeadbeef");
        assert_eq!(raw_hex(&[0x00, 0xFF]), "0x00ff");
    }

    // ---------------------------------------------------------------
    // decode_descriptor_data
    // ---------------------------------------------------------------

    #[test]
    fn test_decode_le_descriptor() {
        let result = decode_descriptor_data("PCI Vendor ID", &[0x47, 0x16]);
        assert_eq!(result, "0x1647");
    }

    #[test]
    fn test_decode_non_le_descriptor() {
        let result = decode_descriptor_data("UUID", &[0x01, 0x02]);
        assert_eq!(result, "0x0102");
    }

    // ---------------------------------------------------------------
    // format_uuid_bytes edge cases
    // ---------------------------------------------------------------

    #[test]
    fn test_format_uuid_short() {
        assert_eq!(format_uuid_bytes(&[0x01, 0x02]), "");
    }

    #[test]
    fn test_format_uuid_exact_16() {
        let bytes: Vec<u8> = (0..16).collect();
        let result = format_uuid_bytes(&bytes);
        assert_eq!(result, "00010203-0405-0607-0809-0a0b0c0d0e0f");
    }

    // ---------------------------------------------------------------
    // padded_hex_le edge cases
    // ---------------------------------------------------------------

    #[test]
    fn test_padded_hex_le_empty() {
        assert_eq!(padded_hex_le(&[]), "0x");
    }

    #[test]
    fn test_padded_hex_le_single_byte() {
        assert_eq!(padded_hex_le(&[0x42]), "0x42");
    }

    // ---------------------------------------------------------------
    // get_ap_sku_from_descriptors
    // ---------------------------------------------------------------

    #[test]
    fn test_get_ap_sku_apsku() {
        let descs = vec![RawDescriptor {
            is_initial: false,
            descriptor_type: 0xFFFF,
            data: vec![],
            vendor_title: Some("APSKU".into()),
            vendor_data: Some(vec![0x12, 0x34, 0xAB]),
        }];
        let result = PLDM::get_ap_sku_from_descriptors(&descs, "GPU");
        assert_eq!(result, "0x1234ab");
    }

    #[test]
    fn test_get_ap_sku_ecsku_for_erot() {
        let descs = vec![
            RawDescriptor {
                is_initial: false,
                descriptor_type: 0xFFFF,
                data: vec![],
                vendor_title: Some("ECSKU".into()),
                vendor_data: Some(vec![0xFF, 0x01]),
            },
            RawDescriptor {
                is_initial: false,
                descriptor_type: 0xFFFF,
                data: vec![],
                vendor_title: Some("APSKU".into()),
                vendor_data: Some(vec![0xAA]),
            },
        ];
        let result = PLDM::get_ap_sku_from_descriptors(&descs, "erot");
        assert_eq!(result, "0xff01");
    }

    #[test]
    fn test_get_ap_sku_missing() {
        let descs = vec![RawDescriptor {
            is_initial: false,
            descriptor_type: 0xFFFF,
            data: vec![],
            vendor_title: Some("OTHER".into()),
            vendor_data: Some(vec![0x00]),
        }];
        let result = PLDM::get_ap_sku_from_descriptors(&descs, "GPU");
        assert_eq!(result, "");
    }

    // ---------------------------------------------------------------
    // PLDM struct basic operations
    // ---------------------------------------------------------------

    #[test]
    fn test_pldm_apname_version_dict_empty() {
        let pldm = PLDM::new();
        assert!(pldm.apname_version_dict().is_empty());
    }

    // ---------------------------------------------------------------
    // TarPkg basic operations
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn test_tarpkg_apname_version_dict() {
        let mut tar = TarPkg::new();
        tar.parse_manifest_file(None, "my_pkg.tar", true).await;
        let dict = tar.apname_version_dict();
        assert!(dict.contains_key("my_pkg.tar"));
    }

    // ---------------------------------------------------------------
    // get_applicable_indices
    // ---------------------------------------------------------------

    #[test]
    fn test_get_applicable_indices() {
        let result = PldmParsedData::get_applicable_indices(0b1011, 4);
        assert_eq!(result, vec![0, 1, 3]);
    }

    #[test]
    fn test_get_applicable_indices_empty() {
        let result = PldmParsedData::get_applicable_indices(0, 8);
        assert!(result.is_empty());
    }

    #[test]
    fn test_get_applicable_indices_all_set() {
        let result = PldmParsedData::get_applicable_indices(0xFF, 8);
        assert_eq!(result, vec![0, 1, 2, 3, 4, 5, 6, 7]);
    }
}
