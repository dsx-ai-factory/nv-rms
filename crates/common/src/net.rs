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

//! Network/URL safety helpers shared by the workspace HTTP transports.
//!
//! The single primitive here, [`resolve_same_origin`], turns a device-supplied
//! request endpoint into a fully-resolved URL only when doing so cannot move
//! the request off the client's own authority. Both the Redfish `HttpClient`
//! and the NVUE `Transport` build request URLs from an authority-only base plus
//! a path that may be read verbatim from a device response, so they share this
//! guard rather than each hand-parsing.

use url::{ParseError, Url};

/// Why [`resolve_same_origin`] refused to build a request URL.
#[derive(Debug)]
pub enum EndpointRejection {
    /// The authority-only base URL did not parse as an absolute URL. Carries
    /// the parser error so a caller can log the cause; the base is typically
    /// derived from a caller-supplied host, so treat this as bad input rather
    /// than an internal fault.
    UnparseableBase(ParseError),

    /// The endpoint did not resolve to a safe request target: it was not an
    /// absolute path, moved off the base's origin, carried dot-segment
    /// traversal, or (when a prefix was required) resolved outside that
    /// subtree. The offending value is intentionally *not* carried so it
    /// cannot leak into a returned error; log it at the call site if needed.
    OffOriginOrMalformed,
}

/// True when a URL path segment is a WHATWG "double-dot" (parent) segment.
///
/// `Url::join` resolves not only a literal `..` but its percent-encoded forms
/// (`.%2e`, `%2e.`, `%2e%2e`, case-insensitive) to a path pop, so a raw
/// `== ".."` check misses them: an endpoint like `/redfish/v1/%2e%2e/admin`
/// carries no literal `..` yet resolves to `/redfish/admin`. Rejecting all of
/// these up front keeps the traversal guard sound even for a caller that sets
/// no `required_prefix` fence to catch the post-resolution path shift.
fn is_double_dot_segment(segment: &str) -> bool {
    matches!(
        segment.to_ascii_lowercase().as_str(),
        ".." | ".%2e" | "%2e." | "%2e%2e"
    )
}

/// True when the *path* portion of `endpoint` carries a percent-encoded path
/// separator: `%2f`/`%2F` for `/` or `%5c`/`%5C` for `\`.
///
/// The WHATWG parser preserves these encodings, so the segment split never sees
/// them as separators and `url.path()` still sits under the prefix. But a
/// BMC/NVUE HTTP stack that decodes `%2f`/`%5c` before routing or normalizing
/// would then see real separators -- turning e.g. `/redfish/v1/%2f..%2fadmin`
/// into `/redfish/admin` and escaping the allowed API tree on the same origin
/// (with Basic auth attached). Reject them before `join` so the guard holds
/// regardless of downstream decoding.
///
/// Only the path is inspected (everything before the first `?` or `#`); query
/// strings stay exempt so legitimate values like `rev=changeset%2f42` still
/// work.
fn has_encoded_path_separator(endpoint: &str) -> bool {
    let path = endpoint
        .split_once(['?', '#'])
        .map_or(endpoint, |(path, _)| path)
        .to_ascii_lowercase();
    path.contains("%2f") || path.contains("%5c")
}

/// Resolve a request `endpoint` against an authority-only `base_url`, refusing
/// anything that would move the request off the base's origin, that is not a
/// well-formed absolute path, that carries dot-segment traversal, or (when
/// `required_prefix` is `Some`) that resolves outside that API subtree.
///
/// Device-supplied endpoints (Redfish `target`/`@odata.id`, or any
/// caller-provided NVUE path) are frequently read verbatim from responses.
/// Because `base_url` is authority-only (`scheme://host:port`, no path), naive
/// string concatenation would let a crafted value such as `//evil/x`,
/// `https://evil/x`, or `@evil/x` reopen the URL authority and steer the
/// request -- and any Basic-auth credentials -- to an attacker-chosen host.
///
/// The endpoint is resolved with the `url` crate (the WHATWG parser reqwest
/// also uses) and the *authoritative* guard is that the resolved URL must share
/// the base's [`Origin`](url::Url::origin) (scheme, host, port). The
/// absolute-path, dot-segment, and prefix checks add well-formedness,
/// traversal, and protocol fences on top.
///
/// `required_prefix`, when set, must already be normalized to an absolute path
/// with no trailing slash (e.g. `/redfish/v1`, `/nvue_v1`). The match is exact
/// or at a `/` boundary, so `/redfish/v1` admits `/redfish/v1` and
/// `/redfish/v1/...` but not a sibling such as `/redfish/v1beta`.
pub fn resolve_same_origin(
    base_url: &str,
    endpoint: &str,
    required_prefix: Option<&str>,
) -> Result<Url, EndpointRejection> {
    let base = Url::parse(base_url).map_err(EndpointRejection::UnparseableBase)?;

    let is_absolute_path = endpoint.starts_with('/');
    // Split on both separators: `Url::join` normalizes backslashes to slashes
    // for special (http/https) schemes, so `/redfish\v1\..\admin` would resolve
    // to `/redfish/admin`. Splitting only on `/` would miss that `..`, letting
    // backslash-separated traversal slip past this guard when no prefix fence is
    // set to catch the post-resolution path shift.
    let has_traversal_segment = endpoint.split(['/', '\\']).any(is_double_dot_segment);
    // Encoded separators (`%2f`/`%5c`) survive `join` intact, so the split
    // above cannot see them; reject them in the path so a stack that decodes
    // before routing cannot smuggle traversal past the prefix/origin fence.
    let has_encoded_separator = has_encoded_path_separator(endpoint);

    // `join` returns Err on an unparseable reference; treat that the same as
    // any other rejection. The origin comparison is what actually blocks a
    // host swap -- it catches `//host`, absolute URLs, and parser quirks (e.g.
    // backslash normalization) alike.
    let resolved = base
        .join(endpoint)
        .ok()
        .filter(|resolved| resolved.origin() == base.origin());

    let within_allowed_tree = |url: &Url| match required_prefix {
        Some(prefix) => {
            let path = url.path();
            path == prefix
                || path
                    .strip_prefix(prefix)
                    .is_some_and(|rest| rest.starts_with('/'))
        }
        None => true,
    };

    match resolved {
        Some(url)
            if is_absolute_path
                && !has_traversal_segment
                && !has_encoded_separator
                && within_allowed_tree(&url) =>
        {
            Ok(url)
        }
        _ => Err(EndpointRejection::OffOriginOrMalformed),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const BASE: &str = "https://10.0.0.1:8443";

    #[test]
    fn accepts_nested_absolute_paths_on_same_origin() {
        // Device-supplied Redfish action targets are absolute and must keep
        // working, including ones carrying `@`/`:` in the path: these resolve
        // to the same origin, so they are not host swaps.
        for endpoint in [
            "/redfish/v1/Systems/System_0",
            "/redfish/v1/Chassis/powershelf/Actions/Chassis.ForceOff",
            "/redfish/v1/Managers/BMC@0",
            "/nvue_v1/system/gnmi-server?rev=changeset%2f42",
        ] {
            let url = resolve_same_origin(BASE, endpoint, None)
                .unwrap_or_else(|_| panic!("endpoint {endpoint:?} must resolve"));
            assert_eq!(url.origin(), Url::parse(BASE).unwrap().origin());
        }
    }

    #[test]
    fn rejects_authority_reopening_endpoints() {
        // Each would otherwise redirect the request (and any Basic-auth) to
        // another host, or is otherwise not a well-formed absolute path.
        for endpoint in [
            "@evil.com/x",           // userinfo-style: not an absolute path
            "evil.com/x",            // relative -> not an absolute path
            "//evil.com/x",          // protocol-relative authority (origin swap)
            "///evil.com/x",         // authority via extra slashes (origin swap)
            "https://evil.com/x",    // absolute URL with scheme (origin swap)
            "http://evil.com/x",     // scheme + host swap
            "\\evil.com/x",          // backslash normalization
            "/\\evil.com/x",         // slash-then-backslash authority trick
            "",                      // empty
            "/redfish/../../secret", // path traversal
        ] {
            assert!(
                matches!(
                    resolve_same_origin(BASE, endpoint, None),
                    Err(EndpointRejection::OffOriginOrMalformed)
                ),
                "endpoint {endpoint:?} must be rejected"
            );
        }
    }

    #[test]
    fn rejects_percent_encoded_traversal_without_prefix_fence() {
        // `Url::join` normalizes percent-encoded dot segments to a path pop, so
        // these carry no literal `..` yet would climb the tree even with no
        // prefix fence to catch the post-resolution shift.
        for endpoint in [
            "/redfish/v1/../admin",
            "/redfish/v1/%2e%2e/admin",
            "/redfish/v1/%2E%2E/admin",
            "/redfish/v1/.%2e/admin",
            "/redfish/v1/%2e./admin",
            "/a/b/%2e%2e/%2e%2e/etc/pwd",
        ] {
            assert!(
                matches!(
                    resolve_same_origin(BASE, endpoint, None),
                    Err(EndpointRejection::OffOriginOrMalformed)
                ),
                "traversal endpoint {endpoint:?} must be rejected"
            );
        }

        // A `%2e` that is only *part* of a segment (a real filename, not a dot
        // segment) must not be mistaken for traversal.
        assert!(
            resolve_same_origin(BASE, "/redfish/v1/firmware%2ebin", None).is_ok(),
            "an encoded dot inside a longer segment is not traversal"
        );
    }

    #[test]
    fn rejects_backslash_separated_traversal_without_prefix_fence() {
        // `Url::join` normalizes backslashes to slashes for special schemes, so
        // these collapse to a climbed path (e.g. `/redfish/admin`). The
        // traversal guard must treat `\` and `/` as equivalent separators, or
        // they slip past with no prefix fence to catch the post-resolution shift.
        for endpoint in [
            r"/redfish\v1\..\admin", // all backslash separators
            r"/redfish/v1\..\admin", // mixed separators
            r"/a\b\..\..\etc\pwd",   // stacked backslash traversal
        ] {
            assert!(
                matches!(
                    resolve_same_origin(BASE, endpoint, None),
                    Err(EndpointRejection::OffOriginOrMalformed)
                ),
                "backslash traversal {endpoint:?} must be rejected"
            );
        }
    }

    #[test]
    fn rejects_encoded_path_separators_that_could_be_decoded_downstream() {
        // `Url::join` keeps `%2f`/`%5c` encoded, so they pass the origin,
        // segment-split, and prefix checks as-is. A BMC/NVUE stack that decodes
        // them before routing would then see `/` or `\` and could climb out of
        // the allowed tree, so reject them in the path up front.
        for endpoint in [
            "/redfish/v1/%2f..%2fadmin",
            "/redfish/v1/%2F..%2Fadmin",
            "/nvue_v1/%5c..%5credfish/v1/x",
            "/nvue_v1/%5C..%5Cx",
            "/redfish/v1/Systems/%2e%2e%2fadmin", // encoded `../` via %2f
            "/redfish/v1/a%2fb",                  // bare encoded slash in a segment
        ] {
            assert!(
                matches!(
                    resolve_same_origin(BASE, endpoint, None),
                    Err(EndpointRejection::OffOriginOrMalformed)
                ),
                "encoded-separator endpoint {endpoint:?} must be rejected"
            );
            // Also rejected when the matching prefix would otherwise admit it.
            let prefix = if endpoint.starts_with("/nvue_v1") {
                "/nvue_v1"
            } else {
                "/redfish/v1"
            };
            assert!(
                matches!(
                    resolve_same_origin(BASE, endpoint, Some(prefix)),
                    Err(EndpointRejection::OffOriginOrMalformed)
                ),
                "encoded-separator endpoint {endpoint:?} must be rejected under {prefix:?}"
            );
        }
    }

    #[test]
    fn allows_encoded_separators_confined_to_the_query_string() {
        // The smuggling risk is only in the path; a `%2f`/`%5c` inside the
        // query is a legitimate opaque value and must keep working.
        for endpoint in [
            "/nvue_v1/system/gnmi-server?rev=changeset%2f42",
            "/redfish/v1/Systems?filter=a%5cb",
            "/redfish/v1/Systems#frag%2fnot-a-path",
        ] {
            assert!(
                resolve_same_origin(BASE, endpoint, None).is_ok(),
                "query/fragment-only encoded separator {endpoint:?} must resolve"
            );
        }
    }

    #[test]
    fn enforces_required_prefix_at_path_boundary() {
        for prefix in ["/redfish/v1", "/nvue_v1"] {
            // The prefix itself and children resolve...
            assert!(resolve_same_origin(BASE, prefix, Some(prefix)).is_ok());
            assert!(
                resolve_same_origin(BASE, &format!("{prefix}/Systems/x"), Some(prefix)).is_ok()
            );

            // ...while off-tree, sibling, and other-protocol paths are refused.
            for suffix in ["", "beta/x", "0/Systems"] {
                let sibling = format!("{prefix}{suffix}/x");
                if suffix.is_empty() {
                    continue;
                }
                assert!(
                    matches!(
                        resolve_same_origin(BASE, &sibling, Some(prefix)),
                        Err(EndpointRejection::OffOriginOrMalformed)
                    ),
                    "sibling {sibling:?} must be rejected for prefix {prefix:?}"
                );
            }
            for endpoint in ["/", "/admin/backdoor", "/other/tree"] {
                assert!(
                    matches!(
                        resolve_same_origin(BASE, endpoint, Some(prefix)),
                        Err(EndpointRejection::OffOriginOrMalformed)
                    ),
                    "off-tree {endpoint:?} must be rejected for prefix {prefix:?}"
                );
            }
        }
    }

    #[test]
    fn unparseable_base_is_reported_distinctly() {
        assert!(matches!(
            resolve_same_origin("not a url", "/redfish/v1", None),
            Err(EndpointRejection::UnparseableBase(_))
        ));
    }

    #[test]
    fn is_double_dot_segment_matches_whatwg_forms() {
        for segment in ["..", "%2e%2e", "%2E%2E", ".%2e", "%2e.", ".%2E", "%2E."] {
            assert!(is_double_dot_segment(segment), "{segment:?} is a `..` form");
        }
        for segment in ["", ".", "%2e", "v1", "redfish", "foo..bar", "%2e%2e%2f"] {
            assert!(
                !is_double_dot_segment(segment),
                "{segment:?} is not a `..` segment"
            );
        }
    }
}
