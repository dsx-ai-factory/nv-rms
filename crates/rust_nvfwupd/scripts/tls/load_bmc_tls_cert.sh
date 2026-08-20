#!/usr/bin/env bash
set -euo pipefail

# NOTE: This helper is retained for lab TLS validation and is intended for use
# once customer discussions for NVFWUPD TLS enablement have completed.

usage() {
    cat <<'USAGE'
Upload a generated server key/certificate to an OpenBMC-style HTTPS certificate slot.

Usage:
  load_bmc_tls_cert.sh --ip <bmc-ip> --user <user> --password <password> --cert-dir <dir>
  load_bmc_tls_cert.sh --ip <bmc-ip> --user <user> --password <password> --server-key <key> --server-cert <cert>

Options:
  --certificate-uri <uri>  Optional Redfish certificate resource URI.
                           If omitted, the script tries common OpenBMC manager IDs.

Examples:
  crates/rust_nvfwupd/scripts/tls/load_bmc_tls_cert.sh \
    --ip 192.0.2.10 \
    --user username \
    --password password \
    --cert-dir /tmp/bmc-192.0.2.10-tls

  crates/rust_nvfwupd/scripts/tls/load_bmc_tls_cert.sh \
    --ip 192.0.2.10 \
    --user username \
    --password password \
    --cert-dir /tmp/bmc-192.0.2.10-tls \
    --certificate-uri /redfish/v1/Managers/BMC_0/NetworkProtocol/HTTPS/Certificates/1

Notes:
  This replaces the BMC HTTPS server certificate. It does not enable client
  certificate authentication and does not install a BMC truststore CA.

  Some BMCs, including some LiteOn PowerShelf firmware, expose an empty HTTPS
  certificate collection and cannot replace /Certificates/1. The script
  automatically falls back to posting the certificate to the collection itself
  when the BMC reports that shape.

  This test/development helper passes credentials to curl with -u user:password,
  which can expose them in local process listings. Use only in controlled test
  environments.
USAGE
}

die() {
    echo "error: $*" >&2
    exit 1
}

require_cmd() {
    command -v "$1" >/dev/null 2>&1 || die "required command not found: $1"
}

json_escape_pem_file() {
    awk '{
        gsub(/\\/, "\\\\");
        gsub(/"/, "\\\"");
        printf "%s\\n", $0
    }' "$1"
}

ip=""
user=""
password=""
cert_dir=""
server_key=""
server_cert=""
certificate_uri=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --ip)
            [[ $# -ge 2 ]] || die "--ip requires a value"
            ip="$2"
            shift 2
            ;;
        --user)
            [[ $# -ge 2 ]] || die "--user requires a value"
            user="$2"
            shift 2
            ;;
        --password)
            [[ $# -ge 2 ]] || die "--password requires a value"
            password="$2"
            shift 2
            ;;
        --cert-dir)
            [[ $# -ge 2 ]] || die "--cert-dir requires a value"
            cert_dir="$2"
            shift 2
            ;;
        --server-key)
            [[ $# -ge 2 ]] || die "--server-key requires a value"
            server_key="$2"
            shift 2
            ;;
        --server-cert)
            [[ $# -ge 2 ]] || die "--server-cert requires a value"
            server_cert="$2"
            shift 2
            ;;
        --certificate-uri)
            [[ $# -ge 2 ]] || die "--certificate-uri requires a value"
            certificate_uri="$2"
            shift 2
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            die "unknown argument: $1"
            ;;
    esac
done

[[ -n "$ip" ]] || die "--ip is required"
[[ -n "$user" ]] || die "--user is required"
[[ -n "$password" ]] || die "--password is required"

if [[ -n "$cert_dir" ]]; then
    [[ -z "$server_key" ]] || die "use either --cert-dir or --server-key/--server-cert, not both"
    [[ -z "$server_cert" ]] || die "use either --cert-dir or --server-key/--server-cert, not both"
    server_key="$cert_dir/server-key.pem"
    server_cert="$cert_dir/server-cert.pem"
fi

[[ -n "$server_key" ]] || die "--server-key or --cert-dir is required"
[[ -n "$server_cert" ]] || die "--server-cert or --cert-dir is required"
[[ -f "$server_key" ]] || die "server key not found: $server_key"
[[ -f "$server_cert" ]] || die "server certificate not found: $server_cert"

require_cmd curl
require_cmd sed
require_cmd awk

tmp_dir="$(mktemp -d)"
trap 'rm -rf "$tmp_dir"' EXIT

combined_pem="$tmp_dir/server-key-plus-cert.pem"
cat "$server_key" "$server_cert" > "$combined_pem"
cert_string="$(json_escape_pem_file "$combined_pem")"

replace_endpoint="https://${ip}/redfish/v1/CertificateService/Actions/CertificateService.ReplaceCertificate/"

write_replace_payload() {
    local payload_file="$1"
    local uri="${2:-}"

    if [[ -n "$uri" ]]; then
        cat > "$payload_file" <<EOF
{
  "CertificateString": "$cert_string",
  "CertificateUri": {
    "@odata.id": "$uri"
  },
  "CertificateType": "PEM"
}
EOF
    else
        cat > "$payload_file" <<EOF
{
  "CertificateString": "$cert_string",
  "CertificateType": "PEM"
}
EOF
    fi
}

post_replace_certificate() {
    local payload="$1"

    curl -k -sS \
        -u "${user}:${password}" \
        -H "Content-Type: application/json" \
        -d @"$payload" \
        -X POST \
        -o "$last_response" \
        -w "%{http_code}" \
        "$replace_endpoint" || true
}

post_certificate_collection() {
    local payload="$1"
    local collection_uri="$2"

    curl -k -sS \
        -u "${user}:${password}" \
        -H "Content-Type: application/json" \
        -d @"$payload" \
        -X POST \
        -o "$last_response" \
        -w "%{http_code}" \
        "https://${ip}${collection_uri}" || true
}

response_rejects_certificate_uri() {
    local response
    response="$(<"$last_response")"
    [[ "$response" == *"ActionParameterNotSupported"* && "$response" == *"CertificateUri"* ]]
}

response_reports_missing_certificate_uri() {
    local response
    response="$(<"$last_response")"
    [[ "$response" == *"PropertyMissing"* && "$response" == *"CertificateUri"* ]]
}

response_reports_missing_certificate_member() {
    local response
    response="$(<"$last_response")"
    [[ "$response" == *"ResourceNotFound"* && "$response" == *"HTTPS certificate"* ]]
}

certificate_collection_uri_for() {
    local uri="$1"
    echo "${uri%/*}"
}

candidate_uris=()
if [[ -n "$certificate_uri" ]]; then
    candidate_uris+=("$certificate_uri")
else
    candidate_uris+=(
        "/redfish/v1/Managers/bmc/NetworkProtocol/HTTPS/Certificates/1"
        "/redfish/v1/Managers/BMC_0/NetworkProtocol/HTTPS/Certificates/1"
        "/redfish/v1/Managers/BMC/NetworkProtocol/HTTPS/Certificates/1"
        "/redfish/v1/Managers/Manager/NetworkProtocol/HTTPS/Certificates/1"
    )
fi

last_response="$tmp_dir/last-response.txt"
last_status=""
tried_collection_uris=" "

for uri in "${candidate_uris[@]}"; do
    payload="$tmp_dir/replace-cert.json"
    write_replace_payload "$payload" "$uri"

    echo "Trying certificate URI: $uri"
    status="$(post_replace_certificate "$payload")"
    last_status="$status"

    if [[ "$status" =~ ^2[0-9][0-9]$ ]]; then
        echo "BMC HTTPS certificate replacement accepted with HTTP $status."
        echo "The BMC web service may briefly restart or drop connections."
        echo
        echo "Validate locally with:"
        echo "  openssl s_client -connect ${ip}:443 -CAfile <cert-dir>/ca-cert.pem -verify_return_error </dev/null"
        echo
        echo "Validate NVFWUPD with:"
        echo "  target/debug/nvfwupd -t ip=${ip} user=${user} password=<password> servertype=GB200 bmc_ca_cert=<cert-dir>/ca-cert.pem show_version"
        exit 0
    fi

    echo "Certificate URI $uri was not accepted (HTTP $status)."
    if [[ -s "$last_response" ]]; then
        sed 's/^/  /' "$last_response"
    fi

    should_try_collection=false
    if [[ "$status" == "404" ]] && response_reports_missing_certificate_member; then
        should_try_collection=true
    fi

    if [[ "$status" == "400" ]] && response_rejects_certificate_uri; then
        echo "BMC rejected CertificateUri; retrying ReplaceCertificate without it."
        write_replace_payload "$payload"
        status="$(post_replace_certificate "$payload")"
        last_status="$status"

        if [[ "$status" =~ ^2[0-9][0-9]$ ]]; then
            echo "BMC HTTPS certificate replacement accepted without CertificateUri (HTTP $status)."
            echo "The BMC web service may briefly restart or drop connections."
            echo
            echo "Validate locally with:"
            echo "  openssl s_client -connect ${ip}:443 -CAfile <cert-dir>/ca-cert.pem -verify_return_error </dev/null"
            echo
            echo "Validate NVFWUPD with:"
            echo "  target/debug/nvfwupd -t ip=${ip} user=${user} password=<password> servertype=GB200 bmc_ca_cert=<cert-dir>/ca-cert.pem show_version"
            exit 0
        fi

        echo "ReplaceCertificate without CertificateUri was not accepted (HTTP $status)."
        if [[ -s "$last_response" ]]; then
            sed 's/^/  /' "$last_response"
        fi

        if response_reports_missing_certificate_uri; then
            should_try_collection=true
        fi
    fi

    collection_uri="$(certificate_collection_uri_for "$uri")"
    if [[ "$should_try_collection" == true && "$tried_collection_uris" != *" $collection_uri "* ]]; then
        tried_collection_uris+="$collection_uri "
        echo "Trying certificate collection install: $collection_uri"
        write_replace_payload "$payload"
        status="$(post_certificate_collection "$payload" "$collection_uri")"
        last_status="$status"

        if [[ "$status" =~ ^2[0-9][0-9]$ ]]; then
            echo "BMC HTTPS certificate install accepted by collection (HTTP $status)."
            echo "The BMC web service may briefly restart or drop connections."
            echo
            echo "Validate locally with:"
            echo "  openssl s_client -connect ${ip}:443 -CAfile <cert-dir>/ca-cert.pem -verify_return_error </dev/null"
            echo
            echo "Validate NVFWUPD with:"
            echo "  target/debug/nvfwupd -t ip=${ip} user=${user} password=<password> servertype=GB200 bmc_ca_cert=<cert-dir>/ca-cert.pem show_version"
            exit 0
        fi

        echo "Certificate collection install $collection_uri was not accepted (HTTP $status)."
        if [[ -s "$last_response" ]]; then
            sed 's/^/  /' "$last_response"
        fi
    fi
done

echo
echo "Failed to replace BMC HTTPS certificate."
echo "Last HTTP status: ${last_status:-none}"
echo "If this BMC uses a different Manager ID or certificate action shape, pass --certificate-uri explicitly or inspect CertificateService."
exit 1
