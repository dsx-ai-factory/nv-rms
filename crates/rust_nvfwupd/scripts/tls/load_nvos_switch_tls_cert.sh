#!/usr/bin/env bash
set -euo pipefail

# NOTE: This helper is retained for lab TLS validation and is intended for use
# once customer discussions for NVFWUPD TLS enablement have completed.

usage() {
    cat <<'USAGE'
Upload a generated server key/certificate to an NVOS switch for the NVUE REST API.

Usage:
  load_nvos_switch_tls_cert.sh --ip <switch-ip> --user <user> --password <password> --cert-dir <dir>
  load_nvos_switch_tls_cert.sh --ip <switch-ip> --user <user> --password <password> --server-key <key> --server-cert <cert>

Options:
  --cert-id <id>           NVOS certificate ID to create/use.
                           Default: nvfwupd-api-<ip>-<timestamp>
  --remote-dir <dir>       Temporary directory on the switch.
                           Default: /tmp/nvfwupd-tls-<pid>
  --delete-existing        Delete an existing certificate with --cert-id before import.
  --no-save                Apply the running config but skip "nv config save".

Examples:
  crates/rust_nvfwupd/scripts/tls/load_nvos_switch_tls_cert.sh \
    --ip 192.0.2.10 \
    --user username \
    --password password \
    --cert-dir /tmp/switch-192.0.2.10-tls

Notes:
  This configures only the switch HTTPS/NVUE REST API server certificate:
    nv action import system security certificate <cert-id> ...
    nv set system api certificate <cert-id>
    nv config apply

  This does not enable API mTLS and does not configure client certificate
  authentication. NVFWUPD can validate the server certificate with:
    bmc_ca_cert=<cert-dir>/ca-cert.pem

  This test/development helper passes the password to sshpass with -p, which
  can expose it in local process listings. Use only in controlled test
  environments.

  The script requires sshpass, ssh, and scp on the local machine.
USAGE
}

die() {
    echo "error: $*" >&2
    exit 1
}

require_cmd() {
    command -v "$1" >/dev/null 2>&1 || die "required command not found: $1"
}

shell_quote() {
    printf "'%s'" "$(printf "%s" "$1" | sed "s/'/'\\\\''/g")"
}

ssh_run() {
    sshpass -p "$password" ssh "${ssh_opts[@]}" "${user}@${ip}" "$@"
}

scp_to_switch() {
    local src="$1"
    local dst="$2"
    sshpass -p "$password" scp "${ssh_opts[@]}" "$src" "${user}@${ip}:$dst"
}

ip=""
user=""
password=""
cert_dir=""
server_key=""
server_cert=""
cert_id=""
remote_dir=""
delete_existing="false"
save_config="true"

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
        --cert-id)
            [[ $# -ge 2 ]] || die "--cert-id requires a value"
            cert_id="$2"
            shift 2
            ;;
        --remote-dir)
            [[ $# -ge 2 ]] || die "--remote-dir requires a value"
            remote_dir="$2"
            shift 2
            ;;
        --delete-existing)
            delete_existing="true"
            shift
            ;;
        --no-save)
            save_config="false"
            shift
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

require_cmd sshpass
require_cmd ssh
require_cmd scp
require_cmd sed

safe_ip="$(printf "%s" "$ip" | sed 's/[^A-Za-z0-9_.-]/_/g')"
if [[ -z "$cert_id" ]]; then
    cert_id="nvfwupd-api-${safe_ip}-$(date +%Y%m%d%H%M%S)"
fi
if [[ -z "$remote_dir" ]]; then
    remote_dir="/tmp/nvfwupd-tls-$$"
fi

remote_cert="${remote_dir}/server-cert.pem"
remote_key="${remote_dir}/server-key.pem"
remote_cert_uri="file://${remote_cert}"
remote_key_uri="file://${remote_key}"

ssh_opts=(
    -o BatchMode=no
    -o ConnectTimeout=10
    -o StrictHostKeyChecking=accept-new
)

echo "Preparing temporary directory on switch: $remote_dir"
ssh_run "mkdir -p $(shell_quote "$remote_dir") && chmod 700 $(shell_quote "$remote_dir")"

cleanup_remote() {
    ssh_run "rm -rf $(shell_quote "$remote_dir")" >/dev/null 2>&1 || true
}
trap cleanup_remote EXIT

echo "Copying certificate and key to switch..."
scp_to_switch "$server_cert" "$remote_cert"
scp_to_switch "$server_key" "$remote_key"

if [[ "$delete_existing" == "true" ]]; then
    echo "Deleting existing NVOS certificate ID if present: $cert_id"
    ssh_run "nv action delete system security certificate $(shell_quote "$cert_id") || true"
fi

echo "Importing NVOS entity certificate: $cert_id"
ssh_run \
    "nv action import system security certificate $(shell_quote "$cert_id") uri-public-key $(shell_quote "$remote_cert_uri") uri-private-key $(shell_quote "$remote_key_uri")"

echo "Setting NVUE REST API certificate to: $cert_id"
ssh_run "nv set system api certificate $(shell_quote "$cert_id")"

echo "Applying NVUE config..."
ssh_run "nv config apply"

if [[ "$save_config" == "true" ]]; then
    echo "Saving NVUE config..."
    ssh_run "nv config save"
fi

echo "Switch TLS server certificate configured for NVUE REST API."
echo
echo "Useful checks:"
echo "  sshpass -p '<password>' ssh ${user}@${ip} 'nv show system api'"
echo "  sshpass -p '<password>' ssh ${user}@${ip} 'nv show system security certificate ${cert_id} installed'"
echo
echo "Validate NVFWUPD with:"
echo "  target/debug/nvfwupd -t ip=${ip} user=${user} password=<password> servertype=GB200Switch bmc_ca_cert=<cert-dir>/ca-cert.pem show_version"
