#!/usr/bin/env bash
set -euo pipefail

usage() {
    cat <<'USAGE'
Generate a local test CA and an IP-SAN server certificate for BMC TLS testing.

Usage:
  generate_bmc_tls_certs.sh --ip <bmc-ip> [--out-dir <dir>] [--dns <name>] [--days <days>] [--force]

Examples:
  rust_nvfwupd/scripts/tls/generate_bmc_tls_certs.sh --ip 10.85.14.142 --out-dir /tmp/bmc-10.85.14.142-tls
  rust_nvfwupd/scripts/tls/generate_bmc_tls_certs.sh --ip 10.85.14.142 --dns bmc.example.com

Outputs:
  ca-key.pem
  ca-cert.pem            Pass this to NVFWUPD as bmc_ca_cert=<path>
  server-key.pem
  server-cert.pem        Upload this plus server-key.pem to the BMC HTTPS cert slot
  server.csr
  openssl-ca.cnf
  openssl-server.cnf
USAGE
}

die() {
    echo "error: $*" >&2
    exit 1
}

require_cmd() {
    command -v "$1" >/dev/null 2>&1 || die "required command not found: $1"
}

ip=""
dns_name=""
out_dir=""
days="365"
force="false"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --ip)
            [[ $# -ge 2 ]] || die "--ip requires a value"
            ip="$2"
            shift 2
            ;;
        --dns)
            [[ $# -ge 2 ]] || die "--dns requires a value"
            dns_name="$2"
            shift 2
            ;;
        --out-dir)
            [[ $# -ge 2 ]] || die "--out-dir requires a value"
            out_dir="$2"
            shift 2
            ;;
        --days)
            [[ $# -ge 2 ]] || die "--days requires a value"
            days="$2"
            shift 2
            ;;
        --force)
            force="true"
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
[[ "$days" =~ ^[0-9]+$ ]] || die "--days must be a positive integer"
[[ "$days" -gt 0 ]] || die "--days must be greater than zero"

require_cmd openssl

if [[ -z "$out_dir" ]]; then
    safe_ip="${ip//[:\/]/_}"
    out_dir="./bmc-tls-${safe_ip}"
fi

if [[ -e "$out_dir" && "$force" != "true" ]]; then
    die "output directory already exists: $out_dir (use --force to overwrite files in it)"
fi

mkdir -p "$out_dir"

ca_key="$out_dir/ca-key.pem"
ca_cert="$out_dir/ca-cert.pem"
server_key="$out_dir/server-key.pem"
server_csr="$out_dir/server.csr"
server_cert="$out_dir/server-cert.pem"
ca_cnf="$out_dir/openssl-ca.cnf"
server_cnf="$out_dir/openssl-server.cnf"

cat > "$ca_cnf" <<EOF
[ req ]
distinguished_name = req_distinguished_name
x509_extensions = v3_ca
prompt = no

[ req_distinguished_name ]
CN = NVFWUPD Test CA

[ v3_ca ]
basicConstraints = critical, CA:true
keyUsage = critical, keyCertSign, cRLSign
subjectKeyIdentifier = hash
EOF

cat > "$server_cnf" <<EOF
[ req ]
distinguished_name = req_distinguished_name
req_extensions = v3_req
prompt = no

[ req_distinguished_name ]
CN = ${ip}

[ v3_req ]
basicConstraints = CA:false
keyUsage = digitalSignature, keyEncipherment, keyAgreement
extendedKeyUsage = serverAuth
subjectAltName = @alt_names

[ alt_names ]
IP.1 = ${ip}
EOF

if [[ -n "$dns_name" ]]; then
    cat >> "$server_cnf" <<EOF
DNS.1 = ${dns_name}
EOF
fi

openssl genrsa -out "$ca_key" 2048 >/dev/null 2>&1
openssl req -x509 -new -nodes \
    -key "$ca_key" \
    -sha256 \
    -days "$days" \
    -config "$ca_cnf" \
    -out "$ca_cert" >/dev/null 2>&1

openssl genrsa -out "$server_key" 2048 >/dev/null 2>&1
openssl req -new \
    -key "$server_key" \
    -config "$server_cnf" \
    -out "$server_csr" >/dev/null 2>&1
openssl x509 -req \
    -in "$server_csr" \
    -CA "$ca_cert" \
    -CAkey "$ca_key" \
    -CAcreateserial \
    -out "$server_cert" \
    -days "$days" \
    -sha256 \
    -extensions v3_req \
    -extfile "$server_cnf" >/dev/null 2>&1

chmod 600 "$ca_key" "$server_key"
chmod 644 "$ca_cert" "$server_cert"

echo "Generated BMC TLS test material in: $out_dir"
echo
echo "Upload to BMC:"
echo "  rust_nvfwupd/scripts/tls/load_bmc_tls_cert.sh --ip $ip --user <user> --password <password> --cert-dir $out_dir"
echo
echo "Validate with NVFWUPD:"
echo "  target/debug/nvfwupd -t ip=$ip user=<user> password=<password> servertype=GB200 bmc_ca_cert=$ca_cert show_version"
echo
echo "Certificate subject/SAN:"
openssl x509 -in "$server_cert" -noout -subject -issuer -ext subjectAltName
