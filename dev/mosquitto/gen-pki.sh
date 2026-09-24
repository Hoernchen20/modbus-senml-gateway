#!/bin/sh
# Creates a self-signed CA, a broker certificate signed by it and the
# broker password file in /pki. Existing files are kept, so the CA stays
# the same across restarts; delete dev/pki/ to start over.
set -eu
umask 077
cd /pki

if [ ! -f ca.pem ] || [ ! -f server.pem ]; then
    apk add --no-cache openssl >/dev/null

    cat > ca.ext <<EXT
basicConstraints = critical, CA:TRUE
keyUsage = critical, keyCertSign, cRLSign
subjectKeyIdentifier = hash
EXT
    cat > server.ext <<EXT
basicConstraints = critical, CA:FALSE
keyUsage = critical, digitalSignature, keyEncipherment
extendedKeyUsage = serverAuth
subjectAltName = DNS:mosquitto, DNS:localhost, IP:127.0.0.1
authorityKeyIdentifier = keyid
EXT

    openssl req -new -newkey rsa:2048 -nodes -subj "/CN=Modbus2SenML dev CA" \
        -keyout ca.key -out ca.csr 2>/dev/null
    openssl x509 -req -in ca.csr -signkey ca.key -days 3650 \
        -extfile ca.ext -out ca.pem 2>/dev/null

    openssl req -new -newkey rsa:2048 -nodes -subj "/CN=mosquitto" \
        -keyout server.key -out server.csr 2>/dev/null
    openssl x509 -req -in server.csr -CA ca.pem -CAkey ca.key -CAcreateserial \
        -days 825 -extfile server.ext -out server.pem 2>/dev/null

    rm -f ca.csr server.csr ca.ext server.ext ca.srl
    echo "pki: created CA and broker certificate"
fi

if [ ! -f passwd ]; then
    touch passwd
    mosquitto_passwd -b passwd gateway gateway-dev
    mosquitto_passwd -b passwd viewer viewer-dev
    echo "pki: created password file"
fi

# The broker runs as the mosquitto user and must read its key and passwd.
chown mosquitto:mosquitto server.key passwd
chmod 0600 server.key passwd ca.key
chmod 0644 ca.pem server.pem
