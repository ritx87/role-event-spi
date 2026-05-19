#!/bin/bash
set -e

mkdir -p /certs
cd /certs

# 1. Generate CA
echo "Generating CA..."
openssl req -new -x509 -keyout ca-key -out ca-cert -days 3650 -passout pass:confluent -subj "/CN=Kafka-CA"

# 2. Generate Keystore for Kafka Broker
echo "Generating Kafka Broker Keystore..."
keytool -genkey -noprompt \
                 -alias kafka \
                 -dname "CN=kafka, OU=Dev, O=Org, L=City, S=State, C=US" \
                 -ext "SAN=DNS:kafka,DNS:localhost,DNS:star-kafka-kafka-bootstrap.kafka.svc,IP:127.0.0.1" \
                 -keystore kafka.server.keystore.jks \
                 -keyalg RSA \
                 -storepass confluent \
                 -keypass confluent

# 3. Create CSR for Kafka Broker
keytool -keystore kafka.server.keystore.jks -alias kafka -certreq -file kafka.csr -storepass confluent -keypass confluent

# 4. Sign the Kafka Broker CSR with CA
echo "Signing Kafka Broker CSR..."
echo "subjectAltName = DNS:kafka,DNS:localhost,DNS:star-kafka-kafka-bootstrap.kafka.svc,IP:127.0.0.1" > ext.cnf
openssl x509 -req -CA ca-cert -CAkey ca-key -in kafka.csr -out kafka.signed-cert -days 3650 -CAcreateserial -passin pass:confluent -extfile ext.cnf

# 5. Import CA and Signed Cert into Kafka Broker Keystore
echo "Importing CA and Signed Cert to Broker Keystore..."
keytool -keystore kafka.server.keystore.jks -alias CARoot -import -file ca-cert -storepass confluent -noprompt
keytool -keystore kafka.server.keystore.jks -alias kafka -import -file kafka.signed-cert -storepass confluent -noprompt

# 6. Create Truststore for Broker
echo "Creating Truststore..."
keytool -keystore kafka.server.truststore.jks -alias CARoot -import -file ca-cert -storepass confluent -noprompt

# Copy ca-cert for Keycloak (PEM format)
cp ca-cert ca.crt

# Set permissions (skipped on windows host)
# chmod 777 /certs/*

echo "Certificates generated successfully!"
