#!/bin/bash

# Certificate needed for connecting to Keyspaces
mkdir -p /home/hadoop/.certs
curl https://certs.secureserver.net/repository/sf-class2-root.crt -o /home/hadoop/.certs/sf-class2-root.crt

# Create a JKS keystore from the certificate
PASS={trust-store-password}
printf "%s\n%s" $PASS $PASS | keytool \
  -importcert -keystore /home/hadoop/.certs/cassandra_keystore.jks \
  -file /home/hadoop/.certs/sf-class2-root.crt \
  -alias cassandra \
  -noprompt

# copy the Cassandra Connector config
aws s3 cp s3://{YOUR S3 BUCKET HERE}/EMR/app.config /home/hadoop/app.config
