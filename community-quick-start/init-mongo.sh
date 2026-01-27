#!/bin/bash
set -e
echo "Starting MongoDB initialization..."
sleep 2

# TLS connection parameters
TLS_OPTS="--tls --tlsCAFile /etc/mongod-tls/ca.pem --tlsCertificateKeyFile /etc/mongod-tls/client-combined.pem"

# TLS connection parameters for mongorestore (uses --ssl prefix)
RESTORE_TLS_OPTS="--ssl --sslCAFile /etc/mongod-tls/ca.pem --sslPEMKeyFile /etc/mongod-tls/client-combined.pem"

# Create user using local connection (no port specification needed)
echo "Creating user..."
mongosh $TLS_OPTS --eval "
const adminDb = db.getSiblingDB('admin');
try {
  adminDb.createUser({
     user: 'mongotUser',
     pwd: 'mongotPassword',
     roles: [{ role: 'searchCoordinator', db: 'admin' }]
  });
  print('User mongotUser created successfully');
} catch (error) {
  if (error.code === 11000) {
     print('User mongotUser already exists');
  } else {
     print('Error creating user: ' + error);
  }
}
"
# Check for existing data
echo "Checking for existing sample data..."
if mongosh $TLS_OPTS --quiet --eval "db.getSiblingDB('sample_mflix').getCollectionNames().includes('movies')" | grep -q "true"; then
  echo "Sample data already exists. Skipping restore."
else
  echo "Sample data not found. Running mongorestore..."
  if [ -f "/sampledata.archive" ]; then
     mongorestore $RESTORE_TLS_OPTS --archive=/sampledata.archive
     echo "Sample data restored successfully."
  else
     echo "Warning: sampledata.archive not found, skipping loading sample data."
  fi
fi
echo "MongoDB initialization completed."