This directory contains a docker-compose example
to quickly get a MongoDB Community Search running in a local docker container alongside mongod.

The steps listed will:

* Generate SSL certificates for both `mongod` and `mongot`
* Start a MongoDB Community Edition Server (`mongod`) with a single node replica set on port 27017
* Start a MongoDB Community Search (`mongot`) search server on port 27028
* Persistant data volumes on both ports
* Pre-loaded sample data

## Before You Begin:

* Download Docker v4.40 or higher
* Download Docker Compose
* Download the `curl` and `openssl` commands
* Download `mongosh` locally or have access to it through Docker
* Download the sample data:

```shell
curl https://atlas-education.s3.amazonaws.com/sampledata.archive -o community-quick-start/sampledata.archive
```

* If you would like to change the password, update the `pwfile` and `init-mongo.sh` with
the new password before starting the services.

## Starting `mongod` and `mongot`

MongoDB Community Search supports two build modes:

### Local Mode (Default)
Builds `mongot` from your local source code. This is useful for development and testing local changes.

```shell
make docker.up
# or explicitly:
make docker.up MODE=local
```

The script will automatically:
- Detect your platform (ARM64 for Apple Silicon, AMD64 for Intel/AMD)
- Build the mongot tarball using Bazel
- Create a Docker image with your local build
- Start both `mongod` and `mongot` containers

### Latest Mode
Uses the pre-built `mongot` image from Docker Hub. This is faster and doesn't require building from source.

```shell
make docker.up MODE=latest
```

### Stopping Services

To stop all running containers:

```shell
make docker.down
```

### Clearing All Data

To stop all running containers and remove all data:

```shell
make docker.clear
```


### Useful Commands

```shell
# Check container status
make docker.ps

# View logs for specific service
make docker.logs                # for mongot logs in local mode 
make docker.logs SERVICE=mongot # for mongot logs in latest mode
make docker.logs SERVICE=mongod # for mongod logs

# Restart services
make docker.down
make docker.up

# Connect to MongoDB
make docker.connect # Simple alias for:
mongosh --tls --tlsCAFile ./community-quick-start/tls/ca.pem --tlsCertificateKeyFile ./community-quick-start/tls/client-combined.pem

# Check metrics endpoint
curl http://localhost:9946/metrics
```

## Create a MongoDB Search index

1. Connect to MongoDB with mongosh

```shell
make docker.connect
```

2. In the MongoDB shell, run the following commands to create a search index on the sample data

```mongodb-json
// Switch to the sample database
use sample_mflix

// Create a search index on the movies collection
db.movies.createSearchIndex(
   "default",
   { mappings:
      { dynamic: true }
   }
)
```

3. Test search functionality:

```mongodb-json
// Search for movies with "baseball" in the plot
db.movies.aggregate( [
   {
      $search: {
         "text": {
         "query": "baseball",
         "path": "plot"
         }
      }
   },
   {
      $limit: 5
   },
   {
      $project: {
         "_id": 0,
         "title": 1,
         "plot": 1
      }
   }
] )
```