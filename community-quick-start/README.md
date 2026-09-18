This directory contains a docker-compose example
to quickly get a MongoDB Community Search running in a local docker container alongside mongod.

The steps listed will:

* Generate SSL certificates for both `mongod` and `mongot`
* Start a MongoDB Community Edition Server (`mongod`) with a single node replica set on port 27017
* Start a MongoDB Community Search (`mongot`) search server on port 27028
* Persistent data volumes on both ports
* Pre-loaded sample data

## Before You Begin:

* Download Docker v4.40 or higher
  * When running Docker Desktop on macOS note that v4.87+ of Docker Desktop uses a Linux VM with Kernel 7.0.12+. Which is incompatible with MongoDB Community 8.3.11 and below. Installing Docker Desktop v4.86 can workaround this issue.
* Download Docker Compose.
* Download the `curl`
* (macOS Users) Install `openssl@3`. macOS ships with an older version LibreSSL that is incompatible with the certificate generation scripts in the community `Makefile`. Install the latest OpenSSL (e.g. with Homebrew: `brew install openssl@3`) and ensure it shadows the system binary.

  Add the following to your shell init script (e.g. `~/.zshrc`, `~/.bashrc`) and restart your shell:
  ```shell
  # Apple Silicon:
  export PATH="/opt/homebrew/opt/openssl@3/bin:$PATH"
  # Intel Mac:
  export PATH="/usr/local/opt/openssl@3/bin:$PATH"
  ```
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
cd community-quick-start/
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
mongosh --tls --tlsCAFile ./tls/ca.pem --tlsCertificateKeyFile ./tls/client-combined.pem

# Check metrics endpoint
curl http://localhost:9946/metrics
```

## Query tracing (opt-in)

Self-managed mongot can export OpenTelemetry spans for `$search` and `$vectorSearch`
as OTLP-JSON through its structured logs. Tracing is **disabled by default**; it is
only intended to be enabled while diagnosing slow or failing queries.

To enable it, set `tracing.enabled: true` in `mongot.conf` and start (or restart) the
stack:

```shell
make docker.up MODE=local
```

Then run a query and look for `com.xgen.mongot.trace.Slf4jExporter` records in the
mongot logs:

```shell
make docker.logs | grep Slf4jExporter
```

Each record contains a `traceId`, the root span (`SearchCommand.run` or
`VectorSearchCommand.getSearchResults`), child stage timings, and result counts.
Traces are local to mongot — they are not stitched to mongod/mongos — and are emitted
once the processor's sampling window drains (internally a 300s window exporting the
10 slowest traces). The Community config exposes only enablement and sampling:

```yaml
tracing:
   enabled: true
   searchSamplingStrategy: tail
   vectorSamplingStrategy: head
   searchTracingSamplerRate: 0.01
   vectorTracingSamplerRate: 0.01
```

`searchSamplingStrategy` and `vectorSamplingStrategy` default to `tail` and `head`
respectively. The `*TracingSamplerRate` values apply **only when the corresponding
strategy is `head`**; tail sampling keeps a trace regardless of rate and selects the
slowest traces for export after the query runs. With the defaults, every `$search`
query is recorded (`tail`), but only about 1% of `$vectorSearch` queries are (`head` at
`0.01`); when diagnosing vector queries, raise `vectorTracingSamplerRate` (for example to
`1.0`) or set `vectorSamplingStrategy: tail`.

Cost is bounded in a few ways. Export is capped at the 10 slowest traces per 300s window
regardless of sampling rate, so log volume is bounded. In-flight trace state is capped at
10,000 traces with a 30-minute expiry. The remaining cost is per-query span recording:
every query under `tail`, and `rate` × queries under `head`. Traces appear when the 300s
window drains; traces still buffered are flushed on graceful shutdown.

`enabled` is read once at startup and there is no runtime kill switch: to stop tracing,
set `tracing.enabled: false` and restart mongot.

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