<p align="center">
  <a href="https://www.tigrisdata.com/"><img src="https://www.tigrisdata.com/docs/logo/dark.png" alt="Tigris" width="298" /></a> 
</p>

<p align="center">
Tigris is a Serverless NoSQL Database and Search Platform that offers an open source alternative to MongoDB and DynamoDB.
Tigris is built on FoundationDB and combines the consistency of ACID transactions with the scale and flexibility of NoSQL, at a fraction of the cost.
</p>

<p align="center">
<a href="https://goreportcard.com/report/github.com/tigrisdata/tigris"> 
<img src="https://goreportcard.com/badge/github.com/tigrisdata/tigris" alt="Go Report">
</a>
<a href="">
<img src="https://github.com/tigrisdata/tigris/workflows/Go/badge.svg" alt="Build Status">
</a>
<a href="CODE_OF_CONDUCT.md">
<img src="https://img.shields.io/badge/Contributor%20Covenant-2.1-4baaaa.svg" alt="Contributor Covenant">
</a>
</p>

<p align="center">
  <a href="https://www.tigrisdata.com/">Website</a> |
  <a href="https://www.tigrisdata.com/docs/quickstarts/">Quickstart</a> |
  <a href="https://www.tigrisdata.com/docs/references/api/">API Reference</a> |
  <a href="https://www.tigrisdata.com/discord/">Discord</a> | 
  <a href="https://twitter.com/TigrisData">Twitter</a>
</p>

# Helpful Links

- [Quickstart](https://www.tigrisdata.com/docs/quickstarts/)
- [Architecture](https://www.tigrisdata.com/docs/concepts/architecture/)
- [Databases and Collections](https://www.tigrisdata.com/docs/concepts/database/)
- [Documents](https://www.tigrisdata.com/docs/concepts/database/documents/)
- [Search](https://www.tigrisdata.com/docs/concepts/searching/)
- [Observability](https://www.tigrisdata.com/docs/concepts/platform/cloud/metrics/)
- [TypeScript Reference](https://www.tigrisdata.com/docs/sdkstools/typescript/)
- [Go Reference](https://www.tigrisdata.com/docs/sdkstools/golang/)
- [Java Reference](https://www.tigrisdata.com/docs/sdkstools/java/getting-started/)
- [CLI](https://www.tigrisdata.com/docs/sdkstools/cli/)
- [Guides](https://www.tigrisdata.com/docs/guides/)

# Community & Support

- [Discord Community](https://www.tigrisdata.com/discord/)
- [GitHub Issues](https://github.com/tigrisdata/tigris/issues)
- [GitHub Discussions](https://github.com/tigrisdata/tigris/discussions)

# Developing

## Setup local development environment

To setup your local development environment. Make sure you have Go installed or run:

```sh
sh scripts/install_go.sh
```

Add your `$GOPATH` to your `$PATHS` environment variable. And then to download and
install the build dependencies and FoundationDB run:

```sh
sh scripts/install_build_deps.sh
```

### Building with Docker Containers

Start local Tigris server listening on `http://localhost:8081` by running:

```sh
make run
```

This would bring dependencies and server up in the docker containers with all
your changes.

Alternatively, you can run `make run_full` to bring up monitoring tools as well.

- Grafana: http://localhost:3000
- Prometheus: http://localhost:9090

### Running tests

#### Run in the docker container

Tests are executed using `make test`. This runs both unit and integration
tests in the docker containers.

#### Run in the IDE

Run `make run` to bring the server up in the docker container.
Now you can run individual tests in the IDE of your choice.
Entire test suite can be run using `make local_test`.

#### Debugging the server in the IDE

Run `make local_run` to start Tigris server on the host.
Now you can attach to the process and debug from the IDE.

# License

This software is licensed under the [Apache 2.0](LICENSE).
