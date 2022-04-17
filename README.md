# TigrisDB

[![Go Report](https://goreportcard.com/badge/github.com/tigrisdata/tigrisdb)](https://goreportcard.com/report/github.com/tigrisdata/tigrisdb)
[![Build Status](https://github.com/tigrisdata/tigrisdb/workflows/Go/badge.svg)]()
[![Contributor Covenant](https://img.shields.io/badge/Contributor%20Covenant-2.1-4baaaa.svg)](CODE_OF_CONDUCT.md)

# Getting started

These instructions will get you through setting up TigrisDB locally as Docker
containers.

## Prerequisites

* Make sure that you have Docker and Docker Compose installed
    * Windows or
      MacOS: [Install Docker Desktop](https://www.docker.com/get-started)
    * Linux: [Install Docker](https://www.docker.com/get-started) and then
      [Docker Compose](https://github.com/docker/compose)

## Running locally

The [docker/local](docker/local) directory contains the docker-compose.yaml
which describes the configuration of TigrisDB components. You can run TigrisDB
in a local environment by executing:

```shell
cd docker/local
docker-compose up -d
```

## Connecting using CLI

Install the CLI:

```shell
go install github.com/tigrisdata/tigrisdb-cli@latest
```

Make sure to include the installed binary in your PATH.

Test that TigrisDB is up and running locally:

```shell
export TIGRISDB_URL=http://localhost:8081
tigrisdb-cli db list databases
```

More elaborate example of CLI usage can be
found [here](https://github.com/tigrisdata/tigrisdb-cli/).

Golang client example can be
found [here](https://github.com/tigrisdata/tigrisdb-client-go/).

# Documentation

* [Development Overview](docs/development/overview.md)
* [Testing](docs/development/testing.md)
* [Configuration](docs/configuration.md)
* [Deployment to Kubernetes](docs/deploy_k8s.md)

# Roadmap
* [Alpha Release](https://github.com/orgs/tigrisdata/projects/1/views/1)

# Community & Support

* [Slack Community](https://join.slack.com/t/tigrisdatacommunity/shared_invite/zt-16fn5ogio-OjxJlgttJIV0ZDywcBItJQ)
* [GitHub Issues](https://github.com/tigrisdata/tigrisdb/issues)
* [GitHub Discussions](https://github.com/tigrisdata/tigrisdb/discussions)

# License

This software is licensed under the [Apache 2.0](LICENSE).
