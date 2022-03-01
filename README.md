# TigrisDB

[![Build Status](https://github.com/tigrisdata/tigrisdb/workflows/Go/badge.svg)]()

# Configuration

## Connecting to FoundationDB
The FoundationDB cluster file can be specified in the configuration file `server.yaml`.
It can also be passed through the environment variable `TIGRISDB_SERVER_FOUNDATIONDB_CLUSTER_FILE`. The environment
variable has higher precedence and will override the cluster file configuration define in `server.yaml`.

# Development

## Prerequisite

### Mac OSX
It is recommended to have a package manager such as `brew` installed

## Building and Testing in Docker Containers
Running `make test` in the root of the repository installs build and test
dependencies and runs all the tests in the docker containers.

Running `make run` would bring dependencies and server up in the docker
containers, after that tests can be run on the host by running `make local_test`
or in the IDE.

## Building and Testing on the Host

To run the server and tests on the host, required dependencies need to be
installed by running:

```sh
sh scripts/install_build_deps.sh
sh scripts/install_test_deps.sh
```

On OSX you can run `make osx_test` to test against FDB running on the host.

# License
This software is licensed under the [Apache 2.0](LICENSE).
