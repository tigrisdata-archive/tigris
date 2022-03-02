# TigrisDB

[![Build Status](https://github.com/tigrisdata/tigrisdb/workflows/Go/badge.svg)]()

# Configuration

The configuration is specified either through the configuration file
`server.yaml` or through environment variables.

Example configuration is shown below:

```yaml
api:
  port: 8081

foundationdb:
  cluster_file: "/etc/foundationdb/fdb.cluster"
```

When the configuration options are specified through environment 
variables they have a higher precedence. The environment variables
have the following structure:

- The variables are in uppercase
- They are prefixed by `TIGRISDB_SERVER_`
- Multi-level variables are specified by replacing `.` with `_`

Examples:

- To specify the FoundationDB cluster file set the variable `TIGRISDB_SERVER_FOUNDATIONDB_CLUSTER_FILE`
- To specify the API listen port set the variable `TIGRISDB_SERVER_API_PORT`

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

Running `make osx_run` on OSX would bring the server up on the host. You can 
then run `make osx_test` to test against FDB running on the host.

# License
This software is licensed under the [Apache 2.0](LICENSE).
