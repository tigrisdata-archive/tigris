# Testing

## Running tests in docker containers

Tests are executed using `make test`. This runs both unit and integration 
tests. The FoundationDB container auto-generates the cluster file which is
shared between containers using a docker volume.

The docker volume `fdbdata` is mounted at:

* /var/fdb/fdb.cluster in tigris_fdb container
* /etc/foundationdb/fdb.cluster in tigris_test and tigris_server containers,
  which is the default location where the client will search for the cluster 
  file

## Running tests on your local machine

Once FoundationDB and tigris server have been started up using `make run`, 
the tests can be run using `make local_test`.

The config file is shared between FoundationDB and tigris server the same way 
as described in the section above.

Go tests use `config/fdb.cluster` file for connection, which is configured
by the `getTestFDBConfig` function.

The tests can also be executed from within the IDE.

## OSX

Due to various issues with Docker on Apple M1, it is recommended to install
FoundationDB on the host and run the server on the host directly as well.

To run the server on the host, required dependencies need to be installed by
running:

```shell
sh scripts/install_build_deps.sh
sh scripts/install_test_deps.sh
```

Once the dependencies are installed, you can run the test on your local 
machine as follows:

```shell
make osx_test
```
