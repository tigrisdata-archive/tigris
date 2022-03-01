# Test FDB configuration

For different test use cases FDB configured as follows:

  * Docker tests / Github action tests

    Can be run using `make test`, this run both unit and integration tests
    In this case auto-generated cluster file is shared between containers
    using Docker volume.

    Mounted at:

    * /var/fdb/fdb.cluster in tigris_fdb container
    * /etc/foundationdb/fdb.cluster in tigris_test and tigris_server containers, which is default location, so no need to pass cluster file to the init function

    Docker compose file sets "test" config environment variables so `getTestFDBConfig` passes empty cluster file.
    
  * Running unit and integration tests on the host (server in the docker)

    FDB and server should be started using `make run`. The tests then can be run using `make local_test`.

    Between FDB and server config file is shared same way is in fully docker tests, described above.

    Go tests are using `config/fdb.cluster` file for connection, which is configured by `getTestFDBConfig` function.

  * Running unit and integration tests in IDE (server in the docker)

    Same as hosts tests containers should be started using `make run`

    After that individual tests can be run in the IDE.

    Unittests are using `config/fdb.cluster` for conneting to FDB and configured by `getTestFDBConfig`

  * Integration tests with server running on the host

    In this case FDB can be start using: `docker-compose -f docker/docker-compose.yml up tigris_fdb`

    Server can be compiled by `make bins`, the server then can be started manually on the host by running `server/service` in the root of the repository

    The server uses `config/config.yaml` in this case which is configured to use `config/fdb.cluster` file

# Known issues

  * There is a network issue when server is run on the Mac OS X host. The server cannot connect to FDB, which exposes port 4500 on the localhost

    So it should be started using `make run`