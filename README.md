TigrisDB
--------

[![Build Status](https://github.com/tigrisdata/tigrisdb/workflows/Go/badge.svg)]()


Development
-----------

Running `make test`in the root of the repository installs build and test
dependencies and run all the tests in the docker containers. 

To run the server and tests on the host, required dependencies need to be
installed by running:

```sh
sudo sh scripts/install_build_deps.sh
sh scripts/install_test_deps.sh
```

Running `make run` would bring dependencies and server up in the docker
containers, after that tests can be run on the host by running `make local_test`
or in the IDE.

License
-------
This software is licensed under the [Apache 2.0](LICENSE).
