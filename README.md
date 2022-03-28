# TigrisDB

[![Go Report](https://goreportcard.com/badge/github.com/tigrisdata/tigrisdb)](https://goreportcard.com/report/github.com/tigrisdata/tigrisdb)
[![Build Status](https://github.com/tigrisdata/tigrisdb/workflows/Go/badge.svg)]()

# Development

Start local TigrisDB server listening on `http://localhost:8081` by running:

```sh
make run
```

Install the CLI:

```shell
go install github.com/tigrisdata/tigrisdb-cli@latest
```

Test that server is up and running:

```sh
export TIGRISDB_URL=http://localhost:8081
tigrisdb-cli list databases
```

More elaborate example of CLI
usage [here](https://github.com/tigrisdata/tigrisdb-cli/)

Golang client example [here](https://github.com/tigrisdata/tigrisdb-client-go/)

# Documentation

* [Development Overview](docs/development/overview.md)
* [Testing](docs/development/testing.md)
* [Configuration](docs/configuration.md)
* [Deployment to Kubernetes](docs/deploy_k8s.md)

# Community & Support

* [Slack Community](https://join.slack.com/t/tigrisdatacommunity/shared_invite/zt-16fn5ogio-OjxJlgttJIV0ZDywcBItJQ)
* [GitHub Issues](https://github.com/tigrisdata/tigrisdb/issues)
* [GitHub Discussions](https://github.com/tigrisdata/tigrisdb/discussions)

# License

This software is licensed under the [Apache 2.0](LICENSE).
