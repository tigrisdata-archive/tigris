<p align="center">
  <a href="https://www.tigrisdata.com/"><img src="https://docs.tigrisdata.com/logo/dark.png" alt="Tigris" width="298" /></a> 
</p>

<p align="center">
Tigris is the all-in-one open source developer data platform. 
</p>

<p align="center">
Use it as a
scalable transactional document store. Perform real-time search across your
data stores automatically. Build event-driven apps with real-time event
streaming. All provided to you through a unified serverless API enabling you
to focus on building applications and stop worrying about the data
infrastructure.
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
  <a href="https://docs.tigrisdata.com/quickstart">Quickstart</a> |
  <a href="https://docs.tigrisdata.com/apidocs/">API Reference</a> |
  <a href="https://join.slack.com/t/tigrisdatacommunity/shared_invite/zt-16fn5ogio-OjxJlgttJIV0ZDywcBItJQ">Slack Community</a> | 
  <a href="https://twitter.com/TigrisData">Twitter</a>
</p>

# Helpful Links

- [Quickstart](https://docs.tigrisdata.com/quickstart)
- [Architecture](https://docs.tigrisdata.com/overview/architecture)
- [Databases and Collections](https://docs.tigrisdata.com/overview/databases)
- [Documents](https://docs.tigrisdata.com/documents/)
- [Event Streaming](https://docs.tigrisdata.com/events/)
- [Search](https://docs.tigrisdata.com/searching/)
- [Observability](https://docs.tigrisdata.com/observability/)
- [TypeScript Reference](https://docs.tigrisdata.com/typescript/)
- [Go Reference](https://docs.tigrisdata.com/golang/)
- [Java Reference](https://docs.tigrisdata.com/java/)
- [CLI](https://docs.tigrisdata.com/cli)
- [Guides](https://docs.tigrisdata.com/guides/)

# Community & Support

* [Slack Community](https://join.slack.com/t/tigrisdatacommunity/shared_invite/zt-16fn5ogio-OjxJlgttJIV0ZDywcBItJQ)
* [GitHub Issues](https://github.com/tigrisdata/tigris/issues)
* [GitHub Discussions](https://github.com/tigrisdata/tigris/discussions)

# Developing

### Building with Docker Containers

Start local Tigris server listening on `http://localhost:8081` by running:

```sh
make run
```

This would bring dependencies and server up in the docker containers with all
your changes.

Alternatively, you can run `make run_full` to bring up monitoring tools as well.
  * Graphana: http://localhost:3000
  * Prometheus: http://localhost:9090

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
