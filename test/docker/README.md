# Docker

Docker container related files for tigris. This directory has a Dockerfile to build a tigris_server container from the current code and a docker-compose.yml to run this freshly built containers in various setups.

## Use cases

### Testing

The `make test` command will run both the unit and integration tests in these containers. 

### Interactive usage

The `make run` command will start a foundationdb container and the tigris server container, which will be left running for interactive usage.

The `make run_full` command will do everything that `make run` does, but it will start additional components on top of that. At the time of writing this readme, it brings up additional grafana and prometheus containers. 

A password can be set to grafana with the `GRAFANA_PASSWORD` environment variable.
