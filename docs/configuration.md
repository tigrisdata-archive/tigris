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

When the configuration options are specified through environment variables they
have a higher precedence. The environment variables have the following
structure:

- The variables are in uppercase
- They are prefixed by `TIGRIS_SERVER_`
- Multi-level variables are specified by replacing `.` with `_`

Examples:

- To specify the FoundationDB cluster file set the
  variable `TIGRIS_SERVER_FOUNDATIONDB_CLUSTER_FILE`
- To specify the API listen port set the variable `TIGRIS_SERVER_SERVER_PORT`