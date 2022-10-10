# User Quota

Quota module allows to protect the system from overloading and enforce per user resource limits.
The module is composable and allow to control the following
  * [Per user overall storage usage](#storage-size-limiting)
  * Allows to [protect the node from overloading](#maximum-per-node-rate-limiting), by enforcing absolute maximum 
    of request rate single node of Tigris service allowed to handle.
  * [Coordinated per-user request rate limiting](#namespace-request-rate-limiting) across all Tigris service nodes,
    by leveraging Datadog metrics backend.

Every limiter has `Enabled` config variable and can be enabled and disabled independently.
Quota is checked and enforced in the server middleware. 

# Storage size limiting

Storage limiter keeps per-user in memory cache of current storage used, the cache is updated in the background thread
every `config.DefaultConfig.Quota.Storage.RefreshInterval`. Background thread also updates collection sizes, database
sizes and namespaces sizes and sends them to observability service as metrics.

Current storage size is checked against per namespace or default storage size limit and request is rejected with
`datasize limit exceeded` (HTTP: 429) error if storage size exceeds the limit.

# Request limiter

Request limiters are used to enforce node and namespace quotas. There are two interface methods limiter provides:
    1. Allow: this method return and error immediately if the request rate exceeded configured values.
    2. Wait: Even if the limits are exceeded at this particular moment, wait allows to reserve a token and schedule 
       execution at some point-in-time in the future. The execution can be delayed upto the request context timeout. 

Request rate limiter is configured by the number of:
   * ReadUnits
   * WriteUnits

Every request consumes at least one request unit.
Number of units consumed calculated by the following formula:

readUnits = {request size in bytes} / ReadUnitSize
writeUnits = {request size in bytes} / WriteUnitSize

plus one unit if remainder of above division is not zero.

where:
  * ReadUnitSize = 4096 bytes
  * WriteUnitSize  = 1024 bytes

# Namespace request rate limiting

Namespace rate limiter enforces cluster wide quota using current performance rate from the metrics backend, currently
only Datadog backend is supported. By default, per cluster per namespace limits are configured by
    `config.DefaultConfig.Quota.Namespace.Default.(Read|Write)Units`.
Specific per namespace limits can be set by
    `config.DefaultConfig.Quota.Namespace.Namespaces[{ns}].(Read|Write)Units`.

Namespace rate limiter periodically updates current per namespace rates in the background thread. 
Depending on the current rates received from the metrics backend background thread reduce or increase allowed limits.
Limits are increased or reduced by 10% of maximum limits allowed per namespace per node. Maximum limit is configured by
    `config.DefaultConfig.Quota.Namespace.Node.(Read|Write)Units`

# Maximum per node rate limiting

This rate limiter is used to enforce maximum request rate across all the user of single node.
Configured by
    `config.DefaultConfig.Quota.Node.(Read|Write)Units`
