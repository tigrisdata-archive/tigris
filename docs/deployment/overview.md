# Deployment

The `deployment` directory contains manifests that can be used to deploy to
Kubernetes. The directory layout is follows Kustomize best practices and 
uses the concepts of bases and overlays. 

## Kubernetes Manifests
The default manifests are defined in the `deployment/manifests/base`
directory. 

Before these can be applied they need to be kustomized by passing in the
container image URI, relevant server configuration and the FoundationDB 
cluster config map.

Included is an overlay `deployment/manifests/overlays/sample` that shows 
how the base manifest can be kustomized.

### Passing custom server configuration
Through the use of Kustomize configmap generator custom configuration can be
generated and passed to the server container.
The `deployment/manifests/overlays/sample` directory has an example to
show how custom configuration file can be generated and made available to the
container.

### FoundationDB cluster configuration file
FDB cluster exposes the cluster configuration through configmap. The
configmap is named <cluster_name>-config. The configmap is mounted as a
volume `fdb-cluster-config-volume` and the configuration can then be
accessed by the server container to be able to connect to FoundationDB.

Take a look at the `overlays/sample` manifests to see how this is done.

### Container image
The deployment manifest contains a placeholder for the image, and it must be
replaced before applying the manifest too Kubernetes.

### Applying the manifest
```shell
kubectl -k ./deployment/manifests/overlays/sample
```
