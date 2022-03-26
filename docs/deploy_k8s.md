# Deployment to Kubernetes

The `deployment` directory contains manifests that can be used to deploy to
Kubernetes. The directory layout follows the Kustomize best practices and uses
the concepts of bases and overlays.

## Kubernetes Manifests

The default manifests are defined in the `deployment/manifests/base`
directory.

Before these can be applied they need to be kustomized by passing in the
FoundationDB cluster config map.

Included is an overlay `deployment/manifests/overlays/sample` that shows how the
base manifest can be kustomized.

### Passing custom server configuration

Through the use of Kustomize configmap generator, custom configuration can be
generated and passed to the server container. The `deployment/manifests/base`
directory has an example to show how custom configuration file can be generated
and made available to the container.

This is achieved by defining the `configMapGenerator` in kustomization file
which references the server configuration file `server.yaml`:

```yaml
configMapGenerator:
  - name: tigrisdb-server-config
    files:
      - server.yaml
```

The deployment configuration then mounts the configmap as a volume and makes it
available to the server.

### FoundationDB cluster configuration file

The FoundationDB cluster created
through [fdb-kubernetes-operator](https://github.com/FoundationDB/fdb-kubernetes-operator)
exposes the cluster configuration through a configmap. The configmap is named
`<cluster_name>-config`. The configmap is mounted as a volume named
`fdb-cluster-config-volume` and the configuration can then be accessed by the
server container to be able to connect to FoundationDB.

Take a look at the `overlays/sample` manifests to see how this is done.

### Applying the manifest

```shell
kubectl -k ./deployment/manifests/overlays/sample
```
