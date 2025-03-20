# cgw

This Helm chart helps to deploy OpenLAN CGW (further on refered as __Gateway__) to the Kubernetes clusters. It is mainly used in [assembly chart](https://github.com/Telecominfraproject/wlan-cloud-ucentral-deploy/tree/main/cgwchart) as Gateway requires other services as dependencies that are considered in that Helm chart. This chart is purposed to define deployment logic close to the application code itself and define default values that could be overriden during deployment.


## TL;DR;

```bash
$ helm install .
```

## Introduction

This chart bootstraps the Gateway on a [Kubernetes](http://kubernetes.io) cluster using the [Helm](https://helm.sh) package manager.

## Installing the Chart

Currently this chart is not assembled in charts archives, so [helm-git](https://github.com/aslafy-z/helm-git) is required for remote the installation

To install the chart with the release name `my-release`:

```bash
$ helm install --name my-release git+https://github.com/Telecominfraproject/openlan-cgw@helm/cgw-0.1.0.tgz?ref=master
```

The command deploys the Gateway on the Kubernetes cluster in the default configuration. The [configuration](#configuration) section lists the parameters that can be configured during installation.

> **Tip**: List all releases using `helm list`

## Uninstalling the Chart

To uninstall/delete the `my-release` deployment:

```bash
$ helm delete my-release
```

The command removes all the Kubernetes components associated with the chart and deletes the release.

## Configuration

The following table lists the configurable parameters of the chart and their default values. If Default value is not listed in the table, please refer to the [Values](values.yaml) files for details.

| Parameter | Type | Description | Default |
|-----------|------|-------------|---------|
| replicaCount | number | Amount of replicas to be deployed | `1` |
| strategyType | string | Application deployment strategy | `'Recreate'` |
| nameOverride | string | Override to be used for application deployment |  |
| fullnameOverride | string | Override to be used for application deployment (has priority over nameOverride) |  |
| images.cgw.repository | string | Docker image repository |  |
| images.cgw.tag | string | Docker image tag | `'master'` |
| images.cgw.pullPolicy | string | Docker image pull policy | `'Always'` |
| services.cgw.type | string | Gateway service type | `'ClusterIP'` |
| services.cgw.ports.websocket.servicePort | number | Websocket endpoint port to be exposed on service | `15002` |
| services.cgw.ports.websocket.targetPort | number | Websocket endpoint port to be targeted by service | `15002` |
| services.cgw.ports.websocket.protocol | string | Websocket endpoint protocol | `'TCP'` |
| services.cgw.ports.metrics.servicePort | number | Metrics API endpoint port to be exposed on service | `15003` |
| services.cgw.ports.metrics.targetPort | number | Metrics API endpoint port to be targeted by service | `8080` |
| services.cgw.ports.metrics.protocol | string | Metrics API endpoint protocol | `'TCP'` |
| services.cgw.ports.grpc.servicePort | string | Internal REST API endpoint port to be exposed on service | `15051` |
| services.cgw.ports.grpc.targetPort | number | Internal REST API endpoint port to be targeted by service | `50051` |
| services.cgw.ports.grpc.protocol | string | Internal REST API endpoint protocol | `'TCP'` |
| checks.cgw.liveness.httpGet.path | string | Liveness check path to be used | `'/health'` |
| checks.cgw.liveness.httpGet.port | number | Liveness check port to be used (should be pointint to ALB endpoint) | `15003` |
| checks.cgw.readiness.httpGet.path | string | Readiness check path to be used | `'/health'` |
| checks.cgw.readiness.httpGet.port | number | Readiness check port to be used (should be pointint to ALB endpoint) | `15003` |
| volumes.cgw | array | Defines list of volumes to be attached to the Gateway |  |
| persistence.enabled | boolean | Defines if the Gateway requires Persistent Volume (required for permanent files storage and SQLite DB if enabled) | `True` |
| persistence.accessModes | array | Defines PV access modes |  |
| persistence.size | string | Defines PV size | `'10Gi'` |
| podIP_as_grpc_host | boolean | If True `CGW_GRPC_PUBLIC_HOST` environment variable will be populated with PodIP. | `false` |
| public\_env\_variables | hash | Defines list of environment variables to be passed to the Gateway via ConfigMaps | |
| secret\_env\_variables | hash | Defines list of secret environment variables to be passed to the Gateway via secrets | |
| existingEnvSecret | hash | Defines list of secret environment variables to be passed to the Gateway via secrets | |
| cgw\_certs | hash | Defines files (keys and certificates) that should be passed to the Gateway (PEM format is adviced to be used) (see `volumes.cgw` on where it is mounted). If `existingCgwCertsSecret` is set, certificates passed this way will not be used. |  |
| existingCgwCertsSecret | string | Existing Kubernetes secret containing all environment variables to the Gateway. If set, environment variables from `secret_env_variables` key are ignored | `""` |
| db\_cert | hash | Defines root certificate which should be passed to Gateway to postgres via SSL `(see volumes.cgw` on where it is mounted). If `existingDBCertsSecret` is set, certificates passed this way will not be used. Required if `CGW_DB_TLS = "yes"` or `CGW_REDIS_TLS: "yes"` |  |
| existingDBCertsSecret | string | Existing Kubernetes secret containing root certificate required for microservice to connect to postgres database. If set, certificates from `db_cert` key are ignored. Required if `CGW_DB_TLS = "yes"` or `CGW_REDIS_TLS: "yes"` | `""` |
| certsCAs | hash | Defines files with CAs that should be passed to the Gateway (see `volumes.cgw` on where it is mounted) |  |


Specify each parameter using the `--set key=value[,key=value]` argument to `helm install`. For example,

```bash
$ helm install --name my-release --set replicaCount=1 .
```

The above command sets that only 1 instance of your app should be running

Alternatively, a YAML file that specifies the values for the parameters can be provided while installing the chart. For example,

```bash
$ helm install --name my-release -f values.yaml .
```

> **Tip**: You can use the default [values.yaml](values.yaml) as a base for customization.
