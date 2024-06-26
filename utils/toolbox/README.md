# CGW Toolbox

## About

Toolbox can be used to run the various utilities in the _openlan-cgw/utils/_ directory within the
confines of the cluster (and with full access to the various services.)
It has utilities to access redis, kafka and postgres.

## Usage

```bash
# To run a temporary toolbox (disappears when you exit the session):
kubectl -n NAMESPACE run -it cgwtools --rm --image tip-tip-wlan-cloud-ucentral.jfrog.io/cgw-toolbox:latest --command -- /bin/bash

# Keep it around:
kubectl -n NAMESPACE run -it cgwtools --image tip-tip-wlan-cloud-ucentral.jfrog.io/cgw-toolbox:latest --command -- /bin/bash
# You can then attach to it with this command at any time
kubectl -n NAMESPACE attach cgwtools -c cgwtools -it

# Then delete it when you're done
kubectl -n NAMESPACE delete pod cgwtools
```

Once inside you can run various commands:

TODO show some available commands
