# Spark and OpenShift AI

## Setup

1. Install Spark operator through Operator Hub.
2. Instantiate SparkOperator within a given namespace.
3. Deploy Spark cluster role from `manifests/spark-clusterrole.yaml`.
4. Create or switch to namespace in which Spark jobs should be executed.
5. Prepare RBAC by deploying `manifests/spark-namespace-rbac.yaml`.
6. (optional) Test the setup by deploying `manifests/pi-sparkapp.yaml`. The Spark driver and executor pods should run through successfully.