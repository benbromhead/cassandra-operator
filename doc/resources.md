## Resources and Labels
Currently the operator creates the following k8s resources:
- A stateful set that manages Cassandra nodes.
  - Each stateful set is responsible for managing a separate rack for Cassandra cluster.
- A seed discovery headless service (two nodes per DC are included via label selections). This is used as the Cassandra seed list.
- A cluster headless service (good for client discovery)
