
# Dgraph Spark Connector

The current implementation of the Spark connector supports *read* requests
and transforms retrieved vertices and edges into a GraphFrame for advanced
graph analytics.

The implementation is based on the work of [G-Research](https://github.com/G-Research/spark-dgraph-connector) 
and was integrated into this project as the original connector does not support 
Scala version 2.11.