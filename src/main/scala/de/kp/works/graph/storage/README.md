
# Graph Database Connectors

List of supported Graph Databases:

## [Nebula Graph](https://nebula-graph.io/)

**Nebula Graph** is an open-source graph database capable of hosting super large scale graphs 
with dozens of billions of vertices (nodes) and trillions of edges, with milliseconds of 
latency.

**DeepGraph** leverages Nebula Graph's Spark Connector to read & write vertices and edges from
and to Nebula Graph. 

## [Dgraph](https://dgraph.io)

**Dgraph** is a native GraphQL graph database that is built to be distributed. This makes it 
highly scalable, performant, and blazing fast – even for complex queries over terabytes of data.

**DeepGraph** leverages Dgraph's Spark Connector to read & write vertices and edges from
and to Dgraph.

## [HGraphDB](https://github.com/rayokota/hgraphdb)

**HGraphDB** exposes Apache HBase as a TinkerPop Graph Database. **DeepGraph** leverages a modification
of Hortonworks* Spark-on-HBase Connector (SHC) to read vertices and edges from HGraphDB's HBase backend
and transforms them int a GraphFrame for further analysis.
