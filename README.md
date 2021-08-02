# DeepGraph
DeepGraph supports a 360° view on graph technology and supports analytics, storage and visualization.
It is one of the building blocks of a 4D architecture for AI-driven applications.

DeepGraph complemented by DeepPipes, DeepLearning and DeepThreat supports the entire journey from
data to information to knowledge. DeepGraph is based on GraphFrames, extends its functionality and 
thereby supports GraphFrame's mission to provide a unified approach from data to knowledge.

## Analytics

**DeepGraph** supports a GraphFrame-based interface to the following algorithms:

### Centrality

#### Betweenness
Betweenness centrality measures the number of times a node lies on the shortest path between 
other nodes. **DeepGraph** supports a space-efficient parallel algorithm developed by Edmonds,
and also an optimized version developed by Hua et al.

Betweenness centrality is a way of detecting the amount of influence a node has over the flow 
of information in a graph. It is often used to find nodes that serve as a bridge from one part 
of a graph to another.
```
def betweenness(graphframe:GraphFrame, `type`:String = "edmonds"):DataFrame
```

#### Closeness
Closeness centrality scores each node based on its *closeness* to all other nodes in the 
network. This measure calculates the shortest paths between all nodes, then assigns each
node a score based on the inverted sum of shortest paths (farness) from a given node to 
all other nodes.

Closeness centrality is a way of detecting nodes that are able to spread information very 
efficiently through a graph.
```
def closeness(graphframe:GraphFrame):DataFrame
```

#### Clustering
The local clustering coefficient of a vertex determines how close its neighbors are. It is 
calculated as the number of existing connections in the neighborhood divided by number of 
all possible connections.
```
def clustering(graphframe:GraphFrame):DataFrame
```

#### Degree
Degree centrality assigns an importance score based simply on the number of (in- and outgoing)
links held by each vertex.
```
def degree(graphframe:GraphFrame):DataFrame
```

#### Eigenvector
Eigenvector centrality is an algorithm that measures the transitive influence of nodes based on the
based on the number of links it has to other nodes in the network. Eigenvector centrality extends link
degree centrality by also taking into account how well-connected a node is, and how many links its 
connections have, and so on through the network.

By calculating the extended connections of a node, Eigen centrality can identify nodes with  influence 
over the whole network, not just those directly connected to it. It is a good 'all-round' SNA score, 
handy for understanding human social networks, but also for understanding networks like malware propagation.
```
def eigenvector(graphframe:GraphFrame):DataFrame
```

#### Freeman's Network Centrality
TBD

#### Neighborhood Connectivity
TBD

#### Page Rank
TBD

### Community Detection

#### Community Detection (Sparkling)...

#### Connected Components
TBD

#### Label Propagation
TBD

#### Louvain

**DeepGraph** supported a distributed version of the Louvain algorithm to detect
communities in large networks.It maximizes a modularity score for each community, 
where the modularity quantifies the quality of an assignment of nodes to communities. 

The Louvain algorithm is a hierarchical clustering algorithm, that recursively merges 
communities into a single node and executes the modularity clustering on the condensed 
graphs.
```
def louvain(graphframe:GraphFrame, minProgress:Int, progressCounter:Int):DataFrame
```

#### Network Modularity
TBD

#### Strongly Connected Components
TBD

### Embeddedness
TBD

### HITS
TBD

### Link Prediction

#### Adamic Adar
Adamic Adar is a measure used to compute the closeness of nodes based on their
shared neighbors. This measure is defined as the inverted sum of degrees of the
common neighbours for given two vertices.
```
def adamicAdar(graphframe:GraphFrame):DataFrame
```

#### Common Neighbors

TBD

### Path Finding

#### Shortest Paths

This algorithm computes the shortest paths from each vertex to a given set of landmark 
vertices, taking edge direction into account.
```
def shortestPaths(graphframe:GraphFrame, landmarks:Array[Any]):DataFrame
```

## Storage

### Dgraph

## Visualization

