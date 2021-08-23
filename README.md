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
Freeman's network centrality is a means to determine the heterogeneity
of the degree centrality among all the vertices of the network.
```
def freeman(graphframe:GraphFrame):Double
```

#### Neighborhood Connectivity
Neighborhood connectivity is a measure based on degree centrality, and computes the average connectivity
of the neighbors of a given vertex. Here, connectivity is the degree centrality of a vertex.
```
def neighborhood(graphframe:GraphFrame):DataFrame
```

#### Page Rank
PageRank is a variant of EigenCentrality, also assigning nodes a score 
based on their connections, and their connections’ connections. 

The difference is that PageRank also takes link direction and weight into 
account – so links can only pass influence in one direction, and pass different 
amounts of influence.

This measure uncovers nodes whose influence extends beyond their direct connections 
into the wider network. Because it takes into account direction and connection weight, 
PageRank can be helpful for understanding citations and authority.

PageRank is famously one of the ranking algorithms behind the original Google search 
engine (the ‘Page’ part of its name comes from the creator and Google founder, Sergei Brin).
```
def pageRank(graphframe:GraphFrame, maxIter:Int = 20, resetProbability:Double=0.15):DataFrame
```

**DeepGraph** also supports the personalized PageRank algorithm.
```
def personalizedPageRank(graphframe:GraphFrame, landmarks:Array[Any], maxIter:Int = 20, resetProbability:Double=0.15): DataFrame
```

### Community Detection

#### Connected Components
```
def connectedComponents(graphframe:GraphFrame):DataFrame
```

#### Label Propagation (LPA)
Each node in the network is initially assigned to its own community.
At every super step, nodes send their community affiliation to all
neighbors and update their state to the mode community affiliation
of incoming messages.

LPA is a standard community detection algorithm for graphs. It is very
inexpensive computationally, although (1) convergence is not guaranteed
and (2) one can end up with trivial solutions (all nodes are identified
into a single community).

This method runs the GraphFrame Label Propagation Algorithm
for detecting communities in networks.
```
def detectCommunities(graphframe:GraphFrame, iterations:Int):DataFrame
```

This method leverages the GraphX implementation of Community
Detection as an alternative approach.
```
def detectCommunities(graphframe:GraphFrame, epsilon:Double=0.1):DataFrame
```

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
Modularity measures strength of division of a network into communities (modules,clusters). 
Measures take values from range [-1, 1]. Values close to 1 indicates strong community structure. 
A value of 0 indicates that the community division is not better than random.
```
def modularity(graphframe:GraphFrame):Double
```

#### Strongly Connected Components
```
def stronglyConnectedComponents(graphframe:GraphFrame, maxIter:Int = 10):DataFrame
```

### Embeddedness
This method computes the average embeddedness of neighbours
of a given vertex.
```
def embeddedness(graphframe:GraphFrame):DataFrame
```

### HITS
After measure computation, each vertex of graph will have assigned
two scores (hub, authority). Where hub score is proportional to the
sum of authority score of its neighbours, and authority score is
proportional to sum of hub score of its neighbours.
```
def hits(graphframe:GraphFrame):DataFrame
```

### Link Prediction

#### Adamic Adar
Adamic Adar is a measure used to compute the closeness of nodes based on their
shared neighbors. This measure is defined as the inverted sum of degrees of the
common neighbours for given two vertices.
```
def adamicAdar(graphframe:GraphFrame):DataFrame
```

#### Common Neighbors
Common Neighbours measure is defined as the number of common
neighbours of two given vertices.
```
def commonNeighbors(graphframe:GraphFrame):DataFrame
```

### Path Finding

#### Shortest Paths

This algorithm computes the shortest paths from each vertex to a given set of landmark 
vertices, taking edge direction into account.
```
def shortestPaths(graphframe:GraphFrame, landmarks:Array[Any]):DataFrame
```

## Storage

**DeepGraph** supports a handpicked list of awesome distributed graph databases. An important 
use case is to leverage the power of connected data for contextualization of DataFrame-centric 
machine & deep learning. As important is the generation of knowledge graphs from insight and 
foresight revealed by the full spectrum of machine intelligence. 

### [Dgraph](https://dgraph.io)

**Dgraph** is a native GraphQL graph database that is built to be distributed. This makes it
highly scalable, performant, and blazing fast – even for complex queries over terabytes of data.

**DeepGraph** leverages Dgraph's Spark Connector to read & write vertices and edges from
and to Dgraph.

### [HGraphDB](https://github.com/rayokota/hgraphdb)

**HGraphDB** exposes Apache HBase as a TinkerPop Graph Database. **DeepGraph** leverages a modification
of Hortonworks Spark-on-HBase Connector (SHC) to read vertices and edges from HGraphDB's HBase backend
and transforms them int a GraphFrame for further analysis.
