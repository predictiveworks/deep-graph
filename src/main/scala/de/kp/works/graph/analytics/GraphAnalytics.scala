package de.kp.works.graph.analytics
/*
 * Copyright (c) 2019 - 2021 Dr. Krusche & Partner PartG. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 * @author Stefan Krusche, Dr. Krusche & Partner PartG
 *
 */

import de.kp.works.spark.Session
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}
import org.graphframes.GraphFrame

object GraphAnalytics {

  private val session = Session.getSession
  /**
   * Betweenness centrality measures the number of times a node lies on the
   * shortest path between other nodes.
   *
   * This measure shows which nodes are ‘bridges’ between nodes in a network.
   * It does this by identifying all the shortest paths and then counting how
   * many times each node falls on one.
   *
   * It is made to find the individuals who influence the flow around a system.
   *
   * Betweenness is useful for analyzing communication dynamics, but should be
   * used with care. A high betweenness count could indicate someone holds authority
   * over disparate clusters in a network, or just that they are on the periphery of
   * both clusters.
   */
  def betweenness(graphframe:GraphFrame, `type`:String = "edmonds"):DataFrame = {
    /*
     * This method leverages the GraphX implementation as Betweenness
     * is currently not supported by GraphFrames.
     *
     * STEP #1: As a first step, the GraphFrames representation of the
     * graph is transformed into the GraphX format.
     *
     * Note, GraphFrames automatically ensures that networks whose `id`
     * columns does not contain numeric identifiers are transformed.
     */
    val g:Graph[Row, Row] = graphframe.toGraphX
    /*
     * The Betweenness implementation of Sparkling-Graph requires
     * an `edge` representation that is numeric. As the betweenness
     * does not affect any edge, we streamline the edge attributes
     * to a constant value
     */
    val v = g.vertices
    val e = g.edges.map(edge => Edge(edge.srcId, edge.dstId, 1))

    val sample = Graph(v, e)
    /*
     * STEP #2: Apply the betweenness operator. The result is
     * a 2-column dataframe, with `vertex` and `measure`.
     */
    val operator = new Betweenness[Row, Int]()
    val result = operator.transform(sample)
    /*
     * STEP #3: Extend vertex schema and join with the operator
     * result to provide a vertex dataframe that is enriched with
     * the betweenness measure.
     */
    val schema = StructType(
      Array(StructField("vertex", LongType, nullable = false)) ++
        graphframe.vertices.schema.fields)

    val vertices = session.createDataFrame(v.map(vertex => {
      val values = Seq(vertex._1.toLong) ++ vertex._2.toSeq
      Row.fromSeq(values)
    }), schema)

    vertices.join(result, Seq("vertex"))
      .drop("vertex")
      .withColumnRenamed("measure", "betweenness")

  }
  /**
   * Closeness centrality scores each node based on their ‘closeness’ to all other
   * nodes in the network.
   *
   * This measure calculates the shortest paths between all nodes, then assigns each
   * node a score based on its sum of shortest paths.
   *
   * Its made to find the individuals who are best placed to influence the entire network
   * most quickly.
   *
   * Closeness centrality can help find good ‘broadcasters’, but in a highly-connected
   * network, you will often find all nodes have a similar score. What may be more useful
   * is using Closeness to find influencers in a single cluster.
   */
  def closeness(graphframe:GraphFrame):DataFrame = {
    /*
     * This method leverages the GraphX implementation as Betweenness
     * is currently not supported by GraphFrames.
     *
     * STEP #1: As a first step, the GraphFrames representation of the
     * graph is transformed into the GraphX format.
     *
     * Note, GraphFrames automatically ensures that networks whose `id`
     * columns does not contain numeric identifiers are transformed.
     */
    val g:Graph[Row, Row] = graphframe.toGraphX
    /*
     * The Closeness implementation of Sparkling-Graph requires
     * an `edge` representation that is numeric. As the closeness
     * does not affect any edge, we streamline the edge attributes
     * to a constant value
     */
    val v = g.vertices
    val e = g.edges.map(edge => Edge(edge.srcId, edge.dstId, 1))

    val sample = Graph(v, e)
    /*
     * STEP #2: Apply the closeness operator. The result is
     * a 2-column dataframe, with `vertex` and `measure`.
     */
    val operator = new Closeness[Row, Int]()
    val result = operator.transform(sample)
    /*
     * STEP #3: Extend vertex schema and join with the operator
     * result to provide a vertex dataframe that is enriched with
     * the closeness measure.
     */
    val schema = StructType(
      Array(StructField("vertex", LongType, nullable = false)) ++
        graphframe.vertices.schema.fields)

    val vertices = session.createDataFrame(v.map(vertex => {
      val values = Seq(vertex._1.toLong) ++ vertex._2.toSeq
      Row.fromSeq(values)
    }), schema)

    vertices.join(result, Seq("vertex"))
      .drop("vertex")
      .withColumnRenamed("measure", "closeness")

  }
  /**
   * Local Clustering Coefficient for vertex tells us howe close its neighbors
   * are. It’s number of existing connections in neighborhood divided by number
   * of all possible connections.
   */
  def clustering(graphframe:GraphFrame):DataFrame = {
    /*
     * This method leverages the GraphX implementation as Clustering
     * is currently not supported by GraphFrames.
     *
     * STEP #1: As a first step, the GraphFrames representation of the
     * graph is transformed into the GraphX format.
     *
     * Note, GraphFrames automatically ensures that networks whose `id`
     * columns does not contain numeric identifiers are transformed.
     */
    val g:Graph[Row, Row] = graphframe.toGraphX
    /*
     * The Clustering implementation of Sparkling-Graph requires
     * an `edge` representation that is numeric. As the closeness
     * does not affect any edge, we streamline the edge attributes
     * to a constant value
     */
    val v = g.vertices
    val e = g.edges.map(edge => Edge(edge.srcId, edge.dstId, 1))

    val sample = Graph(v, e)
    /*
     * STEP #2: Apply the clustering operator. The result is
     * a 2-column dataframe, with `vertex` and `measure`.
     */
    val operator = new Clustering[Row, Int]()
    val result = operator.transform(sample)
    /*
     * STEP #3: Extend vertex schema and join with the operator
     * result to provide a vertex dataframe that is enriched with
     * the clustering measure.
     */
    val schema = StructType(
      Array(StructField("vertex", LongType, nullable = false)) ++
        graphframe.vertices.schema.fields)

    val vertices = session.createDataFrame(v.map(vertex => {
      val values = Seq(vertex._1.toLong) ++ vertex._2.toSeq
      Row.fromSeq(values)
    }), schema)

    vertices.join(result, Seq("vertex"))
      .drop("vertex")
      .withColumnRenamed("measure", "clustering")

  }
  /**
   * Common Neighbours measure is defined as the number of common
   * neighbours of two given vertices.
   */
  def commonNeighbors(graphframe:GraphFrame):DataFrame = {
    /*
     * This method leverages the GraphX implementation as Clustering
     * is currently not supported by GraphFrames.
     *
     * STEP #1: As a first step, the GraphFrames representation of the
     * graph is transformed into the GraphX format.
     *
     * Note, GraphFrames automatically ensures that networks whose `id`
     * columns does not contain numeric identifiers are transformed.
     */
    val g:Graph[Row, Row] = graphframe.toGraphX
    /*
     * STEP #2: Apply the clustering operator. The result is
     * a 2-column dataframe, with `vertex` and `measure`.
     */
    val operator = new CommonNeighbors[Row, Row]()
    val result = operator.transform(g)
    /*
     * STEP #3: Extend edge schema and join with the operator
     * result to provide an edge dataframe that is enriched with
     * the common neighbors measure.
     */
    val schema = StructType(
      Array(
        StructField("s_vertex", LongType, nullable = false),
        StructField("d_vertex", LongType, nullable = false)) ++
        graphframe.edges.schema.fields)

    val edges = session.createDataFrame(g.edges.map(edge => {
      val values = Seq(edge.srcId, edge.dstId) ++ edge.attr.toSeq
      Row.fromSeq(values)
    }), schema)

    edges.join(result, Seq("s_vertex", "d_vertex"))
      .drop("s_vertex", "d_vertex")
      .withColumnRenamed("measure", "neighbors")

  }
  /**
   * Degree centrality assigns an importance score based simply on the
   * number of (in- and outgoing) links held by each node.
   *
   * This specifies how many direct, ‘one hop’ connections each node has
   * to other nodes in the network.
   *
   * It is made to find very connected individuals, popular individuals,
   * individuals who are likely to hold most information or individuals who
   * can quickly connect with the wider network.
   */
  def degree(graphframe:GraphFrame):DataFrame = {
    /*
     * This method directly uses the `inDegrees` and `outDegrees` method
     * of a GraphFrame graph and joins both degrees into a single result
     */
    val inDegrees = graphframe.inDegrees
    val outDegrees = graphframe.outDegrees
    /*
     * Both individual datasets are 2-column dataframes with `id` and
     * `inDegree` and `outDegree` column. These frames are joined to
     * provide a full degree description for each node of the network.
     */
    val result = inDegrees.join(outDegrees, Seq("id"))
    /*
     * `id`, `inDegree` and `outDegree`.
     */
    result
  }
  /**
   * Like degree centrality, EigenCentrality measures a node’s influence based on the number
   * of links it has to other nodes in the network. EigenCentrality then goes a step further
   * by also taking into account how well connected a node is, and how many links their connections
   * have, and so on through the network.
   *
   * By calculating the extended connections of a node, EigenCentrality can identify nodes with
   * influence over the whole network, not just those directly connected to it.
   *
   * EigenCentrality is a good ‘all-round’ SNA score, handy for understanding human social networks,
   * but also for understanding networks like malware propagation.
   */
  def eigenvector(graphframe:GraphFrame):DataFrame = {
    /*
     * This method leverages the GraphX implementation as Eigenvector
     * Centrality is currently not supported by GraphFrames.
     *
     * STEP #1: As a first step, the GraphFrames representation of the
     * graph is transformed into the GraphX format.
     *
     * Note, GraphFrames automatically ensures that networks whose `id`
     * columns does not contain numeric identifiers are transformed.
     */
    val g:Graph[Row, Row] = graphframe.toGraphX
    /*
     * The Eigenvector Centrality implementation of Sparkling-Graph
     * requires an `edge` representation that is numeric.
     *
     * As the centrality does not affect any edge, we streamline the
     * edge attributes to a constant value
     */
    val v = g.vertices
    val e = g.edges.map(edge => Edge(edge.srcId, edge.dstId, 1))

    val sample = Graph(v, e)
    /*
     * STEP #2: Apply the eigenvector operator. The result is
     * a 2-column dataframe, with `vertex` and `measure`.
     *
     * Eigenvector centrality measure give us information about
     * how a given node is important in network. It is based on
     * degree centrality. In here we have more sophisticated version,
     * where connections are not equal.
     *
     * Eigenvector centrality is a more general approach than PageRank.
     */
    val operator = new Eigenvector[Row, Int]()
    val result = operator.transform(sample)
    /*
     * STEP #3: Extend vertex schema and join with the operator
     * result to provide a vertex dataframe that is enriched with
     * the eigenvector measure.
     */
    val schema = StructType(
      Array(StructField("vertex", LongType, nullable = false)) ++
        graphframe.vertices.schema.fields)

    val vertices = session.createDataFrame(v.map(vertex => {
      val values = Seq(vertex._1.toLong) ++ vertex._2.toSeq
      Row.fromSeq(values)
    }), schema)

    vertices.join(result, Seq("vertex"))
      .drop("vertex")
      .withColumnRenamed("measure", "importance")

  }
  /**
   * This method computes the average embeddedness of neighbours
   * of a given vertex.
   */
  def embeddedness(graphframe:GraphFrame):DataFrame = {
    /*
     * This method leverages the GraphX implementation as Embeddedness
     * is currently not supported by GraphFrames.
     *
     * STEP #1: As a first step, the GraphFrames representation of the
     * graph is transformed into the GraphX format.
     *
     * Note, GraphFrames automatically ensures that networks whose `id`
     * columns does not contain numeric identifiers are transformed.
     */
    val g:Graph[Row, Row] = graphframe.toGraphX
    /*
     * The Embeddedness implementation of Sparkling-Graph requires an
     * `edge` representation that is numeric.
     *
     * As the embeddedness does not affect any edge, we streamline the
     * edge attributes to a constant value
     */
    val v = g.vertices
    val e = g.edges.map(edge => Edge(edge.srcId, edge.dstId, 1))

    val sample = Graph(v, e)
    /*
     * STEP #2: Apply the embeddedness operator. The result is
     * a 2-column dataframe, with `vertex` and `measure`.
     */
    val operator = new Embeddedness[Row, Int]()
    val result = operator.transform(sample)
    /*
     * STEP #3: Extend vertex schema and join with the operator
     * result to provide a vertex dataframe that is enriched with
     * the embeddedness measure.
     */
    val schema = StructType(
      Array(StructField("vertex", LongType, nullable = false)) ++
        graphframe.vertices.schema.fields)

    val vertices = session.createDataFrame(v.map(vertex => {
      val values = Seq(vertex._1.toLong) ++ vertex._2.toSeq
      Row.fromSeq(values)
    }), schema)

    vertices.join(result, Seq("vertex"))
      .drop("vertex")
      .withColumnRenamed("measure", "embeddedness")

  }
  /**
   * Freeman’s centrality tells us how heterogeneous the
   * degree centrality is among the vertices of the network.
   *
   * For a start network, we will get a value 1.
   */
  def freeman(graphframe:GraphFrame):Double = {
    /*
     * This method leverages the GraphX implementation as Freeman
     * Centrality is currently not supported by GraphFrames.
     *
     * STEP #1: As a first step, the GraphFrames representation of the
     * graph is transformed into the GraphX format.
     *
     * Note, GraphFrames automatically ensures that networks whose `id`
     * columns does not contain numeric identifiers are transformed.
     */
    val g:Graph[Row, Row] = graphframe.toGraphX
    /*
     * STEP #2: Apply the Freeman operator.
     */
    val operator = new Freeman[Row, Row]()
    operator.transform(g)

  }
  /**
   * After measure computation, each vertex of graph will have assigned
   * two scores (hub, authority). Where hub score is proportional to the
   * sum of authority score of its neighbours, and authority score is
   * proportional to sum of hub score of its neighbours.
   */
  def hits(graphframe:GraphFrame):DataFrame = {
    /*
     * This method leverages the GraphX implementation as Hits Score
     * is currently not supported by GraphFrames.
     *
     * STEP #1: As a first step, the GraphFrames representation of the
     * graph is transformed into the GraphX format.
     *
     * Note, GraphFrames automatically ensures that networks whose `id`
     * columns does not contain numeric identifiers are transformed.
     */
    val g:Graph[Row, Row] = graphframe.toGraphX
    /*
     * The Hits Score implementation of Sparkling-Graph requires an
     * `edge` representation that is numeric.
     *
     * As the scoring does not affect any edge, we streamline the
     * edge attributes to a constant value
     */
    val v = g.vertices
    val e = g.edges.map(edge => Edge(edge.srcId, edge.dstId, 1))

    val sample = Graph(v, e)
    /*
     * STEP #2: Apply the Hits operator. The result is
     * a 3-column dataframe, with `vertex`, `auth` and
     * `hub`.
     */
    val operator = new Hits[Row, Int]()
    val result = operator.transform(sample)
    /*
     * STEP #3: Extend vertex schema and join with the operator
     * result to provide a vertex dataframe that is enriched with
     * the Hits measure.
     */
    val schema = StructType(
      Array(StructField("vertex", LongType, nullable = false)) ++
        graphframe.vertices.schema.fields)

    val vertices = session.createDataFrame(v.map(vertex => {
      val values = Seq(vertex._1.toLong) ++ vertex._2.toSeq
      Row.fromSeq(values)
    }), schema)

    vertices.join(result, Seq("vertex"))
      .drop("vertex")

  }
  /**
   * Modularity measures the strength of the division of a network into
   * communities (modules, clusters). Measures takes values from range [-1, 1].
   *
   * A value close to 1 indicates strong community structure. When Q=0 then the
   * community division is not better than random.
   */
  def modularity(graphframe:GraphFrame):Double = {
    /*
     * This method leverages the GraphX implementation as Modularity
     * is currently not supported by GraphFrames.
     *
     * STEP #1: As a first step, the GraphFrames representation of the
     * graph is transformed into the GraphX format.
     *
     * Note, GraphFrames automatically ensures that networks whose `id`
     * columns does not contain numeric identifiers are transformed.
     */
    val g:Graph[Row, Row] = graphframe.toGraphX

    val v = g.vertices.map{vertex => (vertex._1, 1L)}
    val e = g.edges

    val sample = Graph(v, e)
    /*
     * STEP #2: Apply the modularity operator.
     */
    val operator = new Modularity[Row]()
    operator.transform(sample)

  }
  /**
   * Neighborhood connectivity is a measure based on degree centrality.
   * Connectivity of a vertex is its degree. Neighborhood connectivity
   * is average connectivity of neighbours of given vertex.
   */
  def neighborhood(graphframe:GraphFrame):DataFrame = {
    /*
     * This method leverages the GraphX implementation as Neighborhood
     * Connectivity is currently not supported by GraphFrames.
     *
     * STEP #1: As a first step, the GraphFrames representation of the
     * graph is transformed into the GraphX format.
     *
     * Note, GraphFrames automatically ensures that networks whose `id`
     * columns does not contain numeric identifiers are transformed.
     */
    val g:Graph[Row, Row] = graphframe.toGraphX
    /*
     * The Neighborhood Connectivity implementation of Sparkling-Graph
     * requires an `edge` representation that is numeric.
     *
     * As the connectivity does not affect any edge, we streamline the
     * edge attributes to a constant value
     */
    val v = g.vertices
    val e = g.edges.map(edge => Edge(edge.srcId, edge.dstId, 1))

    val sample = Graph(v, e)
    /*
     * STEP #2: Apply the neighborhood operator. The result is
     * a 2-column dataframe, with `vertex` and `measure`.
     */
    val operator = new Neighborhood[Row, Int]()
    val result = operator.transform(sample)
    /*
     * STEP #3: Extend vertex schema and join with the operator
     * result to provide a vertex dataframe that is enriched with
     * the neighborhood measure.
     */
    val schema = StructType(
      Array(StructField("vertex", LongType, nullable = false)) ++
        graphframe.vertices.schema.fields)

    val vertices = session.createDataFrame(v.map(vertex => {
      val values = Seq(vertex._1.toLong) ++ vertex._2.toSeq
      Row.fromSeq(values)
    }), schema)

    vertices.join(result, Seq("vertex"))
      .drop("vertex")
      .withColumnRenamed("measure", "neighborhood")

  }
  def pscan(graphframe:GraphFrame, epsilon:Double=0.1):Unit = {

  }
  /**
   * PageRank is a variant of EigenCentrality, also assigning nodes a score based on their connections,
   * and their connections’ connections. The difference is that PageRank also takes link direction and
   * weight into account – so links can only pass influence in one direction, and pass different amounts
   * of influence.
   *
   * This measure uncovers nodes whose influence extends beyond their direct connections into the wider
   * network.
   *
   * Because it takes into account direction and connection weight, PageRank can be helpful for understanding
   * citations and authority.
   *
   * PageRank is famously one of the ranking algorithms behind the original Google search engine (the ‘Page’
   * part of its name comes from creator and Google founder, Sergei Brin).
   */
  def pageRank():Unit = {
    throw new Exception("Not implemented yet.")
  }
}
