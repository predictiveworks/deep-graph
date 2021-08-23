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
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, explode, udf}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}
import org.graphframes.GraphFrame

object GraphAnalytics {

  private val session = Session.getSession

  /**
   * Adamic/Adar measures is defined as inverted sum
   * of degrees of common neighbours for given two vertices.
   */
  def adamicAdar(graphframe:GraphFrame):DataFrame = {
    /*
     * This method leverages the GraphX implementation as Adamic/Adar
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
     * STEP #2: Apply the AdamicAdar operator.
     */
    val operator = new AdamicAdar[Row, Row]()
    val result = operator.transform(g)
    /*
     * STEP #3: Extend edge schema and join with the operator
     * result to provide an edge dataframe that is enriched with
     * the Adamic/Adar measure.
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

  }
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
    val result = operator.transform(sample, `type`)
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

  }
  /**
   * Closeness centrality scores each node based on its ‘closeness’ to all other
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

  }
  /**
   * Common Neighbours measure is defined as the number of common
   * neighbours of two given vertices.
   */
  def commonNeighbors(graphframe:GraphFrame):DataFrame = {
    /*
     * This method leverages the GraphX implementation as Common
     * Neighbors is currently not supported by GraphFrames.
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

  }
  def connectedComponents(graphframe:GraphFrame):DataFrame = {

    val result = graphframe
      .connectedComponents
      .run

    result

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
   * This method runs the GraphFrame Label Propagation Algorithm
   * for detecting communities in networks.
   *
   * Each node in the network is initially assigned to its own community.
   * At every super step, nodes send their community affiliation to all
   * neighbors and update their state to the mode community affiliation
   * of incoming messages.
   *
   * LPA is a standard community detection algorithm for graphs. It is very
   * inexpensive computationally, although (1) convergence is not guaranteed
   * and (2) one can end up with trivial solutions (all nodes are identified
   * into a single community).
   */
  def detectCommunities(graphframe:GraphFrame, iterations:Int):DataFrame = {

    val result = graphframe.labelPropagation.maxIter(iterations).run()
    /*
     * The algorithm assigns a `label` (Long) to each vertex
     * of the provided graph. This enables a subsequent stage
     * to organize vertices in communities.
     */
    result

  }

  def detectCommunities(graphframe:GraphFrame, epsilon:Double=0.1):DataFrame = {
    /*
     * This method leverages the GraphX implementation of Community
     * Detection as an alternative approach.
     *
     * STEP #1: As a first step, the GraphFrames representation of the
     * graph is transformed into the GraphX format.
     *
     * Note, GraphFrames automatically ensures that networks whose `id`
     * columns does not contain numeric identifiers are transformed.
     */
    val g:Graph[Row, Row] = graphframe.toGraphX
   /*
     * STEP #2: Apply the community operator.
     */
    val operator = new Community[Row, Row]()
    val result = operator.detectCommunities(g, epsilon)
    /*
     * STEP #3: Extend vertex schema and join with the operator
     * result to provide a vertex dataframe that is enriched with
     * the detected community label.
     */
    val v = g.vertices

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
   * Like degree centrality, EigenCentrality measures a node’s influence based on the number
   * of links it has to other nodes in the network. EigenCentrality then goes a step further
   * by also taking into account how well connected a node is, and how many links their connections
   * have, and so on through the network.
   *
   * By calculating the extended connections of a node, Eigen Centrality can identify nodes with
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
  def louvain(graphframe:GraphFrame, minProgress:Int, progressCounter:Int):DataFrame = {
    /*
     * This method leverages the GraphX implementation as Louvain
     * Community Detection is currently not supported by GraphFrames.
     *
     * STEP #1: As a first step, the GraphFrames representation of the
     * graph is transformed into the GraphX format.
     *
     * Note, GraphFrames automatically ensures that networks whose `id`
     * columns does not contain numeric identifiers are transformed.
     */
    val g:Graph[Row, Row] = graphframe.toGraphX
    /*
     * The Louvain implementation requires an `edge` representation that
     * is a Long.
     *
     * As the community detection does not affect any edge, we streamline
     * the edge attributes to a constant value
     */
    val v = g.vertices.map(vertex => (vertex._1, "*"))
    val e = g.edges.map(edge => Edge(edge.srcId, edge.dstId, 1L))

    val sample = Graph(v, e)
    /*
     * STEP #2: Apply the Louvain operator.
     */
    val operator = new Louvain[String, Long]
    val result = operator.transform(sample)
    /*
     * STEP #3: Extend vertex schema and join with the operator
     * result to provide a vertex dataframe that is enriched with
     * the neighborhood measure.
     */
    val schema = StructType(
      Array(StructField("vertex", LongType, nullable = false)) ++
        graphframe.vertices.schema.fields)

    val vertices = session.createDataFrame(g.vertices.map(vertex => {
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
  def pageRank(graphframe:GraphFrame, maxIter:Int = 20, resetProbability:Double=0.15):DataFrame = {

    val rankGraph = graphframe
      .pageRank
      .resetProbability(resetProbability)
      .maxIter(maxIter)
      .run()
    /*
     * This method restricts the result to the vertices of the graph
     */
    rankGraph.vertices

  }

  /**
   * This method retrieves the pagerank for each vertex in relation
   * to the provided landmarks.
   */
  def personalizedPageRank(graphframe:GraphFrame, landmarks:Array[Any], maxIter:Int = 20, resetProbability:Double=0.15): DataFrame = {
    /*
     * Run personalised page rank with the landmarks
     * provided and retrieve connections importance
     * relative to the landmarks.
     */
    val rankGraph = graphframe
      .parallelPersonalizedPageRank
      .resetProbability(resetProbability)
      .maxIter(maxIter)
      .sourceIds(landmarks)
      .run()

    /* Determine landmark datatype */
    val landmark = landmarks(0)
    var landmarkType:String = null

    landmark match {
      case _: String => landmarkType = "String"
      case _: Int => landmarkType = "Int"
      case _: Long => landmarkType = "Long"
      case _ => throw new Exception("Landmark datatype is not supported.")
    }

    /* Compute importance */

    def importances_udf(landmarks:Array[Any], landmarkType:String) =
      udf((pageRank: Vector) => {
        pageRank.toArray.zipWithIndex.map({ case (importance, id) =>
          val landmark = landmarks(id)
          if (landmarkType == "Int")
            (landmark.asInstanceOf[Int].toString, importance)

          else if (landmarkType == "Long")
            (landmark.asInstanceOf[Long].toString, importance)

          else
            (landmark.asInstanceOf[String], importance)
        })
      })

    val dropCols = Seq("importances", "importance", "pageranks")
    /*
     * This method restricts the result to the vertices of the graph
     */
    val output = rankGraph.vertices
      .withColumn("importances", importances_udf(landmarks, landmarkType)(col("pageranks")))
      .withColumn("importance", explode(col("importances")))
      .withColumn("reference", col("importance._1"))
      .withColumn("pagerank",  col("importance._2"))
      .drop(dropCols: _*)

    if (landmarkType == "Int")
      output.withColumn("reference", col("reference").cast(IntegerType))

    else if (landmarkType == "Long")
      output.withColumn("reference", col("reference").cast(LongType))

    else
      output.withColumn("reference", col("reference").cast(StringType))

  }

  def shortestPaths(graphframe:GraphFrame, landmarks:Array[Any]):DataFrame = {

    val pathGraph = graphframe.shortestPaths
      .landmarks(landmarks)
      .run

    /* Determine landmark datatype */
    val landmark = landmarks(0)
    var landmarkType:String = null

    landmark match {
      case _: String => landmarkType = "String"
      case _: Int => landmarkType = "Int"
      case _: Long => landmarkType = "Long"
      case _ => throw new Exception("Landmark datatype is not supported.")
    }

    val distances_to_seq = distancesToSeq(landmarkType)
    val dropCols = Seq("distances")

    val output = pathGraph
      .withColumn("distances", distances_to_seq(col("distances")))
      .withColumn("distance",  explode(col("distances")))
      .withColumn("reference", col("distance._1"))
      .withColumn("distance",  col("distance._2"))
      .drop(dropCols: _*)

    if (landmarkType == "Int")
      output.withColumn("reference", col("reference").cast(IntegerType))

    else if (landmarkType == "Long")
      output.withColumn("reference", col("reference").cast(LongType))

    else
      output.withColumn("reference", col("reference").cast(StringType))

  }

  def stronglyConnectedComponents(graphframe:GraphFrame, maxIter:Int = 10):DataFrame = {

    val result = graphframe
      .stronglyConnectedComponents
      .maxIter(maxIter)
      .run

    result
  }

  private def distancesToSeq(landmarkType:String):UserDefinedFunction = {

    if (landmarkType == "Int") {
      udf((m:Map[Int,Int]) => m.toSeq)

    } else if (landmarkType == "Long") {
      udf((m: Map[Long, Int]) => m.toSeq)

    } else {
      udf((m: Map[String, Int]) => m.toSeq)

    }
  }
}
