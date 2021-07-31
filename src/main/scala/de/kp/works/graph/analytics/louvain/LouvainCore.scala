package de.kp.works.graph.analytics.louvain
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
 * from: Sotera Defense Solutions
 */

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx._

import scala.reflect.ClassTag

/**
 * Provides low level louvain community detection algorithm functions.
 * For details on the sequential algorithm see:
 *
 * Fast unfolding of communities in large networks, Blondel 2008
 */
class LouvainCore extends Serializable {

  var verbose = false
  /**
   * Generates a new graph of type Graph[VertexState,Long]
   * based on an input graph of type Graph[VD,Long].
   *
   * The resulting graph can be used for louvain computation.
   */
  def createLouvainGraph[VD: ClassTag](graph: Graph[VD, Long]): Graph[LouvainData, Long] = {

    /* Create the initial Louvain graph */

    val nodeWeights = graph.aggregateMessages(
      (e:EdgeContext[VD,Long,Long]) => {
        e.sendToSrc(e.attr)
        e.sendToDst(e.attr)
      },
      (e1: Long, e2: Long) => e1 + e2
    )

    graph.outerJoinVertices(nodeWeights)((vid, data, weightOption) => {
      val weight = weightOption.getOrElse(0L)
      val name = data.asInstanceOf[String]
      new LouvainData(name, vid, name, weight, 0L, weight, false)
    }).partitionBy(PartitionStrategy.EdgePartition2D).groupEdges(_ + _)
  }
  /**
   * Transform a graph from [VD,Long] to a a [VertexState,Long]
   * graph and label each vertex with a community to maximize
   * global modularity (without compressing the graph)
   */
  def louvainFromStandardGraph[VD: ClassTag](sc: SparkContext, graph: Graph[VD, Long], minProgress: Int = 1, progressCounter: Int = 1): (Double, Graph[LouvainData, Long], Int) = {
    val louvainGraph = createLouvainGraph(graph)
    louvain(sc, louvainGraph, minProgress, progressCounter)
  }

  /**
   * For a graph of type Graph[VertexState,Long] label each vertex
   * with a community to maximize global modularity (without compressing
   * the graph)
   */
  def louvain(sc: SparkContext, graph: Graph[LouvainData, Long], minProgress: Int = 1, progressCounter: Int = 1): (Double, Graph[LouvainData, Long], Int) = {

    var louvainGraph = graph.cache()

    val graphWeight = louvainGraph.vertices.map(louvainVertex => {
      val (vertexId, louvainData) = louvainVertex
      louvainData.internalWeight + louvainData.nodeWeight
    }).reduce(_ + _)

    val totalGraphWeight = sc.broadcast(graphWeight)
    if (verbose) println("Total Edge Weight: " + totalGraphWeight.value)
    /*
     * Gather community information from each vertex's
     * local neighborhood
     */
    var communityRDD = louvainGraph.aggregateMessages(sendCommunityData,mergeCommunityMessages)
    /*
     * Materializes the msgRDD and caches it in memory
     */
    var activeMessages = communityRDD.count()

    var updated = 0L - minProgress
    var even = false
    var count = 0
    val maxIter = 100000
    var stop = 0
    var updatedLastPhase = 0L
    do {
      count += 1
      even = !even

      /*
       * label each vertex with its best community based
       * on neighboring community information
       */
      val labeledVertices = louvainVertJoin(louvainGraph, communityRDD, totalGraphWeight, even).cache()

      if (verbose) {
        println("Labeled Vertices:")
        labeledVertices.collect().foreach(println)
      }

      /*
       * Calculate new sigma total value for each community
       * (total weight of each community)
       */
      val communityUpdate = labeledVertices
        .map({ case (vid, vdata) => (vdata.community, vdata.nodeWeight + vdata.internalWeight)})
        .reduceByKey(_ + _).cache()

      /*
       * Map each vertex ID to its updated community information
       */
      val communityMapping = labeledVertices
        .map({ case (vid, vdata) => (vdata.community, vid)})
        .join(communityUpdate)
        .map({ case (community, (vid, sigmaTot)) => (vid, (community, sigmaTot))})
        .cache()

      /*
       * Join the community labeled vertices with the updated
       * community info
       */
      val updatedVertices = labeledVertices.join(communityMapping).map({ case (vertexId, (louvainData, communityTuple)) =>
        val (community, communitySigmaTot) = communityTuple
        louvainData.community = community
        louvainData.communitySigmaTot = communitySigmaTot
        (vertexId, louvainData)
      }).cache()

      labeledVertices.unpersist(blocking = false)
      communityUpdate.unpersist(blocking = false)
      communityMapping.unpersist(blocking = false)

      val prevG = louvainGraph
      louvainGraph = louvainGraph.outerJoinVertices(updatedVertices)((vid, old, newOpt) => newOpt.getOrElse(old))
      louvainGraph.cache()

      /*
       * Gather community information from each vertex's
       * local neighborhood
       */
      val oldMessages = communityRDD

      communityRDD = louvainGraph.aggregateMessages(sendCommunityData, mergeCommunityMessages).cache()
      /*
       * Materializes the graph by forcing computation
       */
      activeMessages = communityRDD.count()

      oldMessages.unpersist(blocking = false)
      updatedVertices.unpersist(blocking = false)
      prevG.unpersistVertices(blocking = false)
      /*
       * Half of the communities can switch on even cycles
       * and the other half on odd cycles (to prevent deadlocks)
       *
       * So we only want to look for progress on odd cycles
       * (after all vertices have had a chance to move)
       */
      if (even) updated = 0
      updated = updated + louvainGraph.vertices.filter(_._2.changed).count

      if (!even) {

        if (verbose) println("Vertices moved: " + java.text.NumberFormat.getInstance().format(updated))
        if (updated >= updatedLastPhase - minProgress) stop += 1

        updatedLastPhase = updated
      }
    } while (stop <= progressCounter && (even || (updated > 0 && count < maxIter)))

    if (verbose) println("Completed in " + count + " cycles")

    /*
     * Use each vertex's neighboring community data to calculate
     * the global modularity of the graph
     */
    val newVertices = louvainGraph.vertices.innerJoin(communityRDD)((vertexId, louvainData, communityMap) => {

      var accumulatedInternalWeight = louvainData.internalWeight
      val sigmaTot = louvainData.communitySigmaTot.toDouble

      def accumulateTotalWeight(totalWeight: Long, item: ((Long, (String, Long)), Long)) = {
        val ((communityId, _), communityEdgeWeight) = item
        if (louvainData.community == communityId)
          totalWeight + communityEdgeWeight
        else
          totalWeight
      }

      accumulatedInternalWeight = communityMap.foldLeft(accumulatedInternalWeight)(accumulateTotalWeight)

      val M = totalGraphWeight.value
      val k_i = louvainData.nodeWeight + louvainData.internalWeight

      val q = (accumulatedInternalWeight.toDouble / M) - ((sigmaTot * k_i) / math.pow(M, 2))
      if (q < 0)
        0
      else
        q
    })
    val actualQ = newVertices.values.reduce(_ + _)
    /*
     * Return the modularity value of the graph along with the
     * graph. vertices are labeled with their community
     */
    (actualQ, louvainGraph, count / 2)
  }

  /**
   * Creates the messages passed between each vertex to convey
   * neighborhood community data.
   */
  private def sendCommunityData(e: EdgeContext[LouvainData, Long, Map[(Long, (String, Long)), Long]]): Unit = {

    val m1 = Map((e.srcAttr.community, (e.srcAttr.communityName, e.srcAttr.communitySigmaTot)) -> e.attr)
    val m2 = Map((e.dstAttr.community, (e.dstAttr.communityName, e.dstAttr.communitySigmaTot)) -> e.attr)

    e.sendToSrc(m2)
    e.sendToDst(m1)

  }

  /**
   * Merge neighborhood community data into a single message
   * for each vertex
   */
  private def mergeCommunityMessages(m1: Map[(Long, (String, Long)), Long], m2: Map[(Long, (String, Long)), Long]) = {
    val newMap = scala.collection.mutable.HashMap[(Long, (String, Long)), Long]()
    m1.foreach({ case (k, v) =>
      if (newMap.contains(k)) newMap(k) = newMap(k) + v
      else newMap(k) = v
    })
    m2.foreach({ case (k, v) =>
      if (newMap.contains(k)) newMap(k) = newMap(k) + v
      else newMap(k) = v
    })
    newMap.toMap
  }

  /**
   * Join vertices with community data from their neighborhood and
   * select the best community for each vertex to maximize change
   * in modularity.
   *
   * Returns a new set of vertices with the updated vertex state.
   */
  private def louvainVertJoin(louvainGraph: Graph[LouvainData, Long], msgRDD: VertexRDD[Map[(Long, (String, Long)), Long]], totalEdgeWeight: Broadcast[Long], even: Boolean) = {
    louvainGraph.vertices.innerJoin(msgRDD)((vid, louvainData, communityMessages) => {

      var bestCommunity = louvainData.community
      var bestCommunityName = louvainData.communityName

      val startingCommunityId = bestCommunity
      var maxDeltaQ = BigDecimal(0.0)

      var bestSigmaTot = 0L

      communityMessages.foreach({ case ((communityId, (communityName, sigmaTotal)), communityEdgeWeight) =>

        val deltaQ = q(startingCommunityId, communityId, sigmaTotal, communityEdgeWeight, louvainData.nodeWeight, louvainData.internalWeight, totalEdgeWeight.value)
        if (deltaQ > maxDeltaQ || (deltaQ > 0 && (deltaQ == maxDeltaQ && communityId > bestCommunity))) {
          maxDeltaQ = deltaQ
          bestCommunity = communityId
          bestCommunityName = communityName
          bestSigmaTot = sigmaTotal
        }
      })
      /*
       * Only allow changes from low to high communities on
       * even cycles and high to low on odd cycles
       */
      //
      if (louvainData.community != bestCommunity && ((even && louvainData.community > bestCommunity) || (!even && louvainData.community < bestCommunity))) {

        louvainData.community = bestCommunity
        louvainData.communityName = bestCommunityName

        louvainData.communitySigmaTot = bestSigmaTot
        louvainData.changed = true

      }
      else {
        louvainData.changed = false
      }
      if (louvainData == null && verbose)
        println("vdata is null: " + vid)

      louvainData
    })
  }

  /**
   * Returns the change in modularity that would result
   * from a vertex moving to a specified community.
   */
  private def q(currCommunityId: Long, testCommunityId: Long, testSigmaTot: Long, edgeWeightInCommunity: Long, nodeWeight: Long, internalWeight: Long, totalEdgeWeight: Long): BigDecimal = {

    val isCurrentCommunity = currCommunityId.equals(testCommunityId)
    val M = BigDecimal(totalEdgeWeight)

    val k_i_in_L = if (isCurrentCommunity) edgeWeightInCommunity + internalWeight else edgeWeightInCommunity
    val k_i_in = BigDecimal(k_i_in_L)

    val k_i = BigDecimal(nodeWeight + internalWeight)
    val sigma_tot = if (isCurrentCommunity) BigDecimal(testSigmaTot) - k_i else BigDecimal(testSigmaTot)

    var deltaQ = BigDecimal(0.0)

    if (!(isCurrentCommunity && sigma_tot.equals(BigDecimal.valueOf(0.0)))) {
      deltaQ = k_i_in - (k_i * sigma_tot / M)
     }
    deltaQ
  }

  /**
   * Compress a graph by its communities, aggregate both
   * internal node weights and edge weights within communities.
   */
  def compressGraph(graph: Graph[LouvainData, Long], debug: Boolean = true): Graph[LouvainData, Long] = {
    /*
     * Aggregate the edge weights of self loops. edges with
     * both src and dst in the same community.
     *
     * WARNING: We cannot use graph.mapReduceTriplets because
     * we are mapping to new vertexIds
     */
    val internalEdgeWeights = graph.triplets.flatMap(et => {
      if (et.srcAttr.community == et.dstAttr.community) {
        /* Count the weight from both nodes */
        Iterator(((et.srcAttr.community, et.srcAttr.communityName), 2 * et.attr))
      }
      else Iterator.empty
    }).reduceByKey(_ + _)

    /*
     * Aggregate the internal weights of all nodes in each community
     */
    val internalWeights = graph.vertices.values
      .map(vdata => ((vdata.community, vdata.communityName), vdata.internalWeight)).reduceByKey(_ + _)

    /*
     * Join internal weights and self edges to find new internal
     * weight of each community
     */
    val newVertices = internalWeights.leftOuterJoin(internalEdgeWeights).map({ case ((vid, communityName), (weight1, weight2Option)) =>
      val weight2 = weight2Option.getOrElse(0L)
      val state = new LouvainData()
      state.name = communityName
      state.community = vid
      state.communityName = communityName
      state.changed = false
      state.communitySigmaTot = 0L
      state.internalWeight = weight1 + weight2
      state.nodeWeight = 0L
      (vid, state)
    }).cache()

    /* Translate each vertex edge to a community edge */
    val edges = graph.triplets.flatMap(et => {
      val src = math.min(et.srcAttr.community, et.dstAttr.community)
      val dst = math.max(et.srcAttr.community, et.dstAttr.community)
      if (src != dst) Iterator(new Edge(src, dst, et.attr))
      else Iterator.empty
    }).cache()
    /*
     * Generate a new graph where each community of the
     * previous graph is now represented as a single vertex
     */
    val compressedGraph = Graph(newVertices, edges)
      .partitionBy(PartitionStrategy.EdgePartition2D).groupEdges(_ + _)

    /*
     * Calculate the weighted degree of each node
     */
    val nodeWeights = compressedGraph.aggregateMessages(
      (e:EdgeContext[LouvainData,Long,Long]) => {
        e.sendToSrc(e.attr)
        e.sendToDst(e.attr)
      },
      (e1: Long, e2: Long) => e1 + e2
    )
    /*
     * Fill in the weighted degree of each node
     */
    val louvainGraph = compressedGraph.outerJoinVertices(nodeWeights)((vid, data, weightOption) => {
      val weight = weightOption.getOrElse(0L)
      data.communitySigmaTot = weight + data.internalWeight
      data.nodeWeight = weight
      data
    }).cache()
    /*
     * Materialize the graph
     */
    louvainGraph.vertices.count()
    louvainGraph.triplets.count()

    newVertices.unpersist(blocking = false)
    edges.unpersist(blocking = false)

    louvainGraph
  }

}
