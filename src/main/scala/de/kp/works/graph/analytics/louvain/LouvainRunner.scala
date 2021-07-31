package de.kp.works.graph.analytics.louvain

import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph

import scala.reflect.ClassTag

class LouvainRunner(var minProgress: Int = 1, var progressCounter: Int = 1) {

  var verbose = false

  def run[VD: ClassTag](sc: SparkContext, graph: Graph[VD, Long]): Graph[LouvainData, Long] = {

    val louvainCore = new LouvainCore
    var louvainGraph = louvainCore.createLouvainGraph(graph)

    /* The number of times the graph has been compressed */
    var compressionLevel = -1

    /* The current modularity value */
    var q_modularityValue = -1.0
    var halt = false
    do {
      compressionLevel += 1
      if (verbose) println(s"Starting Louvain level $compressionLevel")

      /*
       * Label each vertex with its best community choice at
       * this level of compression
       */
      val (currentQModularityValue, currentGraph, _) =
        louvainCore.louvain(sc, louvainGraph, minProgress, progressCounter)

      louvainGraph.unpersistVertices(blocking = false)
      louvainGraph = currentGraph
       /*
       * If modularity was increased by at least 0.001 compress
       * the graph and repeat.
       *
       * Halt immediately if the community labeling took less
       * than 3 passes.
       */
      if (currentQModularityValue > q_modularityValue + 0.001) {
        q_modularityValue = currentQModularityValue
        louvainGraph = louvainCore.compressGraph(louvainGraph)
      } else {
        halt = true
      }

    } while (!halt)

    louvainGraph

  }

}
