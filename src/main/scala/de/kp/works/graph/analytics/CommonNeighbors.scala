package de.kp.works.graph.analytics

import ml.sparkling.graph.operators.measures.edge.CommonNeighbours
import org.apache.spark.graphx.{Graph, VertexId}
import org.apache.spark.sql.{DataFrame, Row}

import scala.reflect.ClassTag

class CommonNeighbors[VD: ClassTag, ED: ClassTag]
  extends BaseAnalytics[Closeness[VD, ED]] {

  def transform(g:Graph[VD, ED], undirected:Boolean = false):DataFrame = {
    /**
     * Common Neighbours measure is defined as the
     * number of common neighbours of two given vertices.
     */
    val result = CommonNeighbours.computeWithPreprocessing(g, undirected)
    val rdd = result.edges.map(edge => {
      Row(edge.srcId, edge.dstId, edge.attr)
    })

    session.createDataFrame(rdd, neighborsSchema)

  }

}
