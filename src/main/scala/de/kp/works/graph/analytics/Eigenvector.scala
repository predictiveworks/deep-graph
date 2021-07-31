package de.kp.works.graph.analytics

import ml.sparkling.graph.api.operators.IterativeComputation.wholeGraphBucket
import ml.sparkling.graph.api.operators.measures.VertexMeasureConfiguration
import ml.sparkling.graph.operators.measures.vertex.eigenvector.EigenvectorCentrality
import org.apache.spark.graphx.{Graph, VertexId}
import org.apache.spark.sql.{DataFrame, Row}

import scala.reflect.ClassTag

class Eigenvector[VD: ClassTag, ED: ClassTag]
  extends BaseAnalytics {

  private var vertexMeasureConfiguration: VertexMeasureConfiguration[VD, ED] =
    new VertexMeasureConfiguration[VD, ED]( wholeGraphBucket[VD, ED])

  def setVertexMeasureCfg(value:VertexMeasureConfiguration[VD, ED]): Eigenvector[VD, ED] = {
    vertexMeasureConfiguration = value
    this
  }

  def transform(g:Graph[VD, ED])(implicit num:Numeric[ED]):DataFrame = {
    /**
     * Eigenvector centrality measure give us information about how a given node
     * is important in network. It is based on degree centrality. In here we have
     * more sophisticated version, where connections are not equal.
     *
     * Eigenvector centrality is a more general approach than PageRank.
     */
    val result = EigenvectorCentrality.compute(g,vertexMeasureConfiguration)
    /*
     * The result of this method represents a VertexRDD
     * with (VertexId, Double (measure)
     */
    val rdd = result.vertices
      .map{case(vid:VertexId, measure:Double) =>
        Row(vid.toLong, measure)
      }

    session.createDataFrame(rdd, measureSchema)
  }

}
