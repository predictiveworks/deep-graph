package ml.sparkling.graph.api.operators.measures

import org.apache.spark.graphx.Graph

import scala.reflect.ClassTag

/**
 * Measure computed for each vertex of graph
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 */
trait VertexMeasure[OV] {

  def compute[VD:ClassTag,ED:ClassTag](graph:Graph[VD,ED])(implicit num:Numeric[ED]):Graph[OV,ED] =
    compute(graph,VertexMeasureConfiguration[VD,ED]())

  def compute[VD:ClassTag,ED:ClassTag](graph:Graph[VD,ED],vertexMeasureConfiguration: VertexMeasureConfiguration[VD,ED])(implicit num:Numeric[ED]):Graph[OV,ED]

}
