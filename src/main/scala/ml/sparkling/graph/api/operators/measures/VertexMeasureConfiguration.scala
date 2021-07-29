package ml.sparkling.graph.api.operators.measures

import ml.sparkling.graph.api.operators.IterativeComputation._

import scala.reflect.ClassTag

/**
 * Configuration of vertex measure alghoritms
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 */
case class VertexMeasureConfiguration[VD: ClassTag, ED: ClassTag](bucketSizeProvider: BucketSizeProvider[VD, ED],
                                                                  treatAsUndirected: Boolean = false)

object VertexMeasureConfiguration {

  def apply[VD:ClassTag, ED:ClassTag]() =
    new VertexMeasureConfiguration[VD, ED]( wholeGraphBucket[VD, ED])

  def apply[VD:ClassTag, ED:ClassTag](treatAsUndirected:Boolean) =
    new VertexMeasureConfiguration[VD, ED]( wholeGraphBucket[VD, ED],treatAsUndirected)

  def apply[VD:ClassTag, ED:ClassTag](treatAsUndirected:Boolean,bucketSizeProvider:BucketSizeProvider[VD, ED]) =
    new VertexMeasureConfiguration[VD, ED](bucketSizeProvider,treatAsUndirected)

  def apply[VD:ClassTag, ED:ClassTag](bucketSizeProvider:BucketSizeProvider[VD, ED]) =
    new VertexMeasureConfiguration[VD, ED](bucketSizeProvider)

}
