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

import ml.sparkling.graph.api.operators.IterativeComputation.wholeGraphBucket
import ml.sparkling.graph.api.operators.measures.VertexMeasureConfiguration
import ml.sparkling.graph.operators.measures.vertex.clustering.LocalClustering
import org.apache.spark.graphx.{Graph, VertexId}
import org.apache.spark.sql.{DataFrame, Row}

import scala.reflect.ClassTag

class Clustering[VD: ClassTag, ED: ClassTag]
  extends BaseAnalytics[Closeness[VD, ED]] {

  private var vertexMeasureConfiguration: VertexMeasureConfiguration[VD, ED] =
    new VertexMeasureConfiguration[VD, ED]( wholeGraphBucket[VD, ED])

  def setVertexMeasureCfg(value:VertexMeasureConfiguration[VD, ED]): Clustering[VD, ED] = {
    vertexMeasureConfiguration = value
    this
  }

  def transform(g:Graph[VD, ED])(implicit num:Numeric[ED]):DataFrame = {
    /**
     * Local Clustering Coefficient for vertex tells us howe close its neighbors
     * are. It’s number of existing connections in neighborhood divided by number
     * of all possible connections.
     */
    val result:Graph[Double, ED] = LocalClustering.compute(g, vertexMeasureConfiguration)
    /*
     * The result of this method represents a VertexRDD
     * with (VertexId, Double (embeddedness)
     */
    val rdd = result.vertices
      .map{case(vid:VertexId, measure:Double) =>
        Row(vid.toLong, measure)
      }

    session.createDataFrame(rdd, measureSchema)

  }

}
