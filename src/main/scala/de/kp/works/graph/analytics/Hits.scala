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
import ml.sparkling.graph.operators.measures.vertex.hits.{Hits => HitsML}
import org.apache.spark.graphx.{Graph, VertexId}
import org.apache.spark.sql.{DataFrame, Row}

import scala.reflect.ClassTag

class Hits[VD: ClassTag, ED: ClassTag]
  extends BaseAnalytics {

  private var vertexMeasureConfiguration: VertexMeasureConfiguration[VD, ED] =
    new VertexMeasureConfiguration[VD, ED]( wholeGraphBucket[VD, ED])

  def setVertexMeasureCfg(value:VertexMeasureConfiguration[VD, ED]): Hits[VD, ED] = {
    vertexMeasureConfiguration = value
    this
  }

  def transform(g:Graph[VD, ED])(implicit num:Numeric[ED]):DataFrame = {
    /**
     * After measure computation, each vertex of graph will have assigned
     * two scores (hub, authority). Where hub score is proportional to the
     * sum of authority score of its neighbours, and authority score is
     * proportional to sum of hub score of its neighbours.
     */
    val result:Graph[(Double, Double), ED] = HitsML.compute(g, vertexMeasureConfiguration)
    /*
     * The result of this method represents a VertexRDD
     * with (VertexId, Double (hub), Double(authority)
     */
    val rdd = result.vertices
      .map{case(vid:VertexId, (hub:Double, auth:Double)) =>
        Row(vid.toLong, auth, hub)
      }

    session.createDataFrame(rdd, hitsSchema)

  }

}
