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

import ml.sparkling.graph.operators.measures.vertex.betweenness.edmonds.EdmondsBC
import ml.sparkling.graph.operators.measures.vertex.betweenness.hua.HuaBC
import org.apache.spark.graphx.{Graph, VertexId, VertexRDD}
import org.apache.spark.sql.{DataFrame, Row}

import scala.reflect.ClassTag

class Betweenness[VD: ClassTag, ED: ClassTag]
  extends BaseAnalytics[Closeness[VD, ED]] {

  def transform(g:Graph[VD, ED], option:String="edmonds")(implicit num:Numeric[ED]):DataFrame = {

    option match {
      case "edmonds" =>
        /*
         * The result of this method represents a VertexRDD
         * with (VertexId, Double (measure)
         */
        val result:VertexRDD[Double] = EdmondsBC.computeBC(g)
        val rdd = result
          .map{case(vid:VertexId, distance:Double) =>
               Row(vid.toLong, distance)
          }

        session.createDataFrame(rdd, measureSchema)
      case "hua" =>
        /*
         * The result of this method represents a VertexRDD
         * with (VertexId, Double (measure)
         */
        val result:VertexRDD[Double] = HuaBC.computeBC(g)
        val rdd = result
          .map{case(vid:VertexId, distance:Double) =>
            Row(vid.toLong, distance)
          }

        session.createDataFrame(rdd, measureSchema)
      case _ => throw new Exception(s"Betweenness operator $option is not supported.")
    }

  }

}
