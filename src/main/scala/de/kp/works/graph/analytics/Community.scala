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

import ml.sparkling.graph.operators.algorithms.community.pscan.PSCAN
import org.apache.spark.graphx.{Graph, VertexId}
import org.apache.spark.sql.{DataFrame, Row}

import scala.reflect.ClassTag

class Community[VD: ClassTag, ED: ClassTag]
  extends BaseAnalytics {

  def detectCommunities(g:Graph[VD, ED]): DataFrame = {

    val result = PSCAN.detectCommunities(g)
    val rdd = result.vertices
      .map{case(vid:VertexId, label:Long) =>
        Row(vid.toLong, label)
      }

    session.createDataFrame(rdd, labelSchema)

  }

  def detectCommunities(g:Graph[VD, ED], epsilon:Double): DataFrame = {

    val result = PSCAN.computeConnectedComponents(g, epsilon)
    val rdd = result.vertices
      .map{case(vid:VertexId, label:Long) =>
        Row(vid.toLong, label)
      }

    session.createDataFrame(rdd, labelSchema)

  }
}
