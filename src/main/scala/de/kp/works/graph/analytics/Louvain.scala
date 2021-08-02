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

import de.kp.works.graph.analytics.louvain.{LouvainData, LouvainRunner}
import org.apache.spark.graphx.{Graph, VertexId}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.reflect.ClassTag

class Louvain[VD: ClassTag, ED: ClassTag]
  extends BaseAnalytics {

  private val sc = session.sparkContext

  def transform(g:Graph[VD, Long], minProgress:Int = 1, progressCounter:Int = 1):DataFrame = {
    /**
     * This is an implementation of the Louvain Community Detection algorithm described
     * in "Fast unfolding of communities in large networks" which:
     *
     * (1) Assigns communities to nodes in a graph based on graph structure and statistics.
     *
     * (2) Compresses the community-tagged graph into a smaller one.
     *
     * This process can then be repeated to build several community-aggregated versions
     * of the same original graph.
     *
     * In the original algorithm each vertex examines the communities of its neighbors
     * and chooses a new community based on a function to maximize the calculated change
     * in modularity.
     *
     * In the distributed version all vertices make this choice simultaneously rather than
     * in serial order, updating the graph state after each change. Because choices are made
     * in parallel some choice will be incorrect and will not maximize modularity values,
     * however after repeated iterations community choices become more stable and we get
     * results that closely mirror the serial algorithm.
     *
     *
     *
     */
    val runner = new LouvainRunner(minProgress, progressCounter)
    val result = runner.run[VD](sc, g)

    val rdd = result.vertices
      .map{case(vid:VertexId, data:LouvainData) =>
        Row(vid.toLong, data.community)
      }

    session.createDataFrame(rdd, louvainSchema)

  }
}
