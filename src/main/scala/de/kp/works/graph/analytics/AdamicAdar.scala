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

import ml.sparkling.graph.operators.measures.edge.{AdamicAdar => AdamicAdarML}
import org.apache.spark.graphx.Graph
import org.apache.spark.sql.{DataFrame, Row}

import scala.reflect.ClassTag

class AdamicAdar[VD: ClassTag, ED: ClassTag]
  extends BaseAnalytics {

  def transform(g:Graph[VD, ED], undirected:Boolean = false):DataFrame = {
    /**
     * Adamic/Adar measures is defined as inverted sum of degrees
     * of common neighbours for given two vertices.
     */
    val result = AdamicAdarML.computeWithPreprocessing(g, undirected)
    val rdd = result.edges.map(edge => {
      Row(edge.srcId, edge.dstId, edge.attr)
    })

    session.createDataFrame(rdd, adamicAdarSchema)
  }

}
