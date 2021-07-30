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

import ml.sparkling.graph.operators.measures.graph.{Modularity => ModularityML}
import org.apache.spark.graphx.Graph

import scala.reflect.ClassTag

class Modularity[VD: ClassTag, ED: ClassTag]
  extends BaseAnalytics[Closeness[VD, ED]] {

  def transform(g: Graph[VD, ED]): Double = {
    /**
     * Modularity measures the strength of the division of a network into
     * communities (modules, clusters). Measures takes values from range [-1, 1].
     *
     * A value close to 1 indicates strong community structure. When Q=0 then the
     * community division is not better than random.
     */
    // TYPE MISMATCH found   : org.apache.spark.graphx.Graph[VD,ED] required val modularity = ModularityML.compute(g)
    //val modularity = ModularityML.compute(g)
    //modularity

    /*
    implicit class ModularityDSL[E:ClassTag](graph:Graph[ComponentID,E]){
    def modularity()=Modularity.compute(graph)
  }
     */
    0D
  }
}