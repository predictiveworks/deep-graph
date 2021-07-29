package de.kp.works.graph
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

import de.kp.works.spark.Session
import ml.sparkling.graph.api.operators.measures.VertexMeasureConfiguration
import ml.sparkling.graph.operators.measures.vertex.closeness.{Closeness => ClosenessML}
import org.apache.spark.graphx._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.reflect.ClassTag

/**
 * This is the base wrapper for the Sparkling Graph
 * [Closeness] Operator. Note, with respect to an
 * integration into a GraphFrame based environment,
 * this class specifies the 2nd computation stage.
 *
 * This class requires a vertex that is specified as
 * (VertexId [=Long], Any), and an edge specified as
 * (VertexId, VertexId, Numeric).
 *
 */
class Closeness[VD: ClassTag, ED: ClassTag] {

  private val session: SparkSession = Session.getSession

  private val schema = StructType(Array(
    StructField("vid", LongType, nullable = false),
    StructField("closeness", DoubleType, nullable = false)
  ))

  private var vertexMeasureConfiguration: VertexMeasureConfiguration[VD, ED] =
    VertexMeasureConfiguration()

  def setVertexMeasureCfg(value:VertexMeasureConfiguration[VD, ED]): Closeness[VD, ED] = {
    vertexMeasureConfiguration = value
    this
  }

  /**
   * Measure can be understood as how far away from other nodes
   * given node is located.
   */
  def transform(g:Graph[VD, ED])(implicit num:Numeric[ED]):DataFrame = {
    /**
     * This method computes the closeness of the vertices
     * of the provided graph.
     */
    val result:Graph[Double, ED] = ClosenessML.compute(g, vertexMeasureConfiguration)
    /*
     * The result of this method represents a VertexRDD
     * with (VertexId, Double (distance)
     */
    val rdd = result.vertices
      .map{case(vid:VertexId, distance:Double) =>
        Row(vid.toLong, distance)
      }

    session.createDataFrame(rdd, schema)

  }
}
