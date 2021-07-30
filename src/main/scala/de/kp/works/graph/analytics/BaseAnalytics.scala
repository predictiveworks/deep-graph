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

import de.kp.works.spark.Session
import ml.sparkling.graph.api.operators.IterativeComputation.wholeGraphBucket
import ml.sparkling.graph.api.operators.measures.VertexMeasureConfiguration
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

trait BaseAnalytics[T] {

  protected val session: SparkSession = Session.getSession

  protected val adamicAdarSchema: StructType = StructType(Array(
    StructField("s_vertex", LongType, nullable = false),
    StructField("d_vertex", LongType, nullable = false),
    StructField("measure", IntegerType, nullable = false)
  ))

  protected val hitsSchema: StructType = StructType(Array(
    StructField("vertex", LongType, nullable = false),
    StructField("authority", DoubleType, nullable = false),
    StructField("hub", DoubleType, nullable = false)
  ))

  protected val labelSchema: StructType = StructType(Array(
    StructField("vertex", LongType, nullable = false),
    StructField("label", LongType, nullable = false)
  ))

  protected val measureSchema: StructType = StructType(Array(
    StructField("vertex", LongType, nullable = false),
    StructField("measure", DoubleType, nullable = false)
  ))

  protected val neighborsSchema: StructType = StructType(Array(
    StructField("s_vertex", LongType, nullable = false),
    StructField("d_vertex", LongType, nullable = false),
    StructField("measure", IntegerType, nullable = false)
  ))

}
