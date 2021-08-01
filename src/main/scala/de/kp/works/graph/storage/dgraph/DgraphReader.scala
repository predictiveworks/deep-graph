package de.kp.works.graph.storage.dgraph
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

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, DataFrameReader, SparkSession}
import org.graphframes.GraphFrame
import uk.co.gresearch.spark.dgraph.connector._

object DgraphReader {

  private val idColName:String = "id"

  private val srcColName:String = "src"
  private val dstColName:String = "dst"

  /** GRAPH API **/

  /**
   * The `targets` specify a set of endpoints (e.g. localhost:9080)
   * to access different (distributed) nodes of the Dgraph database.
   */
  def loadGraph(targets: String*)(implicit session: SparkSession): GraphFrame =
    loadGraph(session.read, targets: _*)

  def loadGraph(reader: DataFrameReader, targets: String*): GraphFrame =
    GraphFrame(loadVertices(reader, targets: _*), loadEdges(reader, targets: _*))

  /** VERTEX & EDGE API **/

  def loadVertices(targets: String*)(implicit session: SparkSession): DataFrame =
    loadVertices(session.read, targets: _*)

  def loadEdges(targets: String*)(implicit session: SparkSession): DataFrame =
    loadEdges(session.read, targets: _*)

  /** PRIVATE METHODS **/

  private def loadVertices(reader: DataFrameReader, targets: String*): DataFrame = {

    val vertices =
      DgraphDataFrameReader(reader.option(NodesModeOption, NodesModeWideOption))
        .dgraph.nodes(targets.head, targets.tail: _*)
        .withColumnRenamed("subject", idColName)

    val renamedColumns =
      vertices.columns.map(f =>
        col(s"`$f`").as(f.replace("_", "__").replace(".", "_"))
      )
    vertices.select(renamedColumns: _*)

  }

  private def loadEdges(reader: DataFrameReader, targets: String*): DataFrame =
    DgraphDataFrameReader(reader)
      .dgraph.edges(targets.head, targets.tail: _*)
      .select(
        col("subject").as(srcColName),
        col("objectUid").as(dstColName),
        col("predicate")
      )

//  implicit class GraphFrameDataFrameReader(reader: DataFrameReader) {
//    def dgraph: GraphFramesReader = GraphFramesReader(reader)
//  }

}
