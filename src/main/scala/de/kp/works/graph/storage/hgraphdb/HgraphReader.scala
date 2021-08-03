package de.kp.works.graph.storage.hgraphdb
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

import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.graphframes.GraphFrame

import java.util.Properties

object HgraphReader {

  val HBASE_FORMAT = "org.apache.spark.sql.execution.datasources.hbase"

  val HBASE_NS = "hbase.namespace"

  /** GRAPH API **/

  def loadGraph(properties:Properties)(implicit session: SparkSession): GraphFrame = {
    GraphFrame(loadVertices(properties), loadEdges(properties))
  }

  /** VERTICES API **/

  def loadVertices(properties:Properties)(implicit session: SparkSession): DataFrame = {
    /*
     * Retrieve vertex catalog template
     */
    val spec = Catalog.vertexCatalog
    /*
     * Extract the HBase namespace (or graph name)
     * from the provided properties
     */
    val namespace = properties.getProperty(HBASE_NS)
    val catalog = spec.replace("%namespace", namespace)

    val vertices = withCatalog(catalog, properties)
    vertices

  }

  /** EDGES API **/

  def loadEdges(properties:Properties)(implicit session: SparkSession): DataFrame = {
    /*
     * Retrieve edge catalog template
     */
    val spec = Catalog.edgeCatalog
    /*
     * Extract the HBase namespace (or graph name)
     * from the provided properties
     */
    val namespace = properties.getProperty(HBASE_NS)
    val catalog = spec.replace("%namespace", namespace)

    val edges = withCatalog(catalog, properties)
    edges

  }

  /**
   * HBase connector from HortonWorks and also Apache exist,
   * however, the HBaseTableCatalog is completely different
    */
  private def withCatalog(catalog: String, properties:Properties)(implicit session: SparkSession): DataFrame = {
    session
      .read
      .options(Map(HBaseTableCatalog.tableCatalog -> catalog))
      .format(HBASE_FORMAT)
      .load()
  }
}
