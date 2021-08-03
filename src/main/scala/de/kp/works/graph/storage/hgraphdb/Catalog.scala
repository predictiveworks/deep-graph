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

object Catalog {
  /**
   * We need to specify a schema for the Spark-On-HBase Connector
   * for retrieving vertex and edge data from a graph stored in
   * HGraphDB.
   *
   * The HGraphDB serializer/deserializer used with the connector
   * is specified as tableDecoder below.
   *
   * Furthermore, all HGraphDB columns are stored in a column family
   * named f. Vertex and edge labels are stored in a column with
   * qualifier ~l.
   *
   * The source and destination columns have qualifiers ~f and ~t,
   * respectively.
   *
   * Last but not least, all vertex and edge properties are stored
   * in columns with the qualifiers simply being the name of the
   * property.
   */
  def vertexCatalog: String = s"""{
   |"table":{"namespace":"%namespace", "name":"vertices",
   |  "tableCoder":"org.apache.spark.sql.execution.datasources.hbase.types.HGraphDB", "version":"2.0"},
   |"rowkey":"key",
   |"columns":{
   |"id":{"cf":"rowkey", "col":"key", "type":"string"},
   |"name":{"cf":"f", "col":"name", "type":"string"},
   |"age":{"cf":"f", "col":"age", "type":"int"}
   |}
   |}""".stripMargin

  def edgeCatalog: String = s"""{
   |"table":{"namespace":"%namespace", "name":"edges",
   |  "tableCoder":"org.apache.spark.sql.execution.datasources.hbase.types.HGraphDB", "version":"2.0"},
   |"rowkey":"key",
   |"columns":{
   |"id":{"cf":"rowkey", "col":"key", "type":"string"},
   |"relationship":{"cf":"f", "col":"~l", "type":"string"},
   |"src":{"cf":"f", "col":"~f", "type":"string"},
   |"dst":{"cf":"f", "col":"~t", "type":"string"}
   |}
   |}""".stripMargin
}
