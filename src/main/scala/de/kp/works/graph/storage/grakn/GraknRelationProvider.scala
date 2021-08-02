package de.kp.works.graph.storage.grakn

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

import de.kp.works.graph.storage.grakn.reader.GraknRelation
import de.kp.works.graph.storage.grakn.writer.{GraknEdgeWriter, GraknVertexWriter}
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter
import org.apache.spark.sql.sources.v2.{DataSourceOptions, WriteSupport}
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, DataSourceRegister, RelationProvider}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.slf4j.LoggerFactory

import java.util.Map.Entry
import java.util.Optional
import scala.collection.JavaConversions._

class GraknRelationProvider
  extends CreatableRelationProvider
  with RelationProvider
  with WriteSupport
  with DataSourceRegister {

  private val LOG = LoggerFactory.getLogger(this.getClass)

  override def shortName(): String = "grakn"

  override def createRelation(sqlContext: SQLContext, mode: SaveMode, parameters: Map[String, String], data: DataFrame): BaseRelation = {

    val graknOptions = new GraknOptions(parameters, OperationType.READ)
    GraknRelation(sqlContext, graknOptions)

  }

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    throw new Exception("not implemented")
  }

  override def createWriter(uuid: String, schema: StructType, saveMode: SaveMode, dataSourceOptions: DataSourceOptions): Optional[DataSourceWriter] = {

    var parameters: Map[String, String] = Map()
    for (entry: Entry[String, String] <- dataSourceOptions.asMap().entrySet) {
      parameters += (entry.getKey -> entry.getValue)
    }

    val graknOptions: GraknOptions =
      new GraknOptions(CaseInsensitiveMap(parameters))(OperationType.WRITE)

    if (saveMode == SaveMode.Ignore || saveMode == SaveMode.ErrorIfExists) {
      LOG.warn("Save mode `Ignore` and `ErrorIfExists` is not supported.")
    }
    /*
     * Distinguish between vertices and edges
     */
    val dataType = graknOptions.dataType
    if (DataTypeEnum.VERTEX == DataTypeEnum.withName(dataType)) {
      Optional.of(new GraknVertexWriter(graknOptions, schema))

    } else {
      Optional.of(new GraknEdgeWriter(graknOptions, schema))

    }
  }

}
