package de.kp.works.graph.storage
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

import org.apache.spark.sql.{DataFrame, DataFrameReader, DataFrameWriter, Row}

import java.util.Properties

package object janusgraph {

  implicit class JanusDataFrameReader(reader:DataFrameReader) {

    var properties:Properties = _

    def jgraph(properties:Properties):JanusDataFrameReader = {
      this.properties = properties
      this
    }

    def loadVertices:DataFrame = {
      reader
        .format(classOf[JanusRelationProvider].getName)
        .option(JanusOptions.TYPE, DataTypeEnum.VERTEX.toString)
        .load()
    }

    def loadEdges:DataFrame = {
      reader
        .format(classOf[JanusRelationProvider].getName)
        .option(JanusOptions.TYPE, DataTypeEnum.EDGE.toString)
        .load()
    }

  }

  implicit class JanusDataFrameWriter(writer:DataFrameWriter[Row]) {

    var properties:Properties = _

    def jgraph(properties:Properties):JanusDataFrameWriter = {
      this.properties = properties
      this
    }

    def writeVertices():Unit = {
      writer
        .format(classOf[JanusRelationProvider].getName)
        .option(JanusOptions.TYPE, DataTypeEnum.VERTEX.toString)
        .save()
    }

    def writeEdges():Unit = {
      writer
        .format(classOf[JanusRelationProvider].getName)
        .option(JanusOptions.TYPE, DataTypeEnum.EDGE.toString)
        .save()
    }

  }
}