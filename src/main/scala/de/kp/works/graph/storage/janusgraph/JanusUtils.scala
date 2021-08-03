package de.kp.works.graph.storage.janusgraph
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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.slf4j.LoggerFactory

object JanusUtils {

  private val LOG = LoggerFactory.getLogger(this.getClass)

  var janusOptions: JanusOptions    = _
  var parameters: Map[String, String] = Map()

  type JanusValueGetter = (JanusProperty, InternalRow, Int) => Unit

  def makeGetters(schema: StructType): Array[JanusValueGetter] =
    schema.fields.map(field => makeGetter(field.dataType, field.metadata))

  private def makeGetter(dataType: DataType, metadata: Metadata): JanusValueGetter = {
    dataType match {
      case BooleanType =>
        (prop: JanusProperty, row: InternalRow, pos: Int) =>
          row.setBoolean(pos, prop.getValueAsBoolean)
      case DoubleType =>
        (prop: JanusProperty, row: InternalRow, pos: Int) =>
          row.setDouble(pos, prop.getValueAsDouble)
     case LongType =>
        (prop: JanusProperty, row: InternalRow, pos: Int) =>
          row.setLong(pos, prop.getValueAsLong)
      case FloatType =>
        (prop: JanusProperty, row: InternalRow, pos: Int) =>
          row.setFloat(pos, prop.getValueAsFloat)
      case IntegerType =>
        (prop: JanusProperty, row: InternalRow, pos: Int) =>
          row.setInt(pos, prop.getValueAsInt)
      case _ =>
        (prop: JanusProperty, row: InternalRow, pos: Int) =>
          row.update(pos, UTF8String.fromString(prop.getValue.toString))
    }
  }
}