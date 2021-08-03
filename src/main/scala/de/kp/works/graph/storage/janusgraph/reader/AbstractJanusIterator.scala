package de.kp.works.graph.storage.janusgraph.reader
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

import de.kp.works.graph.storage.janusgraph.JanusUtils.JanusValueGetter
import de.kp.works.graph.storage.janusgraph.{JanusOptions, JanusProperty, JanusUtils}
import org.apache.spark.Partition
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.SpecificInternalRow
import org.apache.spark.sql.types.StructType
import org.slf4j.{Logger, LoggerFactory}

abstract class AbstractJanusIterator extends Iterator[InternalRow] {

  private val LOG: Logger = LoggerFactory.getLogger(classOf[AbstractJanusIterator])

  protected var dataIterator: Iterator[List[JanusProperty]] = _

  private var schema: StructType = _

  def this(split:Partition, janusOptions:JanusOptions, schema:StructType) {
    this()
    this.schema = schema
  }

  override def hasNext: Boolean

  override def next(): InternalRow = {

    val getters: Array[JanusValueGetter] = JanusUtils.makeGetters(schema)
    val mutableRow = new SpecificInternalRow(schema.fields.map(x => x.dataType))

    val resultSet: Array[JanusProperty] = dataIterator.next().toArray
    for (i <- getters.indices) {
      getters(i).apply(resultSet(i), mutableRow, i)
      if (resultSet(i) == null) mutableRow.setNullAt(i)
    }

    mutableRow

  }

}