package de.kp.works.graph.storage.grakn.reader
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

import de.kp.works.graph.storage.grakn.GraknUtils.GraknValueGetter
import de.kp.works.graph.storage.grakn.{GraknOptions, GraknProperty, GraknUtils}
import org.apache.spark.Partition
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.SpecificInternalRow
import org.apache.spark.sql.types.StructType
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable

abstract class AbstractGraknIterator extends Iterator[InternalRow] {

  private val LOG: Logger = LoggerFactory.getLogger(classOf[AbstractGraknIterator])

  protected var dataIterator: Iterator[List[GraknProperty]] = _

  protected var resultValues: mutable.ListBuffer[List[GraknProperty]] =
    mutable.ListBuffer[List[GraknProperty]]()

  private var schema: StructType = _

  def this(split:Partition, graknOptions:GraknOptions, schema:StructType) {
    this()
    this.schema = schema
  }

  override def hasNext: Boolean

  override def next(): InternalRow = {

    val getters: Array[GraknValueGetter] = GraknUtils.makeGetters(schema)
    val mutableRow = new SpecificInternalRow(schema.fields.map(x => x.dataType))

    val resultSet: Array[GraknProperty] = dataIterator.next().toArray
    for (i <- getters.indices) {
      getters(i).apply(resultSet(i), mutableRow, i)
      if (resultSet(i) == null) mutableRow.setNullAt(i)
    }

    mutableRow


  }

}
