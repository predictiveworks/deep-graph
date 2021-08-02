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

import de.kp.works.graph.storage.grakn.{DataTypeEnum, GraknOptions}
import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType

import scala.collection.mutable.ListBuffer

class GraknRDD(val sqlContext: SQLContext, var graknOptions: GraknOptions, schema: StructType)
  extends RDD[InternalRow](sqlContext.sparkContext, Nil) {

  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
    val dataType = graknOptions.dataType

    if (DataTypeEnum.VERTEX == DataTypeEnum.withName(dataType)) {
      new GraknVertexIterator(split, graknOptions, schema)
    }
    else
      new GraknEdgeIterator(split, graknOptions, schema)

  }

  override protected def getPartitions: Array[Partition] = {

    val partitionNumber = graknOptions.partitions.toInt

    val partitions = new Array[Partition](partitionNumber)
    for (i <- 0 until partitionNumber) {
      partitions(i) = GraknPartition(i)
    }

    partitions


  }
}

/**
 * An identifier for a partition in a GraknRDD.
 */
case class GraknPartition(indexNum: Int) extends Partition {
  override def index: Int = indexNum

  /**
   * allocate scanPart to partition
   */
  def getScanParts(totalPart: Int, totalPartition: Int): List[Integer] = {
    val scanParts   = new ListBuffer[Integer]
    var currentPart = indexNum + 1
    while (currentPart <= totalPart) {
      scanParts.append(currentPart)
      currentPart += totalPartition
    }
    scanParts.toList
  }
}
