package uk.co.gresearch.spark.dgraph.connector.partitioner
/*
 * Copyright 2020 G-Research
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import uk.co.gresearch.spark.dgraph.connector
import uk.co.gresearch.spark.dgraph.connector.model.GraphTableModel
import uk.co.gresearch.spark.dgraph.connector.{Filter, Filters, Partition, Uid, UidRange}

case class UidRangePartitioner(partitioner: Partitioner, uidsPerPartition: Int, uidCardinalityEstimator: UidCardinalityEstimator) extends Partitioner {

  if (partitioner == null)
    throw new IllegalArgumentException("partitioner must not be null")

  if (uidsPerPartition <= 0)
    throw new IllegalArgumentException(s"uidsPerPartition must be larger than zero: $uidsPerPartition")

  override def supportsFilters(filters: Set[Filter with Product with Serializable]): Boolean = partitioner.supportsFilters(filters)

  override def withFilters(filters: Filters): UidRangePartitioner = copy(partitioner = partitioner.withFilters(filters))

  override def withProjection(projection: Seq[connector.Predicate]): UidRangePartitioner = copy(partitioner = partitioner.withProjection(projection))

  override def getPartitions(implicit model: GraphTableModel): Seq[Partition] = {
    val partitions = partitioner.getPartitions(model)
    if (partitions.exists(_.uidRange.isDefined))
      throw new IllegalArgumentException(s"UidRangePartitioner cannot be combined with " +
        s"another uid partitioner: ${partitioner.getClass.getSimpleName}")

    partitions.flatMap { partition =>
      val uidCardinality = uidCardinalityEstimator.uidCardinality(partition)
      val parts = uidCardinality.map(uids => ((uids - 1) / uidsPerPartition) + 1)

      if (parts.isDefined && parts.get > 1) {
        if (!parts.get.isValidInt)
          throw new IllegalArgumentException(s"uidsPerPartition of $uidsPerPartition " +
            s"with uidCardinality of ${uidCardinality.get} " +
            s"leads to more then ${Integer.MAX_VALUE} partitions: ${parts.get}")

        (0 to parts.get.toInt)
          .map(idx => 1 + idx * uidsPerPartition)
          .map(Uid(_))
          .sliding(2)
          .map(uids => UidRange(uids.head, uids.last))
          .zipWithIndex
          .map {
            case (range, idx) =>
              Partition(partition.targets.rotateLeft(idx), partition.operators + range)(model)
          }
      } else {
        Seq(partition)
      }
    }
  }

}
