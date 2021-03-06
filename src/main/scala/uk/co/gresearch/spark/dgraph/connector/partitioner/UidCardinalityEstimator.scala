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

import uk.co.gresearch.spark.dgraph.connector.Partition

trait UidCardinalityEstimator {

  /**
   * Estimates the cardinality of uids in the given partition,
   * or None if an estimation is not available.
   *
   * @param partition a partition
   * @return estimated number of uids or None
   */
  def uidCardinality(partition: Partition): Option[Long]

}

/**
 * A base implementation of UidCardinalityEstimator.
 */
abstract class UidCardinalityEstimatorBase extends UidCardinalityEstimator {

  /**
   * Estimates the cardinality of uids in the given partition,
   * or None if an estimation is not available.
   *
   * @param partition a partition
   * @return estimated number of uids or None
   */
  override def uidCardinality(partition: Partition): Option[Long] =
    partition.uidRange.map(_.length).orElse(partition.uids.map(_.size))

}

case class MaxLeaseIdUidCardinalityEstimator(maxLeaseId: Long) extends UidCardinalityEstimatorBase {

  if (maxLeaseId <= 0)
    throw new IllegalArgumentException(s"uidCardinality must be larger than zero: $maxLeaseId")

  /**
   * Estimates the cardinality of uids in the given partition,
   * or None if an estimation is not available.
   *
   * @param partition a partition
   * @return estimated number of uids or None
   */
  override def uidCardinality(partition: Partition): Option[Long] =
    super.uidCardinality(partition).orElse(Some(maxLeaseId))

}

object UidCardinalityEstimator {
  def forMaxLeaseId(maxLeaseId: Long): UidCardinalityEstimator =
    MaxLeaseIdUidCardinalityEstimator(maxLeaseId)
}
