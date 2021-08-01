package uk.co.gresearch.spark.dgraph.connector.executor
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

import uk.co.gresearch.spark.dgraph.connector.{Partition, Transaction}

case class DgraphExecutorProvider(transaction: Option[Transaction]) extends ExecutorProvider {

  /**
   * Provide an executor for the given partition.
   *
   * @param partition a partitioon
   * @return an executor
   */
  override def getExecutor(partition: Partition): JsonGraphQlExecutor =
    DgraphExecutor(transaction, partition.targets)

}
