package uk.co.gresearch.spark.dgraph.connector.sources
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

import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.reader.DataSourceReader
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import uk.co.gresearch.spark.dgraph.connector._
import uk.co.gresearch.spark.dgraph.connector.encoder.{TypedNodeEncoder, WideNodeEncoder}
import uk.co.gresearch.spark.dgraph.connector.executor.{DgraphExecutorProvider, TransactionProvider}
import uk.co.gresearch.spark.dgraph.connector.model.NodeTableModel
import uk.co.gresearch.spark.dgraph.connector.partitioner.PartitionerProvider

import scala.collection.JavaConverters._

class NodeSource() extends TableProviderBase
  with TargetsConfigParser with SchemaProvider
  with ClusterStateProvider with PartitionerProvider
  with TransactionProvider with Logging {

  /**
   * Sets the number of predicates per partition to max int when predicate partitioner is used
   * in conjunction with wide node mode. Otherwise wide nodes cannot be properly loaded.
   *
   * @param options original options
   * @return modified options
   */
  def adjustOptions(options: CaseInsensitiveStringMap): CaseInsensitiveStringMap = {
    if (getStringOption(NodesModeOption, options).contains(NodesModeWideOption) &&
      getStringOption(PartitionerOption, options).forall(_.startsWith(PredicatePartitionerOption))) {
      if (getIntOption(PredicatePartitionerPredicatesOption, options).exists(_ != PredicatePartitionerPredicatesDefault)) {
        log.warn("predicate partitioner enforced to a single partition to support wide node source")
      }

      new CaseInsensitiveStringMap(
        (options.asScala.filterKeys(!_.equalsIgnoreCase(PredicatePartitionerPredicatesOption)) ++
          Map(PredicatePartitionerPredicatesOption -> Int.MaxValue.toString)
          ).asJava
      )
    } else {
      options
    }
  }

  override def shortName(): String = "dgraph-nodes"

  def getNodeMode(options: CaseInsensitiveStringMap): Option[String] =
    getStringOption(NodesModeOption, options)

  override def createReader(options: DataSourceOptions): DataSourceReader = {
    val adjustedOptions = adjustOptions(options)

    val targets = getTargets(adjustedOptions)
    val transaction = getTransaction(targets, options)
    val execution = DgraphExecutorProvider(transaction)
    val schema = getSchema(targets, options).filter(_.isProperty)
    val clusterState = getClusterState(targets, options)
    val partitioner = getPartitioner(schema, clusterState, transaction, adjustedOptions)
    val nodeMode = getNodeMode(adjustedOptions)
    val encoder = nodeMode match {
      case Some(NodesModeTypedOption) => TypedNodeEncoder(schema.predicateMap)
      case Some(NodesModeWideOption) => WideNodeEncoder(schema.predicates)
      case Some(mode) => throw new IllegalArgumentException(s"Unknown node mode: $mode")
      case None => TypedNodeEncoder(schema.predicateMap)
    }
    val chunkSize = getIntOption(ChunkSizeOption, adjustedOptions, ChunkSizeDefault)
    val model = NodeTableModel(execution, encoder, chunkSize)
    TripleScan(partitioner, model)
  }

  override def createReader(schema: StructType, options: DataSourceOptions): DataSourceReader =
    super.createReader(schema, options)

}
