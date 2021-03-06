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

import uk.co.gresearch.spark.dgraph.connector.model.GraphTableModel
import uk.co.gresearch.spark.dgraph.connector.{Partition, Schema, Target}

case class SingletonPartitioner(targets: Seq[Target], schema: Schema) extends Partitioner {

  override def getPartitions(implicit model: GraphTableModel): Seq[Partition] = {
    val langs = schema.predicates.filter(_.isLang).map(_.predicateName)
    Seq(Partition(targets)(model).has(schema.predicates).langs(langs))
  }

}
