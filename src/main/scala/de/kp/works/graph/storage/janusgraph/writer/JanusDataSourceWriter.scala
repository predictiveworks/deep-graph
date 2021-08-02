package de.kp.works.graph.storage.janusgraph.writer
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

import de.kp.works.graph.storage.janusgraph.JanusOptions
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.writer.{DataSourceWriter, DataWriterFactory, WriterCommitMessage}
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

class JanusVertexWriter(janusOptions: JanusOptions, schema: StructType)
  extends DataSourceWriter {

  private val LOG = LoggerFactory.getLogger(this.getClass)

  override def createWriterFactory(): DataWriterFactory[InternalRow] = ???

  override def commit(writerCommitMessages: Array[WriterCommitMessage]): Unit = ???

  override def abort(writerCommitMessages: Array[WriterCommitMessage]): Unit = ???

}

class JanusEdgeWriter(janusOptions: JanusOptions, schema: StructType)
  extends DataSourceWriter {

  private val LOG = LoggerFactory.getLogger(this.getClass)

  override def createWriterFactory(): DataWriterFactory[InternalRow] = ???

  override def commit(writerCommitMessages: Array[WriterCommitMessage]): Unit = ???

  override def abort(writerCommitMessages: Array[WriterCommitMessage]): Unit = ???

}