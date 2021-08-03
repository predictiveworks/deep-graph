package org.apache.spark.sql.execution.datasources.hbase.types
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * File is retrieved from Robert Yokota's contribution to Hortonworks
 * Spark-on-HBase-Connector project (shc).
 */

import io.hgraphdb.ValueUtils
import org.apache.spark.sql.execution.datasources.hbase._

class HGraphDB(f:Option[Field] = None) extends SHCDataType {

  def fromBytes(src: HBaseType): Any = {
    if (f.isDefined) {
      if (f.get.isRowKey) {
        ValueUtils.deserializeWithSalt(src)
      } else {
        ValueUtils.deserialize(src)
      }
    } else {
      throw new UnsupportedOperationException(
        "HGraphDB coder: without field metadata, 'fromBytes' conversion can not be supported")
    }
  }

  def toBytes(input: Any): Array[Byte] = {
    if (f.isDefined) {
      if (f.get.isRowKey) {
        ValueUtils.serializeWithSalt(input)
      } else {
        ValueUtils.serialize(input)
      }
    } else {
      new PrimitiveType().toBytes(input)
    }
  }

  override def isRowKeySupported: Boolean = true

  override def isCompositeKeySupported: Boolean = false
}