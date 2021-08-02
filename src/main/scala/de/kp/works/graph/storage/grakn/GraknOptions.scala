package de.kp.works.graph.storage.grakn
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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap

class GraknOptions(@transient val parameters: CaseInsensitiveMap[String])(
  operationType: OperationType.Value)
  extends Serializable
    with Logging {

  import GraknOptions._

  def this(parameters: Map[String, String], operaType: OperationType.Value) =
    this(CaseInsensitiveMap(parameters))(operaType)

  val dataType: String = parameters(GraknOptions.TYPE)

  var partitions: String = _
  if (operationType == OperationType.READ) {

    require(parameters.isDefinedAt(PARTITIONS), s"Option '$PARTITIONS' is required")
    partitions = parameters(PARTITIONS)

  }

}

object GraknOptions {

  val PARTITIONS: String = "partitions"
  val TYPE: String       = "type"

}
