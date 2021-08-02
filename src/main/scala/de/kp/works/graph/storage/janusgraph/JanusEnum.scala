package de.kp.works.graph.storage.janusgraph

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

object DataTypeEnum extends Enumeration {

  type DataType = Value

  val VERTEX: DataTypeEnum.Value = Value("vertex")
  val EDGE: DataTypeEnum.Value = Value("edge")

  def validDataType(dataType: String): Boolean = {
    dataType.equalsIgnoreCase(VERTEX.toString) || dataType.equalsIgnoreCase(EDGE.toString)
  }
}

object OperationType extends Enumeration {

  type Operation = Value
  val READ: OperationType.Value = Value("read")
  val WRITE: OperationType.Value = Value("write")

}
