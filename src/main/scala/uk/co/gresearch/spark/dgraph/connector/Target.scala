package uk.co.gresearch.spark.dgraph.connector
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

case class Target(target: String) {

  val (host, port) = Option(target.split(":", 2)).map { case Array(h, p) => (h, p.toInt) }.get

  def withPort(port: Int): Target = Target(s"$host:$port")

  override def toString: String = target

}
