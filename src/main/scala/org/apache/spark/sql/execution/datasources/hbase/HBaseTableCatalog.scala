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
 * File modified by Hortonworks, Inc. Modifications are also licensed under
 * the Apache Software License, Version 2.0.
 */

package org.apache.spark.sql.execution.datasources.hbase

import scala.collection.mutable

import org.json4s.JsonAST.JObject
import org.json4s.jackson.JsonMethods._

import org.apache.avro.Schema
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.types._
import org.apache.spark.sql.execution.datasources.hbase.types._

case class CatalogVersion(major: Int, minor: Int) extends Comparable[CatalogVersion] {
  override def compareTo(o: CatalogVersion): Int = {
    if (major > o.major)
      1
    else if (major == o.major)
      minor - o.minor
    else
      -1
  }

  override def toString: String = major + "." + minor
}

object CatalogVersion {
  def apply(s: String): CatalogVersion = {
    // Valid versions: "1.3", "1"
    // Invalid versions: ".3"
    if (!s.matches("^[0-9]{1,9}(\\.[0-9]{1,9})?$"))
      throw new IllegalArgumentException("Invalid version: " + s)

    val arr: Array[String] = s.split("\\.")

    var m: Int = 0
    var n: Int = 0
    try {
      m = Integer.parseInt(arr(0)) // must always have a major version number
      if (arr(1) != null && arr(1).nonEmpty) // minor version number is optional
        n = Integer.parseInt(arr(1))
    } catch {
      case e: NumberFormatException =>
        throw new IllegalArgumentException("Invalid version: " + s)
    }
    CatalogVersion(m, n)
  }
}

// The definition of each column cell, which may be composite type
case class Field(
    colName: String,
    cf: String,
    col: String,
    fCoder: String,
    sType: Option[String] = None,
    avroSchema: Option[String] = None,
    len: Int = -1) extends Logging {

  val isRowKey: Boolean = cf == HBaseTableCatalog.rowKey
  var start: Int = _
  def schema: Option[Schema] = avroSchema.map { x =>
    logDebug(s"avro: $x")
    val p = new Schema.Parser
    p.parse(x)
  }

  lazy val exeSchema: Option[Schema] = schema
  /**
   * Modification made by Dr. Krusche & Partner PartG:
   *
   * [Avro] dependency is removed.
   *
   */
  // converter from avro to catalyst structure

//  lazy val avroToCatalyst: Option[Any => Any] = {
//    schema.map(SchemaConverters.createConverterToSQL)
//  }

  // converter from catalyst to avro

//  lazy val catalystToAvro: Any => Any ={
//    SchemaConverters.createConverterToAvro(dt, colName, "recordNamespace")
//  }

  val dt: DataType =
//    if (avroSchema.isDefined)
//      schema.map(SchemaConverters.toSqlType(_).dataType).get
//    else
      sType.map(CatalystSqlParser.parseDataType).get

  val length: Int = {
    if (len == -1) {
      dt match {
        case BinaryType | StringType => -1
        case BooleanType => Bytes.SIZEOF_BOOLEAN
        case ByteType => 1
        case DoubleType => Bytes.SIZEOF_DOUBLE
        case FloatType => Bytes.SIZEOF_FLOAT
        case IntegerType => Bytes.SIZEOF_INT
        case LongType => Bytes.SIZEOF_LONG
        case ShortType => Bytes.SIZEOF_SHORT
        case _ => -1
      }
    } else {
      len
    }
  }

  override def equals(other: Any): Boolean = other match {
    case that: Field =>
      colName == that.colName && cf == that.cf && col == that.col
    case _ => false
  }
}

// The row key definition, with each key refer to the col defined in Field, e.g.,
// key1:key2:key3
case class RowKey(k: String) {
  val keys: Array[String] = k.split(":")
  var fields: Seq[Field] = _
  var varLength = false
  def length: Int = {
    val tmp = fields.foldLeft(0) { case (x, y) =>
      val yLen = if (y.length == -1) {
        MaxLength
      } else {
        y.length
      }
      x + yLen
    }
    tmp
  }
}

// The map between the column presented to Spark and the HBase field
case class SchemaMap(map: mutable.LinkedHashMap[String, Field]) {
  def toFields: Seq[StructField] = map.map { case (name, field) =>
    StructField(name, field.dt)
  }.toSeq

  def fields: Iterable[Field] = map.values

  def getField(name: String): Field = map(name)
}

// The definition of HBase and Relation relation schema
case class HBaseTableCatalog(
    namespace: String,
    name: String,
    row: RowKey,
    sMap: SchemaMap,
    tCoder: String,
    coderSet: Set[String],
    numReg: Int,
    splitRange: (String, String)) extends Logging {
  def toDataType: StructType = StructType(sMap.toFields)
  def getField(name: String): Field = sMap.getField(name)
  def getRowKey: Seq[Field] = row.fields
  def getPrimaryKey: String = row.keys(0)
  def getColumnFamilies: Seq[String] = {
    sMap.fields.map(_.cf).filter(_ != HBaseTableCatalog.rowKey).toSeq.distinct
  }

  //this is required to read fromBytes column families and qualifiers
  val stringField: Field = Field("","","",tCoder,Some("string"))
  val shcTableCoder: SHCDataType = SHCDataTypeFactory.create(stringField)

  def initRowKey(): Unit = {
    val fields = sMap.fields.filter(_.cf == HBaseTableCatalog.rowKey)
    row.fields = row.keys.flatMap(n => fields.find(_.col == n))

    // If the tCoder is PrimitiveType, We only allowed there is one key at the end
    // that is determined at runtime.
    if (tCoder == SparkHBaseConf.PrimitiveType) {
      if (!row.fields.reverse.tail.exists(_.length == -1)) {
        var start = 0
        row.fields.foreach { f =>
          f.start = start
          start += f.length
        }
      } else {
        throw new Exception("PrimitiveType: only the last dimension of RowKey is allowed to have " +
          "varied length. You may want to add 'length' to the dimensions which have " +
          "varied length or use dimensions which are scala/java primitive data " +
          "types of fixed length.")
      }
    }
  }

  initRowKey()

  def validateCatalogDef(): Unit = {
    if (!shcTableCoder.isRowKeySupported) {
      throw new UnsupportedOperationException(s"$tCoder does not support row key, and can not be " +
        s"the table coder.")
    }

    if (coderSet.size > 1) {
      // Only Avro can be used with anther coder
      /**
       * Modification made by Dr. Krusche & Partner PartG:
       *
       * [Avro] dependency is removed; therefore, the condition
       * below is valid for all coders.
       *
       */
//      if (!coderSet.contains(SparkHBaseConf.Avro))
      throw new UnsupportedOperationException("Two different coders can not be " +
        "used to encode/decode the same Hbase table")

    }

    // If the row key of the table is composite, check if the coder supports composite key
    if (row.fields.size > 1 && !shcTableCoder.isCompositeKeySupported)
      throw new UnsupportedOperationException(s"$tCoder: Composite key is not supported")
  }
  validateCatalogDef()
}

class CatalogDefinitionException(msg: String) extends Exception(msg)

object HBaseTableCatalog {
  val newTable = "newtable"
  // The json string specifying hbase catalog information
  val tableCatalog = "catalog"
  // The row key with format key1:key2 specifying table row key
  val rowKey = "rowkey"
  // The key for hbase table whose value specify namespace and table name
  val table = "table"
  // The namespace of hbase table
  val nameSpace = "namespace"
  // The name of hbase table
  val tableName = "name"
  // The name of columns in hbase catalog
  val columns = "columns"
  val cf = "cf"
  val col = "col"
  val `type` = "type"
  // the name of avro schema json string
  val avro = "avro"
  val delimiter: Byte = 0
  val length = "length"
  val fCoder = "coder"
  val tableCoder = "tableCoder"
  // The version number of catalog
  val cVersion = "version"
  val minTableSplitPoint = "minTableSplitPoint"
  val maxTableSplitPoint = "maxTableSplitPoint"
  /**
   * User provide table schema definition
   * {"tablename":"name", "rowkey":"key1:key2",
   * "columns":{"col1":{"cf":"cf1", "col":"col1", "type":"type1"},
   * "col2":{"cf":"cf2", "col":"col2", "type":"type2"}}}
   *  Note that any col in the rowKey, there has to be one corresponding col defined in columns
   */
  def apply(parameters: Map[String, String]): HBaseTableCatalog = {
    val jString = parameters(tableCatalog)
    val jObj = parse(jString).asInstanceOf[JObject]
    val map = jObj.values
    val tableMeta = map(table).asInstanceOf[Map[String, _]]
    val nSpace = tableMeta.getOrElse(nameSpace, "default").asInstanceOf[String]
    val tName = tableMeta(tableName).asInstanceOf[String]

    // Since the catalog version 2.0, SHC supports Phoenix as coder.
    // If the catalog version specified by users is equal or later than 2.0, tableCoder must be specified.
    // The default catalog version is 1.0, which uses 'PrimitiveType' as the default 'tableCoder'.
    val vNum = tableMeta.getOrElse(cVersion, "1.0").asInstanceOf[String]
    val tCoder = {
      if (CatalogVersion(vNum).compareTo(CatalogVersion("2.0")) < 0) {
        tableMeta.getOrElse(tableCoder, SparkHBaseConf.PrimitiveType).asInstanceOf[String]
      } else {
        val tc = tableMeta.get(tableCoder)
        if (tc.isEmpty) {
          throw new CatalogDefinitionException("Please specify 'tableCoder' in your catalog " +
            "if the catalog version is equal or later than 2.0")
        }
        tc.get.asInstanceOf[String]
      }
    }
    val schemaMap = mutable.LinkedHashMap.empty[String, Field]
    var coderSet = Set(tCoder)
    getColsPreservingOrder(jObj).foreach { case (name, column)=>
      val len = column.get(length).map(_.toInt).getOrElse(-1)
      val sAvro = column.get(avro).map(parameters(_))
      /**
       * Modification made by Dr. Krusche & Partner PartG:
       *
       * [Avro] dependency is removed; therefore, the condition
       * below is valid for all coders.
       *
       */
//      val fc = if (sAvro.isDefined) SparkHBaseConf.Avro else column.getOrElse(fCoder, tCoder)
      val fc = column.getOrElse(fCoder, tCoder)
      coderSet += fc
      val f = Field(name, column.getOrElse(cf, rowKey), column(col),
        fc, column.get(`type`), sAvro, len)
      schemaMap.+= ((name, f))
    }
    val numReg = parameters.get(newTable).map(x => x.toInt).getOrElse(0)
    val rKey = RowKey(map(rowKey).asInstanceOf[String])

    val minSplit = parameters.getOrElse(minTableSplitPoint, "aaaaaa")
    val maxSplit = parameters.getOrElse(maxTableSplitPoint, "zzzzzz")

    HBaseTableCatalog(nSpace, tName, rKey, SchemaMap(schemaMap), tCoder, coderSet, numReg, (minSplit, maxSplit))
  }

  /**
   * Retrieve the columns mapping from the JObject parsed from the catalog string,
   * and preserve the order of the columns specification. Note that we have to use
   * the AST level api of json4s, because if we cast the parsed object to a scala
   * map directly, it would lose the ordering info during the casting.
   */
  def getColsPreservingOrder(jObj: JObject): Seq[(String, Map[String, String])] = {
    val jCols = jObj.obj.find(_._1 == columns).get._2.asInstanceOf[JObject]
    jCols.obj.map { case (name, jvalue) =>
      (name, jvalue.values.asInstanceOf[Map[String, String]])
    }
  }

  def main(args: Array[String]) {
    val complex = s"""MAP<int, struct<varchar:string>>"""
    val schema =
      s"""{"namespace": "example.avro",
         |   "type": "record", "name": "User",
         |    "fields": [ {"name": "name", "type": "string"},
         |      {"name": "favorite_number",  "type": ["int", "null"]},
         |        {"name": "favorite_color", "type": ["string", "null"]} ] }""".stripMargin

    val catalog = s"""{
            |"table":{"namespace":"default", "name":"htable"},
            |"rowkey":"key1:key2",
            |"columns":{
              |"col1":{"cf":"rowkey", "col":"key1", "type":"string"},
              |"col2":{"cf":"rowkey", "col":"key2", "type":"double"},
              |"col3":{"cf":"cf1", "col":"col1", "avro":"schema1"},
              |"col4":{"cf":"cf1", "col":"col2", "type":"binary"},
              |"col5":{"cf":"cf1", "col":"col3", "type":"double"},
              |"col6":{"cf":"cf1", "col":"col4", "type":"$complex"}
            |}
          |}""".stripMargin

    val parameters = Map("schema1"->schema, tableCatalog->catalog)
    val t = HBaseTableCatalog(parameters)
    val d = t.toDataType
    println(d)

    val sqlContext: SQLContext = null
  }
}
