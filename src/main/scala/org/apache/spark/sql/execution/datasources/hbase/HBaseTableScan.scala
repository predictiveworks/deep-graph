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

import org.apache.hadoop.hbase.CellUtil
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.filter.{Filter => HFilter}
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.execution.datasources.hbase
import org.apache.spark.sql.execution.datasources.hbase.HBaseResources._
import org.apache.spark.sql.execution.datasources.hbase.types.{SHCDataType, SHCDataTypeFactory}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types._
import org.apache.spark.util.ShutdownHookManager

import java.util
import scala.collection.mutable
import scala.util.matching.Regex

private[hbase] case class HBaseRegion(
    override val index: Int,
    start: Option[HBaseType] = None,
    end: Option[HBaseType] = None,
    server: Option[String] = None) extends Partition

private[hbase] case class HBaseScanPartition(
    override val index: Int,
    regions: HBaseRegion,
    scanRanges: Array[ScanRange[Array[Byte]]],
    tf: SerializedTypedFilter) extends Partition

private[hbase] class HBaseTableScanRDD(
    relation: HBaseRelation,
    requiredColumns: Array[String],
    filters: Array[Filter]) extends RDD[Row](relation.sqlContext.sparkContext, Nil) {
  val outputs: Seq[AttributeReference] = StructType(requiredColumns.map(relation.schema(_))).toAttributes
  val columnFields: Seq[Field] = relation.splitRowKeyColumns(requiredColumns)._2
  private def sparkConf = SparkEnv.get.conf
  val serializedToken: Array[Byte] = relation.serializedToken

  override def getPartitions: Array[Partition] = {
    val hbaseFilter = HBaseFilter.buildFilters(filters, relation)
    var idx = 0
    val r = RegionResource(relation)
    logDebug(s"There are ${r.size} regions")
    val ps = r.flatMap { x=>
      // HBase take maximum as empty byte array, change it here.
      val pScan = ScanRange(Some(Bound(x.start.get, inc = true)),
        if (x.end.get.length == 0) None else Some(Bound(x.end.get, inc = false)))
      val ranges = ScanRange.and(pScan, hbaseFilter.ranges)(hbase.ord)
      logDebug(s"partition $idx size: ${ranges.length}")
      if (ranges.length > 0) {
        if(log.isDebugEnabled) {
          ranges.foreach(x => logDebug(x.toString))
        }
        val p = Some(HBaseScanPartition(idx, x, ranges,
          TypedFilter.toSerializedTypedFilter(hbaseFilter.tf)))
        idx += 1
        p
      } else {
        None
      }
    }.toArray
    r.release()
    ShutdownHookManager.addShutdownHook { () => HBaseConnectionCache.close() }
    ps.asInstanceOf[Array[Partition]]
  }

  private def extractKey(s: String, r: Regex): Option[String] = s match {
    case r() => Some("")
    case r(x) => Some(x)
    case _ => None
  }

  private def containsDynamic(dt: DataType): Boolean = dt match {
    case m: MapType => m.keyType.isInstanceOf[StringType.type]
    case _ => false
  }

  private def getInternalValueType(dt: DataType): Option[DataType] = dt match {
    case m: MapType => Some(m.valueType)
    case _ => None
  }

  private def keepVersions(dt: DataType): Boolean = dt match {
    case m: MapType => m.keyType.isInstanceOf[LongType.type]
    case _ => false
  }


  // TODO: It is a big performance overhead, as for each row, there is a hashmap lookup.
  def buildRow(fields: Seq[Field], result: Result): Row = {

    val r = result.getRow

    val keySeq = {
      if (relation.isComposite) {
        relation.catalog.shcTableCoder
          .decodeCompositeRowKey(r, relation.catalog.getRowKey)
      } else {
        val f = relation.catalog.getRowKey.head
        Seq((f, SHCDataTypeFactory.create(f).fromBytes(r))).toMap
      }
    }

    import scala.collection.JavaConverters.mapAsScalaMapConverter
    val scalaMap = result.getMap.asScala

    val valuesSeq = scalaMap.flatMap { case (cf, columns) =>

      val cfName = relation.catalog.shcTableCoder.fromBytes(cf).toString
      val cfFields = fields.filter(_.cf == cfName)
      val scalaColumns = columns.asScala

      cfFields.map{ f =>
      val dataType: SHCDataType = SHCDataTypeFactory.create(f)
        if (f.col.isEmpty && containsDynamic(f.dt)) {

          val m = scalaColumns.map { case (q, versions) =>
            val pq = relation.catalog.shcTableCoder.fromBytes(q).toString
            val v = if(getInternalValueType(f.dt).exists(keepVersions)) {
              versions.asScala.mapValues(dataType.fromBytes)
            } else {
              dataType.fromBytes(versions.firstEntry().getValue)
            }
            pq -> v
          }
          cfFields.foreach(f => m.remove(f.col))
          f -> m.toMap

        } else {

          val pq = relation.catalog.shcTableCoder.toBytes(f.col)
          val timeseries = scalaColumns.get(pq)
          val v = if(keepVersions(f.dt)) {
            timeseries.map(_.asScala.mapValues(dataType.fromBytes))
          } else {
            timeseries.map(ver => dataType.fromBytes(ver.firstEntry().getValue))
          }.orNull
          f -> v

        }
      }.toMap
    }

    val unioned = keySeq ++ valuesSeq

//     Return the row ordered by the requested order
    val ordered = fields.map(unioned.getOrElse(_, null))
    val additional = unioned.filterNot { case (f, _) => fields.contains(f) }.values
    val unstructured = ordered ++ additional
    Row.fromSeq(unstructured)
  }

  // TODO: It is a big performance overhead, as for each row, there is a hashmap lookup.
  def buildRowOld(fields: Seq[Field], result: Result): Row = {
    val r = result.getRow
    val keySeq = {
      if (relation.isComposite) {
        relation.catalog.shcTableCoder
          .decodeCompositeRowKey(r, relation.catalog.getRowKey)
      } else {
        val f = relation.catalog.getRowKey.head
        Seq((f, SHCDataTypeFactory.create(f).fromBytes(r))).toMap
      }
    }

    import scala.collection.JavaConverters.mapAsScalaMapConverter
    val scalaMap = result.getNoVersionMap.asScala

    val valuesSeq = scalaMap.flatMap { case (cf, columns) =>

      val cfName = relation.catalog.shcTableCoder.fromBytes(cf).toString
      val cfFields = fields.filter(_.cf == cfName)
      val scalaColumns = columns.asScala

      cfFields.map(f => {
        val dataType = SHCDataTypeFactory.create(f)
        val m: Map[String, Any] = scalaColumns.flatMap { case (q, value) =>
          val pq = relation.catalog.shcTableCoder.fromBytes(q).toString
          extractKey(pq, new Regex(f.col)).map(_ -> dataType.fromBytes(value))
        }.toMap
        m.get("").map(f -> _).getOrElse(f -> m)
      }).toMap
    }

    val unioned = keySeq ++ valuesSeq

//     Return the row ordered by the requested order
    val ordered = fields.map(unioned.getOrElse(_, null))
    val additional = unioned.filterNot { case (f, _) => fields.contains(f) }.values
    val unstructured = ordered ++ additional
    Row.fromSeq(unstructured)
  }

  // TODO: It is a big performance overhead, as for each row, there is a hashmap lookup.
  def buildRows(fields: Seq[Field], result: Result): Set[Row] = {
    val r = result.getRow
    val keySeq: Map[Field, Any] = {
      if (relation.isComposite) {
        relation.catalog.shcTableCoder
          .decodeCompositeRowKey(r, relation.catalog.getRowKey)
      } else {
        val f = relation.catalog.getRowKey.head
        Seq((f, SHCDataTypeFactory.create(f).fromBytes(r))).toMap
      }
    }

    val valueSeq: Seq[Map[Long, (Field, Any)]] = fields.filter(!_.isRowKey).map { x =>
      import scala.collection.JavaConverters.asScalaBufferConverter
      val dataType = SHCDataTypeFactory.create(x)
      val kvs = result.getColumnCells(
        relation.catalog.shcTableCoder.toBytes(x.cf),
        relation.catalog.shcTableCoder.toBytes(x.col)).asScala

      kvs.map(kv => {
        val v = CellUtil.cloneValue(kv)
        (kv.getTimestamp, x -> dataType.fromBytes(v))
      }).toMap.withDefaultValue(x -> null)
    }

    val ts = valueSeq.foldLeft(Set.empty[Long])((acc, map) => acc ++ map.keySet)
    //we are loosing duplicate here, because we didn't support passing version (timestamp) to the row
    ts.map(version => {
      keySeq ++ valueSeq.map(_.apply(version)).toMap
    }).map { unioned =>
      // Return the row ordered by the requested order
      Row.fromSeq(fields.map(unioned.get(_).orNull))
    }
  }

  private def toResultIterator(result: GetResource): Iterator[Result] = {
    val iterator: Iterator[Result] = new Iterator[Result] {
      var idx = 0
      var cur: Option[Result] = None
      override def hasNext: Boolean = {
        while(idx < result.length && cur.isEmpty) {
          val tmp = result(idx)
          idx += 1
          if (!tmp.isEmpty) {
            cur = Some(tmp)
          }
        }
        if (cur.isEmpty) {
          rddResources.release(result)
        }
        cur.isDefined
      }
      override def next(): Result = {
        hasNext
        val ret = cur.get
        cur = None
        ret
      }
    }
    iterator
  }

  private def toResultIterator(scanner: ScanResource): Iterator[Result] = {
    val iterator: Iterator[Result] = new Iterator[Result] {
      var cur: Option[Result] = None
      override def hasNext: Boolean = {
        if (cur.isEmpty) {
          val r = scanner.next()
          if (r == null) {
            rddResources.release(scanner)
          } else {
            cur = Some(r)
          }
        }
        cur.isDefined
      }
      override def next(): Result = {
        hasNext
        val ret = cur.get
        cur = None
        ret
      }
    }
    iterator
  }

  private def toRowIterator(
      it: Iterator[Result]): Iterator[Row] = {

    val iterator: Iterator[Row] = new Iterator[Row] {
      val start: Long = System.currentTimeMillis()
      var rowCount: Int = 0
      val indexedFields: Seq[Field] = relation.getIndexedProjections(requiredColumns).map(_._1)

      override def hasNext: Boolean = {
        if(it.hasNext) {
          true
        }
        else {
          val end = System.currentTimeMillis()
          logInfo(s"returned $rowCount rows from hbase in ${end - start} ms")
          false
        }
      }

      override def next(): Row = {
        rowCount += 1
        val r = it.next()
        buildRow(indexedFields, r)
      }
    }
    iterator
  }

  /**
    * Convert result in to list of rows aggregated by timestamp and flat this list into one iterator of rows
    * This solution stand for fetching more than one version
    */
  private def toFlattenRowIterator(
      it: Iterator[Result]): Iterator[Row] = {

    val iterator: Iterator[Row] = new Iterator[Row] {
      val start: Long = System.currentTimeMillis()
      var rowCount: Int = 0
      var rows: Set[Row] = Set.empty[Row]
      val indexedFields: Seq[Field] = relation.getIndexedProjections(requiredColumns).map(_._1)

      override def hasNext: Boolean = {
        if(rows.nonEmpty || it.hasNext) {
          true
        }
        else {
          val end = System.currentTimeMillis()
          logInfo(s"returned $rowCount rows from hbase in ${end - start} ms")
          false
        }
      }

      private def nextRow(): Row = {
        val row = rows.head
        rows = rows.tail
        row
      }

      override def next(): Row = {
        rowCount += 1
        if(rows.isEmpty) {
          val r = it.next()
          rows = buildRows(indexedFields, r)
          if(rows.isEmpty) {
            // If 'requiredColumns' is empty, 'indexedFields' will be empty, which leads to empty 'rows'.
            // This happens when users' query doesn't require Spark/SHC to return any real data from HBase tables,
            // e.g. dataframe.count()
            Row.fromSeq(Seq.empty)
          } else {
            nextRow()
          }
        } else {
          nextRow()
        }
      }
    }
    iterator
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    split.asInstanceOf[HBaseScanPartition].regions.server.map {
      identity
    }.toSeq
  }

  private def buildScan(
      start: Option[HBaseType],
      end: Option[HBaseType],
      columns: Seq[Field], filter: Option[HFilter]): Scan = {
    val scan = {
      (start, end) match {
        case (Some(lb), Some(ub)) => new Scan(lb, ub)
        case (Some(lb), None) => new Scan(lb)
        case (None, Some(ub)) => new Scan(Array[Byte](), ub)
        case _ => new Scan
      }
    }
    handleTimeSemantics(scan)

    // set fetch size
    // scan.setCaching(scannerFetchSize)
    if(relation.restrictive == HBaseRelation.Restrictive.column) {
      columns.foreach { c =>
        scan.addColumn(
          relation.catalog.shcTableCoder.toBytes(c.cf),
          relation.catalog.shcTableCoder.toBytes(c.col))
      }
    } else if (relation.restrictive == HBaseRelation.Restrictive.family) {
      columns.foreach { c =>
        scan.addFamily(relation.catalog.shcTableCoder.toBytes(c.cf))
      }
    }

    val size = sparkConf.getInt(SparkHBaseConf.CachingSize, SparkHBaseConf.defaultCachingSize)
    scan.setCaching(size)
    filter.foreach(scan.setFilter)
    scan
  }

  private def buildGets(
      tbr: TableResource,
      g: Array[ScanRange[Array[Byte]]],
      columns: Seq[Field],
      filter: Option[HFilter]): Iterator[Result] = {
    val size = sparkConf.getInt(SparkHBaseConf.BulkGetSize, SparkHBaseConf.defaultBulkGetSize)
    g.grouped(size).flatMap{ x =>
      val gets = new util.ArrayList[Get]()
      x.foreach{ y =>
        val g = new Get(y.start.get.point)
        handleTimeSemantics(g)
        columns.foreach{ c =>
          g.addColumn(
            relation.catalog.shcTableCoder.toBytes(c.cf),
            relation.catalog.shcTableCoder.toBytes(c.col))
        }
        filter.foreach(g.setFilter)
        gets.add(g)
      }
      val tmp = tbr.get(gets)
      rddResources.addResource(tmp)
      toResultIterator(tmp)
    }
  }
  lazy val rddResources: RDDResources = RDDResources(new mutable.HashSet[Resource]())

  private def close() {
    rddResources.release()
  }

  override def compute(split: Partition, context: TaskContext): Iterator[Row] = {
    SHCCredentialsManager.processShcToken(serializedToken)
    val ord = hbase.ord//implicitly[Ordering[HBaseType]]
    val partition = split.asInstanceOf[HBaseScanPartition]
    // remove the inclusive upperbound
    val scanRanges = partition.scanRanges.flatMap(ScanRange.split(_)(ord))
    val (g, s) = scanRanges.partition{x =>
      x.start.isDefined && x.end.isDefined && ScanRange.compare(x.start, x.end, ord) == 0
    }
    logDebug(s"${g.length} gets, ${s.length} scans")
    context.addTaskCompletionListener(context => close())
    val tableResource = TableResource(relation)
    val filter = TypedFilter.fromSerializedTypedFilter(partition.tf).filter
    val gIt: Iterator[Result] = {
      if (g.isEmpty) {
        Iterator.empty: Iterator[Result]
      } else {
        buildGets(tableResource, g, columnFields, filter)
      }
    }

    val scans = s.map(x =>
      buildScan(x.get(x.start), x.get(x.end), columnFields, filter))

    val sIts = scans.par.map { scan =>
      val scanner = tableResource.getScanner(scan)
      rddResources.addResource(scanner)
      scanner
    }.map(toResultIterator)

    val rIt = sIts.fold(Iterator.empty: Iterator[Result]){ case (x, y) =>
      x ++ y
    } ++ gIt

    ShutdownHookManager.addShutdownHook { () => HBaseConnectionCache.close() }
    if(relation.mergeToLatest) {
      toRowIterator(rIt)
    } else {
      toFlattenRowIterator(rIt)
    }
  }

  private def handleTimeSemantics(query: Query): Unit = {
    // Set timestamp related values if present
    (query, relation.timestamp, relation.minStamp, relation.maxStamp)  match {
      case (q: Scan, Some(ts), None, None) => q.setTimeStamp(ts)
      case (q: Get, Some(ts), None, None) => q.setTimeStamp(ts)

      case (q:Scan, None, Some(minStamp), Some(maxStamp)) => q.setTimeRange(minStamp, maxStamp)
      case (q:Get, None, Some(minStamp), Some(maxStamp)) => q.setTimeRange(minStamp, maxStamp)

      case (_, None, None, None) =>

      case _ => throw new IllegalArgumentException("Invalid combination of query/timestamp/time range provided")
    }
    if (relation.maxVersions.isDefined) {
      query match {
        case q: Scan => q.setMaxVersions(relation.maxVersions.get)
        case q: Get => q.setMaxVersions(relation.maxVersions.get)
        case _ => throw new IllegalArgumentException("Invalid query provided with maxVersions")
      }
    }
  }
}

case class RDDResources(set: mutable.HashSet[Resource]) {
  def addResource(s: Resource) {
    set += s
  }
  def release() {
    set.foreach(release)
  }
  def release(rs: Resource) {
    try {
      rs.release()
    } finally {
      set.remove(rs)
    }
  }
}
