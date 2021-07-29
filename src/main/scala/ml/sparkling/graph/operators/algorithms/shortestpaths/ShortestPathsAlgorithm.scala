package ml.sparkling.graph.operators.algorithms.shortestpaths

import java.util
import ml.sparkling.graph.api.operators.IterativeComputation._
import ml.sparkling.graph.api.operators.algorithms.shortestpaths.ShortestPathsTypes._
import ml.sparkling.graph.operators.algorithms.shortestpaths.pathprocessors.fastutils.FastUtilWithDistance.DataMap
import ml.sparkling.graph.operators.algorithms.shortestpaths.pathprocessors.fastutils.{FastUtilWithDistance, FastUtilWithPath}
import ml.sparkling.graph.operators.algorithms.shortestpaths.pathprocessors.{PathProcessor, SingleVertexProcessor}
import ml.sparkling.graph.operators.predicates.{AllPathPredicate, ByIdPredicate, ByIdsPredicate}
import org.apache.log4j.Logger
import org.apache.spark.graphx._

import scala.reflect.ClassTag
import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
 * Main object of shortest paths algorithm
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 */
case object ShortestPathsAlgorithm  {
  val logger: Logger =Logger.getLogger(ShortestPathsAlgorithm.getClass)
  /**
   * Path computing main method, should be used for further development and extension, object contains methods for main computations please use them instead of configuring this one
   * @param graph - graph for computation
   * @param pathProcessor - path processor that will handle path type dependent operations (processor for double, set etc.)
   * @param treatAsUndirected - treat graph as undirected (each path will be bidirectional)
   * @param num  - numeric to operate on  edge lengths
   * @tparam VD - vertex type
   * @tparam ED -  edge type
   * @tparam PT - path type
   * @return - Graph where each vertex contains all its shortest paths, type depends on path processor (double, list etc.)
   */
  def computeAllPathsUsing[VD, ED: ClassTag, PT: ClassTag](graph: Graph[VD, ED], vertexPredicate: VertexPredicate[VD], treatAsUndirected: Boolean, pathProcessor: PathProcessor[VD, ED, PT])(implicit num: Numeric[ED]): Graph[PT, ED] = {
    val initDistances = graph.vertices.mapValues(
      (vId,data)=>{
        if(vertexPredicate(vId,data)){
          pathProcessor.putNewPath(pathProcessor.getNewContainerForPaths,vId,num.zero)
        }else{
          pathProcessor.getNewContainerForPaths
        }
      }
    )
    val initMap: Graph[PT, ED] = graph.outerJoinVertices(initDistances)((_, _, newValue) => newValue.getOrElse(pathProcessor.getNewContainerForPaths))
    val out=initMap.pregel[PT](pathProcessor.EMPTY_CONTAINER)(
      vprog = vertexProgram(pathProcessor),
      sendMsg = sendMessage(treatAsUndirected,pathProcessor),
      mergeMsg =  pathProcessor.processNewMessages
    )
    out
  }

  /**
   * Compute all pair shortest paths lengths (each vertex will contains map of shortest paths to other vertices)
   * @param graph
   * @param vertexPredicate - if true for vertexId, then distance to vertex is computed ,by default distances to all vertices are computed
   * @param treatAsUndirected - by default false, if true each edge is treated as bidirectional
   * @param num - numeric parameter for ED
   * @tparam VD - vertex data type
   * @tparam ED - edge data type (must be numeric)
   * @return graph where each vertex has map of its shortest paths lengths
   */
  def computeShortestPathsLengths[VD, ED: ClassTag](graph: Graph[VD, ED], vertexPredicate: VertexPredicate[VD] = AllPathPredicate, treatAsUndirected: Boolean = false)(implicit num: Numeric[ED]): Graph[DataMap, ED] = {
    computeAllPathsUsing(graph, vertexPredicate, treatAsUndirected, new FastUtilWithDistance[VD, ED]())
  }

  /**
   * Compute shortest paths lengths from all vertices to single one (each vertex will contain distance to given vertex)
   * @param graph
   * @param vertexId - vertex id to witch distances will be computed
   * @param treatAsUndirected - by default false, if true each edge is treated as bidirectional
   * @param num - numeric parameter for ED
   * @tparam VD - vertex data type
   * @tparam ED - edge data type (must be numeric)
   * @return graph where each vertex has distance to @vertexId
   */
  def computeSingleShortestPathsLengths[VD, ED: ClassTag](graph: Graph[VD, ED], vertexId: VertexId, treatAsUndirected: Boolean = false)(implicit num: Numeric[ED]): Graph[Double, ED] = {
    computeAllPathsUsing(graph, ByIdPredicate(vertexId), treatAsUndirected, new SingleVertexProcessor[VD,ED](vertexId))
  }

  /**
   * Computes shoretest all pair shortest paths with paths (each vertex will has map of its paths to orher vertiecs,
   * map values are sets of paths (lists) where first element(0) is path length)
   * @param graph
   * @param vertexPredicate - if true for vertexId, then distance to vertex is computed ,by default distances to all vertices are computed
   * @param treatAsUndirected - by default false, if true each edge is treated as bidirectional
   * @param num - numeric parameter for ED
   * @tparam VD - vertex data type
   * @tparam ED - edge data type (must be numeric)
   * @return graph where each vertex has map of its shortest paths
   */
  def computeShortestPaths[VD, ED: ClassTag](graph: Graph[VD, ED], vertexPredicate: VertexPredicate[VD] = AllPathPredicate, treatAsUndirected: Boolean = false)(implicit num: Numeric[ED]): Graph[WithPathContainer, ED] = {
    computeAllPathsUsing(graph, vertexPredicate, treatAsUndirected, new FastUtilWithPath[VD, ED]())
  }

  /**
   * Compute all pair shortest paths lengths (each vertex will contains map of shortest paths to other vertices) in multiple
   * super-steps of size provided by @bucketSizeProvider
   * @param graph
   * @param bucketSizeProvider - method that provides single super-step size
   * @param treatAsUndirected - by default false, if true each edge is treated as bidirectional
   * @param num - numeric parameter for ED
   * @tparam VD - vertex data type
   * @tparam ED - edge data type (must be numeric)
   * @return graph where each vertex has map of its shortest paths
   */
  def computeShortestPathsLengthsIterative[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], bucketSizeProvider: BucketSizeProvider[VD,ED], treatAsUndirected: Boolean = false,checkpointingFrequency:Int=20)(implicit num: Numeric[ED]): Graph[DataMap, ED] = {
    val bucketSize=bucketSizeProvider(graph)
    logger.info(s"Computing APSP using iterative approach with bucket of size $bucketSize")
    graph.cache()
    val vertexIds=graph.vertices.map{case (vId,data)=>vId}.treeAggregate(mutable.ListBuffer.empty[VertexId])(
      (agg:ListBuffer[VertexId],data:VertexId)=>{agg+=data;agg},
      (agg:ListBuffer[VertexId],agg2:ListBuffer[VertexId])=>{agg++=agg2;agg}
    ).toList
    val outGraph:Graph[FastUtilWithDistance.DataMap ,ED] = graph.mapVertices((_,_)=>new FastUtilWithDistance.DataMap)
    outGraph.cache()
    val vertices =vertexIds.grouped(bucketSize.toInt).toList
    val numberOfIterations=vertices.size
    val (out,_)=vertices.foldLeft((outGraph,1)){
      case ((acc,iteration),vertexIds)=>
        logger.info(s"Shortest Paths iteration $iteration from  $numberOfIterations")
        if(iteration%checkpointingFrequency==0){
          logger.info(s"Chceckpointing graph")
          acc.checkpoint()
          acc.vertices.foreachPartition(_=>{})
          acc.edges.foreachPartition(_=>{})
        }
        acc.cache()
        val vertexPredicate=ByIdsPredicate(vertexIds.toSet)
        val computed=computeShortestPathsLengths(graph,vertexPredicate,treatAsUndirected).cache()
        val outGraphInner=acc.outerJoinVertices(computed.vertices)((vId,outMap,computedMap)=>{
          computedMap.flatMap(m=>{outMap.putAll(m);Option(outMap)}).getOrElse(outMap)
        })
        outGraphInner.cache()
        (outGraphInner,iteration+1)
    }
    out

  }

  private def sendMessage[VD, ED, PT](treatAsUndirected: Boolean, pathProcessor: PathProcessor[VD, ED, PT])(edge: EdgeTriplet[PT, ED])(implicit num: Numeric[ED]): Iterator[(VertexId, PT)] = {
    if (treatAsUndirected) {
      val extendedDst = pathProcessor.extendPathsMerging(edge.srcId,edge.dstAttr, edge.dstId, edge.attr, edge.srcAttr)
      val itSrc = if (!edge.srcAttr.equals(extendedDst)) Iterator((edge.srcId, extendedDst)) else Iterator.empty
      val extendedSrc = pathProcessor.extendPathsMerging(edge.dstId,edge.srcAttr, edge.srcId, edge.attr, edge.dstAttr)
      val itDst = if (!edge.dstAttr.equals(extendedSrc)) Iterator((edge.dstId, extendedSrc)) else Iterator.empty
      itSrc ++ itDst
    } else {
      val extendedDst = pathProcessor.extendPathsMerging(edge.srcId,edge.dstAttr, edge.dstId, edge.attr, edge.srcAttr)
      if (!edge.srcAttr.equals(extendedDst)){
        Iterator((edge.srcId, extendedDst))
      } else {
        Iterator.empty
      }
    }
  }


  private def vertexProgram[VD, ED, PT](pathProcessor: PathProcessor[VD, ED, PT])(vId: VertexId, data: PT, message: PT)(implicit num: Numeric[ED]) = {
    pathProcessor.processNewMessages(data, message)
  }

  def computeAPSPToDirectory[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], outDirectory: String, treatAsUndirected: Boolean, bucketSize:Long)(implicit num: Numeric[ED]): Unit = {
    val vertices= graph.vertices.map(_._1).sortBy(k => k).collect()
    val verticesGroups =vertices.grouped(bucketSize.toInt).zipWithIndex.toList
    val numberOfIterations=verticesGroups.length
    graph.cache()
    verticesGroups.foreach{
      case (group,iteration) =>
        logger.info(s"Shortest Paths iteration ${iteration+1} from  $numberOfIterations")
        val shortestPaths = ShortestPathsAlgorithm.computeShortestPathsLengths(graph, new ByIdsPredicate(group.toSet), treatAsUndirected)
        val joinedGraph = graph
          .outerJoinVertices(shortestPaths.vertices)((vId, data, newData) => (data, newData.getOrElse(new FastUtilWithDistance.DataMap)))
        joinedGraph.vertices.values.map {
          case (vertex, data: util.Map[JLong, JDouble]) => {
            val dataStr = data.entrySet()
              .map(e=>s"${e.getKey}:${e.getValue}").mkString(";")
            s"$vertex;$dataStr"
          }
        }.saveAsTextFile(s"$outDirectory/from_${group.head}")
        shortestPaths.unpersist(blocking = false)
    }


    graph.vertices.map(t => List(t._1, t._2).mkString(";")).saveAsTextFile(s"$outDirectory/index")
  }
}
