package ml.sparkling.graph.operators.algorithms.shortestpaths.pathprocessors

import org.apache.spark.graphx.VertexId

/**
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 * Path processors that computes shortest paths lengths using standard scala collections
 */
class WithDistanceProcessor[VD, ED]() extends PathProcessor[VD, ED, Map[VertexId,ED]] {
  def EMPTY_CONTAINER = Map.empty[VertexId,ED]

  def getNewContainerForPaths(): Map[VertexId, ED] = {
    EMPTY_CONTAINER
  }

  def putNewPath(map: Map[VertexId, ED], to: VertexId, weight: ED)(implicit num: Numeric[ED]): Map[VertexId, ED] = {
    map + (to->weight)
  }

  def processNewMessages(map1: Map[VertexId, ED], map2: Map[VertexId, ED])(implicit num: Numeric[ED]): Map[VertexId, ED] = {
    (map1.keySet.par ++ map2.keySet.par).map(vId=>(vId,num.min(map1.getOrElse(vId,map2(vId)),map2.getOrElse(vId,map1(vId))))).toMap.seq.map(identity)
  }

  def extendPathsMerging(targetVertexId:VertexId,map: Map[VertexId, ED], vertexId: VertexId, distance: ED,map2: Map[VertexId, ED])(implicit num: Numeric[ED]) = {
    val extended= map.par.filterKeys(_!=targetVertexId).mapValues(num.plus(_,distance)).toMap.seq.map(identity)
    processNewMessages(extended,map2)
  }

}