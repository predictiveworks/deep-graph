package ml.sparkling.graph.operators.measures.vertex.betweenness.flow.processor

import ml.sparkling.graph.operators.measures.vertex.betweenness.flow.generator.FlowGenerator
import ml.sparkling.graph.operators.measures.vertex.betweenness.flow.struct.{CFBCFlow, CFBCNeighbourFlow, CFBCVertex}
import org.apache.spark.graphx.{EdgeTriplet, Graph, VertexId, VertexRDD}

import scala.reflect.ClassTag

/**
  * Created by mth on 4/23/17.
  */
class CFBCProcessor[VD, ED: ClassTag](graph: Graph[VD, ED], flowGenerator: FlowGenerator[CFBCVertex, Option[CFBCFlow]]) extends Serializable {

  lazy val initGraph: Graph[CFBCVertex, ED] = prepareRawGraph

  lazy val numOfVertices: VertexId = graph.ops.numVertices

  private def prepareRawGraph = {
    val degrees = graph.ops.degrees
    graph.outerJoinVertices(degrees)((id, _, deg) => CFBCVertex(id, deg.getOrElse(0)))
  }

  def createFlow(graph: Graph[CFBCVertex, ED]): Graph[CFBCVertex, ED] = {
    graph.mapVertices((id, data) =>
      flowGenerator.createFlow(data) match {
        case Some(flow) => data.addNewFlow(flow)
        case None => data
      })
  }

  def joinReceivedFlows(vertexId: VertexId, vertex: CFBCVertex, msg: Array[CFBCFlow]): CFBCVertex =
    vertex.applyNeighbourFlows(msg.groupBy(_.key).map({ case(key, it) =>
      if (it.nonEmpty) CFBCNeighbourFlow(it, vertex) else CFBCNeighbourFlow(key) }))


  def applyFlows(epsilon: Double)(id: VertexId, data: CFBCVertex): CFBCVertex = {

    def updateFlow(contextFlow: CFBCFlow, nbhFlow: CFBCNeighbourFlow) = {
      val newPotential = (nbhFlow.sumOfPotential + contextFlow.supplyValue(id)) / data.degree
      val potentialDiff = Math.abs(contextFlow.potential - newPotential)
      val completed = contextFlow.completed || potentialDiff < epsilon
      CFBCFlow(contextFlow.src, contextFlow.dst, newPotential, completed, contextFlow.aliveThrough)
    }

    val newFlows = for (nb <- data.neighboursFlows) yield {
      val flowOpt = data.flowsMap.get(nb.key)
      flowOpt match {
        case Some(flow) => Some(updateFlow(flow, nb))
        case None if !nb.anyCompleted => Some(updateFlow(CFBCFlow.empty(nb.key), nb))
        case _ => None
      }
    }

    val k2 = newFlows.filter(_.nonEmpty).flatten.map(f => (f.key, f)).toMap

    data.updateFlows((data.flowsMap ++ k2).values.toArray)
  }

  def computeBetweenness(vertexId: VertexId, vertex: CFBCVertex): CFBCVertex = {
    val applicableFlows = vertex.neighboursFlows.filter(nf => nf.src != vertexId && nf.dst != vertexId)
    val completedReceivedFlows = applicableFlows.filter(_.allCompleted)
    val completedFlows = completedReceivedFlows.filter(nf => vertex.getFlow(nf.key).completed)

    val currentFlows = completedFlows.map(_.sumOfDifferences / 2)

    vertex.updateBC(currentFlows.toSeq)
  }

  def removeCompletedFlows(vertexId: VertexId, vertex: CFBCVertex): CFBCVertex = {
    val completedReceivedFlows = vertex.neighboursFlows.map(nf => (nf.key, nf.allCompleted)).toMap
    val completedFlows = vertex.vertexFlows.filter(f => f.removable && completedReceivedFlows.getOrElse(f.key, true))

    vertex.removeFlows(completedFlows)
  }

  def extractFlowMessages(graph: Graph[CFBCVertex, ED]): VertexRDD[Array[CFBCFlow]] =
    graph.aggregateMessages[Array[CFBCFlow]](ctx => {

      def send(triplet: EdgeTriplet[CFBCVertex, ED])(dst: VertexId, sendF: (Array[CFBCFlow]) => Unit): Unit = {
        val srcFlows = triplet.otherVertexAttr(dst).vertexFlows
        val dstFlowsKeys = triplet.vertexAttr(dst).vertexFlows.map(_.key).toSet
        val activeFlows = srcFlows.filterNot(_.completed)
        val completedFlows = srcFlows.filter(f => f.completed && dstFlowsKeys.contains(f.key))
        sendF(activeFlows ++ completedFlows)
      }

      val sendDataTo = send(ctx.toEdgeTriplet) _
      sendDataTo(ctx.srcId, ctx.sendToSrc)
      sendDataTo(ctx.dstId, ctx.sendToDst)
    }, _ ++ _)

  def preMessageExtraction(eps: Double)(graph: Graph[CFBCVertex, ED], msg: VertexRDD[Array[CFBCFlow]]): Graph[CFBCVertex, ED] =
    graph.outerJoinVertices(msg)((vertexId, vertex, vertexMsg) => {
      val newVert = joinReceivedFlows(vertexId, vertex, vertexMsg.getOrElse(Array.empty))
      applyFlows(eps)(vertexId, newVert)
    })

  def postMessageExtraction(graph: Graph[CFBCVertex, ED]): Graph[CFBCVertex, ED] =
    graph.mapVertices((id, vertex) => {
      val vertWithBC = computeBetweenness(id, vertex)
      removeCompletedFlows(id, vertWithBC)
    })

}
