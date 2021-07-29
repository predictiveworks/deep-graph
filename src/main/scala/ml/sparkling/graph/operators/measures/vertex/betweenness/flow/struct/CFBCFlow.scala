package ml.sparkling.graph.operators.measures.vertex.betweenness.flow.struct

import org.apache.spark.graphx.VertexId

/**
  * Created by mth on 4/23/17.
  */
class CFBCFlow(val src: VertexId, val dst: VertexId, val potential: Double, val completed: Boolean, val aliveThrough: Int) extends Serializable {
  def supplyValue(vertexId: VertexId): Int = vertexId match {
    case `src` => 1
    case `dst` => -1
    case _ => 0
  }

  val key: (VertexId, VertexId) = (src, dst)

  val removable: Boolean = completed && aliveThrough <= 0

  def countdownVitality: CFBCFlow = if (aliveThrough > 0) CFBCFlow(src, dst, potential, completed, aliveThrough - 1) else this
}

object CFBCFlow extends Serializable {
  def apply(src: VertexId,
            dst: VertexId,
            potential: Double = 1.0,
            completed: Boolean = false,
            aliveThrough: Int = 3
           ): CFBCFlow = new CFBCFlow(src, dst, potential, completed, aliveThrough)

  def updatePotential(flow: CFBCFlow, newPotential: Double, eps: Double = 0.0): CFBCFlow = {
    val completed = Math.abs(flow.potential - newPotential) > eps
    CFBCFlow(flow.src, flow.dst, newPotential, completed, flow.aliveThrough)
  }

  def empty(key: (VertexId, VertexId)): CFBCFlow = key match { case (src, dst) =>  CFBCFlow(src, dst, 0.0) }
}

