package ml.sparkling.graph.operators.measures.vertex.betweenness.hua.struct.messages

import org.apache.spark.graphx.VertexId

/**
  * Created by mth on 5/7/17.
  */
class DFSPointer(val source: VertexId, val next: Option[VertexId], val toSent: Boolean) extends NOMessage[VertexId] {
  override def content: VertexId = source

  override val isDFSPointer = true

  val toRemove: Boolean = toSent

  val returning: Boolean = next.isEmpty

  def asToSent(n: Option[VertexId] = next): DFSPointer = DFSPointer(source, n, toSent = true)

  def asWaiting(n: Option[VertexId]): DFSPointer = DFSPointer(source, n, toSent = false)

  def asReturning: DFSPointer = DFSPointer(source, None, toSent = true)
}

object DFSPointer extends Serializable {
  def apply(source: VertexId,
            next: Option[VertexId],
            toSent: Boolean
           ): DFSPointer = new DFSPointer(source, next, toSent)
}
