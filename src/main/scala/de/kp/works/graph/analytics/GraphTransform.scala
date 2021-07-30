package de.kp.works.graph.analytics

/**
 * This class supports transformation from [GraphFrame]
 * to [GraphX] and vice versa.
 */
class GraphTransform {

  private var nodeIdCol:String = "id"

  /**
   * GraphX vertices, and edges require [Long] identifiers
   */
  def setNodeIdCol(name:String):GraphTransform = {
    nodeIdCol = name
    this
  }
}
