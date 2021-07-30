package de.kp.works.graph.analytics

import de.kp.works.spark.Session
import org.graphframes.GraphFrame

object TransformTest {

  private val session = Session.getSession
  private val sqlc = session.sqlContext

  def main(args:Array[String]):Unit = {

    val v = sqlc.createDataFrame(List(
      ("a", "Alice", 34),
      ("b", "Bob", 36),
      ("c", "Charlie", 30),
      ("d", "David", 29),
      ("e", "Esther", 32),
      ("f", "Fanny", 36),
      ("g", "Gabby", 60)
    )).toDF("id", "name", "age")

    // Edge DataFrame
    val e = sqlc.createDataFrame(List(
      ("a", "b", "friend"),
      ("b", "c", "follow"),
      ("c", "b", "follow"),
      ("f", "c", "follow"),
      ("e", "f", "follow"),
      ("e", "d", "friend"),
      ("d", "a", "friend"),
      ("a", "e", "friend")
    )).toDF("src", "dst", "relationship")

    // Create a GraphFrame
    val g = GraphFrame(v, e)
    val result = GraphAnalytics.detectCommunities(g)

  }
}
