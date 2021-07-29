package ml.sparkling.graph.operators.measures.utils

import ml.sparkling.graph.api.operators.algorithms.shortestpaths.ShortestPathsTypes._

import scala.collection.JavaConversions._
import scala.collection.mutable
/**
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 */
object CollectionsUtils {
   def intersectSize(neighbours1:JSet[JLong],neighbours2:JSet[JLong]): Int ={
    intersect(neighbours1,neighbours2).size
  }

  def intersect(neighbours1:JSet[JLong],neighbours2:JSet[JLong]): mutable.Set[JLong] ={
    neighbours1.intersect(neighbours2)
  }
}
