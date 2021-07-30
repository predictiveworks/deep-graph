package ml.sparkling.graph.operators.algorithms.community.pscan

import ml.sparkling.graph.api.operators.algorithms.community.CommunityDetection.{CommunityDetectionAlgorithm, ComponentID}
import ml.sparkling.graph.operators.measures.utils.CollectionsUtils._
import ml.sparkling.graph.operators.measures.utils.NeighboursUtils
import ml.sparkling.graph.operators.measures.utils.NeighboursUtils.NeighbourSet
import org.apache.log4j.Logger
import org.apache.spark.graphx.{Graph, VertexId}

import scala.collection.mutable
import scala.reflect.ClassTag

/**
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 */
case object PSCAN extends CommunityDetectionAlgorithm{
  val defaultComponentId: ComponentID = -1
  val logger: Logger =Logger.getLogger(getClass)
  def computeConnectedComponents[VD:ClassTag,ED:ClassTag](graph:Graph[VD,ED],epsilon:Double=0.05):Graph[ComponentID,ED]={

    val neighbours: Graph[NeighbourSet, ED] = NeighboursUtils.getWithNeighbours(graph,treatAsUndirected = true)

    val edgesWithSimilarity=neighbours.mapTriplets(edge=>{
      val sizeOfIntersection=intersectSize(edge.srcAttr,edge.dstAttr)
      val denominator = Math.sqrt(edge.srcAttr.size()*edge.dstAttr.size())
      sizeOfIntersection/denominator
    })

    val cutOffGraph=edgesWithSimilarity.filter[NeighbourSet, Double](
      preprocess=g=>g,
      epred=edge=>{
        edge.attr >= epsilon
      })

    val componentsGraph=cutOffGraph.connectedComponents()

    graph.outerJoinVertices(componentsGraph.vertices)((vId,oldData,newData)=>{
      newData.getOrElse(defaultComponentId)
    })
  }

  def computeConnectedComponentsUsing[VD:ClassTag,ED:ClassTag](graph:Graph[VD,ED],requiredNumberOfComponents:Int=32, maxIterations:Int=Int.MaxValue):(Graph[ComponentID,ED],Long)={
    val neighbours: Graph[NeighbourSet, ED] = NeighboursUtils.getWithNeighbours(graph,treatAsUndirected = true)
    val edgesWithSimilarity: Graph[VertexId, Double] =neighbours.mapTriplets(edge=>{
      val sizeOfIntersection=intersectSize(edge.srcAttr,edge.dstAttr)
      val denominator = Math.sqrt(edge.srcAttr.size()*edge.dstAttr.size())
      sizeOfIntersection/denominator
    }).mapVertices((vId,_)=>vId).cache()
    neighbours.unpersist(false)
    val edgesWeightsRaw=edgesWithSimilarity.edges.map(_.attr).mapPartitions(_.toSet.iterator).treeAggregate(mutable.Set.empty[Double])(
      (agg:mutable.Set[Double],data:Double)=>{agg+=data;agg},
      (agg:mutable.Set[Double],agg2:mutable.Set[Double])=>{agg++=agg2;agg}
    ,3)
    val edgesWeights = (edgesWeightsRaw ++ (-1.0 :: 2.0 :: Nil)).toList.sorted
    var min = 0
    var max=edgesWeights.length-1
    val wholeMax=edgesWeights.length-1
    var components=edgesWithSimilarity.mapVertices((vId,_)=>vId).cache()
    var numberOfComponents=graph.numVertices
    var found=false
    logger.info(s"Will try to find optimal epsilon value from ${edgesWeights.length} edges weights")
    while(!found){
      val index=Math.floor((min+max)/2.0).toInt
      val cutOffValue= edgesWeights(index)
      logger.info(s"Evaluating PSCAN for epsilon=$cutOffValue")
      val componentsGraph=new PSCANConnectedComponents(cutOffValue).run(edgesWithSimilarity,maxIterations=maxIterations).cache()
      val currentNumberOfComponents=componentsGraph.vertices.map(_._2).countApproxDistinct()
      logger.info(s"PSCAN resulted in $currentNumberOfComponents components ($requiredNumberOfComponents required)")
      if(currentNumberOfComponents>=requiredNumberOfComponents&&(Math.abs(requiredNumberOfComponents-currentNumberOfComponents)<Math.abs(requiredNumberOfComponents-numberOfComponents)||numberOfComponents<requiredNumberOfComponents)){
        components.unpersist(false)
        components=componentsGraph
        numberOfComponents=currentNumberOfComponents
      }else{
        componentsGraph.unpersist(false)
      }
      if(min==max||index==0||index==wholeMax){
        found=true
      }else{
        if(currentNumberOfComponents<requiredNumberOfComponents){
          val oldMin=min
          min=index-1
          if(oldMin==min){
            found=true
          }
        }else if(currentNumberOfComponents>requiredNumberOfComponents){
          val oldMax=max
          max=index-1
          if(oldMax==max){
            found=true
          }
        }else{
          found=true
        }
      }
    }
    logger.info(s"Using PSCAN with  $numberOfComponents components ($requiredNumberOfComponents required)")
    edgesWithSimilarity.unpersist(false)
    val out=graph.outerJoinVertices(components.vertices)((_,_,newData)=>newData.get).cache()
    (out,numberOfComponents)
  }

  override def detectCommunities[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): Graph[ComponentID, ED] = {
    computeConnectedComponents(graph)
  }

}
