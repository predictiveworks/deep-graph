package de.kp.works.graph.storage.grakn.reader

import de.kp.works.graph.storage.grakn.GraknOptions
import org.apache.spark.Partition
import org.apache.spark.sql.types.StructType

class GraknVertexIterator(split:Partition, graknOptions:GraknOptions, schema:StructType)
  extends AbstractGraknIterator(split, graknOptions, schema) {

}
