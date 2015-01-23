import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

/*
 * Copyright 2015 Pedro Rodriguez. 
 * This file is subject to terms and conditions defined in LICENSE
 */
object App {
  def main(args:Array[String]): Unit = {
    var i = 0
    val arrayLines = Array(
      "human interface computer",
      "survey user computer system response time",
      "eps user interface system",
      "system human system eps",
      "user response time",
      "trees",
      "graph trees",
      "graph minors trees",
      "graph minors survey"
    )
    val masters = Array("local", "local[2]", "local[3]", "local[4]")
    while (i < 4) {
      println(s"Testing: ${masters(i)}")
      val sc = new SparkContext(masters(i), "GraphX Bug")
      val lines = sc.textFile("src/main/resources/data.txt") //THIS FAILS AT local[3]
      //val lines = sc.parallelize(arrayLines) ERASE ABOVE WITH THIS AND IT WORKS
      val docIds = sc.parallelize(0L until lines.count())
      docIds.partitions // THIS IS FINE
      lines.partitions // THIS IS FINE
      val docsWithIds = lines.zip(docIds)
      docsWithIds.partitions // FAILURE OCCURS HERE

      // Old code trying to force a failure, leaving here for now
      //val tokens = docsWithIds.flatMap({ case (doc, id) =>
      //  doc.split(" ").map(w => (id, -math.abs(1 + w.hashCode).toLong, 0))
      //})
      //val f1: Edge[Int] => VertexId = _.srcId
      //val f2: Edge[Int] => VertexId = _.dstId
      //val edges = tokens.mapPartitionsWithIndex({ case (pid, iter) =>
      //  iter.map({case (did, eid, t) =>
      //    Edge(did, eid, t)
      //  })
      //})
      //edges.partitions.length //FAILURE HAPPENS HERE
      //edges.groupBy(f1).count() //AND ALSO HERE FAILURE HAPPENS HERE
      //edges.groupBy(f2).count()
      //edges.groupBy(_.attr).count()
      //val graph = Graph.fromEdgeTuples(edges, new Array[Long](100)).partitionBy(PartitionStrategy.EdgePartition1D)
      //graph.vertices.count()
      //graph.edges.groupBy(e => e.dstId).count()
      //graph.edges.groupBy(e => e.srcId).count()
      //graph.edges.count()
      sc.stop()
      i += 1
    }
  }
}
