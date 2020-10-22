package ru.aermakov

import org.apache.spark.graphx.PartitionStrategy.RandomVertexCut
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{EdgeTriplet, Graph}
import org.apache.spark.rdd.RDD

/**
 * Telecom user circles demo
 * Input: args[0] - file with user calls: src, dst
 * args[1] - depth
 * Output: stdout
 */
object TelecomCircleDemoApp {

  private val MAX_DEPTH = 3;  //max depth of user's circle
  private val MAX_CALLS_COUNT = 10;  //max count of user calls

  private lazy val CONF = new SparkConf()
    .setAppName("Telecom Circle Demo App Maven")
    .setMaster("local[2]")
  lazy val SC = new SparkContext(CONF)

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println("Args: <file path> <search depth>")
      System.exit(-1)
    }
    val path = args.head
    val depth = args(1).toInt
    if (depth > MAX_DEPTH) {
      println(f"Depth $depth exceeded max $MAX_DEPTH")
      System.exit(-1)
    }
    val data = readData(path)
    val circles = findCircles(data, depth)
    circles.saveAsTextFile("result.csv")
  }

  /**
   * Read data from CSV file
   * @param path path to file
   * @return RDD of tuples
   */
  def readData(path: String): RDD[(Long, Long)] = {
    SC.textFile(path)
      .map(x => x.split(","))
      .map { case Array(id1, id2) => (id1.trim.toLong, id2.trim.toLong) }
  }

  /**
   * Find user circles by calls
   * @param calls user calls table
   * @param depth search depth
   * @return user circles table
   */
  def findCircles(calls: RDD[(Long, Long)], depth: Int): RDD[(Long, Set[Long])] = {
    var graph = Graph.fromEdgeTuples(calls, Set[Long](), Some(RandomVertexCut))
      .filter(
        identity,
        (g: EdgeTriplet[Set[Long], Int]) => g.attr <= MAX_CALLS_COUNT
      )
    for (_ <- 1 to depth) {
      val msgs = graph.aggregateMessages[Set[Long]](
        ctx => ctx.sendToSrc(ctx.dstAttr + ctx.dstId),
        _ ++ _
      )
      graph = graph.joinVertices(msgs) { case (_, a, b) => a ++ b }
    }
    graph.vertices.filter(_._2.nonEmpty)
  }

}
