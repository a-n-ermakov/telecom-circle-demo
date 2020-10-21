package ru.aermakov

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{Edge, Graph}
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
    var depth = args(1).toInt
    if (depth > MAX_DEPTH) {
      println(f"Depth $depth exceeded max $MAX_DEPTH")
      System.exit(-1)
    }
    val data = SC.textFile(path)
      .map(x => x.split(","))
      .map({ case Array(id1, id2) => (id1.toLong, id2.toLong) })
    val circles = findCircles(data, depth)
    circles.collect().foreach(println)
  }

  /**
   * Find user circles by calls
   *
   * @param calls user calls table
   * @param depth search depth
   * @return user circles table
   */
  def findCircles(calls: RDD[(Long, Long)], depth: Int): RDD[(Long, Set[Long])] = {
    val vertices = calls.map(t => (t._1, Set[Long]()))
      .union(calls.map(t => (t._2, Set[Long]())))
      .distinct()
    val edges = calls.map({ case (k, v) => Edge(k, v, None) })
    val graphFull = Graph(vertices, edges)
    var graph = graphFull.filter(
      graph => {
        val degrees = graph.outDegrees
        graph.outerJoinVertices(degrees) {(vid, data, deg) => deg.getOrElse(0)}
      },
      vpred = (vid: Long, deg:Int) => deg <= MAX_CALLS_COUNT
    )

    /**
     * Merge two sets
     *
     * @param a set
     * @param b set
     * @return union
     */
    def unionSets(a: Set[Long], b: Set[Long]): Set[Long] = {
      println("++++++++++++++++", a, b)
      a ++ b
    }

    for (_ <- 1 to depth) {
      val msgs = graph.aggregateMessages[Set[Long]](
        ctx => ctx.sendToSrc(ctx.dstAttr + ctx.dstId),
        unionSets
      )
      graph = Graph(
        graph.vertices.leftJoin(msgs) {
          case (vid, a, None) => a
          case (vid, a, Some(b)) => a ++ b
        },
        graph.edges
      )
    }
    graph.vertices.filter(_._2.nonEmpty)
  }

}
