package ru.aermakov

import org.apache.spark.rdd.RDD
import org.scalatest.funsuite.AnyFunSuite

class TelecomCircleDemoAppTest extends AnyFunSuite {

  private val app = TelecomCircleDemoApp

  val calls: RDD[(Long, Long)] = app.SC.parallelize(Seq(
    (1L, 2L), (1L, 3L), (2L, 5L), (2L, 6L), (3L, 7L),
  ))

  test("find circle with depth 1") {
    val circles = app.findCircles(calls, 1).collectAsMap()
    assertResult(Set(1L, 2L, 3L))(circles.keySet)
    assertResult(Set(2L, 3L))(circles(1L))
    assertResult(Set(5L, 6L))(circles(2L))
    assertResult(Set(7L))(circles(3L))
  }

  test("find circle with depth 2") {
    val circles = app.findCircles(calls, 2).collectAsMap()
    assertResult(Set(1L, 2L, 3L))(circles.keySet)
    assertResult(Set(2L, 3L, 5L, 6L, 7L))(circles(1L))
    assertResult(Set(5L, 6L))(circles(2L))
    assertResult(Set(7L))(circles(3L))
  }

}
