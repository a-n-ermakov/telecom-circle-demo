package ru.aermakov

import java.io.{FileOutputStream, PrintWriter}

import org.scalatest.Ignore
import org.scalatest.funsuite.AnyFunSuite

@Ignore
class TelecomCircleDemoAppBigTest extends AnyFunSuite {

  private val app = TelecomCircleDemoApp

  def testUsers(n: String, depth: Int = 1): Unit = {
    val pwd = System.getProperty("user.dir")
    val calls = app.readData(f"$pwd/src/test/resources/$n.csv")
    val circles = app.findCircles(calls, depth).collectAsMap()
    assert(circles != null)
    val pw = new PrintWriter(new FileOutputStream(f"$pwd/src/test/resources/$n-result.csv"))
    try {
      for ((k, v) <- circles) {
        pw.println(f"$k,$v")
      }
    } finally {
      pw.close()
    }
  }

  test("find circle with depth 1 - many users") {
    for (n <- List("10k", "1m")) {
      testUsers(n, 1)
    }
  }

}
