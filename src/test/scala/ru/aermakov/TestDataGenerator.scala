package ru.aermakov

import java.io.{FileOutputStream, PrintWriter}

import scala.util.Random

object TestDataGenerator {

  def main(args: Array[String]): Unit = {
    if (args.length != 3) {
      println("Args: <users count> <min of user calls count> <max of user calls count>")
      System.exit(-1)
    }
    val userCount = args(0).toInt
    val callCountMin = args(1).toInt
    val callCountMax = args(2).toInt
    val pwd = System.getProperty("user.dir")

    val rnd = new Random(42L)
    val pw = new PrintWriter(new FileOutputStream(f"$pwd/src/test/resources/$userCount.csv"))
    try {
      for (src <- 0 to userCount) {
        val callCount = callCountMin + (rnd.nextFloat() * (callCountMax - callCountMin)).toInt
        for (_ <- 0 to callCount) {
          val dst = rnd.nextInt(userCount).toLong
          pw.println(f"$src,$dst")
        }
      }
    } finally {
      pw.close()
    }
  }

}
