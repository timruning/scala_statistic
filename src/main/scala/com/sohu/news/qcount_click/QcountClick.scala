package com.sohu.news.qcount_click

import org.apache.spark.{SparkConf, SparkContext}

object QcountClick {
  def addList(a: List[Data], b: List[Data]): List[Data] = {
    return a.++(b)
  }

  def main(args: Array[String]): Unit = {
    val input = args(0)
    val output = args(1)
    val sc = new SparkContext(new SparkConf())
    val rdd = sc.textFile(input).map(x => {
      val tmp = x.split("\t").map(y => (y.split("#")(0), y.split("#")(1))).toMap
      val uid = tmp.getOrElse("uid", "NULL")
      val recreqcount = tmp.getOrElse("recreqcount", "-1").toInt
      val logTime = try {
        tmp.getOrElse("logTime", "0").toLong
      } catch {
        case e: Exception => {
          0l
        }
      }
      val currentWindow = tmp.getOrElse("currentWindow", "true")
      val recsession = tmp.getOrElse("recsession", "NULL")
      (uid + "###" + recsession, currentWindow, new Data(recreqcount, logTime))
    }).filter(x => {
      if ("true".equals(x._2)) {
        true
      } else {
        false
      }
    }).map(x => {
      (x._1, List[Data](x._3))
    })
      .reduceByKey(addList)
      .map(x => {
        val key = x._1
        val data = x._2.sortBy(y => y.count)
        (key, data)
      })
    val finalData = rdd.map(x => {
      val uid_session = x._1
      val time = x._2.map(y => y.logTime).min
      val qcount = x._2.map(y => y.count).mkString(" ")
      uid_session + "\t" + time + "\t" + qcount
    }).saveAsTextFile(output + "/finalData")

  }
}
