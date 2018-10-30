package com.sohu.news.channel113

import com.sohu.news.util.common.ConfigUtil
import org.apache.spark.{SparkConf, SparkContext}

object main {
  def setSparkConfig(conf: SparkConf): Unit = {
    conf.set("spark.speculation", "false")
    conf.set("spark.akka.frameSize", "2047")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer") //一些默认的类使用kryo序列化
    conf.set("spark.kryoserializer.buffer.max.mb", "2040")
    conf.set("spark.files.overwrite", "true")
    conf.set("spark.hadoop.validateOutputSpecs", "false")
    conf.set("spark.eventLog.overwrite", "true")
    conf.set("spark.streaming.kafka.maxRatePerPartition", "100") //每秒钟最大消费
  }

  def main(args: Array[String]): Unit = {
    val hbaseName = args(0)
    val output = args(1)
    println(hbaseName)
    println(output)
    val conf = new SparkConf()
    setSparkConfig(conf)
    val sc = new SparkContext(conf)
    RddConsole.console(sc, hbaseName, output)
  }
}
