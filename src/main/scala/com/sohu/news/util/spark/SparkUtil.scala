package com.sohu.util.spark

import org.apache.spark.SparkConf

/**
  * Created by T5 on 2016/8/18.
  */
object SparkUtil {
  def setSparkConfig(sparkConf: SparkConf): Unit ={
    sparkConf.set("spark.akka.frameSize", "2047")
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")//一些默认的类使用kryo序列化
    sparkConf.set("spark.kryoserializer.buffer.max.mb", "2040")
    sparkConf.set("spark.files.overwrite","true")
    sparkConf.set("spark.hadoop.validateOutputSpecs", "false")
    sparkConf.set("spark.eventLog.overwrite", "true")
    sparkConf.set("spark.speculation", "false")
  }
}

