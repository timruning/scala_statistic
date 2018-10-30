package com.sohu.news

import java.text.SimpleDateFormat
import java.util.Date

import com.sohu.avro.dao.Event
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by timruning on 17-10-23.
  */
package object Common extends Serializable {
  val dateSet = Array(
    "2017-10-21",
    "2017-10-20"
    ,
    "2017-10-19"
    ,
    "2017-10-18"
    ,
    "2017-10-17"
    ,
    "2017-10-16"
    ,
    "2017-10-15"
    ,
    "2017-10-14"
    ,
    "2017-10-13"
    ,
    "2017-10-12"
    ,
    "2017-10-11"
    ,
    "2017-10-10"
    ,
    "2017-10-09"
    ,
    "2017-10-08"
    ,
    "2017-10-07"
    ,
    "2017-10-06"
    ,
    "2017-10-05"
    ,
    "2017-10-04"
    ,
    "2017-10-03"
    ,
    "2017-10-02"
    ,
    "2017-10-01"
    ,
    "2017-09-30"
    ,
    "2017-09-29"
    ,
    "2017-09-28"
    ,
    "2017-09-27"
    ,
    "2017-09-26"
    ,
    "2017-09-25"
    ,
    "2017-09-24"
    ,
    "2017-09-23"
    ,
    "2017-09-22"
    ,
    "2017-09-21"
    ,
    "2017-09-20"
    ,
    "2017-09-19"
    ,
    "2017-09-18"
    ,
    "2017-09-17"
    ,
    "2017-09-16"
    ,
    "2017-09-15"
    ,
    "2017-09-14"
    ,
    "2017-09-13"
    ,
    "2017-09-12"
    ,
    "2017-09-11"
    ,
    "2017-09-10"
    ,
    "2017-09-09"
    ,
    "2017-09-08"
    ,
    "2017-09-07"
    ,
    "2017-09-06"
    ,
    "2017-09-05"
    ,
    "2017-09-04"
    ,
    "2017-09-03"
    ,
    "2017-09-02"
    ,
    "2017-09-01"
    ,
    "2017-08-31"
    ,
    "2017-08-30"
    ,
    "2017-08-29"
    ,
    "2017-08-28"
    ,
    "2017-08-27"
    ,
    "2017-08-26"
    ,
    "2017-08-25"
    ,
    "2017-08-24"
    ,
    "2017-08-23"
    ,
    "2017-08-22",
    "2017-08-21"
  )

  def getSparkContext(): SparkContext = {
    val sparkConf = new SparkConf().setAppName("fengsong:getChannel_uid_time") //.setMaster("local[3]")
    sparkConf.set("spark.akka.frameSize", "2047")
    sparkConf.set("spark.kryoserializer.buffer.max.mb", "2040")
    sparkConf.set("spark.files.overwrite", "true")
    sparkConf.set("spark.hadoop.validateOutputSpecs", "false")
    sparkConf.set("spark.eventLog.overwrite", "true")
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkConf.set("spark.kryo.registrator", "com.sohu.news.SF.Common.MyRegistrator")
    //    sparkConf.set("spark.driver.maxResultSize", "3g")
    sparkConf.registerKryoClasses(Array(classOf[Event]))
    val sc = new SparkContext(sparkConf)
    return sc
  }

  def dateFormat(time: Long): String = {
    var sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    var date: String = sdf.format(new Date((time)))
    date
  }

  def hourFormat(time: Long): String = {
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss")
    val date: String = sdf.format(new Date(time))
    return date
  }

  def get15min(time: String): String = {
    val format: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss")
    val date: Date = format.parse(time)
    var time_num = date.getTime
    time_num = (time_num / (15 * 60 * 1000) + 1) * (15 * 60 * 1000)
    return format.format(new Date(time_num))
  }

  def parseDateToStrap(time: String): Long = {
    val format: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss")
    val date: Date = format.parse(time)
    return date.getTime
  }
}
