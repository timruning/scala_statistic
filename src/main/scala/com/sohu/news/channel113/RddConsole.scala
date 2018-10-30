package com.sohu.news.channel113

import java.util

import com.sohu.news.util.common.ConfigUtil
import com.sohu.news.util.hbase.{HBaseClientNew, HBaseUtilNew}
import org.apache.hadoop.hbase.client.{Delete, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants}
import org.apache.log4j.Logger
import org.apache.spark.SparkContext

object RddConsole {
  val channel113 = "113"


  def console(sc: SparkContext, hbaseName: String, output: String): Unit = {
    val hbaseTableNewstagName = hbaseName
    val cf = HBaseConfiguration.create()
    cf.set(TableInputFormat.INPUT_TABLE, hbaseTableNewstagName)
    cf.setLong(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, 120000)
    HBaseUtilNew.setHBaseConfig(cf)
    val newsTable = HBaseClientNew.getConnection(cf).getTable(hbaseTableNewstagName)

    val currentTimestamp = System.currentTimeMillis()

    val newsInfo = sc.newAPIHadoopRDD(cf, classOf[TableInputFormat]
      , classOf[ImmutableBytesWritable], classOf[Result]).map(x => x._2)
      .map(x => new NewsInfo(x))
      .filter(x => {
        Logger.getRootLogger.error("#### line 33 RddConsole$\t" + x.newsId + "\t" + x.channel)
        val channelSet = x.channel.split(",").toSet
        if (channelSet.contains(channel113))
          true
        else
          false
      })

    val rdd = newsInfo.map(x => {
      val createTime = x.createTime
      val key_time = (currentTimestamp - createTime) / (24 * 60 * 60 * 1000l)
      val newsStatus = x.status
      val newsType = x.newsType
      val ctr = (1.0 + x.clkRate) / (20.0 + x.pv)

      val ctr_dis = (ctr / 0.1).toInt
      Logger.getRootLogger.error("#### line 58 RddConsole$\t" + x.newsId + "\t" + x.channel)
      val key = newsStatus + "\t" + newsType + "\t" + key_time + "\t" + 1.0 * ctr_dis / 10
      (key, 1)
    }).reduceByKey(_ + _)
      .map(x => x._1 + "\t" + x._2)
      .saveAsTextFile(output)
  }
}
