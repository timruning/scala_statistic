package com.sohu.news.newsNum

import com.sohu.news.util.hbase.{HBaseClientNew, HBaseUtilNew}
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants}
import org.apache.log4j.Logger
import org.apache.spark.SparkContext

object RddConsole {

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
      .map(x => {
        val currentTime = System.currentTimeMillis()
        val click = x.clkRate
        val pv = x.pv
        val ctr = (1.0 + x.clkRate) / (20.0 + x.pv)
        var key = ""
        if (x.status == 0) {
          println("#### line 33\t" + x.newsId)
        }
        key += ("status==" + x.status)
        if (currentTime - x.createTime <= 7 * 24 * 60 * 60 * 1000l) {
          key += "---day<=7"
        } else if (currentTime - x.createTime <= 8 * 24 * 60 * 60 * 1000l) {
          key += ("---<7day<=8\t" + (currentTime - x.createTime) / (20 * 60 * 1000l))
        } else {
          key += "---day>7"
        }
        if (click >= 40) {
          key += "---click>=40"
        } else {
          key += "---click<40"
        }
        if (ctr >= 0.16) {
          key += "---ctr>=0.16"
        } else {
          key += "---ctr<0.16"
        }
        (key, 1)
      })
      .reduceByKey(_ + _)
      .foreach(x => {
        println("#### line 56 RddConsole$\t" + x._1 + "\t" + x._2)
      })
  }
}
