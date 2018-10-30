package com.sohu.news.news_meta

import java.util

import com.sohu.news.common.po.WhiteListPO
import com.sohu.news.common.util.NewsRecDaoUtil
import com.sohu.news.util.hbase.HBaseUtilNew
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}

object WhiteNewsDis {
  val newsRecDao = NewsRecDaoUtil.getInstance()
  val govWhiteListMap = newsRecDao.getCacGovCnWhiteList.getWhiteListPOs
  val whiteMediaList = getWhiteListSet(govWhiteListMap)
  val greyMediaList = getGreyListSet(govWhiteListMap)

  def getWhiteListSet(govWhiteListMap: util.Map[Integer, WhiteListPO]): util.Set[String] = {
    val result = new util.HashSet[String]
    if (govWhiteListMap == null) return result
    val whiteMediaListPO = govWhiteListMap.get(1).getMedias
    if (whiteMediaListPO == null) return result
    for (i <- 0 until whiteMediaListPO.size()) {
      result.add(whiteMediaListPO.get(i).getMedia_id.toString)
    }
    result
  }

  def getGreyListSet(govWhiteListMap: util.Map[Integer, WhiteListPO]): util.Set[String] = {
    val result = new util.HashSet[String]
    if (govWhiteListMap == null) return result
    val whiteMediaListPO = govWhiteListMap.get(2).getMedias
    if (whiteMediaListPO == null) return result
    for (i <- 0 until whiteMediaListPO.size()) {
      result.add(whiteMediaListPO.get(i).getMedia_id.toString)
    }
    result
  }

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
    val output = args(0)
    val cf = HBaseConfiguration.create()
    cf.set(TableInputFormat.INPUT_TABLE, "BD_REC:NewsMeta_N")
    cf.setLong(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, 120000)
    HBaseUtilNew.setHBaseConfig(cf)

    val sparkConf = new SparkConf()
    setSparkConfig(sparkConf)

    val currentTimestamp = System.currentTimeMillis()

    Logger.getRootLogger.error("#### line 66\twhite\t" + this.whiteMediaList.toArray().mkString(","))
    Logger.getRootLogger.error("#### line 67\tgrey\t" + this.greyMediaList.toArray().mkString(","))
    val sc = new SparkContext(sparkConf)
    val newsRdd = sc.newAPIHadoopRDD(cf, classOf[TableInputFormat]
      , classOf[ImmutableBytesWritable], classOf[Result]).map(x => x._2)
      .map(x => {
        val newsInfo = new NewsDao(x)
        newsInfo
      })

    val whiteNews = newsRdd.filter(x => {
      this.whiteMediaList.contains(x.mediaId)
    })
    //    whiteNews.map(x => {
    //      val key = (x.createTime - currentTimestamp) / (24 * 60 * 60 * 1000l)
    //      (key, 1)
    //    })
    //      .reduceByKey(_ + _)
    //      .saveAsTextFile(output + "/whiteNews")
    //
    //    val greyNews = newsRdd.filter(x => {
    //      this.greyMediaList.contains(x.mediaId)
    //    })
    //    greyNews.map(x => {
    //      val key = (x.createTime - currentTimestamp) / (24 * 60 * 60 * 1000l)
    //      (key, 1)
    //    })
    //      .reduceByKey(_ + _)
    //      .saveAsTextFile(output + "/greyNews")

    whiteNews.map(x => {
      val ctr = (1.0 + x.clkRate) / (20.0 + x.pv)
      val key = (ctr / 0.02).toInt * 0.02

      val days = (currentTimestamp - x.createTime) / (24 * 60 * 60 * 1000l)

      if (x.clkRate > 0.1 && x.recStatus == 5) {
        Logger.getRootLogger.error("#### line 83\t" + x.newsId + "\t" + days + "\t" + ctr + "\t" + x.clkRate + "\t" + x.pv + "\t" + x.newsType)
      }
      if (x.recStatus != 5) {
        Logger.getRootLogger.error("#### line 86\t" + x.newsId + "\t" + days + "\t" + ctr + "\t" + x.clkRate + "\t" + x.pv + "\t" + x.newsType)
      }
      ((key, x.recStatus), 1)
    })
      .reduceByKey(_ + _)
      .map(x => x._1._1 + "\t" + x._1._2 + "\t" + x._2)
      .saveAsTextFile(output + "/ctrStatistic")


  }
}
