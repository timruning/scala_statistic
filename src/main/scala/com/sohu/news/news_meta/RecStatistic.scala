package com.sohu.news.news_meta

import java.util

import com.sohu.news.util.hbase.HBaseUtilNew
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants}
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer


object RecStatistic {

  class NewsInfo {
    var newsId: String = null
    var newsSite: Int = -1
    var clkRate: Double = 0.0
    var pv: Double = 0.0
    var readCountSmoothing = 0.0
    var readCount: Long = 0L
    var newsType: Int = -1
    var recStatus: Int = 0
    //  private var createTime: Long = 0L
    //  private var currentTime: Long = 0l
    //  var score = 0.0

    def this(result: Result) {
      this()
      this.newsId = Bytes.toString(result.getRow)
      try {
        this.recStatus = Bytes.toString(result.getValue(Bytes.toBytes("discovery"), Bytes.toBytes("recStatus"))).toInt
      } catch {
        case e: Exception =>
          this.recStatus = 0
      }
      try
        this.newsSite = Bytes.toString(result.getValue(Bytes.toBytes("meta"), Bytes.toBytes("site"))).toInt
      catch {
        case e: Exception =>
          this.newsSite = -1
      }
      try
        this.pv = Bytes.toString(result.getValue(Bytes.toBytes("meta"), Bytes.toBytes("pv"))).toDouble
      catch {
        case e: Exception =>
          this.pv = 0.0
      }
      try
        this.clkRate = Bytes.toString(result.getValue(Bytes.toBytes("meta"), Bytes.toBytes("clkrate"))).toDouble
      catch {
        case e: Exception =>
          this.clkRate = 0.0
      }
      try
        this.readCount = Bytes.toString(result.getValue(Bytes.toBytes("meta"), Bytes.toBytes("read_count"))).toLong
      catch {
        case e: Exception =>
          this.readCount = 0
      }
      try
        this.newsType = Bytes.toString(result.getValue(Bytes.toBytes("meta"), Bytes.toBytes("type"))).toInt
      catch {
        case e: Exception =>
          this.newsType = -1
      }
      //    try
      //      this.createTime = Bytes.toString(result.getValue(Bytes.toBytes("meta"), Bytes.toBytes("createTime"))).toLong * 1000
      //    catch {
      //      case e: Exception =>
      //        this.createTime = 0
      //    }
      //    this.currentTime = System.currentTimeMillis

      this.readCountSmoothing = (1.0 + this.clkRate) / (20.0 + this.pv)

    }
  }

  class SiteInfo {
    var newsNum: Long = 1
    var pvList: util.List[Int] = new util.ArrayList[Int]()
    val clkList: util.List[Int] = new util.ArrayList[Int]()
    val readCountSmoothingList: util.List[Int] = new util.ArrayList[Int]()
    val readCountList: util.List[Int] = new util.ArrayList[Int]()

    for (i <- 0 until 4) {
      pvList.add(0)
      clkList.add(0)
      readCountSmoothingList.add(0)
      readCountList.add(0)
    }

    override def toString: String = {
      val result = new StringBuffer()
      result.append(newsNum + "\t")
      result.append("clkRate" + "\t")
      for (i <- 0 until clkList.size()) {
        result.append(clkList.get(i) + "\t")
      }
      result.append("pv" + "\t")
      for (i <- 0 until pvList.size()) {
        result.append(pvList.get(i) + "\t")
      }
      result.append("rct" + "\t")
      for (i <- 0 until readCountSmoothingList.size()) {
        result.append(readCountSmoothingList.get(i) + "\t")
      }
      result.append("rc" + "\t")
      for (i <- 0 until readCountList.size()) {
        result.append(readCountList.get(i) + "\t")
      }
      return result.toString().trim
    }
  }

  def addSiteInfo(x: SiteInfo, y: SiteInfo): SiteInfo = {
    val result = new SiteInfo
    result.newsNum = x.newsNum + y.newsNum

    for (i <- 0 until 4) {
      val pv = x.pvList.get(i) + y.pvList.get(i)
      result.pvList.set(i, pv)
      val clkRate = x.clkList.get(i) + y.clkList.get(i)
      result.clkList.set(i, clkRate)
      val rct = x.readCountSmoothingList.get(i) + y.readCountSmoothingList.get(i)
      result.readCountSmoothingList.set(i, rct)
      val rc = x.readCountList.get(i) + y.readCountList.get(i)
      result.readCountList.set(i, rc)
    }
    return result
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
    val sparkConf = new SparkConf()
    setSparkConfig(sparkConf)
    val sc = new SparkContext(sparkConf)
    val cf = HBaseConfiguration.create()

    cf.set(TableInputFormat.INPUT_TABLE, "BD_REC:NewsMeta_N")
    cf.setLong(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, 120000)
    HBaseUtilNew.setHBaseConfig(cf)

    val rdd = sc.newAPIHadoopRDD(cf, classOf[TableInputFormat]
      , classOf[ImmutableBytesWritable], classOf[Result]).map(x => x._2)
      .map(x => new NewsInfo(x))
      .filter(x => x.recStatus == 3)
      .mapPartitions(partition => {
        val result = new ArrayBuffer[(Int, SiteInfo)]()
        while (partition.hasNext) {
          val newsInfo = partition.next()
          val site = newsInfo.newsSite
          val siteInfo = new SiteInfo

          val clkRate = newsInfo.clkRate
          val pv = newsInfo.pv
          val rct = newsInfo.readCountSmoothing
          val rc = newsInfo.readCount

          if (clkRate > 0 && clkRate < 50) {
            val x = siteInfo.clkList.get(0)
            siteInfo.clkList.set(0, x + 1)
          } else if (clkRate >= 50 && clkRate < 500) {
            val x = siteInfo.clkList.get(1)
            siteInfo.clkList.set(1, x + 1)
          } else if (clkRate >= 500 && clkRate < 1000) {
            val x = siteInfo.clkList.get(2)
            siteInfo.clkList.set(2, x + 1)
          } else if (clkRate >= 1000) {
            val x = siteInfo.clkList.get(3)
            siteInfo.clkList.set(3, x + 1)
          }

          if (pv > 0 && pv < 50) {
            val x = siteInfo.pvList.get(0)
            siteInfo.pvList.set(0, x + 1)
          } else if (pv >= 50 && pv < 500) {
            val x = siteInfo.pvList.get(1)
            siteInfo.pvList.set(1, x + 1)
          } else if (pv >= 500 && pv < 1000) {
            val x = siteInfo.pvList.get(2)
            siteInfo.pvList.set(2, x + 1)
          } else if (pv >= 1000) {
            val x = siteInfo.pvList.get(3)
            siteInfo.pvList.set(3, x + 1)
          }
          if (rc > 0 && rc < 50) {
            val x = siteInfo.readCountList.get(0)
            siteInfo.readCountList.set(0, x + 1)
          } else if (rc >= 50 && rc < 500) {
            val x = siteInfo.readCountList.get(1)
            siteInfo.readCountList.set(1, x + 1)
          } else if (rc >= 500 && rc < 1000) {
            val x = siteInfo.readCountList.get(2)
            siteInfo.readCountList.set(2, x + 1)
          } else if (rc >= 1000) {
            val x = siteInfo.readCountList.get(3)
            siteInfo.readCountList.set(3, x + 1)
          }
          if (rct > 0 && rct < 0.1) {
            val x = siteInfo.readCountSmoothingList.get(0)
            siteInfo.readCountSmoothingList.set(0, x + 1)
          } else if (rct >= 0.1 && rct < 0.2) {
            val x = siteInfo.readCountSmoothingList.get(1)
            siteInfo.readCountSmoothingList.set(1, x + 1)
          } else if (rct >= 0.2 && rct < 0.5) {
            val x = siteInfo.readCountSmoothingList.get(2)
            siteInfo.readCountSmoothingList.set(2, x + 1)
          } else if (rct >= 0.5) {
            val x = siteInfo.readCountSmoothingList.get(3)
            siteInfo.readCountSmoothingList.set(3, x + 1)
          }
          result += ((site, siteInfo))
        }
        result.iterator
      })
      .reduceByKey(addSiteInfo)
      .map(x => x._1 + "\t" + x._2.toString)
      .saveAsTextFile(output)
  }
}
