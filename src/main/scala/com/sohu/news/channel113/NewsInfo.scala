package com.sohu.news.channel113

import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.util.Bytes

class NewsInfo {
  var newsId: String = null
  var status: Int = 0
  var newsType: Int = -1
  var createTime: Long = 0
  var clkRate: Double = 0.0
  var pv: Double = 0.0
  var channel: String = null

  def this(result: Result) {
    this()
    try
      this.newsId = Bytes.toString(result.getRow)
    catch {
      case e: Exception =>
        this.newsId = null
    }
    this.status = try {
      Bytes.toString(result.getValue(Bytes.toBytes("discovery"), Bytes.toBytes("recStatus"))).toInt
    } catch {
      case e: Exception =>
        0
    }
    try {
      this.channel = Bytes.toString(result.getValue(Bytes.toBytes("meta"), Bytes.toBytes("meteor_news_channels"))) //频道
    } catch {
      case e: Exception =>
        this.channel = ""
    }

    if (this.channel == null) {
      this.channel = ""
    }

    try
      this.newsType = Bytes.toString(result.getValue(Bytes.toBytes("meta"), Bytes.toBytes("type"))).toInt
    catch {
      case e: Exception =>
        this.newsType = -1
    }
    try
      this.createTime = Bytes.toString(result.getValue(Bytes.toBytes("meta"), Bytes.toBytes("createTime"))).toLong * 1000
    catch {
      case e: Exception =>
        this.createTime = 0
    }
    try
      this.clkRate = Bytes.toString(result.getValue(Bytes.toBytes("meta"), Bytes.toBytes("clkrate"))).toDouble
    catch {
      case e: Exception =>
        this.clkRate = 0.0
    }

    try
      this.pv = Bytes.toString(result.getValue(Bytes.toBytes("meta"), Bytes.toBytes("pv"))).toDouble
    catch {
      case e: Exception =>
        this.pv = 0.0
    }
  }

}
