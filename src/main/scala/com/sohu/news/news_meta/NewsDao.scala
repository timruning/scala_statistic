package com.sohu.news.news_meta

import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.util.Bytes

class NewsDao {
  var newsId: String = null
  var mediaId: String = null

  var createTime: Long = 0L
  var crawledTime: Long = 0l
  var newsType: Int = 0
  var pv: Double = .0
  var clkRate: Double = .0

  var recStatus: Int = 0

  def this(result: Result) {
    this()
    try
      this.newsType = Bytes.toString(result.getValue(Bytes.toBytes("meta"), Bytes.toBytes("type"))).toInt
    catch {
      case e: Exception =>
        this.newsType = -1
    }
    try
      this.pv = Bytes.toString(result.getValue(Bytes.toBytes("meta"), Bytes.toBytes("pv"))).toDouble
    catch {
      case e: Exception =>
        this.pv = 0.0
    }
    try {
      this.recStatus = Bytes.toString(result.getValue(Bytes.toBytes("discovery"), Bytes.toBytes("recStatus"))).toInt
    } catch {
      case e: Exception =>
        this.recStatus = 0
    }
    try
      this.clkRate = Bytes.toString(result.getValue(Bytes.toBytes("meta"), Bytes.toBytes("clkrate"))).toDouble
    catch {
      case e: Exception =>
        this.clkRate = 0.0
    }
    try {
      this.mediaId = Bytes.toString(result.getValue(Bytes.toBytes("meta"), Bytes.toBytes("media_id")))
    } catch {
      case e: Exception =>
        this.mediaId = null
    }
    try
      this.newsId = Bytes.toString(result.getRow)
    catch {
      case e: Exception =>
        this.newsId = null
    }
    try {
      this.crawledTime = Bytes.toString(result.getValue(Bytes.toBytes("meta"), Bytes.toBytes("crawledTime"))).toLong * 1000
    } catch {
      case e: Exception =>
        this.crawledTime = 0l
    }
    try
      this.createTime = Bytes.toString(result.getValue(Bytes.toBytes("meta"), Bytes.toBytes("createTime"))).toLong * 1000
    catch {
      case e: Exception =>
        this.createTime = 0
    }
  }
}
