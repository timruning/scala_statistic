package com.sohu.util.kafka

/**
  * Created by T5 on 2016/8/18.
  */
import com.sohu.util.spark.SparkUtil
import kafka.serializer.StringDecoder
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, StringDeserializer}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by ningli210478 on 2016/8/14.
  * spark Streaming 读取kafka中的数据
  */
object SparkDemo {
  def main(args: Array[String]) :Unit={
    val group = "Test-SparkDemo-kafka"
    val topics = "countinfo"
    val zkQuorum="10.16.10.76:2181,10.16.10.94:2181,10.16.10.99:2181,10.16.34.187:2181,10.16.34.166:2181,10.16.34.159:2181,10.16.34.58:2181/kafka-0.8.1"
    val brokerList ="10.16.10.196:8092,10.16.10.197:8092,10.16.10.198:8092,10.16.10.199:8092,10.16.10.200:8092,10.16.10.163:8092,10.16.10.164:8092,10.16.10.165:8092,10.16.10.172:8092,10.16.43.148:8092,10.16.43.149:8092,10.16.43.150:8092,10.16.43.151:8092,10.16.43.152:8092,10.16.43.156:8092,10.16.43.153:8092,10.16.43.154:8092,10.16.43.155:8092"
    // val Array(group, topics, zkQuorum,brokerList) = args

    val sparkConf = new SparkConf().setAppName("Test-SparkDemo-kafka")
    //配置spark
    SparkUtil.setSparkConfig(sparkConf)
    val ssc = new StreamingContext(sparkConf,Seconds(1))
    //ssc.checkpoint("checkpoint")
    //kafka参数
    val groupId ="abc-test"
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> brokerList,
      "group.id" -> groupId,
      "auto.offset.reset" -> "earliest", //latest, earliest, none
      "enable.auto.commit" -> "false",
      "client.id" -> groupId,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[ByteArrayDeserializer]
    )

    val topicSet = topics.split(",").toSet
    val directKafka = KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](topicSet, kafkaParams))
    //    lines.saveAsTextFiles("abcTesto ")

    val lines = directKafka.map(_.value()).filter(x=> x!=null && !x.isEmpty)
    //processing RDD[String]
    lines.foreachRDD(rdd =>{
      if(null!=rdd && !rdd.isEmpty()) {
        //处理each RDD
        processRDD(rdd)
      }
    } )

    ssc.start()
    ssc.awaitTermination()
  }
  //输出方式，print/redis/hbase/kafka
  val outputLocation ="print"
  /**
    * 处理每个rdd
    * @param rdd
    */
  def processRDD(rdd:RDD[String]): Unit ={
    try {
      rdd.filter(x => x != null).foreachPartition(partitionOfRecords => {
        if (null != partitionOfRecords && (!partitionOfRecords.isEmpty)) {
          process(partitionOfRecords)
        }
      })
    }catch {
      case e:Exception=>println("error:valid")
    }
  }
  //处理每个partition
  def process(partition:Iterator[String]): Unit ={
    outputLocation match {
      case "print"=>printEachLine(partition) //直接打印每条记录
//      case "redis"=>RedisDemo.saveRedis(partition)// 存入redis
//      case "hbase"=>HBaseDemo.saveHBase(partition)//存入hbase
//      case "kafka"=>KafkaDemo.saveKafka(partition,"default")//存入kafka,需要指定topic
      case _=> println("invalid")
    }

  }
  //打印每条记录
  def printEachLine(partition:Iterator[String]): Unit ={
    partition.foreach(pair=>{
      //打印每一条记录
      println(pair)
    })
  }
}