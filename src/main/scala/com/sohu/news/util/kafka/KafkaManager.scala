package com.sohu.util.kafka

import java.util.Properties

import kafka.javaapi.producer.Producer
import kafka.producer.{KeyedMessage, ProducerConfig}

import scala.util.parsing.json.JSONObject

/**
 * Created by ningli210478 on 2016/8/11.
 */
object KafkaManager {
  val zkQuorum="10.16.10.76:2181,10.16.10.94:2181,10.16.10.99:2181,10.16.34.187:2181,10.16.34.166:2181,10.16.34.159:2181,10.16.34.58:2181/kafka-0.8.1"
  val groupId="spark_kafka_streaming_real_time"

  val brokerList="10.16.10.196:8092,10.16.10.197:8092,10.16.10.198:8092,10.16.10.199:8092,10.16.10.200:8092,10.16.10.163:8092,10.16.10.164:8092,10.16.10.165:8092,10.16.10.172:8092,10.16.43.148:8092,10.16.43.149:8092,10.16.43.150:8092,10.16.43.151:8092,10.16.43.152:8092,10.16.43.156:8092,10.16.43.153:8092,10.16.43.154:8092,10.16.43.155:8092"
  val kafkaParam=Map[String,String](
    "zookeeper.connect" -> zkQuorum,
    "metadata.broker.list"->brokerList,
    "group.id" -> groupId,
    "auto.offset.reset" -> kafka.api.OffsetRequest.LargestTimeString ,
    "client.id" -> groupId,
    "zookeeper.connection.timeout.ms" -> "10000"
  )


  var producer :Producer[String,String]= null

  /**
   * 创建producer
   * @param props
   * @return
   */
  def createProducer(props:Properties): Producer[String,String] ={

    val kafkaConfig = new ProducerConfig(props)
    val producer = new Producer[String,String](kafkaConfig)
    producer
  }

  /**
   * 配置property
   * @param brokerList
   * @return
   */
  def setProperty(brokerList:String): Properties ={
    val props = new Properties()
    props.put("metadata.broker.list",brokerList)
    props.put("serializer.class","kafka.serializer.StringEncoder")
    props
  }

  /**
   * 生产
   * @param topic
   */
  def produce(topic:String,event:JSONObject): Unit ={
    if(null != producer){
      producer.send(new KeyedMessage[String,String](topic,event.toString()))
    }else{
      println("there is no producer!")
    }
  }

  /**
   * 配置producer
   * @param brokerList
   */
  def setProducer(brokerList:String): Unit ={
    if(null == producer) {
      producer = createProducer(setProperty(brokerList))
    }
  }
}
