package com.sohu.news.util.hbase

import java.io.IOException

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HConstants
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.client.ResultScanner
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.util.Bytes
object HBaseUtil {
//
//  def main(args: Array[String]) {
//    val tableName: String = "BD_RC:cmstestShaochao1"
//    val columnFamily: String = "info"
////    HBaseUtil.create(tableName, columnFamily)
//    //
//    HBaseUtil(tableName, "row1", columnFamily, "cl1", "data")
//    HBaseUtil.get(tableName, "row1")
//    HbaseDemo.scan(tableName)
//    HbaseDemo.delete(tableName)
//  }
//
//  def put(tableName: String, row: String, columnFamily: String, column: String, data: String) {
//    val table: HTable = new HTable(getConfiguration, tableName)
//    val p1: Put = new Put(Bytes.toBytes(row))
//    p1.add(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(data))
//    table.put(p1)
//    System.out.println("put'" + row + "'," + columnFamily + ":" + column + "','" + data + "'")
//  }
  def createHBaseConfMap:collection.mutable.Map[String,String]={
    val cf=collection.mutable.Map[String,String]()
    cf.put(HConstants.HBASE_CLIENT_PAUSE, "3000")
    cf.put(HConstants.HBASE_CLIENT_RETRIES_NUMBER, "5")
    cf.put(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, "60000")

    cf.put(HConstants.ZOOKEEPER_QUORUM,"dn074021.heracles.sohuno.com,kb.heracles.sohuno.com,monitor.heracles.sohuno.com")

    cf.put("hbase.master.keytab.file","/etc/security/keytabs/hbase.service.keytab")
    cf.put("hbase.master.kerberos.principal","hbase/_HOST@HERACLES.SOHUNO.COM")
    cf.put("hbase.master.info.bindAddress","0.0.0.0")
    cf.put("hbase.master.info.port","60010")

    cf.put("hbase.regionserver.keytab.file","/etc/security/keytabs/hbase.service.keytab")
    cf.put("hbase.regionserver.kerberos.principal","hbase/_HOST@HERACLES.SOHUNO.COM")
    cf.put("hbase.regionserver.info.port","60030")

    cf.put("zookeeper.znode.parent","/hbase-secure")
    cf.put("hbase.security.authentication","kerberos")
    cf.put("hbase.security.authorization","true")
    cf.put("hbase.coprocessor.region.classes","org.apache.hadoop.hbase.security.token.TokenProvider,org.apache.hadoop.hbase.security.access.SecureBulkLoadEndpoint,org.apache.hadoop.hbase.security.access.AccessController")

    cf.put("hbase.tmp.dir","/opt/hadoop/hbase")
    cf.put("hbase.rootdir","hdfs://heracles/apps/hbase/data")
    cf.put("hbase.superuser","hbase")
    cf.put("hbase.zookeeper.property.clientPort","2181")
    cf.put("hbase.cluster.distributed","true")
    cf
  }

  def createPut(key:String,cf:String,q:String,v:String):Put={
    val put:Put=new Put(Bytes.toBytes(key))
    put.add(Bytes.toBytes(cf),Bytes.toBytes(q),Bytes.toBytes(v))
  }

  @throws[IOException]
  def create(tableName: String, columnFamily: String) {
    val admin: HBaseAdmin = new HBaseAdmin(getConfiguration)
    if (admin.tableExists(tableName)) System.out.println("table exists!")
    else {
      val tableDesc: HTableDescriptor = new HTableDescriptor(tableName)
      tableDesc.addFamily(new HColumnDescriptor(columnFamily))
      admin.createTable(tableDesc)
      System.out.println("create table success!")
    }
  }

  private def getConfiguration: Configuration = {
    val conf: Configuration = HBaseConfiguration.create
    conf.addResource(new Path("/usr/lib/hbase/conf/hbase-site.xml"))
    conf.set("dfs.socket.timeout", "180000")
    conf
  }
  def setHBaseConfig(cf: Configuration): Unit ={
    createHBaseConfMap.map{
      case(k,v)=>{
        cf.set(k,v)
      }
    }
  }
}
