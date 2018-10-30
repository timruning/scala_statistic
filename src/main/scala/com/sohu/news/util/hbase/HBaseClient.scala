package com.sohu.news.util.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HConstants
import org.apache.hadoop.hbase.client.{HConnection, HConnectionManager}

/**
 * Created by xiaojia on 2016/3/7.
 */
object HBaseClient extends Serializable{
  @transient private var connection:HConnection =null
  def getConnection(cf: Configuration) : HConnection ={
    if(connection == null){
      cf.set(HConstants.HBASE_CLIENT_PAUSE, "3000")
      cf.set(HConstants.HBASE_CLIENT_RETRIES_NUMBER, "5")
      cf.set(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, "60000")

      cf.set(HConstants.ZOOKEEPER_QUORUM,"dn074021.heracles.sohuno.com,kb.heracles.sohuno.com,monitor.heracles.sohuno.com")

      cf.set("hbase.master.keytab.file","/etc/security/keytabs/hbase.service.keytab")
      cf.set("hbase.master.kerberos.principal","hbase/_HOST@HERACLES.SOHUNO.COM")
      cf.set("hbase.master.info.bindAddress","0.0.0.0")
      cf.set("hbase.master.info.port","60010")

      cf.set("hbase.regionserver.keytab.file","/etc/security/keytabs/hbase.service.keytab")
      cf.set("hbase.regionserver.kerberos.principal","hbase/_HOST@HERACLES.SOHUNO.COM")
      cf.set("hbase.regionserver.info.port","60030")

      cf.set("zookeeper.znode.parent","/hbase-secure")
      cf.set("hbase.security.authentication","kerberos")
      cf.set("hbase.security.authorization","true")
      cf.set("hbase.coprocessor.region.classes","org.apache.hadoop.hbase.security.token.TokenProvider,org.apache.hadoop.hbase.security.access.SecureBulkLoadEndpoint,org.apache.hadoop.hbase.security.access.AccessController")

      cf.set("hbase.tmp.dir","/opt/hadoop/hbase")
      cf.set("hbase.rootdir","hdfs://heracles/apps/hbase/data")
      cf.set("hbase.superuser","hbase")
      cf.set("hbase.zookeeper.property.clientPort","2181")
      cf.set("hbase.cluster.distributed","true")
      connection=HConnectionManager.createConnection(cf)
      val hook=new Thread{
        override def run=connection.close()
      }
      sys.addShutdownHook(hook.run)
    }
    connection
  }
}
