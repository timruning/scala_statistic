package com.sohu.news.util.hbase

import org.apache.hadoop.hbase.util.Bytes

/**
 * Created by xiaojia on 2016/3/9.
 */
object Constants {
   //val HBASE_TABLE=Bytes.toBytes("BD_WH:REAL_TIME_DATA")
   val HBASE_TABLE=Bytes.toBytes("BD_REC:spark_streaming_test")
   val SNAP_PREFIX="SNAP_"
   val HOUR_PREFIX="H_"
   val DAY_PREFIX="D_"
}
