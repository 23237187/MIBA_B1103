package org.apache.hadoop.hbase.mapreduce

import org.apache.hadoop.hbase.client.Scan


object MBAHBaseUtil {
  def convertScanToString(scan: Scan): String = {
    TableMapReduceUtil.convertScanToString(scan)
  }
}
