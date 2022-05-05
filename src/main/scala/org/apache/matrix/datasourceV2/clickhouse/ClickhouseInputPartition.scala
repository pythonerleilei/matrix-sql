package org.apache.matrix.datasourceV2.clickhouse

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.reader.{InputPartition, InputPartitionReader}
import org.apache.spark.sql.types.StructType

class ClickhouseInputPartition(schema: StructType, options: ClickhouseDataSourceOptions, url: String) extends InputPartition[InternalRow]{
  override def createPartitionReader(): InputPartitionReader[InternalRow] = new ClickhouseInputPartitionReader(schema, options, url)
}
