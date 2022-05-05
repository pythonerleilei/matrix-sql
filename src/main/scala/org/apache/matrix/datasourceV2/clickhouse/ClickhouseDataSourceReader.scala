package org.apache.matrix.datasourceV2.clickhouse

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.reader.{DataSourceReader, InputPartition}
import org.apache.spark.sql.types.StructType
import collection.JavaConversions._

import java.util.List

class ClickhouseDataSourceReader(options: ClickhouseDataSourceOptions) extends DataSourceReader{

  private val schema = {
    val manager = new ClickhouseManager(options)
    manager.getSparkTableSchema()
  }

  override def readSchema(): StructType = schema

  override def planInputPartitions(): List[InputPartition[InternalRow]] = {
    val urls = options.getURLs
    if(urls.isEmpty){
      throw new RuntimeException("url is empty")
    }
    urls.map(url => new ClickhouseInputPartition(schema, options, url))
  }
}
