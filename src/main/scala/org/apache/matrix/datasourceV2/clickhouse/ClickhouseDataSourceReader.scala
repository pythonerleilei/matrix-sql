package org.apache.matrix.datasourceV2.clickhouse

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.EqualTo
import org.apache.spark.sql.sources.{Filter, GreaterThan, IsNotNull}
import org.apache.spark.sql.sources.v2.reader.{DataSourceReader, InputPartition, SupportsPushDownFilters, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.types.StructType

import java.util.List
import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

class ClickhouseDataSourceReader(options: ClickhouseDataSourceOptions)
  extends DataSourceReader with SupportsPushDownRequiredColumns with SupportsPushDownFilters{

  private var schema = {
    val manager = new ClickhouseManager(options)
    manager.getSparkTableSchema()
  }

  private val supportedFilters: ArrayBuffer[Filter] = ArrayBuffer()

  override def readSchema(): StructType = schema

  override def planInputPartitions(): List[InputPartition[InternalRow]] = {
    val urls = options.getURLs
    if(urls.isEmpty){
      throw new RuntimeException("url is empty")
    }
    urls.map(url => new ClickhouseInputPartition(schema, options, url, pushedFilters()))
  }

  override def pruneColumns(requiredSchema: StructType): Unit = {
    schema = requiredSchema
  }

  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    if(filters == null || filters.isEmpty){
      return filters
    }
    val unsupportedFilters: ArrayBuffer[Filter] = ArrayBuffer()
    filters.foreach{
      case f: EqualTo => supportedFilters += f
      case f: GreaterThan => supportedFilters += f
      case f: IsNotNull => supportedFilters += f
      case f:Filter => unsupportedFilters += f
    }
    unsupportedFilters.toArray
  }

  override def pushedFilters(): Array[Filter] = {
    supportedFilters.toArray
  }
}
