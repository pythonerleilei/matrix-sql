package org.apache.matrix.datasourceV2.clickhouse

import org.apache.matrix.support.{AggregateFunc, SupportsPushDownAggregate}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, EqualTo}
import org.apache.spark.sql.sources.v2.reader.{InputPartition, SupportsPushDownFilters, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.sources.{Filter, GreaterThan, IsNotNull}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

import java.util.List
import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import org.apache.matrix.support.Max

class ClickhouseDataSourceReader(options: ClickhouseDataSourceOptions)
  extends SupportsPushDownAggregate with SupportsPushDownRequiredColumns with SupportsPushDownFilters{

  private var schema = {
    val manager = new ClickhouseManager(options)
    manager.getSparkTableSchema()
  }

  private val supportedFilters: ArrayBuffer[Filter] = ArrayBuffer()
  private var groupingColumns: Array[String] = Array()

  override def readSchema(): StructType = schema

  override def planInputPartitions(): List[InputPartition[InternalRow]] = {
    val urls = options.getURLs
    if(urls.isEmpty){
      throw new RuntimeException("url is empty")
    }
    urls.map(url => new ClickhouseInputPartition(schema, options, url, supportedFilters.toArray, groupingColumns))
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

  override def pushDownAggregate(groupingAttributes: Seq[AttributeReference],
                                 aggregateFunctions: Array[AggregateFunc],
                                 filters: Array[Filter]): Unit = {
    supportedFilters ++= filters
    var fields = groupingAttributes.map(g => StructField(g.name, g.dataType, g.nullable, g.metadata))
    fields ++= aggregateFunctions.map{
      case Max(column, dataType) => StructField(s"max($column)", dataType)
    }
    schema = StructType(fields)
    groupingColumns = groupingAttributes.map(_.name).toArray
  }

  override def unhandledFilters(filters: Array[Filter]): Array[Filter] = {
    if(filters == null || filters.isEmpty){
      return filters
    }
    val unsupportedFilters: ArrayBuffer[Filter] = ArrayBuffer()
    filters.foreach{
      case _: EqualTo =>
      case _: GreaterThan =>
      case _: IsNotNull =>
      case f:Filter => unsupportedFilters += f
    }
    unsupportedFilters.toArray
  }
}
