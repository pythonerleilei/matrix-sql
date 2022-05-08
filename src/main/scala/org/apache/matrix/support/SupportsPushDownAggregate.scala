package org.apache.matrix.support

import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.sources.v2.reader.DataSourceReader

abstract class SupportsPushDownAggregate extends DataSourceReader{

  def pushDownAggregate(groupingAttributes: Seq[AttributeReference],
              aggregateFunctions: Array[AggregateFunc],
              filters: Array[Filter])

  def unhandledFilters(filters: Array[Filter]): Array[Filter] = filters

}
