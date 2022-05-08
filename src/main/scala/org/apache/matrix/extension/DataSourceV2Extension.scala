package org.apache.matrix.extension

import org.apache.matrix.analyze.ResolveSQLOnDataSourceV2
import org.apache.matrix.plan.PushDownAggregation
import org.apache.spark.sql.SparkSessionExtensions

class DataSourceV2Extension extends (SparkSessionExtensions => Unit){
  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectResolutionRule(sparkSession => new ResolveSQLOnDataSourceV2(sparkSession))
    extensions.injectPlannerStrategy(sparkSession => PushDownAggregation(sparkSession))
  }
}
