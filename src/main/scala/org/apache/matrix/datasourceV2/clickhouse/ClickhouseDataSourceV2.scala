package org.apache.matrix.datasourceV2.clickhouse

import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.sources.v2.reader.DataSourceReader
import org.apache.spark.sql.sources.v2.{DataSourceOptions, ReadSupport}

class ClickhouseDataSourceV2 extends ReadSupport with DataSourceRegister{

  override def shortName(): String = "clickhouse"

  override def createReader(options: DataSourceOptions): DataSourceReader = {
    new ClickhouseDataSourceReader(new ClickhouseDataSourceOptions(options.asMap()))
  }
}
