package org.apache.matrix.datasourceV2.clickhouse

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.unsafe.types.UTF8String
import ru.yandex.clickhouse.{ClickHouseConnection, ClickHouseStatement, ClickhouseJdbcUrlParser}

import java.sql.{ResultSet, SQLException}

class ClickhouseInputPartitionReader(schema: StructType,
                                     options: ClickhouseDataSourceOptions,
                                     url: String,
                                     pushedFilter: Array[Filter])
  extends Logging with InputPartitionReader[InternalRow]{

  val manager = new ClickhouseManager(options)
  var connection: ClickHouseConnection = null
  var st: ClickHouseStatement = null
  var rs: ResultSet = null

  override def next(): Boolean = {
    if (null == connection || connection.isClosed && null == st || st.isClosed && null == rs || rs.isClosed){
      connection = manager.getConnection(ClickhouseJdbcUrlParser.JDBC_CLICKHOUSE_PREFIX + "//" +url)
      st = connection.createStatement()
      val sql = manager.getSelectStatement(schema, pushedFilter)
      println("query sql: " + sql)
      rs = st.executeQuery(sql)
    }
    if(null != rs && !rs.isClosed) rs.next() else false
  }

  override def get(): InternalRow = {
    val fields = schema.fields
    val length = fields.length
    val record = new Array[Any](length)
    for (i <- 0 until length) {
      val field = fields(i)
      val name = field.name
      val dataType = field.dataType
      try {
        dataType match {
          case DataTypes.BooleanType => record(i) = rs.getBoolean(name)
          case DataTypes.DateType => record(i) = DateTimeUtils.fromJavaDate(rs.getDate(name))
          case DataTypes.DoubleType => record(i) = rs.getDouble(name)
          case DataTypes.FloatType => record(i) = rs.getFloat(name)
          case DataTypes.IntegerType => record(i) = rs.getInt(name)
          case DataTypes.LongType => record(i) = rs.getLong(name)
          case DataTypes.ShortType => record(i) = rs.getShort(name)
          case DataTypes.StringType => record(i) = UTF8String.fromString(rs.getString(name))
          case DataTypes.TimestampType => record(i) = DateTimeUtils.fromJavaTimestamp(rs.getTimestamp(name))
          case DataTypes.BinaryType => record(i) = rs.getBytes(name)
          case DataTypes.NullType => record(i) = ""
        }
      } catch {
        case e: SQLException => logError(e.getStackTrace.mkString("", scala.util.Properties.lineSeparator, scala.util.Properties.lineSeparator))
      }
    }
    new GenericInternalRow(record)
  }

  override def close(): Unit = {
    manager.closeAll(connection, st, null, rs)
  }
}
