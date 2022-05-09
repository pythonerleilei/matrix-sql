package org.apache.matrix.datasourceV2.clickhouse

import org.apache.matrix.support.Max
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.{EqualTo, NamedExpression}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.sources.{Filter, GreaterThan, IsNotNull}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.javatuples.Triplet
import ru.yandex.clickhouse.domain.ClickHouseDataType
import ru.yandex.clickhouse.response.{ClickHouseResultSet, ClickHouseResultSetMetaData}
import ru.yandex.clickhouse.settings.ClickHouseProperties
import ru.yandex.clickhouse.{ClickHouseConnection, ClickHouseDataSource, ClickHouseStatement, ClickhouseJdbcUrlParser}

import java.sql.{Connection, PreparedStatement, ResultSet, Statement}
import collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

class ClickhouseManager(options: ClickhouseDataSourceOptions) extends Logging with Serializable{

  def getConnection(url: String): ClickHouseConnection = {
    val ds = new ClickHouseDataSource(url, new ClickHouseProperties())
    ds.getConnection(options.getUser, options.getPassword)
  }

  def getSelectStatement(schema: StructType,
                         pushedFilter: Array[Filter],
                         groupingColumns: Array[String]):String = {
    val selected = if(schema == null || schema.isEmpty) "*" else schema.fieldNames.mkString(" ,")
    var sql = s"SELECT $selected FROM ${options.getFullTable}"
    if(pushedFilter != null && pushedFilter.nonEmpty){
      val filter = pushedFilter.map{
        case f: EqualTo => f.sql
        case GreaterThan(attr, value) => s"$attr > ${value.toString}"
        case IsNotNull(attr) => s"$attr is not null"
      }.mkString(" AND ")
      sql = sql + s" where $filter"
    }
    if(groupingColumns != null && groupingColumns.nonEmpty){
      sql = sql + s" group by ${groupingColumns.mkString(" ,")}"
    }
    sql
  }

  def getSparkTableSchema(): StructType = {
    val customSchema: String = options.getCustomSchema
    val customFields = if (customSchema != null && !customSchema.isEmpty) {
      customSchema.split(".").toSeq
    }else {
      Nil
    }
    val schemaInfo = getTableSchema(customFields)
    val fields = ArrayBuffer[StructField]()
    for(si <- schemaInfo) {
      fields += StructField(si.getValue0, getSparkSqlType(si.getValue1))
    }
    StructType(fields)
  }

  private def getTableSchema(customFields: Seq[String] = null): Seq[Triplet[String, String, String]] = {
    val fields = new java.util.LinkedList[Triplet[String, String, String]]
    var connection: ClickHouseConnection = null
    var st: ClickHouseStatement = null
    var rs: ClickHouseResultSet = null
    var metaData: ClickHouseResultSetMetaData = null
    val urls = options.getURLs
    if(urls.isEmpty){
      throw new RuntimeException("url is empty")
    }

    //first , only take one node to infer schema
    val url = ClickhouseJdbcUrlParser.JDBC_CLICKHOUSE_PREFIX + "//" +  urls.head
    try {
      connection = getConnection(url)
      st = connection.createStatement
      val sql = s"SELECT * FROM ${options.getFullTable} WHERE 1=0"
      rs = st.executeQuery(sql).asInstanceOf[ClickHouseResultSet]
      metaData = rs.getMetaData.asInstanceOf[ClickHouseResultSetMetaData]
      val columnCount = metaData.getColumnCount
      for (i <- 1 to columnCount) {
        val columnName = metaData.getColumnName(i)
        val sqlTypeName = metaData.getColumnTypeName(i)
        val javaTypeName = ClickHouseDataType.fromTypeString(sqlTypeName).getJavaClass.getSimpleName
        if (null != customFields && customFields.size > 0) {
          if(customFields.contains(columnName)) fields.add(new Triplet(columnName, sqlTypeName, javaTypeName))
        } else {
          fields.add(new Triplet(columnName, sqlTypeName, javaTypeName))
        }
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      closeAll(connection, st, null, rs)
    }
    fields
  }


  def close(connection: Connection): Unit = closeAll(connection)

  def close(st: Statement): Unit = closeAll(null, st, null, null)

  def close(ps: PreparedStatement): Unit = closeAll(null, null, ps, null)

  def close(rs: ResultSet): Unit = closeAll(null, null, null, rs)

  def closeAll(connection: Connection=null, st: Statement=null, ps: PreparedStatement=null, rs: ResultSet=null): Unit = {
    try {
      if (rs != null && !rs.isClosed) rs.close()
      if (ps != null && !ps.isClosed) ps.close()
      if (st != null && !st.isClosed) st.close()
      if (connection != null && !connection.isClosed) connection.close()
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }

  private def getSparkSqlType(clickhouseDataType: String) = clickhouseDataType match {
    case "IntervalYear" => DataTypes.IntegerType
    case "IntervalQuarter" => DataTypes.IntegerType
    case "IntervalMonth" => DataTypes.IntegerType
    case "IntervalWeek" => DataTypes.IntegerType
    case "IntervalDay" => DataTypes.IntegerType
    case "IntervalHour" => DataTypes.IntegerType
    case "IntervalMinute" => DataTypes.IntegerType
    case "IntervalSecond" => DataTypes.IntegerType
    case "UInt64" => DataTypes.LongType //DataTypes.IntegerType;
    case "UInt32" => DataTypes.LongType
    case "UInt16" => DataTypes.IntegerType
    case "UInt8" => DataTypes.IntegerType
    case "Int64" => DataTypes.LongType
    case "Int32" => DataTypes.IntegerType
    case "Int16" => DataTypes.IntegerType
    case "Int8" => DataTypes.IntegerType
    case "Date" => DataTypes.DateType
    case "DateTime" => DataTypes.TimestampType
    case "Enum8" => DataTypes.StringType
    case "Enum16" => DataTypes.StringType
    case "Float32" => DataTypes.FloatType
    case "Float64" => DataTypes.DoubleType
    case "Decimal32" => DataTypes.createDecimalType
    case "Decimal64" => DataTypes.createDecimalType
    case "Decimal128" => DataTypes.createDecimalType
    case "Decimal" => DataTypes.createDecimalType
    case "UUID" => DataTypes.StringType
    case "String" => DataTypes.StringType
    case "FixedString" => DataTypes.StringType
    case "Nothing" => DataTypes.NullType
    case "Nested" => DataTypes.StringType
    case "Tuple" => DataTypes.StringType
    case "Array" => DataTypes.StringType
    case "AggregateFunction" => DataTypes.StringType
    case "Unknown" => DataTypes.StringType
    case _ => DataTypes.NullType
  }
}
