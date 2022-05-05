package org.apache.matrix.analyze

import org.apache.matrix.MatrixSqlSession
import org.apache.matrix.datasourceV2.clickhouse.ClickhouseDataSourceOptions
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.DataSource
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, ReadSupport}

import scala.collection.JavaConverters._

class ResolveSQLOnDataSourceV2(sparkSession: SparkSession) extends Rule[LogicalPlan]{

  private def maybeSQLDataSourceV2(u: UnresolvedRelation) : Boolean = {
    sparkSession.sessionState.conf.getConfString("spark.runSqlDataSourceV2", "false").toBoolean &&
      u.tableIdentifier.database.isDefined
  }

  private def extractOptions(optionStr: String): Map[String, String] = {
    var optionMap: Map[String, String] = Map()
    if(MatrixSqlSession.datasourceConfigMap.containsKey(optionStr)){
      val dataSourceConfig = MatrixSqlSession.datasourceConfigMap.get(optionStr.trim).asScala.map(k => k._1 -> k._2.toString)
      optionMap ++= dataSourceConfig
    }
    optionMap
  }

  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators  {
    case u: UnresolvedRelation if maybeSQLDataSourceV2(u) =>
      try {
        val dataSourceStr = u.tableIdentifier.database.get
        val dataSourceInfo = dataSourceStr.split("\\.")
        if(dataSourceInfo.length < 3){
          throw new IllegalArgumentException("data source info is invalid");
        }
        val source = dataSourceInfo(0).trim
        val optionStr = dataSourceInfo(1).trim
        val database = dataSourceInfo(2).trim
        val table = u.tableIdentifier.table
        var optionMap: Map[String, String] = extractOptions(optionStr)
        optionMap += "database" -> database
        optionMap += "table" -> table

        val cls = DataSource.lookupDataSource(source, sparkSession.sessionState.conf)
        if (!classOf[ReadSupport].isAssignableFrom(cls)) {
          throw new IllegalArgumentException("only support data source v2");
        }
        val ds = cls.newInstance().asInstanceOf[ReadSupport]
        val v2Options = new DataSourceOptions(optionMap.asJava)
        val reader = ds.createReader(v2Options)
        val attributeReference = reader.readSchema().map(f => AttributeReference(f.name, f.dataType, f.nullable, f.metadata)())
        DataSourceV2Relation(ds, attributeReference, optionMap)
      }catch {
        case _: ClassNotFoundException => u
        case e: Exception => u.failAnalysis(e.getMessage, e)
      }
  }
}
