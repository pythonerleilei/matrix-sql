package org.apache.matrix.datasourceV2.clickhouse

import org.apache.matrix.util.StringUtils

class ClickhouseDataSourceOptions(var originalMap: java.util.Map[String, String]) extends Serializable{

  val URL_KEY: String = "url"
  val USER_KEY: String = "user"
  val PASSWORD_KEY: String = "password"
  val DATABASE_KEY: String = "database"
  val TABLE_KEY: String = "table"
  val CUSTOM_SCHEMA_KEY: String = "customSchema".toLowerCase

  def getValue[T](key: String): T = (if (originalMap.containsKey(key)) originalMap.get(key) else null).asInstanceOf[T]

  def getURLs: Seq[String] = getValue[String](URL_KEY).split(",").distinct.toSeq

  def getUser: String = getValue(USER_KEY)

  def getPassword: String = getValue(PASSWORD_KEY)

  def getDatabase: String = getValue(DATABASE_KEY)

  def getTable: String = getValue(TABLE_KEY)

  def getCustomSchema: String = getValue(CUSTOM_SCHEMA_KEY)

  def getFullTable: String = {
    val database = getDatabase
    val table = getTable
    if (StringUtils.isEmpty(database) && !StringUtils.isEmpty(table)) table else if (!StringUtils.isEmpty(database) && !StringUtils.isEmpty(table)) database+"."+table else table
  }

  def asMap(): java.util.Map[String, String] = this.originalMap

  override def toString: String = originalMap.toString
}
