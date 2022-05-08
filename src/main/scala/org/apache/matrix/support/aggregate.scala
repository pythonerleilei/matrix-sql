package org.apache.matrix.support

import org.apache.spark.sql.types.DataType

sealed abstract class AggregateFunc

/**
 * Here we now have three data types:
 *    LongType, DoubleType and DecimalType.
 *
 * And the corresponding result types must be
 *    java.lang.Long, java.lang.Double and java.math.BigDecimal.
 */
case class Sum(column: String, dataType: DataType) extends AggregateFunc

/**
 * The result type of Count MUST be java.lang.Long
 */
case class Count(column: String) extends AggregateFunc

/**
 * The result type of CountStar MUST be java.lang.Long
 */
case class CountStar() extends AggregateFunc

/**
 * The result type of Max must be same with the column type
 */
case class Max(column: String) extends AggregateFunc

/**
 * The result type of Min must be same with the column type
 */
case class Min(column: String) extends AggregateFunc
