package org.apache.matrix.plan

import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.planning.PhysicalAggregation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.{SparkSession, Strategy}

case class PushDownAggregation(sparkSession: SparkSession) extends Strategy{

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case PhysicalAggregation(groupingExpressions, aggExpressions, resultExpressions, child)
      if aggExpressions.forall(expr => expr.isInstanceOf[AggregateExpression]) =>

      val aggregateExpressions = aggExpressions.map(expr => expr.asInstanceOf[AggregateExpression])

      // if we could push down aggregation into data sources
      if (sparkSession.conf.get("enable.push.down.agg", "false").toBoolean) {
        AggPushDownUtils.plan(
          groupingExpressions,
          aggregateExpressions,
          resultExpressions, child) match {
          case Some(sparkPlans) => return sparkPlans
          case None => Nil
        }
      }else {
        Nil
      }

    case _ => Nil
  }
}
