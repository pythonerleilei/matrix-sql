package org.apache.matrix.plan

import org.apache.matrix.support.SupportsPushDownAggregate
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.CatalystTypeConverters.convertToScala
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeMap, AttributeReference, EmptyRow, Expression, Literal, NamedExpression}
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, expressions}
import org.apache.spark.sql.execution.{PlanLater, SparkPlan}
import org.apache.spark.sql.execution.aggregate.{HashAggregateExec, ObjectHashAggregateExec, SortAggregateExec}
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation, DataSourceV2ScanExec}
import org.apache.spark.sql.internal.SQLConf.USE_OBJECT_HASH_AGG
import org.apache.spark.sql.sources
import org.apache.spark.sql.sources.{BaseRelation, Filter}
import org.apache.spark.sql.types.StringType
import org.apache.spark.unsafe.types.UTF8String
import org.apache.matrix.support
import org.apache.spark.sql.catalyst.expressions.aggregate
import org.apache.spark.sql.sources.v2.DataSourceV2

object AggPushDownUtils extends Logging{

  def plan(groupingExpressions: Seq[NamedExpression],
           aggregateExpressions: Seq[AggregateExpression],
           resultExpressions: Seq[NamedExpression],
           child: LogicalPlan): Option[Seq[SparkPlan]] = {
    val (functionsWithDistinct, _) = aggregateExpressions.partition(_.isDistinct)
    if (functionsWithDistinct.map(_.aggregateFunction.children).distinct.length > 1) {
      // This is a sanity check. We should not reach here when we have multiple distinct
      // column sets. Our MultipleDistinctRewriter should take care this case.
      sys.error("You hit a query analyzer bug. Please report your query to " +
        "Spark user mailing list.")
    }

    if (aggregateExpressions.map(_.aggregateFunction).exists(!supportsPushDown(_))) {
      return None
    }

    // for now, we dont support pushing down distinct aggregation
    if (functionsWithDistinct.isEmpty) {
      val sparkPlans = planAggregateWithoutDistinct(
        groupingExpressions,
        aggregateExpressions,
        resultExpressions,
        child)
      if(sparkPlans.nonEmpty) return Some(sparkPlans)
    }
    None
  }

  private def supportsPushDown(aggregateFunction: AggregateFunction): Boolean = aggregateFunction match {
    case _: Min => true
    case _: Max => true
    case _ => false
  }

  private def planAggregateWithoutDistinct(groupingExpressions: Seq[NamedExpression],
                                    aggregateExpressions: Seq[AggregateExpression],
                                    resultExpressions: Seq[NamedExpression],
                                    child: LogicalPlan): Seq[SparkPlan] = child match {
    case PhysicalOperation(
    _, filters, relationV2 @DataSourceV2Relation(source: DataSourceV2, attributeReference, options, _, _)) =>
      val datasourceReader = relationV2.newReader()
      if(!datasourceReader.isInstanceOf[SupportsPushDownAggregate]){
        return Nil
      }
      val pushDownAggReader = datasourceReader.asInstanceOf[SupportsPushDownAggregate]
      val attributeMap = AttributeMap(attributeReference.map(o => (o, o)))
      val candidatePredicates = filters.map { _ transform {
        case a: AttributeReference => attributeMap(a) // Match original case of attributes.
      }}
      val (pushedFilterExpressions, pushedFilters, handledFilters) = selectFilters(pushDownAggReader, candidatePredicates)
      // If there are some unhandled filters, we cant perform pushed-down aggregation
      if(pushedFilters.length != handledFilters.size) return Nil

      // 1. Create an Aggregate Operator for partial aggregations.
      val nameToAttr = attributeReference.map(_.name).zip(attributeReference).toMap
      val groupingAttributes = groupingExpressions.map {ne =>
        nameToAttr(ne.name)
      }

      val partialAggregateExpressions = aggregateExpressions.map(_.copy(mode = Partial))

      val output: Seq[AttributeReference] = groupingAttributes ++
        partialAggregateExpressions.flatMap(_.aggregateFunction.inputAggBufferAttributes)

      val groupingColumns = groupingAttributes.map(_.name).toArray

      val aggregateFunctions = partialAggregateExpressions
        .map(_.aggregateFunction)
        .flatMap(translateAggregateFunc)
      if(aggregateFunctions.isEmpty) return Nil

      if (output.length != groupingColumns.length + aggregateFunctions.length) {
        return Nil
      }

      logInfo(
        s"""
           |Plan pushed-down aggregate
           | Grouping expressions: ${groupingExpressions.mkString("; ")}
           | Aggregate expressions: ${aggregateExpressions.mkString("; ")}
           | Result expressions: ${resultExpressions.mkString("; ")}
           | Output attributes: ${output.mkString("; ")}
           | Grouping columns: ${groupingColumns.mkString("; ")}
           | Translated aggregate functions: ${aggregateFunctions.mkString("; ")}
           """.stripMargin
      )
      pushDownAggReader.pushDownAggregate(groupingAttributes, aggregateFunctions.toArray, pushedFilters.toArray)

      val scan = DataSourceV2ScanExec(
        output, source, options, pushedFilterExpressions, pushDownAggReader)

      // 2. Create an Aggregate Operator for final aggregations.
      val finalAggregateExpressions = aggregateExpressions.map(_.copy(mode = Final))
      // The attributes of the final aggregation buffer, which is presented as input to the result
      // projection:
      val finalAggregateAttributes = finalAggregateExpressions.map(_.resultAttribute)

      val finalAggregate = createAggregate(
        requiredChildDistributionExpressions = Some(groupingAttributes),
        groupingExpressions = groupingAttributes,
        aggregateExpressions = finalAggregateExpressions,
        aggregateAttributes = finalAggregateAttributes,
        initialInputBufferOffset = groupingExpressions.length,
        resultExpressions = resultExpressions,
        child = scan)

      finalAggregate :: Nil
    case _ => Nil
  }

  private def selectFilters(pushDownAggReader: SupportsPushDownAggregate,
                            predicates: Seq[Expression]): (Seq[Expression], Seq[Filter], Set[Filter]) = {
    val translatedMap: Map[Expression, Filter] = predicates.flatMap { p =>
      translateFilter(p).map(f => p -> f)
    }.toMap

    val pushedFilters: Seq[Filter] = translatedMap.values.toSeq
    val pushedFilterExpressions = translatedMap.keys.toSeq

    val unhandledFilters = pushDownAggReader.unhandledFilters(translatedMap.values.toArray).toSet
    val handledFilters = pushedFilters.toSet -- unhandledFilters
    (pushedFilterExpressions, pushedFilters, handledFilters)
  }

  private def translateFilter(predicate: Expression): Option[Filter] = {
    predicate match {
      case expressions.EqualTo(a: Attribute, Literal(v, t)) =>
        Some(sources.EqualTo(a.name, convertToScala(v, t)))
      case expressions.EqualTo(Literal(v, t), a: Attribute) =>
        Some(sources.EqualTo(a.name, convertToScala(v, t)))

      case expressions.EqualNullSafe(a: Attribute, Literal(v, t)) =>
        Some(sources.EqualNullSafe(a.name, convertToScala(v, t)))
      case expressions.EqualNullSafe(Literal(v, t), a: Attribute) =>
        Some(sources.EqualNullSafe(a.name, convertToScala(v, t)))

      case expressions.GreaterThan(a: Attribute, Literal(v, t)) =>
        Some(sources.GreaterThan(a.name, convertToScala(v, t)))
      case expressions.GreaterThan(Literal(v, t), a: Attribute) =>
        Some(sources.LessThan(a.name, convertToScala(v, t)))

      case expressions.LessThan(a: Attribute, Literal(v, t)) =>
        Some(sources.LessThan(a.name, convertToScala(v, t)))
      case expressions.LessThan(Literal(v, t), a: Attribute) =>
        Some(sources.GreaterThan(a.name, convertToScala(v, t)))

      case expressions.GreaterThanOrEqual(a: Attribute, Literal(v, t)) =>
        Some(sources.GreaterThanOrEqual(a.name, convertToScala(v, t)))
      case expressions.GreaterThanOrEqual(Literal(v, t), a: Attribute) =>
        Some(sources.LessThanOrEqual(a.name, convertToScala(v, t)))

      case expressions.LessThanOrEqual(a: Attribute, Literal(v, t)) =>
        Some(sources.LessThanOrEqual(a.name, convertToScala(v, t)))
      case expressions.LessThanOrEqual(Literal(v, t), a: Attribute) =>
        Some(sources.GreaterThanOrEqual(a.name, convertToScala(v, t)))

      case expressions.InSet(a: Attribute, set) =>
        val toScala = CatalystTypeConverters.createToScalaConverter(a.dataType)
        Some(sources.In(a.name, set.toArray.map(toScala)))

      // Because we only convert In to InSet in Optimizer when there are more than certain
      // items. So it is possible we still get an In expression here that needs to be pushed
      // down.
      case expressions.In(a: Attribute, list) if !list.exists(!_.isInstanceOf[Literal]) =>
        val hSet = list.map(e => e.eval(EmptyRow))
        val toScala = CatalystTypeConverters.createToScalaConverter(a.dataType)
        Some(sources.In(a.name, hSet.toArray.map(toScala)))

      case expressions.IsNull(a: Attribute) =>
        Some(sources.IsNull(a.name))
      case expressions.IsNotNull(a: Attribute) =>
        Some(sources.IsNotNull(a.name))

      case expressions.And(left, right) =>
        // See SPARK-12218 for detailed discussion
        // It is not safe to just convert one side if we do not understand the
        // other side. Here is an example used to explain the reason.
        // Let's say we have (a = 2 AND trim(b) = 'blah') OR (c > 0)
        // and we do not understand how to convert trim(b) = 'blah'.
        // If we only convert a = 2, we will end up with
        // (a = 2) OR (c > 0), which will generate wrong results.
        // Pushing one leg of AND down is only safe to do at the top level.
        // You can see ParquetFilters' createFilter for more details.
        for {
          leftFilter <- translateFilter(left)
          rightFilter <- translateFilter(right)
        } yield sources.And(leftFilter, rightFilter)

      case expressions.Or(left, right) =>
        for {
          leftFilter <- translateFilter(left)
          rightFilter <- translateFilter(right)
        } yield sources.Or(leftFilter, rightFilter)

      case expressions.Not(child) =>
        translateFilter(child).map(sources.Not)

      case expressions.StartsWith(a: Attribute, Literal(v: UTF8String, StringType)) =>
        Some(sources.StringStartsWith(a.name, v.toString))

      case expressions.EndsWith(a: Attribute, Literal(v: UTF8String, StringType)) =>
        Some(sources.StringEndsWith(a.name, v.toString))

      case expressions.Contains(a: Attribute, Literal(v: UTF8String, StringType)) =>
        Some(sources.StringContains(a.name, v.toString))

      case _ => None
    }
  }

  def translateAggregateFunc(aggregateFunction: AggregateFunction): Array[support.AggregateFunc] = aggregateFunction match {
    case aggregate.Max(child) => child match {
      case NamedExpression(name, dataType) => Array(support.Max(name, dataType))
      case _ =>
        logWarning(s"Unexpected child of aggregate.Max: ${child}")
        Array.empty
    }
    case _ => Array.empty
  }

  private def createAggregate(requiredChildDistributionExpressions: Option[Seq[Expression]] = None,
                              groupingExpressions: Seq[NamedExpression] = Nil,
                              aggregateExpressions: Seq[AggregateExpression] = Nil,
                              aggregateAttributes: Seq[Attribute] = Nil,
                              initialInputBufferOffset: Int = 0,
                              resultExpressions: Seq[NamedExpression] = Nil,
                              child: SparkPlan): SparkPlan = {
    val useHash = HashAggregateExec.supportsAggregate(
      aggregateExpressions.flatMap(_.aggregateFunction.aggBufferAttributes))
    if (useHash) {
      HashAggregateExec(
        requiredChildDistributionExpressions = requiredChildDistributionExpressions,
        groupingExpressions = groupingExpressions,
        aggregateExpressions = aggregateExpressions,
        aggregateAttributes = aggregateAttributes,
        initialInputBufferOffset = initialInputBufferOffset,
        resultExpressions = resultExpressions,
        child = child)
    } else {
      val objectHashEnabled = child.sqlContext.getConf(USE_OBJECT_HASH_AGG.key).toBoolean
      val useObjectHash = ObjectHashAggregateExec.supportsAggregate(aggregateExpressions)

      if (objectHashEnabled && useObjectHash) {
        ObjectHashAggregateExec(
          requiredChildDistributionExpressions = requiredChildDistributionExpressions,
          groupingExpressions = groupingExpressions,
          aggregateExpressions = aggregateExpressions,
          aggregateAttributes = aggregateAttributes,
          initialInputBufferOffset = initialInputBufferOffset,
          resultExpressions = resultExpressions,
          child = child)
      } else {
        SortAggregateExec(
          requiredChildDistributionExpressions = requiredChildDistributionExpressions,
          groupingExpressions = groupingExpressions,
          aggregateExpressions = aggregateExpressions,
          aggregateAttributes = aggregateAttributes,
          initialInputBufferOffset = initialInputBufferOffset,
          resultExpressions = resultExpressions,
          child = child)
      }
    }
  }
}
