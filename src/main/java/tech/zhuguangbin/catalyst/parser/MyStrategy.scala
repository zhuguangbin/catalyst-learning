package tech.zhuguangbin.catalyst.parser

import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan

object MyStrategy extends Strategy {
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = {

    println(">>>> my custom strategy for logical plan to physical plan [%s]", plan)

    Nil
  }
}
