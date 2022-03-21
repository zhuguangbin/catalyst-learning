package tech.zhuguangbin.catalyst.parser

import org.apache.spark.sql.catalyst.expressions.{Add, Expression, Literal}
import org.apache.spark.sql.catalyst.plans.logical.{Command, GlobalLimit, Limit, LocalLimit, LocalRelation, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule

object MyOptimizer extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = {

    println(">>>> my custom optimizer for %s", plan)

    // add limit 2
    // this is just an demo, you should take into more consideration for production
    // eg. ddl is not allowed to add limit, only query
//    GlobalLimit(Literal(2), plan)
    plan
  }

}
