package tech.zhuguangbin.catalyst.parser

import org.apache.spark.sql.SparkSessionExtensions

object MySparkExtension extends (SparkSessionExtensions => Unit) {

  override def apply(extension: SparkSessionExtensions): Unit = {
    extension.injectParser {
      (session, parser) => new MyParser(parser)
    }

    extension.injectOptimizerRule {
      session => MyOptimizer
    }

    extension.injectPlannerStrategy {
      session => MyStrategy
    }
  }
}
