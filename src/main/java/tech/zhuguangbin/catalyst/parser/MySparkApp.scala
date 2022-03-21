package tech.zhuguangbin.catalyst.parser

import org.apache.spark.sql.SparkSession

object MySparkApp {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("MySparkApp")
      .master("local[*]")
      .enableHiveSupport()
      .withExtensions(MySparkExtension)
      .getOrCreate()

    /*
         we can also use the following api to inject extra optimizers and strategies,
         other than `withExtensions` method in SparkSessionBuilder
     */
    //    spark.experimental.extraOptimizations = Seq(MyOptimizer)
    //    spark.experimental.extraStrategies = Seq(MyStrategy)

    import spark.implicits._

    spark.sparkContext.setLogLevel("DEBUG")

    val df = Seq(
      ("FirstValue", 1, java.sql.Date.valueOf("2010-01-01")),
      ("SecondValue", 2, java.sql.Date.valueOf("2011-01-02")),
      ("ThirdValue", 3, java.sql.Date.valueOf("2012-02-02")),
      ("FourthValue", 4, java.sql.Date.valueOf("2022-03-04"))
    ).toDF("name", "score", "date_column")

    df.createTempView("p")

    //    val sql = "Select * FROM p"
    val sql = "select score+2 as newscore from p"
    val d = spark.sql(sql)
    println("-------logical plan-----------")
    println(d.queryExecution.logical)
    println("--------resolved logical plan----")
    println(d.queryExecution.analyzed)
    println("--------optimized logical plan------------")
    println(d.queryExecution.optimizedPlan)
    println("-------------physical plan---------------")
    println(d.queryExecution.sparkPlan)
    println("-------------physical plan after preparing(executed plan)---------------")
    println(d.queryExecution.executedPlan)

    d.show()
  }

}
