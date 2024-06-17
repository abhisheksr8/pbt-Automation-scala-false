package io.prophecy.pipelines.automationpbtfalsescala

import io.prophecy.libs._
import io.prophecy.pipelines.automationpbtfalsescala.config._
import io.prophecy.pipelines.automationpbtfalsescala.functions.UDFs._
import io.prophecy.pipelines.automationpbtfalsescala.functions.PipelineInitCode._
import io.prophecy.pipelines.automationpbtfalsescala.graph._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Main {

  def apply(context: Context): Unit = {
    val df_s3_source_dataset = s3_source_dataset(context)
    lookup_by_customer_id_and_email(context, df_s3_source_dataset)
    val df_reformat_data = reformat_data(context, df_s3_source_dataset)
    val df_script_apply  = script_apply(context,  df_reformat_data)
    val df_select_all_from_in0 =
      select_all_from_in0(context, df_s3_source_dataset)
  }

  def main(args: Array[String]): Unit = {
    val config = ConfigurationFactoryImpl.getConfig(args)
    val spark: SparkSession = SparkSession
      .builder()
      .appName("AutomationPBT-falsescala")
      .config("spark.default.parallelism",             "4")
      .config("spark.sql.legacy.allowUntypedScalaUDF", "true")
      .enableHiveSupport()
      .getOrCreate()
    val context = Context(spark, config)
    spark.conf.set("prophecy.metadata.pipeline.uri",
                   "pipelines/AutomationPBTNo-falsescala"
    )
    registerUDFs(spark)
    MetricsCollector.instrument(spark, "pipelines/AutomationPBTNo-falsescala") {
      apply(context)
    }
  }

}
