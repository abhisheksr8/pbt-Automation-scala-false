package io.prophecy.pipelines.automationpbtfalsescala.graph

import io.prophecy.libs._
import io.prophecy.pipelines.automationpbtfalsescala.config.Context
import io.prophecy.pipelines.automationpbtfalsescala.udfs.UDFs._
import io.prophecy.pipelines.automationpbtfalsescala.udfs.PipelineInitCode._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object script_template {
  def apply(context: Context, in0: DataFrame): DataFrame = {
    val spark = context.spark
    val Config = context.config
    print("Successfully Executed Son.")
    var out0=in0
    out0
  }

}
