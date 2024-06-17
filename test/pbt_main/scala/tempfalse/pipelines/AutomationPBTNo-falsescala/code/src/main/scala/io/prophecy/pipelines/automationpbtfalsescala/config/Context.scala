package io.prophecy.pipelines.automationpbtfalsescala.config

import org.apache.spark.sql.SparkSession
case class Context(spark: SparkSession, config: Config)
