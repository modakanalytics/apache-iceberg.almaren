package com.github.music.of.the.ainur.almaren.iceberg

import org.apache.spark.sql.{DataFrame,SaveMode}
import com.github.music.of.the.ainur.almaren.Tree
import com.github.music.of.the.ainur.almaren.builder.Core
import com.github.music.of.the.ainur.almaren.state.core.{Target,Source}

private[almaren] case class SourceIceberg(table:String, options:Map[String,String]) extends Source {
  def source(df: DataFrame): DataFrame = {
    logger.info(s"table:{$table}, options:{$options}")
    df.sparkSession.read.format("iceberg")
      .options(options)
      .load(table)
  }
}

private[almaren] case class TargetIceberg(table:String, options:Map[String,String],saveMode:SaveMode) extends Target {
  def target(df: DataFrame): DataFrame = {
    logger.info(s"table:{$table}, options:{$options}")
    df.write.format("iceberg")
      .options(options)
      .mode(saveMode)
      .save(table)
    df
  }
}

private[almaren] trait IcebergConnector extends Core {
  def targetIceberg(table:String, options:Map[String,String] = Map(),saveMode:SaveMode = SaveMode.ErrorIfExists): Option[Tree] =
     TargetIceberg(table,options,saveMode)

  def sourceIceberg(table:String, options:Map[String,String] = Map()): Option[Tree] =
    SourceIceberg(table,options)
}

object Iceberg {
  implicit class IcebergImplicit(val container: Option[Tree]) extends IcebergConnector
}
