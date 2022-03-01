package com.github.music.of.the.ainur.almaren.iceberg

import org.apache.spark.sql.{DataFrame,SaveMode}
import org.apache.spark.sql.SaveMode.{Append,Overwrite,ErrorIfExists,Ignore}
import com.github.music.of.the.ainur.almaren.Tree
import com.github.music.of.the.ainur.almaren.builder.Core
import com.github.music.of.the.ainur.almaren.state.core.{Target,Source}
import org.apache.spark.sql.Column
import org.apache.spark.sql.CreateTableWriter
import org.apache.spark.sql.Row
import org.apache.spark.sql.DataFrameWriterV2

private[almaren] case class SourceIceberg(table:String, options:Map[String,String]) extends Source {
  def source(df: DataFrame): DataFrame = {
    logger.info(s"table:{$table}, options:{$options}")
    df.sparkSession.read.format("iceberg")
      .options(options)
      .load(table)
  }
}

private[almaren] case class TargetIceberg(table:String, options:Map[String,String],saveMode:SaveMode,partitionedBy:Column*) extends Target {
  def target(df: DataFrame): DataFrame = {
    logger.info(s"table:{$table}, options:{$options}, saveMode:{$saveMode}, partitionedBy:{$partitionedBy}")
    val write = df
    .writeTo(table)
    .options(options)

    if(partitionedBy.isEmpty) {
      saveMode match {
        case Append => write.append()
        case Overwrite => write.createOrReplace()
        case ErrorIfExists => checkIfTableExists(df,table,Right(write))
        case Ignore => write.createOrReplace()
      }
    } 
    else {
      val withPartition = write.partitionedBy(partitionedBy.head,partitionedBy.tail:_*)
      saveMode match {
        case Append => write.append()
        case Overwrite => withPartition.createOrReplace()
        case ErrorIfExists => checkIfTableExists(df,table,Left(withPartition))
        case Ignore => withPartition.createOrReplace()
      }
    }
    df
  }

  private def checkIfTableExists[T](df:DataFrame,tableName:String,writer:Either[CreateTableWriter[T],DataFrameWriterV2[T]]): Unit = 
     if(df.sparkSession.catalog.tableExists(tableName = table))
          throw new Exception(s"Table $table already exists")
     else 
        writer match {
          case Left(l) => l.create()
          case Right(r) => r.create()
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
