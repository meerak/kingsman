package edu.gatech.cse8803.main

import java.text.SimpleDateFormat

import edu.gatech.cse8803.graphconstruct.GraphLoader
import edu.gatech.cse8803.ioutils.CSVUtils
import edu.gatech.cse8803.jaccard._
import edu.gatech.cse8803.model._
import edu.gatech.cse8803.randomwalk._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.postgresql.Driver
import java.sql.Connection
import java.sql.Statement
import org.apache.commons.dbcp2._

import scala.io.Source

object Datasource 
{
  val dbUri = new URI("jdbc:postgresql://localhost:5432/omop_vocabulary_v4")
  val dbUrl = s"jdbc:postgresql://${dbUri.getHost}:${dbUri.getPort}${dbUri.getPath}"
  val connectionPool = new BasicDataSource()

  if (dbUri.getUserInfo != null) 
  {
    connectionPool.setUsername(dbUri.getUserInfo.split(":")(0))
    connectionPool.setPassword(dbUri.getUserInfo.split(":")(1))
  }
  connectionPool.setDriverClassName("org.postgresql.Driver")
  connectionPool.setUrl(dbUrl)
  connectionPool.setInitialSize(3)
}