package edu.gatech.cse8803.main

import java.sql.Connection
import java.sql.Statement
import org.apache.commons.dbcp2._

import scala.io.Source

object Datasource 
{
  //val dbUri = new URI("jdbc:postgresql://localhost:5432/omop_vocabulary_v4?user=sneha")
  def connectServer(dbname: String): BasicDataSource =
  {
    val dbUrl = s"jdbc:postgresql://localhost:5432/" + dbname + "?user=sneha&password=sneha2511"
    val connectionPool = new BasicDataSource()
    /*
    if (dbUri.getUserInfo != null) 
    {
      connectionPool.setUsername(dbUri.getUserInfo.split(":")(0))
      connectionPool.setPassword(dbUri.getUserInfo.split(":")(1))
    }
    */
    connectionPool.setDriverClassName("org.postgresql.Driver")
    connectionPool.setUrl(dbUrl)
    connectionPool.setInitialSize(3)
    connectionPool
  }
}