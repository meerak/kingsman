package edu.gatech.cse8803.main

import java.sql.Connection
import org.apache.commons.dbcp2._
import com.typesafe.config.Config
import scala.io.Source

object Datasource 
{
  def connectServer(conf:Config, dbname: String): BasicDataSource =
  {
    val dbUrl = s"jdbc:postgresql://" + conf.getString("db-setting.host") + ":" + conf.getString("db-setting.port") + "/" + dbname + "?user=" + conf.getString("db-setting.user") + "&password=" + conf.getString("db-setting.password")
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
