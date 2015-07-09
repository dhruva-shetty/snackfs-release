package com.tuplejump.snackfs.server

import org.apache.hadoop.conf.Configuration
import com.tuplejump.snackfs.cassandra.model.SnackFSConfiguration
import com.tuplejump.snackfs.util.LogConfiguration
import com.twitter.logging.Logger
import org.apache.cassandra.service.CassandraDaemon

object SnackFSServer extends CassandraDaemon {

  LogConfiguration.config()
  
  private lazy val log = Logger.get(getClass)
  
  val config = new Configuration
  
  val snackfsConfig = SnackFSConfiguration.get(config)
  
  def main(args: Array[String]) {
      activate
      initializeSnackFS
      Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
        def run() {
          deactivate
        }
      }))
  }
  
  def initializeSnackFS = {
    if(!config.get("snackfs.useSSTable", "false").toBoolean) {
      log.error("Failed to start snackFS server since sstables are disabled -> snackfs.useSSTable: ", snackfsConfig.useSSTable)
      System.exit(-1)
    }
    
    snackfsConfig.clientMode match {
      case "netty" => 
        log.info("Starting netty server-> client.mode: %s", snackfsConfig.clientMode)
        new SnackFSNettyServer(snackfsConfig)
        
      case "akka" =>
        log.info("Starting akka server-> client.mode: %s", snackfsConfig.clientMode)
        new SnackFSAkkaServer(snackfsConfig)
        
      case _ =>
        throw new Exception(s"Failed to start server due to unsupported configuration -> client.mode: ${snackfsConfig.clientMode}")
        System.exit(-1)
    }
  }
  
  def getSnackfsConfig = snackfsConfig
}