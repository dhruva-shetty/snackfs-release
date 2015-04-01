package com.tuplejump.snackfs.server

import org.apache.hadoop.conf.Configuration

import com.tuplejump.snackfs.cassandra.model.SnackFSConfiguration
import com.tuplejump.snackfs.server.actors.ReadSBlockActor
import com.tuplejump.snackfs.util.LogConfiguration
import com.typesafe.config.ConfigFactory

import akka.actor.ActorSystem
import akka.actor.Props

object SnackFSServer extends App {
  
  LogConfiguration.config()
  val snackfsConfig = SnackFSConfiguration.get(new Configuration)
  val system = ActorSystem("SnackFSServer", ConfigFactory.parseString(s"""
    akka {
      loglevel = "INFO"
      log-dead-letters = off
      actor {
        provider = "akka.remote.RemoteActorRefProvider"
      }
      remote {
        enabled-transports = ["akka.remote.netty.tcp"]
        netty.tcp {
		      tcp-nodelay = on
          hostname = "localhost"
          port = 55252
          maximum-frame-size = ${snackfsConfig.subBlockSize * 2}
        }
      }
    }
  """))
  
  val readSBlock = system.actorOf(Props[ReadSBlockActor], name = "read_sblock")
  
  def getSnackfsConfig = snackfsConfig
}

