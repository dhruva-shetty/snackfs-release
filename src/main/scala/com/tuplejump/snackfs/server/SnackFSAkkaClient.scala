package com.tuplejump.snackfs.server

import java.io.InputStream
import java.io.RandomAccessFile
import java.nio.channels.FileChannel
import java.util.UUID

import scala.concurrent.Await
import scala.concurrent.Promise
import scala.concurrent.duration.DurationInt

import org.apache.cassandra.utils.ByteBufferUtil

import com.tuplejump.snackfs.cassandra.model.SnackFSConfiguration
import com.tuplejump.snackfs.server.actors.RequestSBlockActor
import com.tuplejump.snackfs.server.messages.ReadSSTableRequest
import com.tuplejump.snackfs.server.messages.ReadSSTableResponse
import com.tuplejump.snackfs.util.AsyncUtil
import com.tuplejump.snackfs.util.LogConfiguration
import com.twitter.logging.Logger
import com.typesafe.config.ConfigFactory

import akka.actor.ActorSystem
import akka.actor.Props
import akka.pattern.ask
import akka.util.Timeout

class SnackFSAkkaClient(configuration: SnackFSConfiguration) {
  
  private lazy val log = Logger.get(getClass)
  
  val system = ActorSystem("SnackFSAkkaClient", ConfigFactory.parseString(s"""
    akka {
      loglevel = "INFO"
      log-dead-letters = off
      actor {
        provider = "akka.remote.RemoteActorRefProvider"
      }
      remote {
        netty.tcp {
          maximum-frame-size = ${configuration.subBlockSize * 2}
          tcp-nodelay = on
          port = 0
        }
      }
    }
  """))
  
  val requestSBlock = system.actorOf(Props[RequestSBlockActor], name = "request_sblock")

  implicit val timeout = Timeout(10.second)
  
  implicit val ec = AsyncUtil.getExecutionContext
  
  def readSSTable(blockUUId: UUID, subBlockUUId: UUID, compressed: Boolean): InputStream = {
    val prom = Promise[InputStream]
    val f = requestSBlock ? ReadSSTableRequest(blockUUId, subBlockUUId)
    f onSuccess {
      case ReadSSTableResponse(data) =>
        var result: InputStream = null
        if(data != null) {
          if (LogConfiguration.isDebugEnabled) log.debug("blockID: %s, sblockID: %s, fileName: %s, position: %s, length %s", blockUUId, subBlockUUId, data.fileName, data.positionToSeek, data.length)
          if(data.length != 0){
            val raf: RandomAccessFile = new RandomAccessFile(data.fileName, "r")
            result = {
              if(compressed) {
                val bytes = Array.ofDim[Byte](data.length)
                raf.seek(data.positionToSeek)
                raf.read(bytes)
                raf.close
                AsyncUtil.convertByteArrayToStream(bytes, compressed)
              } else {
                ByteBufferUtil.inputStream(raf.getChannel.map(FileChannel.MapMode.READ_ONLY, data.positionToSeek, data.length))
              }
            }
          }
        }
        prom success result
      case _ =>
        log.info(Thread.currentThread.getName() + " readSSTable got something unexpected.")
    }
    f onFailure {
      case f =>
        log.error(f, "Failed to receive server-response for blockID: %s, sblockID: %s", blockUUId, subBlockUUId)
        prom failure f
    }
    Await.result(prom.future, (10 seconds))
  }
}
