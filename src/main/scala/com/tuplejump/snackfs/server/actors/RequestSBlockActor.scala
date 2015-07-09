package com.tuplejump.snackfs.server.actors

import scala.concurrent.duration.DurationInt
import com.tuplejump.snackfs.server.messages.ReadSSTableRequest
import com.tuplejump.snackfs.server.messages.ReadSSTableResponse
import com.tuplejump.snackfs.util.AsyncUtil
import com.twitter.logging.Logger
import akka.actor.Actor
import akka.actor.actorRef2Scala
import akka.pattern.ask
import akka.util.Timeout
import com.tuplejump.snackfs.cassandra.model.SnackFSConfiguration
import org.apache.hadoop.conf.Configuration

class RequestSBlockActor extends Actor {
  
  private lazy val log = Logger.get(getClass)
  
  val snackfsConfig = SnackFSConfiguration.get(new Configuration)
  
  val remote = context.actorSelection(s"akka.tcp://SnackFSServer@localhost:${snackfsConfig.snackFSServerPort}/user/read_sblock")

  implicit val timeout = Timeout(60.second)
  implicit val ec = AsyncUtil.getExecutionContext
  
  def receive = {
    case ReadSSTableRequest(blockUUId, subBlockUUId) =>
      log.info(Thread.currentThread.getName() + " request_sblock: sending blockID: %s, sblockID: %s", blockUUId, subBlockUUId)
      val f = remote ? ReadSSTableRequest(blockUUId, subBlockUUId) 
      val client = sender
      f onSuccess {
        case ReadSSTableResponse(bytes) =>
          client ! ReadSSTableResponse(bytes)
      } 
      f onFailure {
        case f =>
          log.error(f, "Failed to receive server-response for blockID: %s, sblockID: %s", blockUUId, subBlockUUId)
          client ! f
      }
    case _ =>
      log.info(Thread.currentThread.getName() + " request_sblock: got something unexpected.")
  }

}