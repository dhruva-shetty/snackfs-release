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

class RequestSBlockActor extends Actor {
  
  private lazy val log = Logger.get(getClass)
  
  val remote = context.actorSelection("akka.tcp://SnackFSServer@localhost:55252/user/read_sblock")

  implicit val timeout = Timeout(60.second)
  implicit val ec = AsyncUtil.getExecutionContext
  
  def receive = {
    case ReadSSTableRequest(blockUUId, subBlockUUId) =>
      log.info(Thread.currentThread.getName() + " request_sblock: sending blockId %s, sblockID %s", blockUUId, subBlockUUId)
      val f = remote ? ReadSSTableRequest(blockUUId, subBlockUUId) 
      val client = sender
      f onSuccess {
        case ReadSSTableResponse(bytes) =>
          client ! ReadSSTableResponse(bytes)
      } 
      f onFailure {
        case f =>
          log.error(f, "Failed to receive server-response for blockId: %s, sblockID: %s", blockUUId, subBlockUUId)
          client ! f
      }
    case _ =>
      log.info(Thread.currentThread.getName() + " request_sblock: got something unexpected.")
  }

}