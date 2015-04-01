package com.tuplejump.snackfs.server.actors

import java.util.UUID

import scala.collection.mutable.Map

import org.apache.cassandra.io.sstable.SSTableReader

import com.tuplejump.snackfs.cassandra.sstable.DirectSSTableReader
import com.tuplejump.snackfs.server.SnackFSServer
import com.tuplejump.snackfs.server.messages.ReadSSTableRequest
import com.tuplejump.snackfs.server.messages.ReadSSTableResponse
import com.twitter.logging.Logger

import akka.actor.Actor
import akka.actor.actorRef2Scala

class ReadSBlockActor extends Actor {
  
	private lazy val log = Logger.get(getClass)

  private val config = SnackFSServer.getSnackfsConfig
  
  private val reader: DirectSSTableReader = {
    val reader = DirectSSTableReader(true, config.keySpace, config.sstableLocation)
    reader.initialize
    reader
  }
    
  def receive = {
    case ReadSSTableRequest(blockUUId, subBlockUUId) =>
      log.info(Thread.currentThread.getName() + " read_sblock: received blockId %s, sblockID %s", blockUUId, subBlockUUId)
      sender ! ReadSSTableResponse(reader.readSSTable(Map[UUID, (String, SSTableReader)]() empty, blockUUId, subBlockUUId))
    case _ =>
      log.info(Thread.currentThread.getName() + " read_sblock: got something unexpected.")
  }
}