package com.tuplejump.snackfs.server

import java.lang.Boolean
import java.util.UUID

import scala.collection.mutable.Map
import scala.compat.Platform

import org.apache.cassandra.io.sstable.SSTableReader

import com.tuplejump.snackfs.SnackFSMode.NETTY
import com.tuplejump.snackfs.cassandra.model.SnackFSConfiguration
import com.tuplejump.snackfs.cassandra.sstable.DirectSSTableReader
import com.tuplejump.snackfs.cassandra.sstable.SubBlockData
import com.tuplejump.snackfs.server.messages.MessageEncoder
import com.tuplejump.snackfs.server.messages.ReadSSTableRequest
import com.twitter.logging.Logger

import io.netty.bootstrap.ServerBootstrap
import io.netty.channel.ChannelFuture
import io.netty.channel.ChannelFutureListener
import io.netty.channel.ChannelHandler
import io.netty.channel.ChannelHandler.Sharable
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.ChannelInitializer
import io.netty.channel.ChannelOption
import io.netty.channel.SimpleChannelInboundHandler
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.handler.codec.serialization.ClassResolvers
import io.netty.handler.codec.serialization.ObjectDecoder
import io.netty.handler.logging.LogLevel
import io.netty.handler.logging.LoggingHandler

class SnackFSNettyServer(configuration: SnackFSConfiguration) {
  
  private lazy val log = Logger.get(getClass)

  val nettyPort: Int = configuration.snackFSServerPort

  val reader: DirectSSTableReader = {
    val reader = DirectSSTableReader(NETTY, configuration.keySpace, configuration.sstableLocation)
    reader.initialize
    reader
  }

  val bossGroup, workerGroup = new NioEventLoopGroup()
  
  try {
    val server = new ServerBootstrap().group(bossGroup, workerGroup)
                                      .channel(classOf[NioServerSocketChannel])
                                      .option(ChannelOption.SO_REUSEADDR, Boolean.TRUE)
                                      .option(ChannelOption.SO_BACKLOG, Int.box(100))
                                      .localAddress(nettyPort)
                                      .handler(new LoggingHandler(LogLevel.INFO))
                                      .childHandler(new ChannelInitializer[SocketChannel] {
                                        override def initChannel(ch: SocketChannel) {
                                          ch.pipeline.addLast("encoder", new MessageEncoder)
                                          ch.pipeline.addLast("decoder", new ObjectDecoder(ClassResolvers.cacheDisabled(null)))
                                          ch.pipeline.addLast("handler", new FileHandler)
                                        }
                                      })
                                      
    val f = server.bind.sync
    f.channel.closeFuture.sync
  } finally {
    bossGroup.shutdownGracefully()
    workerGroup.shutdownGracefully()
  }

  @Sharable
  final class FileHandler extends SimpleChannelInboundHandler[ReadSSTableRequest] {

    override def channelRead0(ctx: ChannelHandlerContext, request: ReadSSTableRequest): Unit = {
  		log.info(Thread.currentThread.getName() + " read_sblock: received blockId %s, sblockID %s", request.blockUUId, request.subBlockUUId)
      val data: SubBlockData = reader.readSSTable(Map[UUID, (String, SSTableReader)]() empty, request.blockUUId, request.subBlockUUId) 
      val start = Platform.currentTime
      if (data != null && data.fileRegion != null) {
        log.info(Thread.currentThread.getName() + " read_sblock: found blockID: %s sblockID: %s at position: %s with length: %s", request.blockUUId, request.subBlockUUId, data.positionToSeek, data.length)
        ctx.writeAndFlush(data).addListener( new ChannelFutureListener(){
          override def operationComplete(future: ChannelFuture) {
            future.isSuccess match {
              case true =>
                log.info(Thread.currentThread.getName() + " read_sblock: finished processing request for blockID: %s sblockID: %s elapsed-time: %s", request.blockUUId, request.subBlockUUId, (Platform.currentTime - start))
                
              case false => 
                log.error(future.cause(), Thread.currentThread.getName() + " read_sblock: failed for blockID: %s sblockID: %s", request.blockUUId, request.subBlockUUId)
            }
          }
        })
      } else {
        ctx.writeAndFlush(new SubBlockData(null, -1L, -1, null, null))
      }
    }
    
    override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
      log.error(cause, Thread.currentThread.getName() + " Handling netty-server exception: %s", cause.getMessage)
    }
  }
}