package com.tuplejump.snackfs.server

import java.lang.Integer
import java.lang.Boolean
import java.io.InputStream
import java.nio.ByteBuffer
import java.util.UUID

import scala.annotation.implicitNotFound
import scala.concurrent.Await
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.concurrent.duration.DurationInt

import com.tuplejump.snackfs.cassandra.model.SnackFSConfiguration
import com.tuplejump.snackfs.server.messages.ReadSSTableRequest
import com.tuplejump.snackfs.util.AsyncUtil
import com.tuplejump.snackfs.util.LogConfiguration
import com.twitter.logging.Logger

import io.netty.bootstrap.Bootstrap
import io.netty.buffer.ByteBuf
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.ChannelInitializer
import io.netty.channel.ChannelOption
import io.netty.channel.SimpleChannelInboundHandler
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioSocketChannel
import io.netty.handler.codec.serialization.ObjectEncoder

class SnackFSNettyClient(configuration: SnackFSConfiguration) {
  
  private lazy val log = Logger.get(getClass)

  val nettyPort: Int = configuration.snackFSServerPort

  val workerGroup = new NioEventLoopGroup()
  
  var client = new Bootstrap().group(workerGroup)
                              .channel(classOf[NioSocketChannel])
                              .option(ChannelOption.TCP_NODELAY, Boolean.TRUE)
                              .option(ChannelOption.SO_KEEPALIVE, Boolean.TRUE)
                              .remoteAddress("localhost", nettyPort)

  def readSSTable(blockUUId: UUID, subBlockUUId: UUID, compressed: Boolean): InputStream = {
    client.handler(new ChannelInitializer[SocketChannel] {
    			override def initChannel(ch: SocketChannel) {
    				ch.pipeline.addLast("encoder", new ObjectEncoder)
    				ch.pipeline.addLast("handler", new FileClientHandler(new ReadSSTableRequest(blockUUId, subBlockUUId)))
    			}
    		})
		val f = client.connect.sync
		val handler = f.channel.pipeline.last.asInstanceOf[FileClientHandler]
    try {
      val data: ByteBuffer = Await.result(handler.future, (configuration.snackFSClientTimeout milliseconds))
      if(data != null && data.array.length != 0) {
        return AsyncUtil.convertByteArrayToStream(data.array, compressed)
      }
    } catch {
      case e: Throwable =>
        log.error(e.getCause, " request_sblock failed blockID: %s, sblockID: %s cause: %s", blockUUId, subBlockUUId, e.getMessage)
    } finally {
      handler.clear
      f.channel.close
    }
    null
  }

  final class FileClientHandler(request: ReadSSTableRequest) extends SimpleChannelInboundHandler[ByteBuf] {
    
    implicit val ec:ExecutionContext = AsyncUtil.getExecutionContext
    
    private var expectedLength: Long = -1L
    private var promise = Promise[ByteBuffer]
    private var bytesRead: Long = 0L
    private var data: Array[Byte] = null
     
    def future: Future[ByteBuffer] = promise.future
    
    def clear = {
      data = null
      promise = null
    }
    
    override def channelActive(ctx: ChannelHandlerContext): Unit = {
      val future = ctx.writeAndFlush(request)
      if(future.isSuccess()) {
    	  log.info(Thread.currentThread.getName() + " request_sblock: sent blockID: %s, sblockID: %s", request.blockUUId, request.subBlockUUId)
      } else {
    	  log.error(Thread.currentThread.getName() + " request_sblock: failed to send blockID: %s, sblockID: %s, cause: %s", request.blockUUId, request.subBlockUUId, future.cause.getMessage)
      }
    }

    override def channelRead0(ctx: ChannelHandlerContext, buf: ByteBuf): Unit = {
      if(expectedLength == -1) {
        expectedLength = buf.readLong
        if(expectedLength > 0) {
          data = new Array[Byte](expectedLength.toInt)
        } else {
          promise failure new Exception(s"blockId: ${request.blockUUId} sblockId ${request.subBlockUUId} not found got expected-length: ${expectedLength}")
        }
      }
      val length = buf.readableBytes.toInt
      if(length > 0) {
        val position = bytesRead.toInt
        bytesRead += buf.readableBytes
        buf.readBytes(data, position, length)
        buf.clear
        if(LogConfiguration.isDebugEnabled()) log.debug(Thread.currentThread.getName() + " request_sblock: received chunk for blockId: %s, sblockId: %s, expected-length %s total-length %s", request.blockUUId, request.subBlockUUId, expectedLength, bytesRead)
        if(bytesRead == expectedLength) {
        	log.info(Thread.currentThread.getName() + " request_sblock: finished reading blockId: %s, sblockId: %s, expected-length %s total-length %s", request.blockUUId, request.subBlockUUId, expectedLength, bytesRead)
          promise success ByteBuffer.wrap(data)
        }
      }
    }

    override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
      log.error(cause, Thread.currentThread.getName() + " Handling netty-client exception: %s", cause.getMessage)
      promise failure cause
      ctx.close
    }
  }
}