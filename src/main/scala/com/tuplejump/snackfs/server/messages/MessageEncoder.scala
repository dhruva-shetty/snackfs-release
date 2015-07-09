package com.tuplejump.snackfs.server.messages

import io.netty.buffer.ByteBuf
import io.netty.channel.ChannelHandler
import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec.MessageToMessageEncoder
import io.netty.channel.DefaultFileRegion
import java.util.List
import io.netty.channel.FileRegion
import com.tuplejump.snackfs.cassandra.sstable.SubBlockData

@ChannelHandler.Sharable
class MessageEncoder extends MessageToMessageEncoder[SubBlockData] {

  def encode(ctx:ChannelHandlerContext, msg: SubBlockData, out: List[Object]) = {
    val headerLength: Int = 8
    val header: ByteBuf = ctx.alloc().heapBuffer(headerLength)
    if (msg != null && msg.fileRegion != null) {
    	val body = msg.fileRegion
      val dataLength: Long = body.count
      header.writeLong(dataLength)
      out.add(new MessageWithHeader(header, body, dataLength))
    } else {
      header.writeLong(0L)
      out.add(header)
    }
  }
}